"""Bybit V5 public market adapter (linear USDT perpetuals).

Same contract as the Binance adapter — normalise the wire format, report state
honestly, touch nothing private — but Bybit's public API differs from Binance's
in four ways that each require real handling rather than a rename.

**1. The snapshot arrives on the socket.**  Binance makes you pull a REST
snapshot and splice the diff stream onto it.  Bybit pushes ``type: "snapshot"``
on the order-book topic itself, then ``type: "delta"``.  So the book is seeded
in-stream, and the REST endpoint is only needed to re-seed after a gap.  The
adapter emits a ``SNAPSHOT`` event and the data engine applies it directly.

**2. Sequencing is a single counter.**  Binance carries ``U``/``u``/``pu``;
Bybit carries one ``u`` that increments by exactly one per delta.  That maps
onto the internal update as ``first_id = final_id = u`` and
``prev_final_id = u - 1``, which the existing book validation then checks the
same way it checks Binance's ``pu`` chain.  A ``u`` of 1 means Bybit restarted
the topic and is resending a snapshot.

**3. The heartbeat is ours to send.**  Binance sends protocol-level ping frames
and the library answers them.  Bybit expects an application-level
``{"op": "ping"}`` roughly every 20 s and will drop a silent connection.

**4. The taker side is stated directly.**  ``publicTrade`` gives ``S: "Buy"``
meaning the taker bought.  Binance instead gives ``m``, "the buyer is the
market maker", which has to be inverted.  Getting this backwards inverts every
order-flow feature while leaving them all plausible, so it is asserted in tests
for both venues.

One further quirk: for linear contracts the ``tickers`` topic is a *delta*
stream — a push contains only the fields that changed — so the adapter keeps a
merged view per symbol rather than reading each message as complete.

This module contains no private endpoint, no signing and no order placement.
Bybit API keys are neither read nor accepted anywhere in this system.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import random
import time
from typing import Any

import httpx

from ..domain import EventKind, FeedState, MarketEvent, Side
from ..logging_setup import get_logger
from .base import DepthSnapshot, MarketFeed

log = get_logger("adapters.bybit")

try:  # websockets >= 13 ships the new asyncio client; older versions do not.
    from websockets.asyncio.client import connect as ws_connect  # type: ignore
except ImportError:  # pragma: no cover - depends on installed version
    from websockets import connect as ws_connect  # type: ignore

#: Topics sent per subscribe frame.  Bybit caps the size of a single request;
#: chunking keeps every frame comfortably inside it regardless of symbol count.
MAX_ARGS_PER_SUBSCRIBE = 10

#: Bybit drops a connection that stops talking. The documented guidance is a
#: ping every 20 s; 15 leaves room for a slow round trip.
PING_INTERVAL_S = 15.0


def _f(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


class BybitLinearFeed(MarketFeed):
    kind = "live"

    def __init__(
        self,
        symbols: list[str],
        *,
        rest_base: str,
        ws_base: str,
        ws_path: str = "/v5/public/linear",
        depth_path: str = "/v5/market/orderbook",
        instruments_path: str = "/v5/market/instruments-info",
        time_path: str = "/v5/market/time",
        category: str = "linear",
        ws_depth: int = 50,
        streams: tuple[str, ...] = ("depth", "aggTrade", "bookTicker", "markPrice"),
        heartbeat_timeout_s: float = 20.0,
        reconnect_initial_s: float = 1.0,
        reconnect_max_s: float = 60.0,
        reconnect_factor: float = 2.0,
        reconnect_jitter: float = 0.25,
        http_timeout_s: float = 10.0,
    ) -> None:
        super().__init__(symbols)
        self.rest_base = rest_base.rstrip("/")
        self.ws_base = ws_base.rstrip("/")
        self.ws_path = ws_path if ws_path.startswith("/") else f"/{ws_path}"
        self.depth_path = depth_path
        self.instruments_path = instruments_path
        self.time_path = time_path
        self.category = category
        self.ws_depth = ws_depth
        self.streams = tuple(streams)
        self.heartbeat_timeout_s = heartbeat_timeout_s
        self.reconnect_initial_s = reconnect_initial_s
        self.reconnect_max_s = reconnect_max_s
        self.reconnect_factor = reconnect_factor
        self.reconnect_jitter = reconnect_jitter
        self.http_timeout_s = http_timeout_s

        self._client: httpx.AsyncClient | None = None
        self._task: asyncio.Task[None] | None = None
        self._running = False
        #: ``tickers`` is a delta stream for linear contracts, so the last
        #: complete view of each symbol has to be kept and merged into.
        self._ticker_state: dict[str, dict[str, Any]] = {}
        self.subscribe_errors: list[str] = []

    # ------------------------------------------------------------ stream set

    def topics(self) -> list[str]:
        names: list[str] = []
        for symbol in self.symbols:
            if "depth" in self.streams:
                names.append(f"orderbook.{self.ws_depth}.{symbol}")
            if "aggTrade" in self.streams:
                names.append(f"publicTrade.{symbol}")
            # One tickers topic carries mark price, index price, funding *and*
            # best bid/ask, so both of these map onto the same subscription.
            if ("markPrice" in self.streams or "bookTicker" in self.streams) and not any(
                t == f"tickers.{symbol}" for t in names
            ):
                names.append(f"tickers.{symbol}")
        return names

    def ws_url(self) -> str:
        return f"{self.ws_base}{self.ws_path}"

    # -------------------------------------------------------------- lifecycle

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(self.http_timeout_s), headers={"User-Agent": "aurum-edge-lab/1.0"}
        )
        self.stats.subscriptions = len(self.topics())
        self.stats.connections = 1
        self.stats.endpoint = self.ws_url()
        self._set_state(FeedState.CONNECTING, f"{self.stats.subscriptions} topic(s)")
        self._task = asyncio.create_task(self._run(), name="bybit-ws")
        log.info(
            "feed starting",
            extra={"symbols": len(self.symbols), "topics": self.stats.subscriptions,
                   "depth": self.ws_depth, "url": self.ws_url()},
        )

    async def stop(self) -> None:
        self._running = False
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await self._task
            self._task = None
        if self._client is not None:
            await self._client.aclose()
            self._client = None
        self._set_state(FeedState.DISCONNECTED, "stopped")

    # ------------------------------------------------------------- websocket

    async def _run(self) -> None:
        delay = self.reconnect_initial_s
        url = self.ws_url()
        while self._running:
            ping_task: asyncio.Task[None] | None = None
            try:
                self._set_state(FeedState.CONNECTING)
                # ping_interval is disabled: Bybit wants an application-level
                # {"op":"ping"}, not a protocol ping frame, and a library that
                # answers protocol pings will not keep this connection alive.
                async with ws_connect(url, ping_interval=None, max_queue=4096) as socket:
                    await self._subscribe(socket)
                    ping_task = asyncio.create_task(self._ping_loop(socket), name="bybit-ping")
                    self.stats.connected_since_ms = int(time.time() * 1000)
                    self._set_state(FeedState.LIVE)
                    delay = self.reconnect_initial_s  # a clean connect resets the backoff
                    log.info("ws connected", extra={"topics": self.stats.subscriptions})
                    await self._read_loop(socket)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.stats.errors += 1
                self.stats.last_error = f"{type(exc).__name__}: {exc}"
                self._set_state(FeedState.ERROR, self.stats.last_error)
                log.warning("ws error", extra={"error": self.stats.last_error})
            finally:
                if ping_task is not None:
                    ping_task.cancel()
                    with contextlib.suppress(asyncio.CancelledError, Exception):
                        await ping_task
            if not self._running:
                break
            self.stats.reconnects += 1
            sleep_for = delay * (1.0 + random.uniform(-self.reconnect_jitter, self.reconnect_jitter))
            await asyncio.sleep(max(0.1, sleep_for))
            delay = min(self.reconnect_max_s, delay * self.reconnect_factor)

    async def _subscribe(self, socket: Any) -> None:
        topics = self.topics()
        for start in range(0, len(topics), MAX_ARGS_PER_SUBSCRIBE):
            chunk = topics[start : start + MAX_ARGS_PER_SUBSCRIBE]
            await socket.send(json.dumps({"op": "subscribe", "args": chunk}))

    async def _ping_loop(self, socket: Any) -> None:
        while self._running:
            await asyncio.sleep(PING_INTERVAL_S)
            with contextlib.suppress(Exception):
                await socket.send(json.dumps({"op": "ping"}))

    async def _read_loop(self, socket: Any) -> None:
        while self._running:
            try:
                raw = await asyncio.wait_for(socket.recv(), timeout=self.heartbeat_timeout_s)
            except TimeoutError as exc:
                # These topics tick far faster than the heartbeat window, so
                # silence means a half-open socket. Drop it and let the backoff
                # reconnect rather than sitting on a dead connection.
                raise ConnectionError(
                    f"no frame for {self.heartbeat_timeout_s:.0f}s (heartbeat timeout)"
                ) from exc
            self._handle_frame(raw)

    def _handle_frame(self, raw: str | bytes) -> None:
        recv_ms = int(time.time() * 1000)
        try:
            message = json.loads(raw)
        except (TypeError, ValueError):
            self.stats.errors += 1
            self.stats.last_error = "unparseable frame"
            return
        if not isinstance(message, dict):
            return

        # Control frames: subscription acks and pongs.
        if "topic" not in message:
            op = message.get("op") or message.get("ret_msg")
            if message.get("success") is False:
                detail = f"{op}: {message.get('ret_msg', 'subscribe rejected')}"
                self.subscribe_errors.append(detail)
                self.stats.errors += 1
                self.stats.last_error = detail
                log.error("subscription rejected", extra={"detail": detail})
            return

        for event in self._normalise(message, recv_ms):
            self._emit(event)

    def _normalise(self, message: dict[str, Any], recv_ms: int) -> list[MarketEvent]:
        topic = str(message.get("topic", ""))
        kind = topic.split(".", 1)[0]
        # ``cts`` is the matching-engine timestamp where Bybit provides it and is
        # closer to when the event actually happened than the send time in ``ts``.
        ts_ms = int(message.get("cts") or message.get("ts") or recv_ms)

        if kind == "orderbook":
            return self._normalise_orderbook(message, ts_ms, recv_ms)
        if kind == "publicTrade":
            return self._normalise_trades(message, recv_ms)
        if kind == "tickers":
            return self._normalise_ticker(message, ts_ms, recv_ms)
        return []

    def _normalise_orderbook(
        self, message: dict[str, Any], ts_ms: int, recv_ms: int
    ) -> list[MarketEvent]:
        data = message.get("data") or {}
        symbol = str(data.get("s", "")).upper()
        if not symbol:
            return []
        update_id = int(data.get("u", 0))
        bids = [(_f(p), _f(q)) for p, q in data.get("b", ())]
        asks = [(_f(p), _f(q)) for p, q in data.get("a", ())]

        # ``u == 1`` means the topic restarted and this is a fresh snapshot,
        # whatever the type field says.
        is_snapshot = message.get("type") == "snapshot" or update_id == 1
        if is_snapshot:
            return [
                MarketEvent(
                    symbol=symbol,
                    kind=EventKind.SNAPSHOT,
                    ts_ms=ts_ms,
                    recv_ms=recv_ms,
                    payload={"lastUpdateId": update_id, "bids": bids, "asks": asks,
                             "seq": int(data.get("seq", 0))},
                )
            ]

        return [
            MarketEvent(
                symbol=symbol,
                kind=EventKind.DEPTH,
                ts_ms=ts_ms,
                recv_ms=recv_ms,
                # Bybit's single counter expressed in the internal three-field
                # form: this delta is exactly one step past the previous one.
                payload={"U": update_id, "u": update_id, "pu": update_id - 1,
                         "b": bids, "a": asks},
            )
        ]

    def _normalise_trades(self, message: dict[str, Any], recv_ms: int) -> list[MarketEvent]:
        events: list[MarketEvent] = []
        for row in message.get("data") or ():
            symbol = str(row.get("s", "")).upper()
            if not symbol:
                continue
            # ``S`` is the taker's side directly: "Buy" means the aggressor
            # bought. No inversion, unlike Binance's maker flag.
            aggressor = Side.BUY if str(row.get("S", "Buy")).lower() == "buy" else Side.SELL
            events.append(
                MarketEvent(
                    symbol=symbol,
                    kind=EventKind.TRADE,
                    ts_ms=int(row.get("T") or message.get("ts") or recv_ms),
                    recv_ms=recv_ms,
                    payload={
                        "price": _f(row.get("p")),
                        "qty": _f(row.get("v")),
                        "aggressor": aggressor.value,
                        "trade_id": _trade_id(row.get("i")),
                    },
                )
            )
        return events

    def _normalise_ticker(
        self, message: dict[str, Any], ts_ms: int, recv_ms: int
    ) -> list[MarketEvent]:
        data = message.get("data") or {}
        symbol = str(data.get("symbol", "")).upper()
        if not symbol:
            return []

        # For linear contracts this topic is a delta: a push carries only the
        # fields that changed. Reading one as complete would blank mark price
        # and funding every time only the best bid moved.
        merged = self._ticker_state.setdefault(symbol, {})
        merged.update({k: v for k, v in data.items() if v not in (None, "")})

        events: list[MarketEvent] = []
        if "markPrice" in merged or "indexPrice" in merged:
            events.append(
                MarketEvent(
                    symbol=symbol,
                    kind=EventKind.MARK_PRICE,
                    ts_ms=ts_ms,
                    recv_ms=recv_ms,
                    payload={
                        "mark_price": _f(merged.get("markPrice")),
                        "index_price": _f(merged.get("indexPrice")),
                        "settlement_price": _f(merged.get("prevPrice24h")),
                        "funding_rate": _f(merged.get("fundingRate")),
                        "next_funding_ms": int(_f(merged.get("nextFundingTime"))),
                    },
                )
            )
        if "bid1Price" in merged and "ask1Price" in merged:
            events.append(
                MarketEvent(
                    symbol=symbol,
                    kind=EventKind.BOOK_TICKER,
                    ts_ms=ts_ms,
                    recv_ms=recv_ms,
                    payload={
                        "bid": _f(merged.get("bid1Price")),
                        "bid_qty": _f(merged.get("bid1Size")),
                        "ask": _f(merged.get("ask1Price")),
                        "ask_qty": _f(merged.get("ask1Size")),
                        "update_id": 0,
                    },
                )
            )
        return events

    # ------------------------------------------------------------------ REST

    def _http(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(self.http_timeout_s), headers={"User-Agent": "aurum-edge-lab/1.0"}
            )
        return self._client

    @staticmethod
    def _unwrap(payload: dict[str, Any]) -> dict[str, Any]:
        """Bybit reports application errors inside a 200 response.

        ``retCode`` is the real status; treating HTTP 200 as success would make
        a rejected request look like an empty book.
        """
        code = payload.get("retCode")
        if code not in (0, None):
            raise RuntimeError(f"bybit retCode={code}: {payload.get('retMsg', 'unknown error')}")
        return payload.get("result") or {}

    async def fetch_depth_snapshot(self, symbol: str, limit: int = 500) -> DepthSnapshot:
        response = await self._http().get(
            f"{self.rest_base}{self.depth_path}",
            params={"category": self.category, "symbol": symbol.upper(), "limit": min(limit, 500)},
        )
        response.raise_for_status()
        result = self._unwrap(response.json())
        recv_ms = int(time.time() * 1000)
        return DepthSnapshot(
            symbol=symbol.upper(),
            last_update_id=int(result.get("u", 0)),
            ts_ms=int(result.get("ts") or recv_ms),
            recv_ms=recv_ms,
            bids=[(_f(p), _f(q)) for p, q in result.get("b", ())],
            asks=[(_f(p), _f(q)) for p, q in result.get("a", ())],
        )

    async def sync_time(self) -> int | None:
        response = await self._http().get(f"{self.rest_base}{self.time_path}")
        response.raise_for_status()
        result = self._unwrap(response.json())
        nano = result.get("timeNano")
        if nano:
            return int(int(nano) // 1_000_000)
        second = result.get("timeSecond")
        return int(float(second) * 1000) if second else None

    async def verify_endpoints(self) -> dict[str, Any]:
        """Prove the configured host answers and lists every symbol as a
        tradable linear perpetual."""
        result: dict[str, Any] = {
            "checked": True,
            "venue": "bybit_linear",
            "rest_base": self.rest_base,
            "ws_base": self.ws_base,
            "ws_url": self.ws_url(),
            "category": self.category,
            "ws_depth": self.ws_depth,
            "ok": False,
        }
        try:
            started = time.monotonic()
            server_time = await self.sync_time()
            result["server_time_ms"] = server_time
            result["rest_latency_ms"] = round((time.monotonic() - started) * 1000, 1)

            listed: set[str] = set()
            cursor: str | None = None
            for _ in range(10):  # bounded: instruments-info is paginated
                params: dict[str, Any] = {"category": self.category, "limit": 1000}
                if cursor:
                    params["cursor"] = cursor
                response = await self._http().get(
                    f"{self.rest_base}{self.instruments_path}", params=params
                )
                response.raise_for_status()
                info = self._unwrap(response.json())
                for row in info.get("list", ()):
                    if row.get("status") in (None, "Trading") and str(
                        row.get("contractType", "LinearPerpetual")
                    ).startswith("Linear"):
                        listed.add(str(row.get("symbol", "")).upper())
                cursor = info.get("nextPageCursor") or None
                if not cursor:
                    break

            missing = [s for s in self.symbols if s not in listed]
            result["symbols_listed"] = len(listed)
            result["missing_symbols"] = missing
            result["ok"] = not missing
            if missing:
                result["error"] = (
                    f"not tradable {self.category} perpetuals on this venue: {', '.join(missing)}"
                )
        except Exception as exc:
            result["ok"] = False
            result["error"] = f"{type(exc).__name__}: {exc}"
            result["hint"] = (
                "Check market.rest_base / market.ws_base against the current official Bybit V5 "
                "documentation (https://bybit-exchange.github.io/docs/v5/intro). This build could "
                "not reach Bybit to verify them."
            )
        return result


def _trade_id(value: Any) -> int:
    """Bybit trade ids are UUID-like strings; the internal field is numeric.

    A stable hash keeps the id useful for de-duplication without pretending it
    is the venue's own sequence number.
    """
    if value is None:
        return 0
    try:
        return int(value)
    except (TypeError, ValueError):
        return abs(hash(str(value))) % (2**53)
