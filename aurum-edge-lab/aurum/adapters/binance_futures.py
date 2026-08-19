"""Binance USD-M Futures public market adapter.

Endpoint routing is read from configuration, never baked in as a constant.
Binance has changed WebSocket routing before and the blueprint is explicit that
URLs must not be copied from old examples; :meth:`verify_endpoints` therefore
runs at startup, hits the configured REST host, and puts the answer in
``/health``.  If a route moves, the operator sees a failed verification with the
exact URL that failed instead of an inexplicably quiet feed.

Streams used (combined, one connection per shard):

    <symbol>@depth@<speed>   incremental depth diffs (U / u / pu sequence fields)
    <symbol>@aggTrade        aggregated trades, ``m`` gives the aggressor side
    <symbol>@bookTicker      best bid/ask, faster than depth for top-of-book
    <symbol>@markPrice@1s    mark price, index price, funding rate

This module contains no private endpoint, no signing, and no order placement.
"""

from __future__ import annotations

import asyncio
import json
import random
import time
from typing import Any, Iterable

import httpx

from ..domain import EventKind, FeedState, MarketEvent, Side
from ..logging_setup import get_logger
from .base import DepthSnapshot, MarketFeed

log = get_logger("adapters.binance")

try:  # websockets >= 13 ships the new asyncio client; older versions do not.
    from websockets.asyncio.client import connect as ws_connect  # type: ignore
except ImportError:  # pragma: no cover - depends on installed version
    from websockets import connect as ws_connect  # type: ignore

#: One connection carries at most this many streams.  Binance allows more, but
#: a smaller shard means a reconnect costs fewer symbols their continuity.
MAX_STREAMS_PER_CONNECTION = 40


def _f(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


class BinanceFuturesFeed(MarketFeed):
    kind = "live"

    def __init__(
        self,
        symbols: list[str],
        *,
        rest_base: str,
        ws_base: str,
        ws_path: str = "/stream",
        depth_path: str = "/fapi/v1/depth",
        exchange_info_path: str = "/fapi/v1/exchangeInfo",
        time_path: str = "/fapi/v1/time",
        depth_speed: str = "100ms",
        streams: Iterable[str] = ("depth", "aggTrade", "bookTicker", "markPrice"),
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
        self.exchange_info_path = exchange_info_path
        self.time_path = time_path
        self.depth_speed = depth_speed
        self.streams = tuple(streams)
        self.heartbeat_timeout_s = heartbeat_timeout_s
        self.reconnect_initial_s = reconnect_initial_s
        self.reconnect_max_s = reconnect_max_s
        self.reconnect_factor = reconnect_factor
        self.reconnect_jitter = reconnect_jitter
        self.http_timeout_s = http_timeout_s

        self._client: httpx.AsyncClient | None = None
        self._tasks: list[asyncio.Task[None]] = []
        self._running = False
        self._shard_state: dict[int, FeedState] = {}

    # ------------------------------------------------------------ stream set

    def stream_names(self) -> list[str]:
        names: list[str] = []
        for symbol in self.symbols:
            lower = symbol.lower()
            if "depth" in self.streams:
                names.append(f"{lower}@depth@{self.depth_speed}")
            if "aggTrade" in self.streams:
                names.append(f"{lower}@aggTrade")
            if "bookTicker" in self.streams:
                names.append(f"{lower}@bookTicker")
            if "markPrice" in self.streams:
                names.append(f"{lower}@markPrice@1s")
        return names

    def _shards(self) -> list[list[str]]:
        names = self.stream_names()
        return [
            names[i : i + MAX_STREAMS_PER_CONNECTION]
            for i in range(0, len(names), MAX_STREAMS_PER_CONNECTION)
        ]

    def _shard_url(self, shard: list[str]) -> str:
        return f"{self.ws_base}{self.ws_path}?streams={'/'.join(shard)}"

    # -------------------------------------------------------------- lifecycle

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(self.http_timeout_s), headers={"User-Agent": "aurum-edge-lab/1.0"}
        )
        shards = self._shards()
        self.stats.subscriptions = len(self.stream_names())
        self.stats.connections = len(shards)
        self.stats.endpoint = self.ws_base + self.ws_path
        self._set_state(FeedState.CONNECTING, f"{len(shards)} connection(s)")
        for index, shard in enumerate(shards):
            self._tasks.append(asyncio.create_task(self._run_shard(index, shard), name=f"binance-ws-{index}"))
        log.info(
            "feed starting",
            extra={"symbols": len(self.symbols), "streams": self.stats.subscriptions, "shards": len(shards)},
        )

    async def stop(self) -> None:
        self._running = False
        for task in self._tasks:
            task.cancel()
        for task in self._tasks:
            try:
                await task
            except (asyncio.CancelledError, Exception):  # noqa: BLE001 - shutdown must not raise
                pass
        self._tasks.clear()
        if self._client is not None:
            await self._client.aclose()
            self._client = None
        self._set_state(FeedState.DISCONNECTED, "stopped")

    # ------------------------------------------------------------- websocket

    async def _run_shard(self, index: int, shard: list[str]) -> None:
        """Connect, read, and reconnect forever with exponential backoff."""
        delay = self.reconnect_initial_s
        url = self._shard_url(shard)
        while self._running:
            try:
                self._shard_state[index] = FeedState.CONNECTING
                async with ws_connect(url, ping_interval=20, ping_timeout=20, max_queue=4096) as socket:
                    self._shard_state[index] = FeedState.LIVE
                    self.stats.connected_since_ms = int(time.time() * 1000)
                    self._refresh_state()
                    delay = self.reconnect_initial_s  # a clean connect resets the backoff
                    log.info("ws connected", extra={"shard": index, "streams": len(shard)})
                    await self._read_loop(socket)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - every failure is a reconnect
                self.stats.errors += 1
                self.stats.last_error = f"{type(exc).__name__}: {exc}"
                self._shard_state[index] = FeedState.ERROR
                self._refresh_state()
                log.warning("ws error", extra={"shard": index, "error": self.stats.last_error})
            if not self._running:
                break
            self.stats.reconnects += 1
            # Jitter keeps ten shards from stampeding the venue in lockstep.
            sleep_for = delay * (1.0 + random.uniform(-self.reconnect_jitter, self.reconnect_jitter))
            await asyncio.sleep(max(0.1, sleep_for))
            delay = min(self.reconnect_max_s, delay * self.reconnect_factor)

    async def _read_loop(self, socket: Any) -> None:
        """Read frames until the socket dies or goes quiet past the heartbeat."""
        while self._running:
            try:
                raw = await asyncio.wait_for(socket.recv(), timeout=self.heartbeat_timeout_s)
            except (TimeoutError, asyncio.TimeoutError) as exc:
                # No frame within the heartbeat window: the venue pings every few
                # minutes and these streams tick far faster, so silence means a
                # half-open socket. Drop it and let the backoff reconnect.
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
        # Combined streams wrap the payload; raw streams do not. Accept both so
        # a change of ws_path does not silently stop producing events.
        data = message.get("data", message) if isinstance(message, dict) else None
        if not isinstance(data, dict):
            return
        event = self._normalise(data, recv_ms)
        if event is not None:
            self._emit(event)

    def _normalise(self, data: dict[str, Any], recv_ms: int) -> MarketEvent | None:
        kind = data.get("e")
        symbol = str(data.get("s", "")).upper()
        if not symbol:
            return None

        if kind == "depthUpdate":
            return MarketEvent(
                symbol=symbol,
                kind=EventKind.DEPTH,
                ts_ms=int(data.get("T") or data.get("E") or recv_ms),
                recv_ms=recv_ms,
                payload={
                    "U": int(data.get("U", 0)),
                    "u": int(data.get("u", 0)),
                    "pu": int(data.get("pu", 0)),
                    "b": [(_f(p), _f(q)) for p, q in data.get("b", ())],
                    "a": [(_f(p), _f(q)) for p, q in data.get("a", ())],
                },
            )

        if kind == "aggTrade":
            # ``m`` is "buyer is the market maker", so m=True means the taker —
            # the aggressor whose direction we care about — was the seller.
            maker_is_buyer = bool(data.get("m", False))
            return MarketEvent(
                symbol=symbol,
                kind=EventKind.TRADE,
                ts_ms=int(data.get("T") or data.get("E") or recv_ms),
                recv_ms=recv_ms,
                payload={
                    "price": _f(data.get("p")),
                    "qty": _f(data.get("q")),
                    "aggressor": (Side.SELL if maker_is_buyer else Side.BUY).value,
                    "trade_id": int(data.get("a", 0)),
                },
            )

        if kind == "bookTicker" or ("b" in data and "a" in data and "B" in data and "A" in data):
            return MarketEvent(
                symbol=symbol,
                kind=EventKind.BOOK_TICKER,
                ts_ms=int(data.get("T") or data.get("E") or recv_ms),
                recv_ms=recv_ms,
                payload={
                    "bid": _f(data.get("b")),
                    "bid_qty": _f(data.get("B")),
                    "ask": _f(data.get("a")),
                    "ask_qty": _f(data.get("A")),
                    "update_id": int(data.get("u", 0)),
                },
            )

        if kind == "markPriceUpdate":
            return MarketEvent(
                symbol=symbol,
                kind=EventKind.MARK_PRICE,
                ts_ms=int(data.get("E") or recv_ms),
                recv_ms=recv_ms,
                payload={
                    "mark_price": _f(data.get("p")),
                    "index_price": _f(data.get("i")),
                    "settlement_price": _f(data.get("P")),
                    "funding_rate": _f(data.get("r")),
                    "next_funding_ms": int(data.get("T", 0)),
                },
            )
        return None

    def _refresh_state(self) -> None:
        states = set(self._shard_state.values())
        if not states:
            self._set_state(FeedState.DISCONNECTED)
        elif states == {FeedState.LIVE}:
            self._set_state(FeedState.LIVE)
        elif FeedState.LIVE in states:
            self._set_state(FeedState.LIVE, "partial: some shards reconnecting")
        elif FeedState.CONNECTING in states:
            self._set_state(FeedState.CONNECTING)
        else:
            self._set_state(FeedState.ERROR, self.stats.last_error)

    # ------------------------------------------------------------------ REST

    def _http(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(self.http_timeout_s), headers={"User-Agent": "aurum-edge-lab/1.0"}
            )
        return self._client

    async def fetch_depth_snapshot(self, symbol: str, limit: int = 1000) -> DepthSnapshot:
        url = f"{self.rest_base}{self.depth_path}"
        response = await self._http().get(url, params={"symbol": symbol.upper(), "limit": limit})
        response.raise_for_status()
        payload = response.json()
        recv_ms = int(time.time() * 1000)
        return DepthSnapshot(
            symbol=symbol.upper(),
            last_update_id=int(payload["lastUpdateId"]),
            ts_ms=int(payload.get("T") or payload.get("E") or recv_ms),
            recv_ms=recv_ms,
            bids=[(_f(p), _f(q)) for p, q in payload.get("bids", ())],
            asks=[(_f(p), _f(q)) for p, q in payload.get("asks", ())],
        )

    async def sync_time(self) -> int | None:
        response = await self._http().get(f"{self.rest_base}{self.time_path}")
        response.raise_for_status()
        return int(response.json()["serverTime"])

    async def verify_endpoints(self) -> dict[str, Any]:
        """Prove the configured REST host answers and lists our symbols."""
        result: dict[str, Any] = {
            "checked": True,
            "rest_base": self.rest_base,
            "ws_base": self.ws_base,
            "ws_url_sample": self._shard_url(self._shards()[0])[:180] if self.symbols else "",
            "ok": False,
        }
        try:
            started = time.monotonic()
            server_time = await self.sync_time()
            result["server_time_ms"] = server_time
            result["rest_latency_ms"] = round((time.monotonic() - started) * 1000, 1)

            response = await self._http().get(f"{self.rest_base}{self.exchange_info_path}")
            response.raise_for_status()
            info = response.json()
            listed = {
                s["symbol"]
                for s in info.get("symbols", ())
                if s.get("contractType") in (None, "PERPETUAL") and s.get("status") in (None, "TRADING")
            }
            missing = [s for s in self.symbols if s not in listed]
            result["symbols_listed"] = len(listed)
            result["missing_symbols"] = missing
            result["ok"] = not missing
            if missing:
                result["error"] = f"not tradable perpetuals on this venue: {', '.join(missing)}"
        except Exception as exc:  # noqa: BLE001 - the verdict is the return value
            result["ok"] = False
            result["error"] = f"{type(exc).__name__}: {exc}"
            result["hint"] = (
                "Check market.rest_base / market.ws_base against the current official "
                "Binance USD-M Futures documentation; routing has changed before."
            )
        return result
