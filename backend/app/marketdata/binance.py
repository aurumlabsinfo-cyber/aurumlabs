"""Binance public market data adapters.

Only public endpoints are used - no API key is required or accepted for market
data. Documented endpoints:

* combined stream: ``wss://stream.binance.com:9443/stream?streams=...``
  - ``<symbol>@trade``            individual trades
  - ``<symbol>@bookTicker``       best bid/ask
  - ``<symbol>@depth@100ms``      diff depth updates
* REST snapshot: ``GET /api/v3/depth?symbol=..&limit=1000``
* REST time:     ``GET /api/v3/time``
* REST klines:   ``GET /api/v3/klines``

The USD-M futures adapter adds mark price/funding, open interest and forced
liquidation events.
"""

from __future__ import annotations

import asyncio
import json

import httpx
import websockets

from app.core.clock import now_ms
from app.core.logging_conf import get_logger
from app.marketdata.base import Capability, Emit, ExchangeAdapter
from app.marketdata.types import (
    BookTicker,
    DepthSnapshot,
    DepthUpdate,
    DerivativesTick,
    Liquidation,
    Side,
    Trade,
)

log = get_logger(__name__)


def _levels(raw: list[list[str]]) -> list[tuple[float, float]]:
    return [(float(p), float(q)) for p, q in raw]


class BinanceSpotAdapter(ExchangeAdapter):
    name = "binance_spot"
    capabilities = {
        Capability.TRADES,
        Capability.BOOK_TICKER,
        Capability.DEPTH_DIFF,
        Capability.KLINES,
    }

    def __init__(
        self,
        symbol: str,
        ws_base: str = "wss://stream.binance.com:9443",
        rest_base: str = "https://api.binance.com",
        depth_speed_ms: int = 100,
        rest_timeout_s: float = 10.0,
        stale_timeout_s: float = 10.0,
    ) -> None:
        super().__init__(symbol)
        self.ws_base = ws_base.rstrip("/")
        self.rest_base = rest_base.rstrip("/")
        self.depth_speed_ms = depth_speed_ms
        self.stale_timeout_s = stale_timeout_s
        self._http = httpx.AsyncClient(
            base_url=self.rest_base,
            timeout=rest_timeout_s,
            headers={"User-Agent": "btc-5s-quant-engine/1.0"},
        )

    # ------------------------------------------------------------ streaming
    def stream_names(self) -> list[str]:
        s = self.symbol.lower()
        return [f"{s}@trade", f"{s}@bookTicker", f"{s}@depth@{self.depth_speed_ms}ms"]

    @property
    def ws_url(self) -> str:
        return f"{self.ws_base}/stream?streams={'/'.join(self.stream_names())}"

    async def _stream_once(self, emit: Emit) -> None:
        async with websockets.connect(
            self.ws_url,
            ping_interval=20,
            ping_timeout=20,
            close_timeout=5,
            max_queue=2048,
        ) as ws:
            self.state.connected = True
            self.state.connected_since = now_ms()
            self.state.last_error = None
            log.info("binance.connected", url=self.ws_url)
            while True:
                try:
                    raw = await asyncio.wait_for(
                        ws.recv(), timeout=self.stale_timeout_s
                    )
                except asyncio.TimeoutError as exc:
                    # A silent socket is a dead socket: force a reconnect so the
                    # order book resyncs rather than serving stale state.
                    raise ConnectionError("no message within stale timeout") from exc
                self.mark_message()
                self._handle(json.loads(raw), emit)

    def _handle(self, msg: dict, emit: Emit) -> None:
        payload = msg.get("data") or msg
        stream = msg.get("stream", "")
        etype = payload.get("e") or ("bookTicker" if "b" in payload and "a" in payload else "")
        ts = now_ms()

        if etype == "trade":
            emit(
                Trade(
                    exchange=self.name,
                    symbol=payload["s"],
                    trade_id=int(payload["t"]),
                    price=float(payload["p"]),
                    quantity=float(payload["q"]),
                    is_buyer_maker=bool(payload["m"]),
                    exchange_ts=int(payload["T"]),
                    server_ts=ts,
                )
            )
        elif etype == "bookTicker" or stream.endswith("@bookTicker"):
            emit(
                BookTicker(
                    exchange=self.name,
                    symbol=payload["s"],
                    bid_price=float(payload["b"]),
                    bid_qty=float(payload["B"]),
                    ask_price=float(payload["a"]),
                    ask_qty=float(payload["A"]),
                    # Spot bookTicker carries no event time; use the transaction
                    # time when present, else the receive time (latency 0).
                    exchange_ts=int(payload.get("E") or payload.get("T") or ts),
                    server_ts=ts,
                    update_id=int(payload.get("u", 0)) or None,
                )
            )
        elif etype == "depthUpdate":
            emit(
                DepthUpdate(
                    exchange=self.name,
                    symbol=payload["s"],
                    first_update_id=int(payload["U"]),
                    final_update_id=int(payload["u"]),
                    prev_final_update_id=(
                        int(payload["pu"]) if "pu" in payload else None
                    ),
                    bids=_levels(payload.get("b", [])),
                    asks=_levels(payload.get("a", [])),
                    exchange_ts=int(payload.get("E", ts)),
                    server_ts=ts,
                )
            )

    # ----------------------------------------------------------------- REST
    async def fetch_depth_snapshot(self, limit: int = 1000) -> DepthSnapshot:
        r = await self._http.get(
            "/api/v3/depth", params={"symbol": self.symbol, "limit": limit}
        )
        r.raise_for_status()
        d = r.json()
        return DepthSnapshot(
            exchange=self.name,
            symbol=self.symbol,
            last_update_id=int(d["lastUpdateId"]),
            bids=_levels(d["bids"]),
            asks=_levels(d["asks"]),
            server_ts=now_ms(),
        )

    async def fetch_server_time(self) -> int | None:
        r = await self._http.get("/api/v3/time")
        r.raise_for_status()
        return int(r.json()["serverTime"])

    async def fetch_klines(
        self, interval: str = "1s", limit: int = 1000, end_ms: int | None = None
    ) -> list[dict]:
        params: dict = {"symbol": self.symbol, "interval": interval, "limit": limit}
        if end_ms:
            params["endTime"] = end_ms
        r = await self._http.get("/api/v3/klines", params=params)
        r.raise_for_status()
        return [
            {
                "open_time": int(k[0]),
                "open": float(k[1]),
                "high": float(k[2]),
                "low": float(k[3]),
                "close": float(k[4]),
                "volume": float(k[5]),
                "close_time": int(k[6]),
                "quote_volume": float(k[7]),
                "trades": int(k[8]),
                "taker_buy_base": float(k[9]),
                "taker_buy_quote": float(k[10]),
            }
            for k in r.json()
        ]

    async def fetch_recent_trades(self, limit: int = 1000) -> list[Trade]:
        r = await self._http.get(
            "/api/v3/trades", params={"symbol": self.symbol, "limit": limit}
        )
        r.raise_for_status()
        ts = now_ms()
        return [
            Trade(
                exchange=self.name,
                symbol=self.symbol,
                trade_id=int(t["id"]),
                price=float(t["price"]),
                quantity=float(t["qty"]),
                is_buyer_maker=bool(t["isBuyerMaker"]),
                exchange_ts=int(t["time"]),
                server_ts=ts,
            )
            for t in r.json()
        ]

    async def close(self) -> None:
        await self._http.aclose()


class BinanceFuturesAdapter(ExchangeAdapter):
    """USD-M futures: mark price/funding, liquidations, open interest.

    Used as an *auxiliary* feed. The spot adapter remains the price authority
    unless it is explicitly configured as primary.
    """

    name = "binance_futures"
    capabilities = {
        Capability.TRADES,
        Capability.FUNDING,
        Capability.OPEN_INTEREST,
        Capability.LIQUIDATIONS,
    }

    def __init__(
        self,
        symbol: str,
        ws_base: str = "wss://fstream.binance.com",
        rest_base: str = "https://fapi.binance.com",
        rest_timeout_s: float = 10.0,
        stale_timeout_s: float = 15.0,
        open_interest_interval_s: float = 30.0,
    ) -> None:
        super().__init__(symbol)
        self.ws_base = ws_base.rstrip("/")
        self.rest_base = rest_base.rstrip("/")
        self.stale_timeout_s = stale_timeout_s
        self.open_interest_interval_s = open_interest_interval_s
        self._http = httpx.AsyncClient(
            base_url=self.rest_base,
            timeout=rest_timeout_s,
            headers={"User-Agent": "btc-5s-quant-engine/1.0"},
        )
        self._oi_task: asyncio.Task | None = None

    def stream_names(self) -> list[str]:
        s = self.symbol.lower()
        return [f"{s}@markPrice@1s", f"{s}@forceOrder", f"{s}@aggTrade"]

    @property
    def ws_url(self) -> str:
        return f"{self.ws_base}/stream?streams={'/'.join(self.stream_names())}"

    async def _stream_once(self, emit: Emit) -> None:
        self._oi_task = asyncio.create_task(self._poll_open_interest(emit))
        try:
            async with websockets.connect(
                self.ws_url, ping_interval=20, ping_timeout=20, close_timeout=5
            ) as ws:
                self.state.connected = True
                self.state.connected_since = now_ms()
                while True:
                    try:
                        raw = await asyncio.wait_for(
                            ws.recv(), timeout=self.stale_timeout_s
                        )
                    except asyncio.TimeoutError as exc:
                        raise ConnectionError("futures feed stale") from exc
                    self.mark_message()
                    self._handle(json.loads(raw), emit)
        finally:
            if self._oi_task:
                self._oi_task.cancel()
                self._oi_task = None

    def _handle(self, msg: dict, emit: Emit) -> None:
        payload = msg.get("data") or msg
        etype = payload.get("e")
        ts = now_ms()
        if etype == "markPriceUpdate":
            emit(
                DerivativesTick(
                    exchange=self.name,
                    symbol=payload["s"],
                    server_ts=ts,
                    exchange_ts=int(payload["E"]),
                    mark_price=float(payload["p"]),
                    index_price=float(payload.get("i", 0)) or None,
                    funding_rate=float(payload.get("r", 0) or 0),
                    next_funding_ts=int(payload.get("T", 0)) or None,
                )
            )
        elif etype == "forceOrder":
            o = payload["o"]
            emit(
                Liquidation(
                    exchange=self.name,
                    symbol=o["s"],
                    side=Side.BUY if o["S"] == "BUY" else Side.SELL,
                    price=float(o["ap"] or o["p"]),
                    quantity=float(o["q"]),
                    exchange_ts=int(o["T"]),
                    server_ts=ts,
                )
            )
        elif etype == "aggTrade":
            emit(
                Trade(
                    exchange=self.name,
                    symbol=payload["s"],
                    trade_id=int(payload["a"]),
                    price=float(payload["p"]),
                    quantity=float(payload["q"]),
                    is_buyer_maker=bool(payload["m"]),
                    exchange_ts=int(payload["T"]),
                    server_ts=ts,
                )
            )

    async def _poll_open_interest(self, emit: Emit) -> None:
        while True:
            try:
                r = await self._http.get(
                    "/fapi/v1/openInterest", params={"symbol": self.symbol}
                )
                r.raise_for_status()
                d = r.json()
                emit(
                    DerivativesTick(
                        exchange=self.name,
                        symbol=self.symbol,
                        server_ts=now_ms(),
                        exchange_ts=int(d.get("time", now_ms())),
                        open_interest=float(d["openInterest"]),
                    )
                )
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                log.debug("futures.open_interest_failed", error=str(exc))
            await asyncio.sleep(self.open_interest_interval_s)

    async def fetch_server_time(self) -> int | None:
        r = await self._http.get("/fapi/v1/time")
        r.raise_for_status()
        return int(r.json()["serverTime"])

    async def close(self) -> None:
        await self._http.aclose()
