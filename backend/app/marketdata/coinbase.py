"""Coinbase Exchange public feed adapter.

Included to prove the venue abstraction: the engine runs unchanged against a
second exchange. Only the channels that are genuinely public and unauthenticated
are used (`ticker` and `matches`). Coinbase's `level2` channel requires
authentication, so this adapter deliberately does **not** advertise
`DEPTH_DIFF`; the order book is only built from venues that can serve it.
"""

from __future__ import annotations

import asyncio
import json

import httpx
import websockets

from app.core.clock import now_ms
from app.core.logging_conf import get_logger
from app.marketdata.base import Capability, Emit, ExchangeAdapter
from app.marketdata.types import BookTicker, Trade

log = get_logger(__name__)


def _to_product(symbol: str) -> str:
    """BTCUSDT -> BTC-USD (Coinbase quotes USD, not USDT, for the main book)."""
    s = symbol.upper()
    for quote in ("USDT", "USDC", "USD", "EUR", "GBP"):
        if s.endswith(quote):
            base = s[: -len(quote)]
            return f"{base}-{'USD' if quote == 'USDT' else quote}"
    return s


class CoinbaseAdapter(ExchangeAdapter):
    name = "coinbase"
    capabilities = {Capability.TRADES, Capability.BOOK_TICKER}

    def __init__(
        self,
        symbol: str,
        ws_base: str = "wss://ws-feed.exchange.coinbase.com",
        rest_base: str = "https://api.exchange.coinbase.com",
        rest_timeout_s: float = 10.0,
        stale_timeout_s: float = 20.0,
    ) -> None:
        super().__init__(symbol)
        self.product_id = _to_product(symbol)
        self.ws_base = ws_base
        self.stale_timeout_s = stale_timeout_s
        self._http = httpx.AsyncClient(
            base_url=rest_base.rstrip("/"),
            timeout=rest_timeout_s,
            headers={"User-Agent": "btc-5s-quant-engine/1.0"},
        )
        self._seq = 0

    def stream_names(self) -> list[str]:
        return [f"ticker:{self.product_id}", f"matches:{self.product_id}"]

    async def _stream_once(self, emit: Emit) -> None:
        async with websockets.connect(
            self.ws_base, ping_interval=20, ping_timeout=20, close_timeout=5
        ) as ws:
            await ws.send(
                json.dumps(
                    {
                        "type": "subscribe",
                        "product_ids": [self.product_id],
                        "channels": ["ticker", "matches"],
                    }
                )
            )
            self.state.connected = True
            self.state.connected_since = now_ms()
            while True:
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=self.stale_timeout_s)
                except asyncio.TimeoutError as exc:
                    raise ConnectionError("coinbase feed stale") from exc
                self.mark_message()
                self._handle(json.loads(raw), emit)

    def _handle(self, msg: dict, emit: Emit) -> None:
        mtype = msg.get("type")
        ts = now_ms()
        if mtype == "error":
            raise ConnectionError(f"coinbase error: {msg.get('message')}")
        if mtype == "ticker" and msg.get("best_bid") and msg.get("best_ask"):
            emit(
                BookTicker(
                    exchange=self.name,
                    symbol=self.symbol,
                    bid_price=float(msg["best_bid"]),
                    bid_qty=float(msg.get("best_bid_size") or 0.0),
                    ask_price=float(msg["best_ask"]),
                    ask_qty=float(msg.get("best_ask_size") or 0.0),
                    exchange_ts=_iso_ms(msg.get("time"), ts),
                    server_ts=ts,
                    update_id=int(msg.get("sequence", 0)) or None,
                )
            )
        elif mtype in ("match", "last_match"):
            self._seq += 1
            emit(
                Trade(
                    exchange=self.name,
                    symbol=self.symbol,
                    trade_id=int(msg.get("trade_id", self._seq)),
                    price=float(msg["price"]),
                    quantity=float(msg["size"]),
                    # Coinbase reports the *maker* side.
                    is_buyer_maker=msg.get("side") == "buy",
                    exchange_ts=_iso_ms(msg.get("time"), ts),
                    server_ts=ts,
                )
            )

    async def fetch_server_time(self) -> int | None:
        r = await self._http.get("/time")
        r.raise_for_status()
        return int(float(r.json()["epoch"]) * 1000)

    async def close(self) -> None:
        await self._http.aclose()


def _iso_ms(value: str | None, fallback: int) -> int:
    if not value:
        return fallback
    try:
        from datetime import datetime

        return int(
            datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp() * 1000
        )
    except ValueError:
        return fallback
