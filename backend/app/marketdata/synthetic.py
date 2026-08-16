"""SYNTHETIC market data generator - NOT REAL MARKET DATA.

Purpose: exercise the full pipeline (order book sequencing, features, agents,
signal lifecycle, persistence, WebSocket fan-out) in environments with no
exchange connectivity - CI, tests, air-gapped hosts.

Guarantees:

* every object it emits carries ``source = DataSource.SYNTHETIC``;
* every database row written from it has ``is_synthetic = true``;
* the API and the dashboard show a permanent SYNTHETIC warning;
* the backtest/edge tooling **refuses** to include synthetic rows.

It is disabled unless ``ALLOW_SYNTHETIC_SOURCE=true``. Statistics produced from
synthetic data describe the simulator, never the market.
"""

from __future__ import annotations

import asyncio
import math
import random

from app.core.clock import now_ms
from app.marketdata.base import Capability, Emit, ExchangeAdapter
from app.marketdata.types import (
    BookTicker,
    DataSource,
    DepthSnapshot,
    DepthUpdate,
    Trade,
)


class SyntheticAdapter(ExchangeAdapter):
    name = "synthetic"
    capabilities = {
        Capability.TRADES,
        Capability.BOOK_TICKER,
        Capability.DEPTH_DIFF,
    }
    synthetic = True

    def __init__(
        self,
        symbol: str = "BTCUSDT",
        start_price: float = 100_000.0,
        tick_size: float = 0.01,
        vol_bps_per_s: float = 4.0,
        interval_ms: int = 100,
        seed: int | None = None,
    ) -> None:
        super().__init__(symbol)
        self.price = start_price
        self.tick_size = tick_size
        self.vol = vol_bps_per_s
        self.interval_ms = interval_ms
        self.rng = random.Random(seed)
        self._update_id = 1_000_000
        self._trade_id = 1
        self._drift = 0.0
        self._levels = 50
        # Last published sides, so diffs can retire levels that are no longer
        # quoted. Without the removals the book would cross as price walks away.
        self._prev_bids: dict[float, float] = {}
        self._prev_asks: dict[float, float] = {}

    def stream_names(self) -> list[str]:
        return ["synthetic@tick", "synthetic@trade", "synthetic@depth"]

    def _round(self, p: float) -> float:
        return round(round(p / self.tick_size) * self.tick_size, 8)

    def _step_price(self, dt_s: float) -> None:
        # Ornstein-Uhlenbeck drift + gaussian noise: produces trends, ranges and
        # occasional bursts so the regime classifier has something to chew on.
        self._drift = 0.97 * self._drift + self.rng.gauss(0, 0.35)
        sigma = self.price * (self.vol / 10_000.0) * math.sqrt(dt_s)
        self.price = max(1.0, self.price + self._drift * sigma * 0.5 + self.rng.gauss(0, sigma))

    def _book_sides(self) -> tuple[list[tuple[float, float]], list[tuple[float, float]]]:
        half = max(self.tick_size, self._round(self.price * 0.00002))
        bids, asks = [], []
        for i in range(self._levels):
            bp = self._round(self.price - half - i * self.tick_size * 5)
            ap = self._round(self.price + half + i * self.tick_size * 5)
            decay = math.exp(-i / 12.0)
            bids.append((bp, round(self.rng.uniform(0.05, 2.5) * decay, 6)))
            asks.append((ap, round(self.rng.uniform(0.05, 2.5) * decay, 6)))
        return bids, asks

    @staticmethod
    def _diff(
        prev: dict[float, float], current: list[tuple[float, float]]
    ) -> list[tuple[float, float]]:
        """Levels to publish: new/changed prices, plus 0-qty removals."""
        cur = dict(current)
        out = [(p, q) for p, q in current if prev.get(p) != q]
        out.extend((p, 0.0) for p in prev if p not in cur)
        return out

    async def fetch_depth_snapshot(self, limit: int = 1000) -> DepthSnapshot:
        bids, asks = self._book_sides()
        self._prev_bids = dict(bids)
        self._prev_asks = dict(asks)
        return DepthSnapshot(
            exchange=self.name,
            symbol=self.symbol,
            last_update_id=self._update_id,
            bids=bids,
            asks=asks,
            server_ts=now_ms(),
            source=DataSource.SYNTHETIC,
        )

    async def fetch_server_time(self) -> int | None:
        return now_ms()

    async def _stream_once(self, emit: Emit) -> None:
        self.state.connected = True
        self.state.connected_since = now_ms()
        dt = self.interval_ms / 1000.0
        while not self._stop.is_set():
            await asyncio.sleep(dt)
            self._step_price(dt)
            ts = now_ms()
            self.mark_message()

            bids, asks = self._book_sides()
            emit(
                BookTicker(
                    exchange=self.name,
                    symbol=self.symbol,
                    bid_price=bids[0][0],
                    bid_qty=bids[0][1],
                    ask_price=asks[0][0],
                    ask_qty=asks[0][1],
                    exchange_ts=ts,
                    server_ts=ts,
                    source=DataSource.SYNTHETIC,
                )
            )

            first = self._update_id + 1
            self._update_id += 1
            bid_diff = self._diff(self._prev_bids, bids)
            ask_diff = self._diff(self._prev_asks, asks)
            self._prev_bids = dict(bids)
            self._prev_asks = dict(asks)
            emit(
                DepthUpdate(
                    exchange=self.name,
                    symbol=self.symbol,
                    first_update_id=first,
                    final_update_id=self._update_id,
                    prev_final_update_id=None,
                    bids=bid_diff,
                    asks=ask_diff,
                    exchange_ts=ts,
                    server_ts=ts,
                    source=DataSource.SYNTHETIC,
                )
            )

            for _ in range(self.rng.randint(0, 4)):
                buy = self.rng.random() < 0.5 + 0.15 * math.tanh(self._drift)
                self._trade_id += 1
                emit(
                    Trade(
                        exchange=self.name,
                        symbol=self.symbol,
                        trade_id=self._trade_id,
                        price=asks[0][0] if buy else bids[0][0],
                        quantity=round(abs(self.rng.gauss(0.05, 0.12)) + 0.001, 6),
                        is_buyer_maker=not buy,
                        exchange_ts=ts,
                        server_ts=ts,
                        source=DataSource.SYNTHETIC,
                    )
                )
