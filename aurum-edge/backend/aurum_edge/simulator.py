"""A deterministic synthetic market, for ``selftest`` and for the test-suite.

This exists so the whole pipeline - scan, decide, execute, record, learn - can be
proven end to end without the exchange.  Two guarantees keep it honest:

* every snapshot it produces carries ``source="fake"``, and the health gate
  refuses LIVE mode on anything that is not ``source="bybit"``;
* it speaks the Bybit V5 wire format, so the code under test is the same parser,
  the same book sequencing and the same feature maths that run against Bybit.

It is never a fallback: if the real feed fails, the system blocks and says so.
It is used only when explicitly asked for.
"""

from __future__ import annotations

import asyncio
import math
import random
from dataclasses import dataclass, field
from typing import Any

from .config import Config
from .scan.market_core import Instrument, MarketCore, SymbolState
from .scan.snapshot import FeedSource
from .util.clock import Clock
from .util.logging_setup import get_logger

log = get_logger("simulator")


@dataclass
class SimSymbol:
    symbol: str
    price: float
    tick_size: float
    qty_step: float
    volatility: float
    base_trade_rate: float
    update_id: int = 1
    drift_bps_per_s: float = 0.0
    burst_until: float = 0.0
    burst_direction: float = 0.0
    open_interest: float = 1_000_000.0
    depth_scale: float = 1.0
    history: list[float] = field(default_factory=list)
    last_bid_levels: set[float] = field(default_factory=set)
    last_ask_levels: set[float] = field(default_factory=set)


class SimConnection:
    """Stands in for a websocket so health reporting stays truthful."""

    def __init__(self, name: str, topics: set[str]) -> None:
        self.name = name
        self.topics = topics
        self.messages = 0
        self.reconnects = 0
        self.is_live = True
        self.last_error = ""

    def health(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "state": "LIVE",
            "live": True,
            "stale": False,
            "age_ms": 0.0,
            "topics": len(self.topics),
            "messages": self.messages,
            "reconnects": self.reconnects,
            "uptime_s": 0.0,
            "last_error": "simulated feed - not the exchange",
        }

    async def stop(self) -> None:
        self.is_live = False


class SimulatedMarketCore(MarketCore):
    """A MarketCore fed by :class:`SyntheticMarket` instead of Bybit."""

    def __init__(
        self,
        cfg: Config,
        clock: Clock,
        symbols: int = 12,
        seed: int = 7,
        step_ms: float = 50.0,
    ) -> None:
        super().__init__(cfg, clock, rest=None, source=FeedSource.FAKE)  # type: ignore[arg-type]
        self.random = random.Random(seed)
        self.symbol_count = symbols
        self.step_ms = step_ms
        self.sim: dict[str, SimSymbol] = {}
        self._feed_task: asyncio.Task[None] | None = None
        self.steps = 0

    # ---------------------------------------------------------------- universe
    async def discover_universe(self) -> list[str]:
        names = [f"SIM{i}USDT" for i in range(self.symbol_count)]
        self.instruments = {}
        self.universe = []
        for i, name in enumerate(names):
            price = round(10.0 * (i + 1) + 0.5, 4)
            tick = 10 ** -(3 if price < 100 else 2)
            instrument = Instrument(
                symbol=name,
                tick_size=tick,
                qty_step=0.001,
                min_qty=0.001,
                min_notional_usd=5.0,
                max_leverage=25.0,
                status="Trading",
                contract_type="LinearPerpetual",
            )
            self.instruments[name] = instrument
            self.universe.append(name)
            self.states[name] = SymbolState(
                symbol=name,
                instrument=instrument,
                history_window_s=self.cfg.scan.history_window_s,
            )
            self.sim[name] = SimSymbol(
                symbol=name,
                price=price,
                tick_size=tick,
                qty_step=0.001,
                volatility=self.random.uniform(3.0, 9.0),
                base_trade_rate=self.random.uniform(3.0, 14.0),
                depth_scale=self.random.uniform(0.6, 2.4),
            )
        self.last_universe_refresh = self.clock.mono()
        return self.universe

    # ---------------------------------------------------------------- lifecycle
    async def start(self) -> None:
        if not self.universe:
            await self.discover_universe()
        self.started_at = self.clock.mono()
        self._pick_focus(initial=True)
        for symbol in self.focus:
            self.states[symbol].reset_book(self.cfg.scan.depth_topic_focus)
        topics = {t for symbol in self.universe for t in self._topics_for(symbol)}
        self.connections = [SimConnection("simulated-0", topics)]  # type: ignore[list-item]
        for symbol in self.universe:
            self._emit_book(self.sim[symbol], snapshot=True)
            self._emit_ticker(self.sim[symbol])
        self._feed_task = asyncio.create_task(self._feed_loop(), name="sim-feed")

    async def stop(self) -> None:
        if self._feed_task:
            self._feed_task.cancel()
            try:
                await self._feed_task
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
                pass
        self.connections = []

    async def wait_ready(self, timeout: float = 20.0) -> bool:
        return True

    async def _feed_loop(self) -> None:
        while True:
            self.step()
            await asyncio.sleep(self.step_ms / 1000.0)

    # ---------------------------------------------------------------- dynamics
    def step(self, steps: int = 1) -> None:
        """Advance the synthetic market.  Callable directly from tests."""
        for _ in range(steps):
            self.steps += 1
            now = self.clock.now_ms()
            for sim in self.sim.values():
                self._advance(sim, now)
                self._emit_book(sim, snapshot=(self.steps % 60 == 0))
                self._emit_trades(sim, now)
                if self.steps % 20 == 0:
                    self._emit_ticker(sim)

    def _advance(self, sim: SimSymbol, now_ms: float) -> None:
        dt = self.step_ms / 1000.0
        if now_ms > sim.burst_until:
            if self.random.random() < 0.010:
                # A burst is a real move, not a wiggle: with 5.5bp taker fees a
                # round trip costs ~15bps, so only genuine moves are tradable.
                sim.burst_direction = 1.0 if self.random.random() < 0.5 else -1.0
                sim.burst_until = now_ms + self.random.uniform(5_000.0, 11_000.0)
                sim.drift_bps_per_s = sim.burst_direction * self.random.uniform(15.0, 55.0)
            else:
                sim.drift_bps_per_s *= 0.90
                sim.burst_direction = 0.0
        drift = sim.drift_bps_per_s / 10_000.0 * dt
        shock = self.random.gauss(0.0, sim.volatility / 10_000.0) * math.sqrt(dt)
        sim.price = max(sim.price * math.exp(drift + shock), sim.tick_size * 10)
        sim.history.append(sim.price)
        del sim.history[:-200]
        sim.open_interest *= 1.0 + (sim.burst_direction * 0.0004 * dt)

    # ---------------------------------------------------------------- emitters
    def _round(self, price: float, tick: float) -> float:
        return round(round(price / tick) * tick, 8)

    def _emit_book(self, sim: SimSymbol, snapshot: bool = False) -> None:
        tick = sim.tick_size
        half_spread = tick * self.random.choice([0.5, 0.5, 1.0, 1.5])
        bid = self._round(sim.price - half_spread, tick)
        ask = self._round(bid + max(tick, half_spread * 2), tick)
        pressure = 1.0 + 0.9 * sim.burst_direction
        bids: list[list[str]] = []
        asks: list[list[str]] = []
        bid_levels: set[float] = set()
        ask_levels: set[float] = set()
        for level in range(25):
            size_b = max(
                (self.random.uniform(40.0, 260.0) * sim.depth_scale * pressure)
                / (1.0 + level * 0.35),
                0.001,
            )
            size_a = max(
                (self.random.uniform(40.0, 260.0) * sim.depth_scale / max(pressure, 0.1))
                / (1.0 + level * 0.35),
                0.001,
            )
            bid_price = self._round(bid - level * tick, tick)
            ask_price = self._round(ask + level * tick, tick)
            bid_levels.add(bid_price)
            ask_levels.add(ask_price)
            bids.append([f"{bid_price}", f"{size_b / sim.price:.4f}"])
            asks.append([f"{ask_price}", f"{size_a / sim.price:.4f}"])

        is_snapshot = snapshot or sim.update_id <= 2
        if not is_snapshot:
            # Bybit deletes a level by sending it with size 0; a delta that only
            # adds would leave the book crossed as the price walks away.
            bids.extend([f"{p}", "0"] for p in sim.last_bid_levels - bid_levels)
            asks.extend([f"{p}", "0"] for p in sim.last_ask_levels - ask_levels)
        sim.last_bid_levels = bid_levels
        sim.last_ask_levels = ask_levels

        sim.update_id += 1
        message = {
            "topic": f"{self.cfg.scan.depth_topic_focus}.{sim.symbol}",
            "type": "snapshot" if is_snapshot else "delta",
            "ts": self.clock.now_ms(),
            "cts": self.clock.now_ms(),
            "data": {
                "s": sim.symbol,
                "b": bids,
                "a": asks,
                "u": sim.update_id,
                "seq": sim.update_id * 3,
            },
        }
        self._deliver(message)

    def _emit_trades(self, sim: SimSymbol, now_ms: float) -> None:
        rate = sim.base_trade_rate * (2.6 if sim.burst_direction else 1.0)
        count = int(rate * self.step_ms / 1000.0)
        if self.random.random() < (rate * self.step_ms / 1000.0) % 1.0:
            count += 1
        if count <= 0:
            return
        buy_probability = 0.5 + 0.32 * sim.burst_direction
        rows = []
        for _ in range(count):
            side = "Buy" if self.random.random() < buy_probability else "Sell"
            size = self.random.uniform(20.0, 400.0) / sim.price
            rows.append(
                {
                    "T": now_ms,
                    "s": sim.symbol,
                    "S": side,
                    "v": f"{size:.4f}",
                    "p": f"{self._round(sim.price, sim.tick_size)}",
                    "L": "PlusTick",
                    "i": f"sim-{sim.update_id}",
                    "BT": False,
                }
            )
        self._deliver(
            {
                "topic": f"publicTrade.{sim.symbol}",
                "type": "snapshot",
                "ts": now_ms,
                "data": rows,
            }
        )

    def _emit_ticker(self, sim: SimSymbol) -> None:
        self._deliver(
            {
                "topic": f"tickers.{sim.symbol}",
                "type": "delta",
                "ts": self.clock.now_ms(),
                "data": {
                    "symbol": sim.symbol,
                    "lastPrice": f"{self._round(sim.price, sim.tick_size)}",
                    "markPrice": f"{self._round(sim.price, sim.tick_size)}",
                    "openInterest": f"{sim.open_interest:.2f}",
                    "turnover24h": f"{80_000_000.0 * sim.depth_scale:.2f}",
                    "fundingRate": "0.0001",
                },
            }
        )

    def _deliver(self, message: dict[str, Any]) -> None:
        if self.connections:
            self.connections[0].messages += 1  # type: ignore[union-attr]
        self._on_public_message(message)

    # ---------------------------------------------------------------- health
    def health(self) -> dict[str, Any]:
        health = super().health()
        health["simulated"] = True
        health["warning"] = "SIMULATED FEED - not the exchange"
        return health
