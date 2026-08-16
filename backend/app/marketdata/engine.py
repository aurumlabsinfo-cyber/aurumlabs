"""Market data engine: adapters -> local order book -> canonical stream.

This is the single source of truth for "what the market looks like right now".
Everything downstream reads from here or from the event bus it publishes to.

It never fabricates a value: if the book is not synced, or the feed has gone
quiet, that is reported as-is and the signal engine stands down.
"""

from __future__ import annotations

import asyncio
from collections import deque
from dataclasses import dataclass, field
from typing import Any

from app.config import Settings
from app.core.bus import EventBus, Topic
from app.core.clock import now_ms
from app.core.logging_conf import get_logger
from app.marketdata.base import Capability, ExchangeAdapter
from app.marketdata.orderbook import LocalOrderBook
from app.marketdata.registry import build_adapter
from app.marketdata.types import (
    BookTicker,
    DataSource,
    DepthUpdate,
    DerivativesTick,
    Liquidation,
    MarketTick,
    Trade,
)

log = get_logger(__name__)


@dataclass
class LatencyStats:
    samples: deque = field(default_factory=lambda: deque(maxlen=500))

    def add(self, value_ms: float) -> None:
        self.samples.append(value_ms)

    def summary(self) -> dict[str, float | None]:
        if not self.samples:
            return {"last": None, "avg": None, "p50": None, "p95": None, "max": None}
        s = sorted(self.samples)
        n = len(s)
        return {
            "last": self.samples[-1],
            "avg": sum(s) / n,
            "p50": s[n // 2],
            "p95": s[min(n - 1, int(n * 0.95))],
            "max": s[-1],
        }


class MarketDataEngine:
    def __init__(self, settings: Settings, bus: EventBus) -> None:
        self.settings = settings
        self.bus = bus
        self.symbol = settings.symbol
        self.started_at: int | None = None

        names = settings.exchange_list
        if not names:
            raise ValueError("EXCHANGES must list at least one venue")
        self.adapters: dict[str, ExchangeAdapter] = {
            n: build_adapter(n, settings) for n in names
        }
        self.primary_name = names[0]
        self.primary = self.adapters[self.primary_name]
        self.book = LocalOrderBook(self.primary_name, self.symbol)

        self.last_ticker: BookTicker | None = None
        self.last_trade: Trade | None = None
        self.last_tick: MarketTick | None = None
        self.derivatives: DerivativesTick | None = None
        self.recent_trades: deque[Trade] = deque(maxlen=2000)
        self.recent_ticks: deque[MarketTick] = deque(maxlen=2000)
        self.recent_liquidations: deque[Liquidation] = deque(maxlen=200)

        self.tick_latency = LatencyStats()
        self.trade_latency = LatencyStats()
        self.errors: deque[dict] = deque(maxlen=100)
        self.counters: dict[str, int] = {
            "tickers": 0, "trades": 0, "depth_updates": 0, "snapshots": 0,
        }
        self._tasks: list[asyncio.Task] = []
        self._resync_event = asyncio.Event()
        self._running = False

    # ------------------------------------------------------------- lifecycle
    async def start(self) -> None:
        self._running = True
        self.started_at = now_ms()
        for name, adapter in self.adapters.items():
            self._tasks.append(
                asyncio.create_task(adapter.run(self._on_message), name=f"feed:{name}")
            )
        if Capability.DEPTH_DIFF in self.primary.capabilities:
            self._resync_event.set()
            self._tasks.append(
                asyncio.create_task(self._book_sync_loop(), name="book-sync")
            )
        self._tasks.append(asyncio.create_task(self._clock_loop(), name="clock-skew"))
        log.info(
            "market_engine.started",
            primary=self.primary_name,
            adapters=list(self.adapters),
            symbol=self.symbol,
            synthetic=self.is_synthetic,
        )

    async def stop(self) -> None:
        self._running = False
        for adapter in self.adapters.values():
            adapter.stop()
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        for adapter in self.adapters.values():
            await adapter.close()
        self._tasks.clear()

    @property
    def is_synthetic(self) -> bool:
        return self.primary.synthetic

    @property
    def source(self) -> DataSource:
        return DataSource.SYNTHETIC if self.is_synthetic else DataSource.LIVE

    # -------------------------------------------------------------- ingestion
    def _on_message(self, msg: Any) -> None:
        try:
            if isinstance(msg, BookTicker):
                self._on_ticker(msg)
            elif isinstance(msg, Trade):
                self._on_trade(msg)
            elif isinstance(msg, DepthUpdate):
                self._on_depth(msg)
            elif isinstance(msg, DerivativesTick):
                self._on_derivatives(msg)
            elif isinstance(msg, Liquidation):
                self.recent_liquidations.append(msg)
                self.bus.publish(Topic.EVENT, {"type": "liquidation", "data": msg})
        except Exception as exc:  # noqa: BLE001 - ingestion must never die
            self.record_error("ingest", f"{type(exc).__name__}: {exc}")

    def _on_ticker(self, bt: BookTicker) -> None:
        if bt.exchange != self.primary_name:
            return  # auxiliary venues do not drive the primary price
        self.counters["tickers"] += 1
        self.last_ticker = bt
        self.tick_latency.add(bt.latency_ms)
        tick = MarketTick(
            exchange=bt.exchange,
            symbol=bt.symbol,
            ts=bt.server_ts,
            exchange_ts=bt.exchange_ts,
            bid_price=bt.bid_price,
            bid_qty=bt.bid_qty,
            ask_price=bt.ask_price,
            ask_qty=bt.ask_qty,
            mid=bt.mid,
            micro_price=bt.micro_price,
            spread=bt.spread,
            spread_bps=bt.spread_bps,
            last_price=self.last_trade.price if self.last_trade else None,
            latency_ms=bt.latency_ms,
            book_synced=self.book.synced,
            source=bt.source,
        )
        self.last_tick = tick
        self.recent_ticks.append(tick)
        self.bus.publish(Topic.TICK, tick)

    def _on_trade(self, t: Trade) -> None:
        if t.exchange != self.primary_name:
            return
        self.counters["trades"] += 1
        self.last_trade = t
        self.trade_latency.add(t.latency_ms)
        self.recent_trades.append(t)
        self.bus.publish(Topic.TRADE, t)

    def _on_depth(self, upd: DepthUpdate) -> None:
        if upd.exchange != self.primary_name:
            return
        self.counters["depth_updates"] += 1
        ok = self.book.apply(upd)
        # The raw diff is published so book research can replay the exact
        # sequence the engine saw, including the ones that failed validation.
        self.bus.publish(Topic.DEPTH, (upd, ok))
        if not ok:
            self._resync_event.set()
        elif self.book.synced:
            self.bus.publish(Topic.BOOK, self.book)

    def _on_derivatives(self, d: DerivativesTick) -> None:
        prev = self.derivatives
        if prev is not None:
            # Merge: mark-price and open-interest arrive on different cadences.
            d.mark_price = d.mark_price if d.mark_price is not None else prev.mark_price
            d.index_price = d.index_price if d.index_price is not None else prev.index_price
            d.funding_rate = (
                d.funding_rate if d.funding_rate is not None else prev.funding_rate
            )
            d.open_interest = (
                d.open_interest if d.open_interest is not None else prev.open_interest
            )
        self.derivatives = d

    # ------------------------------------------------------------ book sync
    async def _book_sync_loop(self) -> None:
        """Fetch snapshots whenever the book needs (re)synchronising."""
        backoff = 0.5
        while self._running:
            await self._resync_event.wait()
            if not self._running:
                return
            try:
                # Give the stream a moment so buffered diffs overlap the snapshot.
                await asyncio.sleep(0.25)
                snap = await self.primary.fetch_depth_snapshot(
                    self.settings.orderbook_depth_limit
                )
                self.counters["snapshots"] += 1
                synced = self.book.apply_snapshot(snap)
                if synced:
                    self._resync_event.clear()
                    backoff = 0.5
                    log.info(
                        "orderbook.synced",
                        last_update_id=self.book.last_update_id,
                        bids=len(self.book.bids),
                        asks=len(self.book.asks),
                        resyncs=self.book.stats.resyncs,
                    )
                    self.bus.publish(
                        Topic.EVENT,
                        {"type": "orderbook_synced", "resyncs": self.book.stats.resyncs},
                    )
                else:
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, 10.0)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                self.record_error("book_sync", f"{type(exc).__name__}: {exc}")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 10.0)

    async def _clock_loop(self) -> None:
        while self._running:
            for adapter in self.adapters.values():
                try:
                    await adapter.measure_clock_skew()
                except Exception as exc:  # noqa: BLE001
                    log.debug("clock_skew.failed", adapter=adapter.name, error=str(exc))
            await asyncio.sleep(60)

    # ---------------------------------------------------------------- health
    def record_error(self, component: str, message: str) -> None:
        entry = {"ts": now_ms(), "component": component, "message": message}
        self.errors.append(entry)
        log.warning("engine.error", **entry)
        self.bus.publish(Topic.EVENT, {"type": "error", **entry})

    @property
    def feed_age_ms(self) -> float | None:
        if self.last_tick is None:
            return None
        return now_ms() - self.last_tick.ts

    @property
    def uptime_s(self) -> float:
        return (now_ms() - self.started_at) / 1000.0 if self.started_at else 0.0

    def data_quality(self) -> dict[str, Any]:
        """Explainable 0..1 quality score. Any hard failure forces 0."""
        s = self.settings
        reasons: list[str] = []
        score = 1.0

        if self.last_tick is None:
            return {
                "score": 0.0,
                "ok": False,
                "reasons": ["no market data received yet"],
                "warmup_complete": False,
            }

        age = self.feed_age_ms or 0.0
        if age > s.max_feed_gap_ms:
            reasons.append(f"feed stale ({age:.0f}ms)")
            score = 0.0
        elif age > s.max_feed_gap_ms / 2:
            reasons.append("feed slow")
            score -= 0.2

        if Capability.DEPTH_DIFF in self.primary.capabilities and not self.book.synced:
            reasons.append(f"order book not synced: {self.book.desync_reason}")
            score = 0.0

        lat = self.tick_latency.summary()["p95"] or 0.0
        if lat > s.max_latency_ms:
            reasons.append(f"latency p95 {lat:.0f}ms > {s.max_latency_ms:.0f}ms")
            score = 0.0
        elif lat > s.max_latency_ms / 2:
            score -= 0.15

        spread_bps = self.last_tick.spread_bps
        if spread_bps > s.max_spread_bps:
            reasons.append(f"spread {spread_bps:.2f}bps > {s.max_spread_bps:.2f}bps")
            score -= 0.3
        if spread_bps <= 0:
            reasons.append("non-positive spread")
            score = 0.0

        warm = self.uptime_s >= s.min_warmup_seconds
        if not warm:
            reasons.append(
                f"warming up ({self.uptime_s:.0f}s / {s.min_warmup_seconds:.0f}s)"
            )

        score = max(0.0, min(1.0, score))
        return {
            "score": round(score, 3),
            "ok": score >= s.min_data_quality and warm,
            "reasons": reasons,
            "warmup_complete": warm,
        }

    def health(self) -> dict[str, Any]:
        return {
            "symbol": self.symbol,
            "primary_exchange": self.primary_name,
            "source": self.source.value,
            "is_synthetic": self.is_synthetic,
            "uptime_s": round(self.uptime_s, 1),
            "counters": dict(self.counters),
            "feed_age_ms": self.feed_age_ms,
            "tick_latency_ms": self.tick_latency.summary(),
            "trade_latency_ms": self.trade_latency.summary(),
            "orderbook": {
                "synced": self.book.synced,
                "desync_reason": self.book.desync_reason,
                "last_update_id": self.book.last_update_id,
                "bid_levels": len(self.book.bids),
                "ask_levels": len(self.book.asks),
                "applied_updates": self.book.stats.applied_updates,
                "gaps_detected": self.book.stats.gaps_detected,
                "resyncs": self.book.stats.resyncs,
                "last_gap_ts": self.book.stats.last_gap_ts,
            },
            "adapters": {
                name: {
                    "connected": a.state.connected,
                    "connected_since": a.state.connected_since,
                    "messages": a.state.messages,
                    "reconnects": a.state.reconnects,
                    "last_message_ts": a.state.last_message_ts,
                    "last_error": a.state.last_error,
                    "clock_skew_ms": round(a.state.clock_skew_ms, 1),
                    "streams": a.state.streams,
                    "synthetic": a.synthetic,
                }
                for name, a in self.adapters.items()
            },
            "data_quality": self.data_quality(),
            "errors_recent": list(self.errors)[-10:],
        }

    def market_snapshot(self) -> dict[str, Any]:
        t = self.last_tick
        d = self.derivatives
        return {
            "symbol": self.symbol,
            "exchange": self.primary_name,
            "source": self.source.value,
            "is_synthetic": self.is_synthetic,
            "ts": t.ts if t else None,
            "exchange_ts": t.exchange_ts if t else None,
            "latency_ms": t.latency_ms if t else None,
            "price": t.mid if t else None,
            "last_price": self.last_trade.price if self.last_trade else None,
            "bid": t.bid_price if t else None,
            "bid_qty": t.bid_qty if t else None,
            "ask": t.ask_price if t else None,
            "ask_qty": t.ask_qty if t else None,
            "spread": t.spread if t else None,
            "spread_bps": t.spread_bps if t else None,
            "micro_price": t.micro_price if t else None,
            "book_synced": self.book.synced,
            "derivatives": (
                {
                    "mark_price": d.mark_price,
                    "index_price": d.index_price,
                    "funding_rate": d.funding_rate,
                    "next_funding_ts": d.next_funding_ts,
                    "open_interest": d.open_interest,
                    "ts": d.server_ts,
                }
                if d
                else None
            ),
            "liquidations_recent": [
                {
                    "side": lvl.side.value,
                    "price": lvl.price,
                    "quantity": lvl.quantity,
                    "ts": lvl.exchange_ts,
                }
                for lvl in list(self.recent_liquidations)[-10:]
            ],
        }
