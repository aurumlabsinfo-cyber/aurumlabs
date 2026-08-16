"""Bus -> database bridge.

Subscribes to the canonical streams and queues rows on the batch writer. Runs
entirely off the hot path: if this task stalls, the bus drops its oldest
messages and the trading engine keeps running.
"""

from __future__ import annotations

import asyncio
from typing import Any, Callable

from app.config import Settings
from app.core.bus import EventBus, Topic
from app.core.clock import now_ms
from app.core.logging_conf import get_logger
from app.db import repository as repo
from app.db.repository import BatchWriter
from app.marketdata.engine import MarketDataEngine
from app.marketdata.types import MarketTick, Trade

log = get_logger(__name__)


class PersistenceService:
    def __init__(
        self,
        settings: Settings,
        bus: EventBus,
        writer: BatchWriter,
        market: MarketDataEngine,
        counters_provider: Callable[[], dict[str, int]] | None = None,
    ) -> None:
        self.settings = settings
        self.bus = bus
        self.writer = writer
        self.market = market
        #: Returns the signal engine's counters (NO TRADE count and friends).
        self.counters_provider = counters_provider
        self._tasks: list[asyncio.Task] = []
        self._running = False
        self._last_snapshot_ts = 0

    async def start(self) -> None:
        self._running = True
        s = self.settings
        if s.persist_market_ticks:
            self._tasks.append(asyncio.create_task(self._ticks(), name="persist-ticks"))
        if s.persist_trades:
            self._tasks.append(
                asyncio.create_task(self._trades(), name="persist-trades")
            )
        if s.persist_features:
            self._tasks.append(
                asyncio.create_task(self._features(), name="persist-features")
            )
        if s.persist_book_updates:
            self._tasks.append(
                asyncio.create_task(self._book_updates(), name="persist-book-diffs")
            )
        if s.shadow_decisions_enabled:
            self._tasks.append(
                asyncio.create_task(self._shadow_decisions(), name="persist-shadow")
            )
        self._tasks.append(asyncio.create_task(self._books(), name="persist-books"))
        self._tasks.append(asyncio.create_task(self._events(), name="persist-events"))
        self._tasks.append(asyncio.create_task(self._metrics(), name="persist-metrics"))

    async def stop(self) -> None:
        self._running = False
        for t in self._tasks:
            t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()

    async def _ticks(self) -> None:
        sub = self.bus.subscribe(Topic.TICK, maxsize=2048)
        try:
            while self._running:
                tick: MarketTick = await sub.queue.get()
                self.writer.add(
                    repo.MarketTickRow,
                    {
                        "ts": tick.ts,
                        "exchange_ts": tick.exchange_ts,
                        "latency_ms": int(tick.latency_ms),
                        "exchange": tick.exchange,
                        "symbol": tick.symbol,
                        "bid_price": tick.bid_price,
                        "bid_qty": tick.bid_qty,
                        "ask_price": tick.ask_price,
                        "ask_qty": tick.ask_qty,
                        "mid": tick.mid,
                        "micro_price": tick.micro_price,
                        "spread": tick.spread,
                        "spread_bps": tick.spread_bps,
                        "last_price": tick.last_price,
                        "book_synced": tick.book_synced,
                        "source": tick.source.value,
                        "is_synthetic": tick.is_synthetic,
                    },
                )
        except asyncio.CancelledError:
            raise
        finally:
            sub.close()

    async def _trades(self) -> None:
        sub = self.bus.subscribe(Topic.TRADE, maxsize=4096)
        try:
            while self._running:
                t: Trade = await sub.queue.get()
                self.writer.add(
                    repo.TradeRow,
                    {
                        "ts": t.server_ts,
                        "exchange_ts": t.exchange_ts,
                        "latency_ms": int(t.latency_ms),
                        "exchange": t.exchange,
                        "symbol": t.symbol,
                        "trade_id": t.trade_id,
                        "price": t.price,
                        "quantity": t.quantity,
                        "notional": t.notional,
                        "is_buyer_maker": t.is_buyer_maker,
                        "aggressor": t.aggressor.value,
                        "source": t.source.value,
                        "is_synthetic": t.source.value == "SYNTHETIC",
                    },
                )
        except asyncio.CancelledError:
            raise
        finally:
            sub.close()

    async def _features(self) -> None:
        sub = self.bus.subscribe(Topic.FEATURES, maxsize=512)
        try:
            while self._running:
                fv: dict[str, Any] = await sub.queue.get()
                f = fv["features"]
                self.writer.add(
                    repo.FeatureRow,
                    {
                        "ts": fv["ts"],
                        "exchange": fv["exchange"],
                        "symbol": fv["symbol"],
                        "mid": f.get("mid") or 0.0,
                        "micro_price": f.get("micro_price") or 0.0,
                        "spread_bps": f.get("spread_bps") or 0.0,
                        "book_synced": bool(f.get("book_synced")),
                        "data_quality": f.get("data_quality") or 0.0,
                        "regime": fv.get("regime"),
                        "payload": _clean(f),
                        "source": fv["source"],
                        "is_synthetic": fv["is_synthetic"],
                    },
                )
        except asyncio.CancelledError:
            raise
        finally:
            sub.close()

    async def _shadow_decisions(self) -> None:
        """Persist the engine's lean on every evaluated window.

        These rows carry no outcome. The result is resolved offline from the
        tick table at `ts + horizon`, so a row written here can never contain
        information that was unavailable at `ts`.
        """
        sub = self.bus.subscribe(Topic.DECISION, maxsize=512)
        try:
            while self._running:
                d: dict[str, Any] = await sub.queue.get()
                self.writer.add(
                    repo.ShadowDecisionRow,
                    {
                        **d,
                        "blocked_by": d.get("blocked_by") or [],
                    },
                )
        except asyncio.CancelledError:
            raise
        finally:
            sub.close()

    async def _book_updates(self) -> None:
        """Persist raw depth diffs, including the ones that failed validation.

        Very high volume (10/s on BTCUSDT), so it is opt-in via
        PERSIST_BOOK_UPDATES. `applied`/`gap_detected` are recorded so book
        research can tell a clean sequence from a resync boundary.
        """
        sub = self.bus.subscribe(Topic.DEPTH, maxsize=4096)
        try:
            while self._running:
                upd, applied = await sub.queue.get()
                self.writer.add(
                    repo.OrderBookUpdateRow,
                    {
                        "ts": upd.server_ts,
                        "exchange_ts": upd.exchange_ts,
                        "exchange": upd.exchange,
                        "symbol": upd.symbol,
                        "first_update_id": upd.first_update_id,
                        "final_update_id": upd.final_update_id,
                        "prev_final_update_id": upd.prev_final_update_id,
                        "applied": bool(applied),
                        "gap_detected": not applied,
                        "bids": [[p, q] for p, q in upd.bids],
                        "asks": [[p, q] for p, q in upd.asks],
                        "source": upd.source.value,
                        "is_synthetic": upd.source.value == "SYNTHETIC",
                    },
                )
        except asyncio.CancelledError:
            raise
        finally:
            sub.close()

    async def _metrics(self) -> None:
        """Periodic snapshot of paper-trading performance.

        Live rows only: simulator trades describe the simulator, so folding
        them into a performance history would corrupt the record.
        """
        from app.signals import statistics as stats

        interval = self.settings.performance_metrics_interval_s
        while self._running:
            await asyncio.sleep(interval)
            try:
                trades = await repo.fetch_all_paper_trades(include_synthetic=False)
                if not trades:
                    continue
                overall = stats.summarise(
                    trades,
                    payout=self.settings.binary_payout,
                    stake=self.settings.paper_stake,
                    no_trade_count=self.counters_provider().get("no_trade")
                    if self.counters_provider else None,
                )
                rows = [
                    {
                        "ts": now_ms(),
                        "scope": "overall",
                        "symbol": self.settings.symbol,
                        "window": "all",
                        "metrics": _clean(overall),
                        "is_synthetic": False,
                    }
                ]
                for regime, bucket in stats.by_bucket(
                    trades, "market_regime", self.settings.binary_payout
                ).items():
                    rows.append(
                        {
                            "ts": now_ms(),
                            "scope": f"regime:{regime}",
                            "symbol": self.settings.symbol,
                            "window": "all",
                            "metrics": _clean(bucket),
                            "is_synthetic": False,
                        }
                    )
                self.writer.add_many(repo.PerformanceMetricRow, rows)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - metrics must not break I/O
                log.warning("persistence.metrics_failed", error=str(exc))

    async def _books(self) -> None:
        """Periodic full snapshots; diffs only when explicitly enabled."""
        while self._running:
            await asyncio.sleep(self.settings.book_snapshot_interval_s)
            book = self.market.book
            if not book.synced:
                continue
            snap = book.snapshot_dict(levels=50)
            self.writer.add(
                repo.OrderBookSnapshotRow,
                {
                    "ts": now_ms(),
                    "exchange": book.exchange,
                    "symbol": book.symbol,
                    "last_update_id": book.last_update_id,
                    "synced": book.synced,
                    "levels": len(snap["bids"]),
                    "bids": snap["bids"],
                    "asks": snap["asks"],
                    "source": self.market.source.value,
                    "is_synthetic": self.market.is_synthetic,
                },
            )

    async def _events(self) -> None:
        sub = self.bus.subscribe(Topic.EVENT, maxsize=512)
        try:
            while self._running:
                evt: dict[str, Any] = await sub.queue.get()
                kind = evt.get("type", "event")
                if kind == "error":
                    self.writer.add(
                        repo.ErrorRow,
                        {
                            "ts": evt.get("ts", now_ms()),
                            "component": evt.get("component", "unknown"),
                            "error_type": "runtime",
                            "message": str(evt.get("message", "")),
                            "context": None,
                        },
                    )
                elif kind in ("orderbook_synced", "liquidation"):
                    self.writer.add(
                        repo.SystemEventRow,
                        {
                            "ts": now_ms(),
                            "component": "market_data",
                            "event": kind,
                            "severity": "INFO",
                            "detail": _clean(
                                {k: v for k, v in evt.items()
                                 if isinstance(v, (int, float, str, bool, type(None)))}
                            ),
                        },
                    )
        except asyncio.CancelledError:
            raise
        finally:
            sub.close()


def _clean(payload: dict) -> dict:
    """Replace NaN/Inf with None so JSONB serialisation never fails."""
    import math

    out = {}
    for k, v in payload.items():
        if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
            out[k] = None
        else:
            out[k] = v
    return out
