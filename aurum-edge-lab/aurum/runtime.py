"""Runtime: the object that owns every component and the loops between them.

Construction order matters and is the dependency order: storage, then market
data, then features, then research, then execution.  Nothing reaches backwards.

Three loops run on top of the market-data engine's own:

``_decision_loop``   consumes feature snapshots, asks the champion and the
                     shadows whether they fire, and routes what fires through
                     risk into the broker.  Every decision is recorded, whether
                     it traded or not — that is what ``/diagnostics`` reads.
``_position_loop``   marks open positions to market and closes the ones whose
                     exit condition has come up.
``_cycle_loop``      watches for wallet failure and runs the post-mortem-then-
                     maybe-reset ritual.

The separation between the champion's path and the shadows' path is the whole
safety story: a shadow strategy's signal is measured and stored and never
reaches the wallet, because the code that would let it simply is not there.
"""

from __future__ import annotations

import asyncio
import uuid
from typing import Any

from .adapters import build_feed
from .bus import TOPIC_FEATURES, TOPIC_SIGNAL, TOPIC_TRADE_CLOSED, TOPIC_WALLET, EventBus
from .clock import Clock
from .config import Config
from .cross_market.engine import CrossMarketEngine
from .diagnostics.collector import DiagnosticsCollector
from .domain import (
    ExitReason,
    FeatureSnapshot,
    RejectionReason,
    Signal,
    now_ms,
)
from .execution.cost_model import CostModel
from .execution.paper_broker import PaperBroker
from .features.engine import FeatureEngine
from .logging_setup import get_logger
from .market.data_engine import DataEngine
from .research.director import ResearchDirector
from .risk.manager import RiskManager
from .storage.repositories import Repositories
from .storage.sqlite_db import open_database
from .strategies.lifecycle import ShadowRecord, Strategy
from .wallet.postmortem import CycleManager
from .wallet.virtual_wallet import VirtualWallet

log = get_logger("runtime")


class AurumRuntime:
    def __init__(self, config: Config) -> None:
        self.config = config
        self.started_ms = 0
        self.running = False
        self._tasks: list[asyncio.Task[None]] = []

        config.data_dir.mkdir(parents=True, exist_ok=True)

        # --- storage -------------------------------------------------------
        self.db = open_database(
            config.db_url,
            batch_size=config.storage.batch_size,
            flush_interval_ms=config.storage.flush_interval_ms,
            queue_size=config.storage.queue_size,
        )
        self.db.connect()
        self.db.create_schema()
        self.repos = Repositories(self.db)

        # --- market data ---------------------------------------------------
        self.bus = EventBus(default_capacity=config.market.queue_size)
        self.clock = Clock()
        self.feed = build_feed(config)
        self.data = DataEngine(config, self.feed, self.bus, self.repos, self.clock)

        # --- derived state ---------------------------------------------------
        self.costs = CostModel(config.costs)
        self.features = FeatureEngine(config, self.data, self.bus, self.repos)
        self.cross_market = CrossMarketEngine(config, self.features, self.costs)

        # --- research --------------------------------------------------------
        self.director = ResearchDirector(config, self.features, self.cross_market, self.costs, self.repos)
        self.lifecycle = self.director.lifecycle

        # --- money -----------------------------------------------------------
        self.wallet = VirtualWallet(
            self.repos.wallet,
            starting_balance=config.wallet.starting_balance_eur,
            currency=config.wallet.currency,
        )
        self.risk = RiskManager(config, self.wallet, self.costs)
        self.broker = PaperBroker(config, self.wallet, self.costs, self.repos.execution)
        self.cycles = CycleManager(config, self.wallet, self.lifecycle, self.repos)

        # --- observability ----------------------------------------------------
        self.diagnostics = DiagnosticsCollector(window_s=config.diagnostics.rejection_window_s)
        self.errors: list[dict[str, Any]] = []
        self._features_channel = self.bus.subscribe(TOPIC_FEATURES, "runtime-decisions", capacity=4000)
        self._strategy_returns: dict[str, list[float]] = {}

    # ------------------------------------------------------------- lifecycle

    async def start(self) -> None:
        if self.running:
            return
        self.running = True
        self.started_ms = now_ms()
        self.repos.system.log(
            "runtime", "start", f"{self.config.app.name} {self.config.app.version} starting",
            detail={"feed": self.config.market.feed, "symbols": len(self.config.market.symbols)},
        )

        self.cycles.start()
        restored = self.broker.restore(self.repos.execution.restore_open_positions())
        if restored:
            log.info("open positions restored", extra={"count": restored})

        await self.data.start()
        await self.features.start()
        await self.cross_market.start()
        await self.director.start()

        self._tasks = [
            asyncio.create_task(self._decision_loop(), name="decisions"),
            asyncio.create_task(self._position_loop(), name="positions"),
            asyncio.create_task(self._cycle_loop(), name="cycles"),
            asyncio.create_task(self._maintenance_loop(), name="maintenance"),
        ]
        log.info(
            "runtime started",
            extra={"feed": self.feed.kind, "symbols": len(self.data.symbols), "cycle": self.cycles.state_name()},
        )

    async def stop(self) -> None:
        if not self.running:
            return
        self.running = False
        # Close open positions before shutting the feed: a position left open
        # across a restart is recoverable, but closing it at a known price is
        # cleaner and the trade record is what research reads.
        books = {s: self.data.book_view(s) for s in self.data.symbols}
        closed = self.broker.close_all(books, ExitReason.SHUTDOWN)
        if closed:
            log.info("positions closed on shutdown", extra={"count": len(closed)})

        for task in self._tasks:
            task.cancel()
        for task in self._tasks:
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass
        self._tasks.clear()

        await self.director.stop()
        await self.cross_market.stop()
        await self.features.stop()
        await self.data.stop()

        self.cycles.sync()
        self.wallet.snapshot()
        self.repos.system.log("runtime", "stop", "runtime stopped")
        self.db.flush(timeout=5.0)
        self.db.close()

    # -------------------------------------------------------------- decisions

    async def _decision_loop(self) -> None:
        while self.running:
            batch = await self._features_channel.get_batch(64, timeout=0.5)
            for snapshot in batch:
                try:
                    self.evaluate(snapshot)
                except Exception as exc:
                    self._record_error("decision", exc)

    def evaluate(self, snapshot: FeatureSnapshot) -> list[Signal]:
        """Ask every eligible strategy whether it fires on this snapshot."""
        signals: list[Signal] = []
        champion = self.lifecycle.champion()

        for strategy in self.lifecycle.shadows():
            signal = self._shadow_signal(strategy, snapshot)
            if signal is not None:
                signals.append(signal)

        if champion is None:
            # Recorded once per snapshot on one symbol only, so the counter
            # reflects "we had nothing to trade", not "we had ten symbols".
            if snapshot.symbol == self.data.symbols[0]:
                self.diagnostics.record_rejection(RejectionReason.NO_CHAMPION, at_ms=snapshot.ts_ms)
            return signals

        signal = self._champion_signal(champion, snapshot)
        if signal is not None:
            signals.append(signal)
        return signals

    def _fires(self, strategy: Strategy, snapshot: FeatureSnapshot) -> bool:
        hypothesis = strategy.hypothesis
        if snapshot.symbol != hypothesis.signal_symbol:
            return False
        if hypothesis.regime_filter is not None and snapshot.regime is not hypothesis.regime_filter:
            return False
        return hypothesis.matches(snapshot.values)

    def _champion_signal(self, champion: Strategy, snapshot: FeatureSnapshot) -> Signal | None:
        hypothesis = champion.hypothesis
        if snapshot.symbol != hypothesis.signal_symbol:
            return None

        if hypothesis.regime_filter is not None and snapshot.regime is not hypothesis.regime_filter:
            self.diagnostics.record_rejection(RejectionReason.REGIME_FILTER, at_ms=snapshot.ts_ms)
            return None
        if not hypothesis.matches(snapshot.values):
            self.diagnostics.record_rejection(RejectionReason.CONDITIONS_NOT_MET, at_ms=snapshot.ts_ms)
            return None

        symbol = hypothesis.execution_symbol
        book = self.data.book_view(symbol)
        exec_snapshot = self.features.snapshot(symbol) or snapshot
        expected_edge = champion.validated_metrics.net_edge_bps + (
            champion.validated_metrics.cost_bps or self.costs.round_trip_bps(exec_snapshot.spread_bps or 1.0)
        )

        signal = Signal(
            signal_id=f"sig-{uuid.uuid4().hex[:12]}",
            ts_ms=snapshot.ts_ms,
            strategy_id=champion.strategy_id,
            hypothesis_id=hypothesis.hypothesis_id,
            symbol=symbol,
            direction=hypothesis.direction,
            confidence=min(1.0, max(0.0, champion.validated_metrics.win_rate or 0.5)),
            expected_edge_bps=expected_edge,
            expected_cost_bps=self.costs.round_trip_bps(exec_snapshot.spread_bps or 1.0),
            horizon_ms=hypothesis.horizon_ms,
            regime=snapshot.regime,
            features=dict(snapshot.values),
        )

        quality_ok, quality_detail = self.data.is_tradable(symbol)
        decision = self.risk.evaluate(
            symbol=symbol,
            direction=hypothesis.direction,
            expected_edge_bps=expected_edge,
            horizon_ms=hypothesis.horizon_ms,
            snapshot=exec_snapshot,
            book=book,
            open_positions=self.broker.open_positions(),
            cycle_active=self.cycles.active,
            quality_ok=quality_ok,
            quality_detail=quality_detail,
            usdt_per_eur=self.config.fx.usdt_per_eur,
            at_ms=snapshot.ts_ms,
        )

        if not decision.allowed or decision.plan is None or book is None:
            signal.accepted = False
            signal.rejection = decision.reason
            signal.rejection_detail = decision.detail
            self.diagnostics.record_rejection(
                decision.reason or RejectionReason.DATA_QUALITY, at_ms=snapshot.ts_ms
            )
            self._publish_signal(signal)
            return signal

        position = self.broker.open(
            signal, decision.plan, book, exec_snapshot,
            cycle_id=self.cycles.cycle.cycle_id if self.cycles.cycle else 0,
            strategy_version=champion.version,
            at_ms=snapshot.ts_ms,
        )
        if position is None:
            signal.accepted = False
            signal.rejection = RejectionReason.INSUFFICIENT_LIQUIDITY
            signal.rejection_detail = "the book could not fill the intended size"
            self.diagnostics.record_rejection(RejectionReason.INSUFFICIENT_LIQUIDITY, at_ms=snapshot.ts_ms)
        else:
            signal.accepted = True
            self.risk.record_signal(symbol, hypothesis.direction, at_ms=snapshot.ts_ms)
            self.diagnostics.record_acceptance(at_ms=snapshot.ts_ms)
        self._publish_signal(signal)
        return signal

    def _shadow_signal(self, strategy: Strategy, snapshot: FeatureSnapshot) -> Signal | None:
        """A shadow strategy's signal is recorded and measured, never traded."""
        if not self._fires(strategy, snapshot):
            return None
        symbol = strategy.hypothesis.execution_symbol
        book = self.data.book_view(symbol)
        if book is None or book.mid is None:
            return None

        spread_bps = book.spread_bps() or 1.0
        cost = self.costs.round_trip_bps(spread_bps)
        self.director.record_shadow_signal(
            ShadowRecord(
                strategy_id=strategy.strategy_id,
                symbol=symbol,
                direction=strategy.hypothesis.direction,
                ts_ms=snapshot.ts_ms,
                entry_price=book.mid,
                horizon_ms=strategy.hypothesis.horizon_ms,
                cost_bps=cost,
            )
        )
        signal = Signal(
            signal_id=f"sig-{uuid.uuid4().hex[:12]}",
            ts_ms=snapshot.ts_ms,
            strategy_id=strategy.strategy_id,
            hypothesis_id=strategy.hypothesis.hypothesis_id,
            symbol=symbol,
            direction=strategy.hypothesis.direction,
            confidence=min(1.0, max(0.0, strategy.validated_metrics.win_rate or 0.5)),
            expected_edge_bps=strategy.validated_metrics.net_edge_bps + cost,
            expected_cost_bps=cost,
            horizon_ms=strategy.hypothesis.horizon_ms,
            regime=snapshot.regime,
            features=dict(snapshot.values),
            accepted=False,
            shadow=True,
            rejection=RejectionReason.SHADOW_ONLY,
            rejection_detail="shadow evaluation: measured live, never traded",
        )
        self.diagnostics.record_shadow()
        self._publish_signal(signal)
        return signal

    def _publish_signal(self, signal: Signal) -> None:
        cycle_id = self.cycles.cycle.cycle_id if self.cycles.cycle else 0
        self.repos.execution.record_signal(signal, cycle_id)
        self.bus.publish(TOPIC_SIGNAL, signal)

    # -------------------------------------------------------------- positions

    async def _position_loop(self) -> None:
        interval = max(0.1, self.config.features.cadence_ms / 1000.0)
        while self.running:
            await asyncio.sleep(interval)
            try:
                self._manage_positions()
            except Exception as exc:
                self._record_error("positions", exc)

    def _manage_positions(self) -> None:
        books = {symbol: self.data.book_view(symbol) for symbol in self.data.symbols}
        self.broker.mark_to_market(books)

        mids = {s: b.mid for s, b in books.items() if b and b.mid}
        self.director.resolve_shadows(mids)

        for symbol, position in list(self.broker.open_positions().items()):
            book = books.get(symbol)
            if book is None:
                continue
            quality_ok, quality_detail = self.data.is_tradable(symbol)
            check = self.broker.check_exit(
                position, book, quality_ok=quality_ok, quality_detail=quality_detail
            )
            if check.should_exit and check.reason is not None:
                trade = self.broker.close(position, book, check.reason)
                self.bus.publish(TOPIC_TRADE_CLOSED, trade)
                returns = self._strategy_returns.setdefault(trade.strategy_id, [])
                returns.append(trade.net_return_bps)
                strategy = self.lifecycle.strategies.get(trade.strategy_id)
                if strategy is not None:
                    net_pnl = strategy.live_net_pnl_eur + trade.net_pnl_eur
                    self.director.update_live_metrics(trade.strategy_id, returns, net_pnl)

        self.bus.publish(TOPIC_WALLET, self.wallet.to_dict())

    # ----------------------------------------------------------------- cycles

    async def _cycle_loop(self) -> None:
        while self.running:
            await asyncio.sleep(2.0)
            try:
                self._manage_cycle()
            except Exception as exc:
                self._record_error("cycle", exc)

    def _manage_cycle(self) -> None:
        reason = self.cycles.check_failure()
        if reason:
            # 1. Block entries first, before anything slow runs.
            self.risk.block_entries(f"cycle failure: {reason}")
            books = {s: self.data.book_view(s) for s in self.data.symbols}
            self.broker.close_all(books, ExitReason.CYCLE_END)
            cycle_id = self.cycles.cycle.cycle_id if self.cycles.cycle else 0
            trades = [t for t in self.broker.closed if t.cycle_id == cycle_id]
            self.cycles.fail_cycle(reason, trades)
            return

        if self.cycles.cycle and self.cycles.cycle.state.value == "AWAITING_EDGE":
            new_cycle = self.cycles.try_reset()
            if new_cycle is not None:
                self.risk.unblock_entries()
                self._strategy_returns.clear()
        elif self.cycles.active:
            self.cycles.sync()
            self.wallet.snapshot()

    # ------------------------------------------------------------ maintenance

    async def _maintenance_loop(self) -> None:
        interval = 300.0
        while self.running:
            await asyncio.sleep(interval)
            try:
                removed = self.db.prune(self.config.storage.retention.model_dump())
                if any(removed.values()):
                    log.info("retention pruned", extra=removed)
            except Exception as exc:
                self._record_error("maintenance", exc)

    # ------------------------------------------------------------------ state

    def _record_error(self, component: str, exc: Exception) -> None:
        message = f"{type(exc).__name__}: {exc}"
        log.exception("runtime error", extra={"component": component})
        self.errors.append({"ts_ms": now_ms(), "component": component, "error": message})
        if len(self.errors) > 100:
            del self.errors[:50]
        self.repos.system.log(component, "error", message, level="ERROR")

    def warmed_up(self) -> bool:
        return self.data.warmed_up() and self.features.warmed_up()

    def health(self) -> dict[str, Any]:
        """Everything /health must include, in one place."""
        champion = self.lifecycle.champion()
        uptime = (now_ms() - self.started_ms) / 1000.0 if self.started_ms else 0.0
        return {
            "status": self._status(),
            "name": self.config.app.name,
            "version": self.config.app.version,
            "env": self.config.app.env,
            "uptime_s": round(uptime, 1),
            "started_ms": self.started_ms,
            "warmed_up": self.warmed_up(),
            "market_feed": self.data.health(),
            "database": {**self.db.stats(), "tables": self.db.table_counts()},
            "connected_symbols": self.data.connected_symbols(),
            "research": {
                "enabled": self.config.research.enabled,
                "running": self.director.running,
                "cycles_run": self.director.cycles_run,
                "hypotheses": len(self.director.hypotheses),
                "strategies": self.lifecycle.counts(),
                "has_champion": champion is not None,
                "no_edge_reason": self.director.no_edge_reason,
            },
            "paper_broker": self.broker.stats(),
            "positions": [p.to_dict() for p in self.broker.open_positions().values()],
            "wallet": self.wallet.to_dict(),
            "cycle": self.cycles.to_dict(),
            "data_quality": self.data.gate.summary(self.data.qualities),
            "risk": self.risk.circuit_breaker_state(),
            "costs": self.costs.to_dict(),
            "fx": {"usdt_per_eur": self.config.fx.usdt_per_eur, "source": self.config.fx.source,
                   "note": "wallet is EUR; contracts are quoted in USDT"},
            "bus": self.bus.stats(),
            "features": self.features.stats(),
            "diagnostics": self.diagnostics.summary_sentence(),
            "errors": self.errors[-10:],
        }

    def _status(self) -> str:
        if not self.running:
            return "STOPPED"
        if self.errors and now_ms() - self.errors[-1]["ts_ms"] < 60_000:
            return "DEGRADED"
        if not self.data.connected_symbols():
            return "NO_FEED"
        if not self.warmed_up():
            return "WARMING_UP"
        if self.cycles.cycle and self.cycles.cycle.state.value == "AWAITING_EDGE":
            return "AWAITING_EDGE"
        if self.lifecycle.champion() is None:
            return "NO_VALIDATED_EDGE"
        return "OK"
