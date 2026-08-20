"""The engine: three blocks, one loop.

    SCAN            market core -> snapshots -> ranked opportunities
    DECIDE          one decision core -> LONG / SHORT / NO TRADE (+ size, risk)
    EXECUTE+LEARN   orders, fills, exits, records, and the champion/challenger loop

Everything below is orchestration.  There are no agents talking to each other:
one loop reads the market, asks one model, and hands one intent to one executor.
"""

from __future__ import annotations

import asyncio
import json
import re
from collections import Counter
from dataclasses import dataclass
from typing import Any

import aiohttp

from .config import Config
from .decide.decision_core import Decision, DecisionCore
from .decide.model import CHAMPION_V1, Model, champion_v1
from .decide.risk import RiskEngine
from .execute.broker import Broker, PaperBroker
from .execute.bybit_broker import BybitBroker
from .execute.execution_core import ExecutionCore, ExitReason
from .execute.reconcile import Reconciler
from .health import HealthMonitor, SystemState
from .learn.pipeline import LearningPipeline
from .scan.bybit_rest import BybitRest
from .scan.market_core import MarketCore
from .scan.scanner import Scanner
from .scan.snapshot import FeedSource, MarketSnapshot
from .storage.db import Database
from .storage.repo import Repo, compute_stats
from .util.clock import Clock
from .util.logging_setup import get_logger

log = get_logger("engine")

CODE_VERSION = "1.0.0"


@dataclass
class PendingOutcome:
    """A decision waiting to find out what the market did next."""

    decision_id: int
    symbol: str
    side: str
    ref_mid: float
    due_ms: float
    cost_bps: float
    horizon_s: float


class Engine:
    def __init__(
        self,
        cfg: Config,
        clock: Clock | None = None,
        *,
        market: MarketCore | None = None,
        broker: Broker | None = None,
        rest: BybitRest | None = None,
        db: Database | None = None,
        session: aiohttp.ClientSession | None = None,
    ) -> None:
        self.cfg = cfg
        self.clock = clock or Clock()
        self.session = session

        self.db = db or Database(cfg.db_path)
        self.repo = Repo(self.db)
        self.rest = rest or BybitRest(cfg.bybit, session=session)
        self.market = market or MarketCore(cfg, self.clock, self.rest, session=session)
        self.scanner = Scanner()
        self.risk = RiskEngine(cfg)
        self.decision_core = DecisionCore(cfg, self.risk)
        self.broker: Broker = broker or (
            BybitBroker(cfg, self.clock, self.rest, session=session)
            if cfg.is_live
            else PaperBroker(cfg, self.clock)
        )
        self.health = HealthMonitor(
            cfg,
            self.clock,
            market_health=self.market.health,
            broker_health=self.broker.health,
            db_health=self._db_health,
            execution_health=lambda: self.execution.health(),
        )
        self.execution = ExecutionCore(
            cfg, self.clock, self.broker, self.repo, self.risk, gate=self.health.gate
        )
        self.reconciler = Reconciler(
            cfg, self.clock, self.execution, self.repo,
            rest=self.rest if cfg.is_live else None,
        )
        self.learning = LearningPipeline(cfg, self.repo)

        self.champion: Model = champion_v1()
        self.shadow: Model | None = None
        self.champion_state = "normal"          # normal | reduced | suspended
        self.running = False
        self.started_at = 0.0
        self.cycles = 0
        self.last_cycle_ms = 0.0
        self.cycle_ms_avg = 0.0
        self.decisions_logged = 0
        self.pending_outcomes: list[PendingOutcome] = []
        self.last_snapshots: dict[str, MarketSnapshot] = {}
        self.reject_window: Counter[str] = Counter()
        self.last_decisions: list[dict[str, Any]] = []
        self.last_reject_flush = 0.0
        self.last_equity_write = 0.0
        self.last_champion_review = 0.0
        self.last_decision_log: dict[str, float] = {}
        self.learning_status: dict[str, Any] = {"status": "idle", "detail": "not run yet"}
        self._tasks: list[asyncio.Task[None]] = []
        self._stop = asyncio.Event()

    # ---------------------------------------------------------------- health
    def _db_health(self) -> dict[str, Any]:
        return {
            "open": self.db._conn is not None,
            "path": self.db.path,
            "schema_version": self.db.schema_version() if self.db._conn else None,
            "run_id": self.db.run_id,
            "write_failures": self.db.write_failures,
            "last_error": self.db.last_write_error,
        }

    # ---------------------------------------------------------------- lifecycle
    async def start(self) -> None:
        self.db.open()
        self.health.on_transition = self._on_health_transition
        self._load_champion()
        self._load_shadow()
        self.db.start_run(
            mode=self.cfg.mode,
            feed_source=self.market.source.value,
            model_version=self.champion.version,
            config_json=json.dumps(self.cfg.to_dict(), default=str),
            code_version=CODE_VERSION,
        )
        log.info(
            "AURUM EDGE %s starting: mode=%s feed=%s db=%s run=%s champion=%s",
            CODE_VERSION, self.cfg.mode.upper(), self.market.source.value,
            self.db.path, self.db.run_id, self.champion.version,
        )
        self.health.set_state(SystemState.BOOTING, "connecting to Bybit")
        self.market.on_reconnect_hook = self._on_reconnect
        if isinstance(self.broker, BybitBroker):
            self.broker.on_reconnect_hook = self._on_reconnect

        await self.market.start()
        await self.broker.start()
        ready = await self.market.wait_ready(timeout=25.0)
        if not ready:
            log.warning("not every public connection reported live within 25s")

        self.health.set_state(SystemState.RECONCILING, "first reconciliation")
        await self.reconciler.reconcile("startup")
        self.health.set_state(SystemState.LIVE_READY, "ready")

        self.running = True
        self.started_at = self.clock.mono()
        self._tasks = [
            asyncio.create_task(self._trading_loop(), name="trading-loop"),
            asyncio.create_task(self._housekeeping_loop(), name="housekeeping"),
        ]
        if self.cfg.learn.enabled:
            self._tasks.append(asyncio.create_task(self._learning_loop(), name="learning"))

    async def stop(self, flatten: bool = True) -> None:
        self.running = False
        self._stop.set()
        if flatten and self.execution.positions:
            log.warning("closing %d open position(s) before shutdown", len(self.execution.positions))
            await self.execution.flatten_all(ExitReason.FLATTEN, self.last_snapshots)
            await asyncio.sleep(0.5)
        for task in self._tasks:
            task.cancel()
        for task in self._tasks:
            try:
                await task
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
                pass
        await self.market.stop()
        await self.broker.stop()
        await self.rest.close()
        self.db.end_run()
        self.db.close()

    def _on_health_transition(self, component: str, state: str, detail: str) -> None:
        self.repo.save_health_event(component, state, detail, self.clock.now_ms())

    async def _on_reconnect(self, name: str) -> None:
        """Reconnect: entries stop until state is proven again."""
        self.execution.block_entries(f"{name} reconnected, waiting for reconciliation")
        self.health.set_state(SystemState.RECONCILING, f"{name} reconnected")
        await asyncio.sleep(self.cfg.execute.post_reconnect_block_s)
        result = await self.reconciler.reconcile(f"reconnect:{name}")
        if result.ok:
            self.health.set_state(SystemState.LIVE_READY, "reconciled after reconnect")
        else:
            self.health.set_state(SystemState.BLOCKED, result.error)

    # ---------------------------------------------------------------- models
    def _load_champion(self) -> None:
        row = self.repo.champion()
        if row:
            try:
                self.champion = Model.from_row(row)
                log.info("champion loaded from database: %s", self.champion.version)
                return
            except ValueError as exc:
                log.error("stored champion unusable (%s) - falling back to %s", exc, CHAMPION_V1)
        self.champion = champion_v1()
        self.repo.save_model_version(self.champion.to_row("champion"))
        self.repo.log_model_event(self.champion.version, "seeded", {"origin": "hand-specified"})
        self.db.kv_set("champion_version", self.champion.version)

    def _load_shadow(self) -> None:
        rows = [r for r in self.repo.model_versions() if r["status"] == "shadow"]
        if not rows:
            self.shadow = None
            return
        try:
            self.shadow = Model.from_row(rows[0])
            log.info("shadow challenger active: %s", self.shadow.version)
        except ValueError as exc:
            log.warning("shadow challenger unusable: %s", exc)
            self.shadow = None

    # ---------------------------------------------------------------- main loop
    async def _trading_loop(self) -> None:
        interval = self.cfg.scan.scan_interval_s
        while not self._stop.is_set():
            started = self.clock.mono()
            try:
                await self.cycle()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - a bad cycle must not kill the engine
                log.exception("cycle failed: %s", exc)
            elapsed = self.clock.mono() - started
            self.last_cycle_ms = elapsed * 1000.0
            self.cycle_ms_avg = (
                self.last_cycle_ms if self.cycles <= 1
                else self.cycle_ms_avg * 0.9 + self.last_cycle_ms * 0.1
            )
            await asyncio.sleep(max(interval - elapsed, 0.0))

    async def cycle(self) -> None:
        """One full SCAN -> DECIDE -> EXECUTE pass."""
        self.cycles += 1
        now_ms = self.clock.now_ms()
        now_mono = self.clock.mono()

        snapshots = self.market.snapshots()
        self.last_snapshots = {s.symbol: s for s in snapshots}
        scan = self.scanner.scan(snapshots, now_ms)
        self.reject_window.update(scan.skipped)

        account = self.execution.account_state(self.last_snapshots)
        decisions: list[Decision] = []
        opened = 0

        for opportunity in scan.top(self.cfg.decide.decision_log_top_k):
            decision = self.decision_core.evaluate(
                opportunity, self.champion, account, now_mono
            )
            decisions.append(decision)
            decision_id = self._persist_decision(decision, force=decision.is_trade)
            if decision_id:
                self._schedule_outcome(decision, decision_id)
            if not decision.is_trade:
                self.reject_window[_reason_key(decision.reason)] += 1
                continue
            if opened >= 1:
                continue    # one entry per cycle: re-read the account before the next
            position = await self.execution.open_position(decision)
            if position is not None:
                opened += 1
                account = self.execution.account_state(self.last_snapshots)

            # shadow: what would the challenger have done with the same snapshot?
            if self.shadow is not None:
                shadow_decision = self.decision_core.evaluate(
                    opportunity, self.shadow, account, now_mono, shadow=True
                )
                shadow_id = self._persist_decision(shadow_decision, force=True)
                if shadow_id:
                    self._schedule_outcome(shadow_decision, shadow_id)

        self.last_decisions = [d.to_dict() for d in decisions]
        await self.execution.manage(self.last_snapshots, self.champion)
        self._resolve_outcomes(now_ms)

    # ---------------------------------------------------------------- records
    def _persist_decision(self, decision: Decision, force: bool = False) -> int | None:
        """Store a decision.  Trades always; NO TRADE throttled per symbol.

        Nothing is lost: what is not stored row-by-row is counted by reason in
        ``reject_counters`` and shown by ``diagnose``.  Set
        ``AURUM_LOG_ALL_NO_TRADE=1`` to store literally every evaluation.
        """
        now = self.clock.mono()
        if not force and not self.cfg.decide.log_all_no_trade:
            last = self.last_decision_log.get(decision.symbol, 0.0)
            if now - last < 1.0:
                return None
        self.last_decision_log[decision.symbol] = now

        snapshot_id = None
        if decision.is_trade or decision.shadow:
            snapshot_id = self.repo.save_snapshot(
                decision.symbol,
                decision.snapshot.ts_local_ms,
                decision.snapshot.source,
                decision.snapshot.to_dict(),
            )
        row = decision.to_row(snapshot_id)
        decision_id = self.repo.save_decision(row)
        if decision_id:
            self.decisions_logged += 1
            setattr(decision, "_db_id", decision_id)
        return decision_id

    def _schedule_outcome(self, decision: Decision, decision_id: int) -> None:
        horizon = max(min(decision.max_hold_s or 30.0, self.cfg.decide.max_hold_s), 5.0)
        cost_bps = (
            decision.expected_cost_eur / decision.notional_eur * 10_000.0
            if decision.notional_eur > 0
            else 2.0 * decision.snapshot.spread_bps + 2.0 * decision.est_slippage_bps
        )
        self.pending_outcomes.append(
            PendingOutcome(
                decision_id=decision_id,
                symbol=decision.symbol,
                side=decision.side,
                ref_mid=decision.snapshot.mid,
                due_ms=decision.ts_ms + horizon * 1000.0,
                cost_bps=cost_bps,
                horizon_s=horizon,
            )
        )

    def _resolve_outcomes(self, now_ms: float) -> None:
        """Label decisions with what the market actually did afterwards."""
        if not self.pending_outcomes:
            return
        still_pending: list[PendingOutcome] = []
        for pending in self.pending_outcomes:
            if now_ms < pending.due_ms:
                still_pending.append(pending)
                continue
            snap = self.last_snapshots.get(pending.symbol)
            if snap is None or snap.mid <= 0 or pending.ref_mid <= 0:
                continue      # symbol left the universe: drop it rather than guess
            sign = 1.0 if pending.side == "LONG" else -1.0
            move_bps = (snap.mid / pending.ref_mid - 1.0) * sign * 10_000.0
            label = 1 if move_bps > pending.cost_bps else 0
            self.repo.set_decision_outcome(
                pending.decision_id, move_bps, pending.cost_bps, label, now_ms, pending.horizon_s
            )
        self.pending_outcomes = still_pending

    # ---------------------------------------------------------------- background
    async def _housekeeping_loop(self) -> None:
        while not self._stop.is_set():
            await asyncio.sleep(1.0)
            try:
                now = self.clock.mono()
                if now - self.last_reject_flush >= 5.0:
                    self._flush_rejects()
                    self.last_reject_flush = now
                if now - self.last_equity_write >= 5.0:
                    self._write_equity()
                    self.last_equity_write = now
                if now - self.last_champion_review >= 30.0:
                    self._review_champion()
                    self.last_champion_review = now
                await self._periodic_reconcile(now)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                log.exception("housekeeping failed: %s", exc)

    async def _periodic_reconcile(self, now_mono: float) -> None:
        interval = self.cfg.execute.reconcile_interval_s
        if interval <= 0:
            return
        last = getattr(self, "_last_reconcile_mono", 0.0) or self.started_at
        if now_mono - last >= interval:
            self._last_reconcile_mono = now_mono
            await self.reconciler.reconcile("periodic")

    def _review_champion(self) -> None:
        """A Champion that stops paying trades smaller, then not at all.

        No new entries does not mean idle: the learning pipeline keeps running on
        the data already collected, which is how a better challenger is found.
        Normal size returns when a promotion or a rollback changes the champion.
        """
        cfg = self.cfg.learn
        trades = self.repo.recent_trades_for_model(
            self.champion.version, cfg.degrade_window_trades
        )
        if len(trades) < cfg.degrade_window_trades:
            return
        stats = compute_stats(trades)
        previous = self.champion_state

        if stats.expectancy_eur <= cfg.suspend_expectancy_eur:
            state = "suspended"
        elif stats.expectancy_eur <= cfg.degrade_expectancy_eur:
            state = "reduced"
        else:
            state = "normal"

        if state == previous:
            return
        self.champion_state = state
        detail = (
            f"expectancy {stats.expectancy_eur:+.3f} EUR over the last "
            f"{len(trades)} trades (WR {stats.win_rate:.0%}, "
            f"net {stats.net_pnl_eur:+.2f} EUR)"
        )
        if state == "suspended":
            self.risk.size_multiplier = cfg.degraded_size_multiplier
            self.execution.block_entries(f"champion underperforming: {detail}")
            log.warning("champion SUSPENDED: %s", detail)
        elif state == "reduced":
            self.risk.size_multiplier = cfg.degraded_size_multiplier
            if previous == "suspended":
                self.execution.allow_entries()
            log.warning("champion size reduced: %s", detail)
        else:
            self.risk.size_multiplier = 1.0
            if previous == "suspended":
                self.execution.allow_entries()
            log.info("champion back to full size: %s", detail)
        self.repo.log_model_event(
            self.champion.version, f"champion_{state}",
            {"detail": detail, "stats": stats.to_dict()},
        )

    def _flush_rejects(self) -> None:
        if not self.reject_window:
            return
        now_ms = self.clock.now_ms()
        for reason, count in self.reject_window.items():
            self.repo.bump_reject(reason, count, now_ms)
        self.reject_window.clear()

    def _write_equity(self) -> None:
        account = self.execution.account_state(self.last_snapshots)
        self.repo.save_equity(
            {
                "ts_ms": self.clock.now_ms(),
                "equity_eur": account.equity_eur,
                "free_eur": account.available_eur,
                "used_eur": account.used_margin_eur,
                "exposure_eur": account.exposure_eur,
                "open_positions": account.open_positions,
                "realized_eur": self.risk.realized_today_eur,
                "unrealized_eur": account.unrealized_eur,
            }
        )

    async def _learning_loop(self) -> None:
        while not self._stop.is_set():
            await asyncio.sleep(self.cfg.learn.interval_s)
            try:
                await self.run_learning()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                log.exception("learning run failed: %s", exc)
                self.learning_status = {"status": "error", "detail": str(exc)}

    async def run_learning(self) -> dict[str, Any]:
        """Train a challenger and, if the evidence is there, promote it."""
        report = await asyncio.to_thread(self.learning.run, self.champion)
        self.learning_status = report.to_dict()
        self._load_shadow()
        promoted, detail = await asyncio.to_thread(self.learning.maybe_promote, self.champion)
        if promoted is not None:
            self.champion = promoted
            self._load_shadow()
            # a fresh champion starts at full size with a clean slate
            self.champion_state = "normal"
            self.risk.size_multiplier = 1.0
            self.execution.allow_entries()
        self.learning_status["promotion"] = detail
        return self.learning_status

    # ---------------------------------------------------------------- controls
    def engage_kill_switch(self, reason: str) -> None:
        self.risk.engage_kill_switch(reason)
        self.execution.block_entries(f"kill switch: {reason}")
        self.health.set_state(SystemState.HALTED, f"kill switch: {reason}")

    async def release_kill_switch(self) -> None:
        self.risk.release_kill_switch()
        result = await self.reconciler.reconcile("kill-switch-release")
        self.health.set_state(
            SystemState.LIVE_READY if result.ok else SystemState.BLOCKED,
            "kill switch released" if result.ok else result.error,
        )

    async def flatten(self, reason: str = "manual") -> int:
        return await self.execution.flatten_all(ExitReason.FLATTEN, self.last_snapshots)

    # ---------------------------------------------------------------- state
    def state(self) -> dict[str, Any]:
        """The single payload the frontend renders.  Backend truth, nothing else."""
        account = self.execution.account_state(self.last_snapshots)
        stats = self.repo.stats()
        scan = self.scanner.last_result
        health = self.health.snapshot()
        no_trade = [d for d in self.last_decisions if d["action"] == "NO_TRADE"][:8]
        return {
            "ts_ms": self.clock.now_ms(),
            "version": CODE_VERSION,
            "mode": self.cfg.mode.upper(),
            "feed_source": self.market.source.value,
            "feed_is_real": self.market.source is FeedSource.BYBIT,
            "run_id": self.db.run_id,
            "db_path": self.db.path,
            "uptime_s": round(self.clock.mono() - self.started_at, 1) if self.started_at else 0.0,
            "cycles": self.cycles,
            "cycle_ms": round(self.cycle_ms_avg, 2),
            "health": health,
            "market": self.market.health(),
            "account": account.to_dict(),
            "risk": self.risk.to_dict(),
            "positions": self.execution.open_positions(self.last_snapshots),
            "opportunities": scan.to_dict() if scan else {"ranked": [], "considered": 0},
            "decisions": self.last_decisions,
            "no_trade_reasons": no_trade,
            "reject_summary": self.repo.reject_summary(),
            "stats": stats.to_dict(),
            "regimes": self.repo.regime_breakdown(),
            "trades": self.repo.trades(limit=25),
            "execution": self.execution.health(),
            "model": {
                "champion": self.champion.version,
                "champion_kind": self.champion.kind,
                "champion_metrics": self.champion.metrics,
                "shadow": self.shadow.version if self.shadow else None,
                "champion_state": self.champion_state,
                "size_multiplier": self.risk.size_multiplier,
                "learning": self.learning_status,
                "versions": self.repo.model_versions()[:10],
                "events": self.repo.model_events(10),
            },
            "counters": {
                "decisions_logged": self.decisions_logged,
                "evaluated": self.decision_core.evaluated,
                "trades_proposed": self.decision_core.trades_proposed,
                "pending_outcomes": len(self.pending_outcomes),
                "labelled_decisions": self.repo.labelled_decision_count(),
            },
            "reconcile": self.reconciler.last_result.to_dict()
            if self.reconciler.last_result else None,
        }

    def diagnose(self) -> dict[str, Any]:
        """Why is nothing being traded right now?  Always answerable."""
        allowed, reasons = self.health.gate()
        scan = self.scanner.last_result
        account = self.execution.account_state(self.last_snapshots)
        portfolio = self.risk.portfolio_blocks(account, "*", self.clock.mono())
        top = []
        if scan:
            for opportunity in scan.top(5):
                decision = self.decision_core.evaluate(
                    opportunity, self.champion, account, self.clock.mono()
                )
                top.append(
                    {
                        "symbol": decision.symbol,
                        "side": decision.side,
                        "action": decision.action,
                        "probability": round(decision.probability, 4),
                        "quality": round(decision.quality, 4),
                        "expected_move_bps": round(decision.expected_move_bps, 2),
                        "required_move_bps": round(decision.required_move_bps, 2),
                        "expectancy_eur": round(decision.expectancy_eur, 3),
                        "reasons": decision.reasons,
                    }
                )
        return {
            "ts_ms": self.clock.now_ms(),
            "mode": self.cfg.mode.upper(),
            "feed_source": self.market.source.value,
            "trading_allowed": allowed,
            "gate_reasons": reasons,
            "portfolio_blocks": [r for r in portfolio if "already holding" not in r],
            "system_state": self.health.system_state.value,
            "symbols_in_universe": len(self.market.universe),
            "symbols_with_snapshot": len(self.last_snapshots),
            "focus_symbols": sorted(self.market.focus),
            "tradable_symbols": sum(1 for s in self.last_snapshots.values() if s.tradable),
            "shortlisted": len(scan.ranked) if scan else 0,
            "scan_skipped": dict(scan.skipped.most_common(10)) if scan else {},
            "reject_summary": self.repo.reject_summary(),
            "top_candidates": top,
            "components": [c.to_dict() for c in self.health.components()],
            "learning": self.learning_status,
        }

    def stats_all_runs(self) -> dict[str, Any]:
        rows = self.repo.trades(run_id="*", limit=100_000)
        return compute_stats(rows).to_dict()


_NUMBERS = re.compile(r"-?\d[\d_.,]*")


def _reason_key(reason: str) -> str:
    """Collapse a reason to a stable bucket so counters stay readable.

    Numbers are replaced rather than kept: "margin cut to 39.12 EUR" and
    "margin cut to 37.02 EUR" are one reason, not two, and the counter table
    stays a summary instead of growing a row per event.
    """
    for marker in (":", " (", " <", " >="):
        if marker in reason:
            reason = reason.split(marker)[0]
            break
    return _NUMBERS.sub("#", reason).strip()[:80]
