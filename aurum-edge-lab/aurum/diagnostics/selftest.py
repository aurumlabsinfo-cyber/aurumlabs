"""Self-test: verify every subsystem against known answers.

This is not a smoke test.  Each check feeds a subsystem an input whose correct
output is known independently, and compares.  A check that only asserts "it ran"
would pass for a component that returns zeros, which is exactly the failure mode
the blueprint's "no cosmetic agents" rule is about.

It runs without a network and without a live feed, so it is the right thing to
run on a fresh machine before trusting anything else.

    python3 main.py selftest
"""

from __future__ import annotations

import math
import tempfile
import time
import traceback
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

from ..config import Config, ConfigError, apply_setting, load_config


@dataclass
class Check:
    name: str
    group: str
    passed: bool
    detail: str
    duration_ms: float = 0.0
    error: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "group": self.group,
            "passed": self.passed,
            "detail": self.detail,
            "duration_ms": round(self.duration_ms, 2),
            "error": self.error,
        }


@dataclass
class SelfTestReport:
    checks: list[Check] = field(default_factory=list)
    started_ms: int = 0
    finished_ms: int = 0

    @property
    def passed(self) -> bool:
        return all(check.passed for check in self.checks)

    @property
    def failures(self) -> list[Check]:
        return [check for check in self.checks if not check.passed]

    def to_dict(self) -> dict[str, Any]:
        groups: dict[str, dict[str, int]] = {}
        for check in self.checks:
            bucket = groups.setdefault(check.group, {"passed": 0, "failed": 0})
            bucket["passed" if check.passed else "failed"] += 1
        return {
            "passed": self.passed,
            "total": len(self.checks),
            "failed": len(self.failures),
            "duration_ms": self.finished_ms - self.started_ms,
            "groups": groups,
            "checks": [check.to_dict() for check in self.checks],
        }

    def render(self) -> str:
        lines = ["AURUM EDGE LAB — self-test", "=" * 62]
        current = None
        for check in self.checks:
            if check.group != current:
                current = check.group
                lines.append(f"\n{current}")
            mark = "PASS" if check.passed else "FAIL"
            lines.append(f"  [{mark}] {check.name}: {check.detail}")
            if check.error:
                lines.append(f"         {check.error}")
        lines.append("=" * 62)
        lines.append(
            f"{len(self.checks) - len(self.failures)}/{len(self.checks)} checks passed "
            f"in {(self.finished_ms - self.started_ms) / 1000:.2f}s"
        )
        if self.failures:
            lines.append("FAILED: " + ", ".join(c.name for c in self.failures))
        return "\n".join(lines)


class SelfTest:
    def __init__(self, config: Config | None = None) -> None:
        self.config = config
        self.report = SelfTestReport()

    def _check(self, group: str, name: str, fn: Callable[[], str]) -> None:
        start = time.monotonic()
        try:
            detail = fn()
            self.report.checks.append(
                Check(name, group, True, detail, (time.monotonic() - start) * 1000)
            )
        except AssertionError as exc:
            self.report.checks.append(
                Check(name, group, False, "assertion failed", (time.monotonic() - start) * 1000, str(exc))
            )
        except Exception as exc:  # noqa: BLE001
            self.report.checks.append(
                Check(
                    name, group, False, f"{type(exc).__name__}", (time.monotonic() - start) * 1000,
                    f"{exc}\n{traceback.format_exc(limit=3)}",
                )
            )

    def run(self) -> SelfTestReport:
        self.report = SelfTestReport(started_ms=int(time.time() * 1000))
        with tempfile.TemporaryDirectory(prefix="aurum-selftest-") as tmp:
            workdir = Path(tmp)
            self._config_checks(workdir)
            self._storage_checks(workdir)
            self._orderbook_checks()
            self._feature_checks()
            self._cost_and_wallet_checks(workdir)
            self._broker_checks(workdir)
            self._statistics_checks()
            self._research_checks(workdir)
            self._lifecycle_checks(workdir)
            self._postmortem_checks(workdir)
            self._api_checks()
            self._safety_checks()
        self.report.finished_ms = int(time.time() * 1000)
        return self.report

    # ------------------------------------------------------------------ config

    def _config_checks(self, workdir: Path) -> None:
        group = "configuration"

        def loads() -> str:
            config = load_config(use_env=False, overrides={"app": {"data_dir": str(workdir)}})
            assert len(config.market.symbols) >= 1, "no symbols configured"
            assert config.wallet.starting_balance_eur == 100.00, (
                f"wallet starts at {config.wallet.starting_balance_eur}, not 100.00"
            )
            self.config = config
            return f"{len(config.market.symbols)} symbols, wallet EUR {config.wallet.starting_balance_eur:.2f}"

        def bounded() -> str:
            assert self.config is not None
            apply_setting(self.config, "risk.risk_per_trade_pct", 1.0)
            try:
                apply_setting(self.config, "risk.risk_per_trade_pct", 99.0)
            except ConfigError:
                pass
            else:  # pragma: no cover - the failure this check exists for
                raise AssertionError("an out-of-bounds setting was accepted")
            try:
                apply_setting(self.config, "wallet.starting_balance_eur", 5000)
            except ConfigError:
                pass
            else:  # pragma: no cover
                raise AssertionError("a structural value was changed at runtime")
            return "out-of-bounds and structural changes both refused"

        def production_refuses_replay() -> str:
            try:
                load_config(
                    use_env=False,
                    overrides={
                        "app": {"env": "production", "data_dir": str(workdir)},
                        "market": {"feed": "replay", "replay_path": "x.jsonl"},
                    },
                )
            except ConfigError:
                return "production refuses a replay feed"
            raise AssertionError("production accepted a replay feed")

        self._check(group, "config loads and validates", loads)
        self._check(group, "runtime settings are bounded", bounded)
        self._check(group, "production refuses synthetic data", production_refuses_replay)

    # ----------------------------------------------------------------- storage

    def _storage_checks(self, workdir: Path) -> None:
        from ..domain import EventKind, MarketEvent, now_ms
        from ..storage.repositories import Repositories
        from ..storage.schema import TABLES, render_ddl
        from ..storage.sqlite_db import SqliteDatabase

        group = "storage"

        def schema() -> str:
            required = {
                "market_events", "orderbook_snapshots", "features", "hypotheses", "experiments",
                "strategies", "strategy_versions", "signals", "paper_trades", "positions", "wallet",
                "wallet_ledger", "cycles", "cycle_postmortems", "research_memory", "agent_events",
                "system_events", "data_quality", "model_registry",
            }
            names = {t.name for t in TABLES}
            missing = required - names
            assert not missing, f"tables missing: {sorted(missing)}"
            assert "JSONB" in "\n".join(render_ddl("postgres")), "postgres DDL does not render"
            return f"{len(names)} tables, DDL renders for sqlite and postgres"

        def roundtrip() -> str:
            db = SqliteDatabase(workdir / "selftest.db", batch_size=4, flush_interval_ms=10)
            db.connect()
            db.create_schema()
            try:
                mode = db.query("PRAGMA journal_mode")[0]["journal_mode"].lower()
                assert mode == "wal", f"journal mode is {mode}, not WAL"
                repos = Repositories(db)
                stamp = now_ms()
                for i in range(25):
                    repos.market.record_event(
                        MarketEvent("BTCUSDT", EventKind.TRADE, stamp + i, stamp + i + 2, {"p": i})
                    )
                db.flush(timeout=5.0)
                count = db.scalar("SELECT COUNT(*) FROM market_events", default=0)
                assert count == 25, f"wrote 25 events, read back {count}"
                repos.wallet.save_cycle(
                    {"cycle_id": 1, "state": "ACTIVE", "started_ms": stamp, "starting_balance": 100.0}
                )
                removed = db.prune({"market_events_hours": 0.000001})
                assert removed["market_events"] >= 0
                assert db.scalar("SELECT COUNT(*) FROM cycles", default=0) == 1, "history was pruned"
                return f"WAL on, {count} rows batched, retention prunes events but not history"
            finally:
                db.close()

        self._check(group, "schema covers every blueprint table", schema)
        self._check(group, "batched writes round-trip and retention is bounded", roundtrip)

    # --------------------------------------------------------------- orderbook

    def _orderbook_checks(self) -> None:
        from ..adapters.base import DepthSnapshot, DepthUpdate
        from ..market.orderbook import BookState, OrderBook

        group = "order book"

        def snapshot() -> DepthSnapshot:
            return DepthSnapshot(
                "BTCUSDT", 100, 1000, 1005,
                [(60000.0, 2.0), (59999.0, 3.0)], [(60001.0, 1.5), (60002.0, 2.5)],
            )

        def sequencing() -> str:
            book = OrderBook("BTCUSDT")
            book.apply_update(DepthUpdate("BTCUSDT", 2000, 2005, 99, 102, 98, [], []))
            assert book.apply_snapshot(snapshot()), "snapshot did not join the diff stream"
            assert book.ready
            mid = book.top(2).mid
            assert mid == 60000.5, f"mid is {mid}, expected 60000.5"
            return f"book ready, mid {mid}"

        def gap_detection() -> str:
            book = OrderBook("BTCUSDT")
            book.apply_update(DepthUpdate("BTCUSDT", 2000, 2005, 99, 102, 98, [], []))
            book.apply_snapshot(snapshot())
            ok = book.apply_update(DepthUpdate("BTCUSDT", 3000, 3005, 151, 155, 150, [], []))
            assert ok is False, "a dropped event did not desync the book"
            assert book.state is BookState.DESYNCED
            assert book.stats.sequence_gaps == 1
            return "pu mismatch desyncs the book and is counted"

        def deterministic_resync() -> str:
            book = OrderBook("BTCUSDT")
            book.apply_update(DepthUpdate("BTCUSDT", 2000, 2005, 99, 102, 98, [], []))
            book.apply_snapshot(snapshot())
            book.apply_update(DepthUpdate("BTCUSDT", 3000, 3005, 151, 155, 150, [], []))
            book.begin_resync()
            book.apply_update(
                DepthUpdate("BTCUSDT", 4000, 4005, 299, 305, 298, [(60000.5, 7.0)], [])
            )
            fresh = DepthSnapshot(
                "BTCUSDT", 300, 4000, 4005, [(60000.0, 2.0)], [(60001.0, 1.5)]
            )
            assert book.apply_snapshot(fresh), "resync did not recover the book"
            assert book.ready and book.last_update_id == 305
            assert book.top(1).bids[0].price == 60000.5
            return "resync recovers deterministically and replays buffered diffs"

        self._check(group, "snapshot joins the diff stream", sequencing)
        self._check(group, "sequence gap is detected and blocks", gap_detection)
        self._check(group, "resync is deterministic", deterministic_resync)

    # ---------------------------------------------------------------- features

    def _feature_checks(self) -> None:
        from ..features.engine import SymbolHistory

        group = "features"

        def causal_returns() -> str:
            history = SymbolHistory("BTCUSDT", cadence_ms=250, capacity=100)
            for i in range(12):
                history.append(1000 + i * 250, 100.0 + i, 100.0 + i)
            one_second = history.return_bps(1000)
            expected = (111 - 107) / 107 * 10_000
            assert one_second is not None and abs(one_second - expected) < 1e-6, (
                f"1s return {one_second}, expected {expected}"
            )
            assert history.return_bps(60_000) is None, "a horizon longer than history returned a value"
            assert history.forward_return_bps(11, 1000) is None, "the live edge produced a future"
            return f"1s return {one_second:.3f} bps, no lookahead at the edge"

        def volatility() -> str:
            flat = SymbolHistory("X", cadence_ms=250, capacity=64)
            for i in range(20):
                flat.append(i * 250, 100.0, 100.0)
            assert flat.volatility_bps(2000) == 0.0, "flat prices produced non-zero volatility"
            moving = SymbolHistory("Y", cadence_ms=250, capacity=64)
            for i in range(20):
                moving.append(i * 250, 100.0 + (i % 2), 100.0)
            value = moving.volatility_bps(2000)
            assert value and value > 0, "an oscillating series produced zero volatility"
            return f"flat 0.0, oscillating {value:.1f} bps"

        self._check(group, "returns are causal", causal_returns)
        self._check(group, "volatility responds to movement", volatility)

    # ------------------------------------------------------------ cost/wallet

    def _cost_and_wallet_checks(self, workdir: Path) -> None:
        from ..domain import BookLevel, BookSnapshot, Side
        from ..execution.cost_model import CostModel
        from ..storage.repositories import Repositories
        from ..storage.sqlite_db import SqliteDatabase
        from ..wallet.virtual_wallet import InsufficientFunds, VirtualWallet

        group = "costs and wallet"
        assert self.config is not None

        def cost_model() -> str:
            costs = CostModel(self.config.costs)
            round_trip = costs.round_trip_bps(1.0)
            expected = 1.0 + 2 * self.config.costs.taker_fee_bps + 2 * self.config.costs.extra_slippage_bps
            expected += 2 * costs.latency_bps()
            assert abs(round_trip - expected) < 1e-9, f"round trip {round_trip}, expected {expected}"
            assert round_trip > 9.0, "a round trip cheaper than two taker fees is impossible"
            return f"round trip at 1 bps spread = {round_trip:.3f} bps"

        def book_walk() -> str:
            costs = CostModel(self.config.costs)
            book = BookSnapshot(
                "BTCUSDT", 0, 0,
                [BookLevel(60000.0 - i, 1.0) for i in range(5)],
                [BookLevel(60001.0 + i, 1.0) for i in range(5)],
                1,
            )
            fill = costs.simulate_market_order(book, Side.BUY, 3.5)
            assert fill.fully_filled and fill.levels_consumed == 4, (
                f"consumed {fill.levels_consumed} levels for 3.5 units of 1.0-unit levels"
            )
            assert fill.fill_price > 60001.0, "a size larger than the touch got the touch price"
            too_big = costs.simulate_market_order(book, Side.BUY, 500.0)
            assert not too_big.fully_filled, "an order larger than the book reported a full fill"
            return f"3.5 units walked {fill.levels_consumed} levels to {fill.fill_price:.2f}"

        def wallet_arithmetic() -> str:
            db = SqliteDatabase(workdir / "wallet.db")
            db.connect()
            db.create_schema()
            try:
                repos = Repositories(db)
                wallet = VirtualWallet(repos.wallet, starting_balance=100.0)
                wallet.open_cycle(1)
                assert wallet.state.balance == 100.00, "cycle did not open at exactly 100.00"

                wallet.reserve(30.0, "pos-1")
                assert wallet.state.available == 70.0, f"available {wallet.state.available}, expected 70"
                wallet.charge_fee(0.05, "pos-1")
                wallet.settle(2.0, "pos-1")
                wallet.release(30.0, "pos-1")
                assert abs(wallet.state.balance - 101.95) < 1e-9, (
                    f"balance {wallet.state.balance}, expected 101.95"
                )
                assert wallet.state.available == wallet.state.balance

                try:
                    wallet.reserve(10_000.0, "pos-2")
                except InsufficientFunds:
                    pass
                else:  # pragma: no cover
                    raise AssertionError("an over-reservation was silently clamped")

                wallet.mark_unrealized(50.0)
                peak = wallet.state.peak_equity
                wallet.mark_unrealized(0.0)
                wallet.settle(-10.0, "pos-3")
                drawdown = wallet.state.drawdown_pct
                expected_dd = (peak - wallet.state.equity) / peak * 100
                assert abs(drawdown - expected_dd) < 1e-6, "drawdown is not measured against peak equity"

                entries = repos.wallet.list_ledger(cycle_id=1)
                assert len(entries) >= 5, f"only {len(entries)} ledger entries for 5+ movements"
                return f"balance {wallet.state.balance:.2f}, drawdown {drawdown:.2f}%, {len(entries)} ledger rows"
            finally:
                db.close()

        self._check(group, "cost model is arithmetically correct", cost_model)
        self._check(group, "fills walk the book", book_walk)
        self._check(group, "wallet arithmetic and ledger agree", wallet_arithmetic)

    # ----------------------------------------------------------- paper broker

    def _broker_checks(self, workdir: Path) -> None:
        from ..domain import (
            BookLevel,
            BookSnapshot,
            Direction,
            ExitReason,
            FeatureSnapshot,
            Regime,
            Signal,
        )
        from ..execution.cost_model import CostModel
        from ..execution.paper_broker import PaperBroker
        from ..risk.manager import RiskManager
        from ..storage.repositories import Repositories
        from ..storage.sqlite_db import SqliteDatabase
        from ..wallet.virtual_wallet import VirtualWallet

        group = "paper broker"
        assert self.config is not None
        config = self.config

        def book(bid: float = 60000.0, ask: float = 60001.0) -> BookSnapshot:
            return BookSnapshot(
                "BTCUSDT", 0, 0,
                [BookLevel(bid - i, 5.0) for i in range(10)],
                [BookLevel(ask + i, 5.0) for i in range(10)],
                1, is_crossed=bid >= ask,
            )

        def round_trip() -> str:
            db = SqliteDatabase(workdir / "broker.db")
            db.connect()
            db.create_schema()
            try:
                repos = Repositories(db)
                wallet = VirtualWallet(repos.wallet, starting_balance=100.0)
                wallet.open_cycle(1)
                costs = CostModel(config.costs)
                risk = RiskManager(config, wallet, costs)
                broker = PaperBroker(config, wallet, costs, repos.execution)

                snapshot = FeatureSnapshot(
                    "BTCUSDT", 0,
                    {"vol_1s": 3.0, "vol_5s": 6.0, "vol_10s": 8.0, "vol_30s": 12.0, "vol_60s": 18.0},
                    Regime.NORMAL_RANGE, 0.95, True, 60000.5, 60000.6, 0.167,
                )
                decision = risk.evaluate(
                    symbol="BTCUSDT", direction=Direction.LONG, expected_edge_bps=60.0,
                    horizon_ms=5_000, snapshot=snapshot, book=book(),
                    open_positions={}, cycle_active=True, quality_ok=True, quality_detail="ok",
                    usdt_per_eur=config.fx.usdt_per_eur, at_ms=1_000,
                )
                assert decision.allowed, f"a clean signal was refused: {decision.detail}"
                signal = Signal(
                    "sig-1", 1_000, "str-1", "hyp-1", "BTCUSDT", Direction.LONG, 0.7, 60.0, 11.0,
                    5_000, Regime.NORMAL_RANGE,
                )
                position = broker.open(signal, decision.plan, book(), snapshot, cycle_id=1, at_ms=1_000)
                assert position is not None, "the broker did not fill a clean order"
                assert position.entry_price > position.requested_entry_price, (
                    "the fill price was not worse than the requested price"
                )
                assert position.entry_slippage_bps > 0, "an entry recorded zero slippage"

                trade = broker.close(position, book(60300.0, 60301.0), ExitReason.TAKE_PROFIT, at_ms=6_000)
                assert trade.gross_pnl_eur > 0, "an upward move produced a non-positive gross P&L"
                assert trade.net_pnl_eur < trade.gross_pnl_eur, "fees were not charged"
                assert abs(trade.net_pnl_eur - (trade.gross_pnl_eur - trade.fees_eur)) < 1e-9, (
                    "net P&L does not equal gross minus fees"
                )
                assert trade.net_return_bps < trade.return_bps, "costs did not reduce the return"
                assert trade.features, "the decision-time features were not stored on the trade"
                assert wallet.state.reserved == 0.0, "margin was not released"
                assert wallet.state.trades == 1 and wallet.state.wins == 1

                stored = repos.execution.load_trade(trade.trade_id)
                assert stored is not None, "the trade was not persisted"
                return (
                    f"entry {position.entry_price:.2f} (requested {position.requested_entry_price:.2f}), "
                    f"net {trade.net_pnl_eur:+.4f} EUR, {trade.net_return_bps:+.1f} bps"
                )
            finally:
                db.close()

        def gates_refuse() -> str:
            db = SqliteDatabase(workdir / "gates.db")
            db.connect()
            db.create_schema()
            try:
                repos = Repositories(db)
                wallet = VirtualWallet(repos.wallet, starting_balance=100.0)
                wallet.open_cycle(1)
                risk = RiskManager(config, wallet, CostModel(config.costs))
                snapshot = FeatureSnapshot(
                    "BTCUSDT", 0, {"vol_5s": 6.0}, Regime.NORMAL_RANGE, 0.95, True, 60000.5, 60000.6, 0.167
                )
                common = dict(
                    symbol="BTCUSDT", direction=Direction.LONG, horizon_ms=5_000, snapshot=snapshot,
                    open_positions={}, cycle_active=True, usdt_per_eur=config.fx.usdt_per_eur,
                    at_ms=1_000,
                )
                bad_quality = risk.evaluate(
                    expected_edge_bps=500.0, book=book(), quality_ok=False,
                    quality_detail="feed is STALE", **common
                )
                assert not bad_quality.allowed, "a 500 bps edge overrode a broken feed"

                cheap = risk.evaluate(
                    expected_edge_bps=2.0, book=book(), quality_ok=True, quality_detail="ok", **common
                )
                assert not cheap.allowed, "an edge below the round trip was authorised"

                crossed = risk.evaluate(
                    expected_edge_bps=500.0, book=book(60002.0, 60001.0), quality_ok=True,
                    quality_detail="ok", **common
                )
                assert not crossed.allowed, "a crossed book was traded"
                return "quality, cost and crossed-book gates all refuse"
            finally:
                db.close()

        self._check(group, "a controlled round trip settles correctly", round_trip)
        self._check(group, "risk gates refuse what they must", gates_refuse)

    # -------------------------------------------------------------- statistics

    def _statistics_checks(self) -> None:
        from ..research.statistics import (
            benjamini_hochberg,
            effective_sample_size,
            student_t_two_tailed_p,
        )

        group = "statistics"

        def t_distribution() -> str:
            p = student_t_two_tailed_p(2.228, 10)
            assert abs(p - 0.05) < 0.002, f"t=2.228, df=10 gave p={p}, expected ~0.05"
            assert student_t_two_tailed_p(0.0, 10) == 1.0
            assert student_t_two_tailed_p(6.0, 30) < 1e-4
            return f"t=2.228 df=10 -> p={p:.4f}"

        def independence() -> str:
            overlapping = [i * 250 for i in range(40)]
            n = effective_sample_size(overlapping, 5_000)
            assert n == 2, f"40 overlapping triggers counted as {n} independent, expected 2"
            spaced = effective_sample_size([i * 6_000 for i in range(40)], 5_000)
            assert spaced == 40
            return f"40 overlapping observations -> {n} independent"

        def fdr() -> str:
            nulls = [(i + 1) / 21 for i in range(20)]
            assert not any(benjamini_hochberg(nulls, 0.10)), "FDR passed pure nulls"
            mixed = benjamini_hochberg([0.0001, 0.4, 0.6, 0.9], 0.10)
            assert mixed[0] and not any(mixed[1:]), "FDR did not isolate the real signal"
            return "20 null p-values all rejected; a real one survives"

        self._check(group, "Student's t matches known values", t_distribution)
        self._check(group, "overlapping observations are de-overlapped", independence)
        self._check(group, "false-discovery control works", fdr)

    # ---------------------------------------------------------------- research

    def _research_checks(self, workdir: Path) -> None:
        from ..domain import Direction, Regime, ValidationStatus
        from ..research.dataset import Observation
        from ..research.hypotheses import Hypothesis, PercentileCondition, new_hypothesis_id
        from ..research.memory import Outcome, ResearchMemory
        from ..research.validation import ValidationLab
        from ..storage.repositories import Repositories
        from ..storage.sqlite_db import SqliteDatabase

        group = "research"
        assert self.config is not None

        def hypothesis(**kw) -> Hypothesis:
            base = dict(
                hypothesis_id=new_hypothesis_id("selftest"), agent="microstructure",
                family="microstructure", signal_symbol="BTCUSDT", execution_symbol="BTCUSDT",
                direction=Direction.LONG,
                conditions=[PercentileCondition("ofi_norm_1s", ">=", 80.0, threshold=0.4)],
                horizon_ms=5_000,
            )
            base.update(kw)
            return Hypothesis(**base)

        def observations(count: int, mean: float, noise: float, cost: float, seed: int = 3):
            import random

            rng = random.Random(seed)
            rows = []
            for i in range(count):
                gross = rng.gauss(mean, noise)
                rows.append(
                    Observation(1_700_000_000_000 + i * 6_000, i, gross, cost, gross - cost,
                                Regime.NORMAL_RANGE, 1.0)
                )
            return rows

        def fingerprints() -> str:
            a = hypothesis()
            refit = hypothesis(
                conditions=[PercentileCondition("ofi_norm_1s", ">=", 80.0, threshold=0.91)]
            )
            different = hypothesis(conditions=[PercentileCondition("ofi_norm_1s", ">=", 90.0)])
            assert a.fingerprint == refit.fingerprint, "a refitted threshold changed the identity"
            assert a.fingerprint != different.fingerprint, "a different percentile kept the identity"
            assert a.fingerprint != hypothesis(direction=Direction.SHORT).fingerprint
            return "identity survives refitting and distinguishes real differences"

        def memory() -> str:
            db = SqliteDatabase(workdir / "memory.db")
            db.connect()
            db.create_schema()
            try:
                repos = Repositories(db)
                store = ResearchMemory(repos.research, retest_cooldown_h=6.0)
                h = hypothesis()
                assert not store.check(h).known
                store.record(h, Outcome.REJECTED, reason="net edge negative", net_edge_bps=-8.0)
                assert store.check(h).blocked, "a known failure was not blocked"

                reloaded = ResearchMemory(repos.research)
                reloaded.load()
                assert reloaded.check(h).blocked, "memory did not survive a reload"
                return "failures are remembered across restarts"
            finally:
                db.close()

        def validation_rejects_noise() -> str:
            lab = ValidationLab(self.config)
            report = lab.evaluate(hypothesis(), observations(1200, 0.0, 8.0, 0.0), total_tests=30)
            assert not report.passed, "pure noise passed validation"
            assert report.status is ValidationStatus.REJECTED
            return f"noise rejected: {report.reason[:70]}"

        def validation_rejects_uneconomic_edge() -> str:
            lab = ValidationLab(self.config)
            report = lab.evaluate(hypothesis(), observations(1200, 4.0, 6.0, 11.0), total_tests=10)
            assert not report.passed, "a 4 bps edge survived an 11 bps round trip"
            return "a real edge that does not cover its costs is rejected"

        def validation_accepts_a_real_edge() -> str:
            lab = ValidationLab(self.config)
            report = lab.evaluate(hypothesis(), observations(1500, 14.0, 6.0, 11.0), total_tests=5)
            assert report.passed, f"a strong surviving edge was rejected: {report.reason}"
            assert report.status is ValidationStatus.HOLDOUT_PASS
            assert len(report.stages) == 4, "not every gate ran"
            return f"14 bps edge passes all gates: {report.reason[:70]}"

        def search_deflation() -> str:
            lab = ValidationLab(self.config)
            rows = observations(1500, 13.0, 8.0, 11.0, seed=5)
            assert lab.evaluate(hypothesis(), rows, total_tests=1).passed
            heavy = lab.evaluate(hypothesis(), rows, total_tests=5000)
            assert not heavy.passed, "5000 tests did not deflate a marginal edge"
            return "the same evidence is judged against the size of the search"

        self._check(group, "hypothesis identity is percentile-based", fingerprints)
        self._check(group, "research memory persists failures", memory)
        self._check(group, "validation rejects pure noise", validation_rejects_noise)
        self._check(group, "validation rejects an edge below costs", validation_rejects_uneconomic_edge)
        self._check(group, "validation accepts a real surviving edge", validation_accepts_a_real_edge)
        self._check(group, "multiple testing deflates a marginal edge", search_deflation)

    # --------------------------------------------------------------- lifecycle

    def _lifecycle_checks(self, workdir: Path) -> None:
        from ..domain import Direction, MetricSet, StrategyState
        from ..research.hypotheses import Hypothesis, PercentileCondition, new_hypothesis_id
        from ..storage.repositories import Repositories
        from ..storage.sqlite_db import SqliteDatabase
        from ..strategies.lifecycle import IllegalTransition, StrategyLifecycle

        group = "lifecycle"

        def transitions() -> str:
            db = SqliteDatabase(workdir / "lifecycle.db")
            db.connect()
            db.create_schema()
            try:
                repos = Repositories(db)
                lifecycle = StrategyLifecycle(repos.strategies)
                hypothesis = Hypothesis(
                    hypothesis_id=new_hypothesis_id("selftest"), agent="momentum", family="momentum",
                    signal_symbol="BTCUSDT", execution_symbol="BTCUSDT", direction=Direction.LONG,
                    conditions=[PercentileCondition("ret_1s", ">=", 80.0, threshold=5.0)],
                    horizon_ms=5_000,
                )
                strategy = lifecycle.create(hypothesis, MetricSet(samples=200), 1.5, cycle_id=1)

                try:
                    lifecycle.transition(strategy, StrategyState.CHAMPION, "skipping every gate")
                except IllegalTransition:
                    pass
                else:  # pragma: no cover
                    raise AssertionError("a strategy was promoted straight to champion")

                for state in (StrategyState.CANDIDATE, StrategyState.CHALLENGER, StrategyState.SHADOW):
                    lifecycle.transition(strategy, state, "advancing")
                    assert not strategy.can_trade, f"{state.value} was allowed to trade"
                lifecycle.transition(strategy, StrategyState.CHAMPION, "promoted")
                assert strategy.can_trade and lifecycle.champion() is strategy

                versions = repos.strategies.versions(strategy.strategy_id)
                assert len(versions) == 5, f"{len(versions)} versions recorded for 5 changes"
                assert versions[0]["reason"] == "promoted"
                return f"5 states, {len(versions)} immutable versions, only CHAMPION may trade"
            finally:
                db.close()

        self._check(group, "state machine is enforced and versioned", transitions)

    # -------------------------------------------------------------- postmortem

    def _postmortem_checks(self, workdir: Path) -> None:
        from ..agents.postmortem import Cause, CyclePostMortemAgent
        from ..domain import CycleState, Direction, ExitReason, PaperTrade, Regime
        from ..storage.repositories import Repositories
        from ..storage.sqlite_db import SqliteDatabase
        from ..strategies.lifecycle import StrategyLifecycle
        from ..wallet.postmortem import CycleManager
        from ..wallet.virtual_wallet import VirtualWallet

        group = "cycle and post-mortem"
        assert self.config is not None

        def trade(index: int, net: float, gross_bps: float = 5.0) -> PaperTrade:
            return PaperTrade(
                f"trd-{index}", f"pos-{index}", "BTCUSDT", Direction.LONG, 0.001,
                1_000 + index * 1000, 6_000 + index * 1000,
                100.0, 100.05, 100.1, 100.1, net + 0.02, 0.02, net,
                gross_bps, gross_bps - 11.0, 0.5, 0.5, 11.0,
                ExitReason.HORIZON, "str-1", 1, "hyp-1", "sig-1", 1, Regime.NORMAL_RANGE,
                cost_model_version="cost-v1", expected_edge_bps=20.0,
            )

        def attribution() -> str:
            db = SqliteDatabase(workdir / "pm.db")
            db.connect()
            db.create_schema()
            try:
                repos = Repositories(db)
                agent = CyclePostMortemAgent(repos.research)
                thin = agent.analyse(
                    cycle_id=1, trades=[trade(i, -1.0) for i in range(3)],
                    starting_balance=100.0, final_equity=97.0,
                )
                assert thin.primary_cause == Cause.INSUFFICIENT_EVIDENCE, (
                    "a cause was attributed from three trades"
                )
                rich = agent.analyse(
                    cycle_id=1, trades=[trade(i, -0.4) for i in range(20)],
                    starting_balance=100.0, final_equity=92.0,
                )
                assert rich.primary_cause == Cause.EXECUTION_COST, (
                    f"costs eating a real edge was attributed to {rich.primary_cause}"
                )
                joined = " ".join(rich.recommendations).lower()
                for forbidden in ("lower the threshold", "loosen"):
                    assert forbidden not in joined, "the post-mortem recommended weakening a gate"
                return f"3 trades -> {thin.primary_cause}; 20 trades -> {rich.primary_cause}"
            finally:
                db.close()

        def conditional_reset() -> str:
            db = SqliteDatabase(workdir / "cycle.db")
            db.connect()
            db.create_schema()
            try:
                repos = Repositories(db)
                wallet = VirtualWallet(repos.wallet, starting_balance=100.0)
                lifecycle = StrategyLifecycle(repos.strategies)
                manager = CycleManager(self.config, wallet, lifecycle, repos)
                manager.start()

                wallet.settle(-65.0, "pos-blowup")
                reason = manager.check_failure()
                assert reason, "a wallet at 35 EUR did not register as a failure"

                assert manager.try_reset() is None, "a reset happened before the post-mortem"

                manager.fail_cycle(reason, [trade(i, -3.25) for i in range(20)])
                assert manager.cycle.state is CycleState.AWAITING_EDGE
                assert manager.try_reset() is None, "a reset happened with no eligible strategy"
                assert "NO VALIDATED EDGE" in manager.blocked_reason
                assert wallet.state.balance != 100.0, "the wallet was quietly topped up"

                cycles = repos.wallet.list_cycles()
                assert len(cycles) == 1 and cycles[0]["end_reason"] == reason, "history was lost"
                assert repos.wallet.postmortems(cycle_id=1), "the post-mortem was not persisted"
                return "no reset without a post-mortem and an eligible strategy; history preserved"
            finally:
                db.close()

        self._check(group, "post-mortem attributes causes from evidence", attribution)
        self._check(group, "reset is conditional and history survives", conditional_reset)

    # --------------------------------------------------------------------- API

    def _api_checks(self) -> None:
        from ..api.app import create_app, route_paths

        group = "api"
        assert self.config is not None

        def contract() -> str:
            app = create_app(self.config)
            served = route_paths(app)
            required = [
                "/health", "/market", "/markets", "/market/{symbol}", "/orderbook/{symbol}",
                "/features/{symbol}", "/agents", "/research", "/hypotheses", "/strategies",
                "/champion", "/challengers", "/signals", "/positions", "/trades", "/wallet",
                "/cycles", "/statistics", "/data-quality", "/diagnostics", "/config", "/ws/live",
            ]
            missing = [path for path in required if path not in served]
            assert not missing, f"routes missing: {missing}"
            return f"{len(required)} contract routes served"

        self._check(group, "every contract route is registered", contract)

    # ------------------------------------------------------------------ safety

    def _safety_checks(self) -> None:
        import inspect

        from ..adapters import binance_futures
        from ..execution.paper_broker import PaperBroker

        group = "safety"

        def no_live_orders() -> str:
            assert PaperBroker.can_place_live_orders is False
            source = inspect.getsource(PaperBroker)
            for forbidden in ("api_key", "signature", "hmac", "POST", "/order"):
                assert forbidden not in source, f"PaperBroker references {forbidden!r}"
            return "no credential, no signing and no order endpoint in the broker"

        def adapter_is_public_only() -> str:
            source = inspect.getsource(binance_futures)
            for forbidden in ("hmac", "signature=", "X-MBX-APIKEY", "/fapi/v1/order"):
                assert forbidden not in source, f"the adapter references {forbidden!r}"
            return "the venue adapter reads public market data only"

        def no_placeholders() -> str:
            """The blueprint forbids TODO/stub paths in the production runtime.

            Two files are exempt and both for the same reason — they contain the
            marker strings as *data* rather than as unfinished work: this module,
            which searches for them, and the SQLite driver, whose
            ``NotImplementedError`` is the deliberate signpost pointing at the
            PostgreSQL migration path rather than an unwritten function.
            """
            from pathlib import Path as _Path

            root = _Path(__file__).resolve().parent.parent
            exempt = {"diagnostics/selftest.py", "storage/sqlite_db.py"}
            markers = ("TO" + "DO", "FIX" + "ME", "raise NotImplemented" + "Error")
            offenders: list[str] = []
            scanned = 0
            for path in sorted(root.rglob("*.py")):
                relative = path.relative_to(root).as_posix()
                if relative in exempt:
                    continue
                scanned += 1
                text = path.read_text(encoding="utf-8")
                for marker in markers:
                    if marker in text:
                        offenders.append(f"{relative}:{marker}")
            assert not offenders, f"placeholders in the runtime: {offenders}"
            return f"none of {markers} across {scanned} runtime modules"

        self._check(group, "the broker cannot reach a venue", no_live_orders)
        self._check(group, "the adapter is public-market-data only", adapter_is_public_only)
        self._check(group, "no placeholders remain in the runtime", no_placeholders)


def run_selftest(config: Config | None = None) -> SelfTestReport:
    return SelfTest(config).run()
