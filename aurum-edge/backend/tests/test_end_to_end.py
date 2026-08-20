"""End to end on a simulated feed: scan -> decide -> execute -> record -> learn.

This is the test that fails if any block stops talking to the next one.
"""

from __future__ import annotations

import asyncio
import dataclasses
import json

import pytest

from aurum_edge.config import Config
from aurum_edge.engine import Engine
from aurum_edge.execute.broker import PaperBroker
from aurum_edge.execute.execution_core import ExitReason
from aurum_edge.simulator import SimulatedMarketCore
from aurum_edge.storage.db import Database
from aurum_edge.util.clock import Clock


async def run_engine(cfg: Config, seconds: float, symbols: int = 10, seed: int = 7) -> Engine:
    clock = Clock()
    market = SimulatedMarketCore(cfg, clock, symbols=symbols, seed=seed, step_ms=25.0)
    engine = Engine(
        cfg, clock, market=market, broker=PaperBroker(cfg, clock), db=Database(cfg.db_path)
    )
    await engine.start()
    await asyncio.sleep(seconds)
    return engine


@pytest.mark.slow
async def test_full_pipeline_opens_closes_and_records(cfg: Config) -> None:
    engine = await run_engine(cfg, seconds=35.0)
    try:
        state = engine.state()

        # SCAN
        assert len(engine.last_snapshots) >= 10
        assert all(s.source == "fake" for s in engine.last_snapshots.values())
        assert state["feed_is_real"] is False, "a simulated feed must never claim to be real"
        assert engine.scanner.last_result.considered >= 10

        # DECIDE
        decisions = engine.repo.recent_decisions(limit=1000)
        assert decisions, "no decision was recorded"
        assert all(row["reason"] for row in decisions), "a decision without a reason"
        actions = {row["action"] for row in decisions}
        assert "NO_TRADE" in actions

        # EXECUTE
        assert engine.execution.trades_opened >= 1, "paper never opened a position"
        await engine.execution.flatten_all(ExitReason.FLATTEN, engine.last_snapshots)
        await asyncio.sleep(0.3)
        trades = engine.repo.trades(limit=200)
        assert trades, "no trade was recorded"

        for trade in trades:
            identity = trade["gross_pnl_eur"] - trade["slippage_eur"] - trade["fees_eur"]
            assert identity == pytest.approx(trade["net_pnl_eur"], abs=1e-9)
            assert trade["fees_eur"] > 0, "a taker trade always pays fees"
            assert trade["entry_price"] > 0 and trade["exit_price"] > 0
            assert trade["entry_reason"] and trade["exit_reason"]
            assert json.loads(trade["features_json"])
            assert json.loads(trade["prediction_json"])["probability"] > 0
            assert trade["mode"] == "paper"

        # every trade traces back to the snapshot that caused it
        for trade in trades:
            if trade["decision_id"]:
                decision = engine.db.query_one(
                    "SELECT * FROM decisions WHERE id=?", (trade["decision_id"],)
                )
                assert decision is not None
                assert decision["executed"] == 1
                if decision["snapshot_id"]:
                    snapshot = engine.db.query_one(
                        "SELECT * FROM snapshots WHERE id=?", (decision["snapshot_id"],)
                    )
                    assert snapshot is not None
                    stored = json.loads(snapshot["data_json"])
                    assert stored["symbol"] == trade["symbol"]
                    assert stored["source"] == "fake"

        # aggregate reporting matches the rows
        stats = engine.repo.stats()
        assert stats.trades == len(trades)
        assert stats.net_pnl_eur == pytest.approx(
            sum(t["net_pnl_eur"] for t in trades), abs=1e-9
        )
        assert stats.gross_pnl_eur - stats.fees_eur - stats.slippage_eur == pytest.approx(
            stats.net_pnl_eur, abs=1e-9
        )

        # LEARN: outcomes are attached to decisions, including NO TRADE
        assert engine.repo.labelled_decision_count() > 0
        labelled = engine.db.query(
            "SELECT action, outcome_label FROM decisions WHERE outcome_label IS NOT NULL"
        )
        assert any(row["action"] == "NO_TRADE" for row in labelled), (
            "the system must learn from what it declined, not only from what it took"
        )

        # nothing was lost on the way to disk
        assert engine.db.write_failures == 0
        assert engine.db.pending_write_failures() == 0
        for table in ("runs", "snapshots", "decisions", "orders", "executions",
                      "positions", "trades", "equity", "model_versions"):
            count = engine.db.query_one(f"SELECT COUNT(*) AS n FROM {table}")["n"]
            assert count > 0, f"{table} was never written"
    finally:
        await engine.stop(flatten=True)


@pytest.mark.slow
async def test_learning_runs_on_collected_data_without_touching_the_champion(
    cfg: Config,
) -> None:
    fast_learn = dataclasses.replace(
        cfg,
        learn=dataclasses.replace(cfg.learn, min_trades_for_training=40, interval_s=1e9,
                                  purge_seconds=1.0, epochs=40),
    )
    engine = await run_engine(fast_learn, seconds=30.0)
    try:
        champion_before = engine.champion.version
        champion_row_before = engine.repo.get_model(champion_before)
        report = await engine.run_learning()

        assert report["status"] in ("skipped", "shadow", "rejected")
        assert engine.champion.version == champion_before
        assert engine.repo.get_model(champion_before) == champion_row_before
        if report["status"] in ("shadow", "rejected"):
            assert report["challenger"]
            row = engine.repo.get_model(report["challenger"])
            assert row["status"] == report["status"]
            assert row["version"] != champion_before
    finally:
        await engine.stop(flatten=True)


@pytest.mark.slow
async def test_reconnect_blocks_entries_until_reconciled(cfg: Config) -> None:
    engine = await run_engine(cfg, seconds=3.0, symbols=6)
    try:
        assert engine.execution.entries_blocked is False
        await engine._on_reconnect("public-0")
        assert engine.reconciler.last_result is not None
        assert engine.reconciler.last_result.trigger == "reconnect:public-0"
        assert engine.execution.entries_blocked is False  # unblocked after a good reconcile
        assert engine.health.system_state.value == "LIVE_READY"
    finally:
        await engine.stop(flatten=True)


async def test_diagnose_answers_even_before_any_data(cfg: Config) -> None:
    clock = Clock()
    market = SimulatedMarketCore(cfg, clock, symbols=4, seed=1, step_ms=25.0)
    engine = Engine(
        cfg, clock, market=market, broker=PaperBroker(cfg, clock), db=Database(cfg.db_path)
    )
    await engine.start()
    try:
        diagnosis = engine.diagnose()
        assert "trading_allowed" in diagnosis
        assert diagnosis["symbols_in_universe"] == 4
        assert diagnosis["components"]
        # before the warm-up finishes there must be a stated reason for silence
        assert (
            diagnosis["gate_reasons"]
            or diagnosis["scan_skipped"]
            or diagnosis["top_candidates"]
            or diagnosis["tradable_symbols"] == 0
        )
    finally:
        await engine.stop(flatten=False)


async def test_live_mode_refuses_a_simulated_feed(cfg: Config) -> None:
    """The rule that makes 'never a fake feed presented as real' structural."""
    live_cfg = dataclasses.replace(cfg, mode="live")
    clock = Clock()
    market = SimulatedMarketCore(live_cfg, clock, symbols=3, seed=2, step_ms=25.0)
    engine = Engine(
        live_cfg, clock, market=market, broker=PaperBroker(live_cfg, clock),
        db=Database(live_cfg.db_path),
    )
    engine.db.open()
    components = {c.name: c for c in engine.health.components()}
    feed = components["feed_source"]
    assert feed.state.value == "DOWN"
    assert "LIVE mode requires the real Bybit feed" in feed.detail
    allowed, reasons = engine.health.gate()
    assert allowed is False
    assert any("feed_source" in reason for reason in reasons)
    engine.db.close()
