"""End-to-end pipeline against a real PostgreSQL instance.

The feed here is the SYNTHETIC adapter, because CI has no exchange
connectivity. That proves the plumbing - order book sequencing, feature
computation, agent evaluation, persistence, WebSocket fan-out - but proves
nothing about the market, and the assertions below check that the system says
so at every layer.
"""

from __future__ import annotations

import asyncio
import os

import pytest
from sqlalchemy import text

from app.config import Settings
from app.db import repository as repo
from app.db.engine import session_scope
from app.services.container import Services, set_services

pytestmark = pytest.mark.integration

DB_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql+asyncpg://postgres:postgres@localhost:5432/btcquant_test",
)


def _settings(**over) -> Settings:
    base = dict(
        env="test",
        database_url=DB_URL,
        exchanges="synthetic",
        allow_synthetic_source=True,
        min_warmup_seconds=0.0,
        feature_interval_ms=50,
        db_flush_interval_ms=100,
        signal_min_confidence=0.55,
        signal_min_edge=0.02,
        signal_cooldown_ms=0,
        signal_wait_timeout_s=3.0,
        book_snapshot_interval_s=1,
        rate_limit_requests=100000,
    )
    base.update(over)
    return Settings(**base)


async def _truncate() -> None:
    async with session_scope() as s:
        await s.execute(
            text(
                "TRUNCATE market_ticks, trades, features, signals, paper_trades, "
                "agent_predictions, order_book_snapshots, order_book_updates, "
                "system_events, errors, performance_metrics, model_versions "
                "RESTART IDENTITY"
            )
        )


@pytest.fixture
async def services():
    svc = Services(_settings())
    set_services(svc)
    await svc.start()
    await _truncate()
    try:
        yield svc
    finally:
        await svc.stop()
        set_services(None)


async def test_pipeline_produces_live_state(services):
    await asyncio.sleep(2.5)
    m = services.market

    assert m.last_tick is not None, "no market tick received"
    assert m.counters["tickers"] > 5
    assert m.counters["trades"] > 0
    assert m.counters["depth_updates"] > 5
    assert m.book.synced is True, m.book.desync_reason
    assert m.book.stats.applied_updates > 0
    assert len(m.book.bids) > 0 and len(m.book.asks) > 0
    assert m.last_tick.bid_price < m.last_tick.ask_price


async def test_features_and_agents_run(services):
    await asyncio.sleep(2.5)
    assert services.features.computed > 10
    fv = services.features.latest
    assert fv is not None
    f = fv["features"]
    assert f["mid"] > 0
    assert f["depth_imbalance_5"] is not None  # book is synced, so depth exists
    assert f["spread_bps"] > 0

    assert services.signals.counters["decisions"] > 0
    decision = services.signals.last_decision
    assert decision is not None
    assert len(decision.agents) == 8
    assert {a.agent for a in decision.agents} == {
        "price_action", "order_book", "order_flow", "volatility", "momentum",
        "mean_reversion", "market_regime", "anomaly",
    }


async def test_rows_reach_the_database(services):
    await asyncio.sleep(2.5)
    await services.writer.flush()
    async with session_scope() as s:
        ticks = (await s.execute(text("SELECT count(*) FROM market_ticks"))).scalar_one()
        trades = (await s.execute(text("SELECT count(*) FROM trades"))).scalar_one()
        feats = (await s.execute(text("SELECT count(*) FROM features"))).scalar_one()
        sigs = (await s.execute(text("SELECT count(*) FROM signals"))).scalar_one()
    assert ticks > 5
    assert trades > 0
    assert feats > 5
    assert sigs > 0


async def test_synthetic_rows_are_flagged_everywhere(services):
    """Rule 1: never let simulator output masquerade as market data."""
    await asyncio.sleep(2.0)
    await services.writer.flush()
    async with session_scope() as s:
        for table in ("market_ticks", "trades", "features", "signals"):
            bad = (
                await s.execute(
                    text(f"SELECT count(*) FROM {table} WHERE is_synthetic = false")
                )
            ).scalar_one()
            assert bad == 0, f"{table} has rows not flagged synthetic"
    health = await services.health()
    assert health["is_synthetic"] is True
    assert "SYNTHETIC DATA" in health["synthetic_warning"]


async def test_health_reports_every_component(services):
    await asyncio.sleep(1.5)
    health = await services.health()
    for component in (
        "websocket", "api", "database", "market_data", "order_book", "model",
        "latency", "error_rate",
    ):
        assert component in health["components"], component
    assert health["components"]["database"]["status"] == "UP"
    assert health["components"]["order_book"]["status"] == "UP"
    assert health["components"]["market_data"]["status"] == "UP"


async def test_backtest_refuses_to_conclude_on_a_tiny_sample(services):
    from app.ml.runner import run_backtest

    await asyncio.sleep(2.0)
    await services.writer.flush()
    report = await run_backtest(
        services.settings, horizons=[5.0], models=["logistic_regression"],
        include_synthetic=True,
    )
    assert report["status"] in ("INSUFFICIENT_DATA", "COMPLETE")
    assert any("SYNTHETIC" in w for w in report["warnings"])
    if report["status"] == "COMPLETE":
        edge = report["horizons"]["5s"].get("edge", {})
        # A couple of seconds of simulator output can never be a proven edge.
        assert edge.get("classification") != "PROVEN EDGE"


async def test_book_diffs_are_persisted_when_enabled():
    """PERSIST_BOOK_UPDATES must actually persist book updates."""
    svc = Services(_settings(persist_book_updates=True))
    set_services(svc)
    await svc.start()
    try:
        await _truncate()
        await asyncio.sleep(2.0)
        await svc.writer.flush()
        async with session_scope() as s:
            row = (
                await s.execute(
                    text(
                        "SELECT count(*) AS n, "
                        "count(*) FILTER (WHERE applied) AS applied "
                        "FROM order_book_updates"
                    )
                )
            ).one()
        assert row.n > 5, "no depth diffs were written"
        assert row.applied > 0
    finally:
        await svc.stop()
        set_services(None)


async def test_book_diffs_are_not_persisted_by_default(services):
    """The default is off: this table is very high volume."""
    await asyncio.sleep(1.5)
    await services.writer.flush()
    async with session_scope() as s:
        n = (
            await s.execute(text("SELECT count(*) FROM order_book_updates"))
        ).scalar_one()
    assert n == 0


async def test_performance_metrics_snapshots_are_written():
    svc = Services(_settings(performance_metrics_interval_s=1))
    set_services(svc)
    await svc.start()
    try:
        await _truncate()
        # Synthetic paper trades are excluded from the performance history by
        # design, so seed a live-looking settled trade to snapshot.
        await repo.upsert_paper_trade(
            {
                "signal_id": "perf-test-1", "ts": 1_700_000_000_000,
                "exchange": "test", "symbol": svc.settings.symbol,
                "direction": "UP", "status": "WIN", "trigger_price": 100.0,
                "entry_price": 100.0, "expiry_price": 101.0, "confidence": 0.8,
                "probability_up": 0.8, "probability_down": 0.2,
                "market_regime": "TREND_UP", "horizon_s": 5.0, "result": "WIN",
                "source": "LIVE", "is_synthetic": False,
            }
        )
        await asyncio.sleep(2.5)
        await svc.writer.flush()
        async with session_scope() as s:
            scopes = [
                r[0] for r in (
                    await s.execute(text("SELECT scope FROM performance_metrics"))
                ).all()
            ]
        assert "overall" in scopes
        assert any(sc.startswith("regime:") for sc in scopes)
    finally:
        await svc.stop()
        set_services(None)


async def test_model_versions_round_trip(services):
    """A saved model must be traceable to its data window and verdict."""
    await repo.record_model_version(
        {
            "model_id": "test_model_h5s_1", "ts": 1_700_000_000_000,
            "algorithm": "logistic_regression", "horizon_s": 5.0,
            "symbol": services.settings.symbol, "feature_names": ["a", "b"],
            "train_start_ts": 1, "train_end_ts": 2, "n_train": 1000,
            "metrics": {"out_of_sample": {"accuracy": 0.52}},
            "edge_classification": "PROMISING",
            "leakage_checks": {"all_passed": True},
            "artifact_path": "/tmp/x.joblib", "is_active": False,
            "data_source": "LIVE",
        }
    )
    versions = await repo.list_model_versions()
    assert any(v["model_id"] == "test_model_h5s_1" for v in versions)
    assert await repo.set_active_model("test_model_h5s_1") is True
    assert await repo.set_active_model("does_not_exist") is False


async def test_live_data_backtest_is_empty_without_live_data(services):
    from app.ml.runner import run_backtest

    await asyncio.sleep(1.0)
    await services.writer.flush()
    report = await run_backtest(
        services.settings, horizons=[5.0], include_synthetic=False
    )
    assert report["status"] == "INSUFFICIENT_DATA"
    assert "before backtesting" in report["conclusion"]
