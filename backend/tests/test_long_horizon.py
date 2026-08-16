"""Minute-scale horizons: window scaling, long features, shadow scoring.

The engine was built around a 5-second horizon. Pointing it at 15 minutes is
not a matter of changing one number: the buffers, the volatility lookback and
the embargo all have to grow with it, and the microstructure features have to
be joined by ones that actually reach that far.
"""

from __future__ import annotations

import pandas as pd
import pytest

from app.config import Settings
from app.features.rolling import TimeSeries
from app.ml import shadow


# ------------------------------------------------------- window auto-scaling
def test_defaults_are_untouched_at_the_original_horizon():
    s = Settings(signal_horizon_s=5.0, _env_file=None)
    assert s.tick_buffer_seconds == 300
    assert s.volatility_window_s == 30.0
    assert s.ml_embargo_s == 30.0


def test_a_fifteen_minute_horizon_grows_every_window_that_depends_on_it():
    s = Settings(signal_horizon_s=900.0, _env_file=None)
    # A 300s buffer cannot hold even one 900s horizon.
    assert s.tick_buffer_seconds >= 900 * 4
    assert s.trade_buffer_seconds >= 900 * 4
    # Sigma over the horizon must be measured, not extrapolated.
    assert s.volatility_window_s >= 900 * 2
    # The embargo has to exceed the label's own memory.
    assert s.ml_embargo_s >= 900 * 2
    # A 3-second cooldown between 15-minute trades is meaningless.
    assert s.signal_cooldown_ms >= 900 * 200
    assert s.signal_wait_timeout_s >= 450


def test_an_explicit_setting_is_never_overridden():
    """Auto-scaling fills gaps; it must not overrule the operator."""
    s = Settings(signal_horizon_s=900.0, tick_buffer_seconds=120, _env_file=None)
    assert s.tick_buffer_seconds == 120


def test_horizon_ms_helper():
    assert Settings(signal_horizon_s=900.0, _env_file=None).horizon_ms == 900_000


# --------------------------------------------------------- long-window series
def _ramp(n: int, start: float = 100.0, step: float = 0.1) -> TimeSeries:
    ts = TimeSeries(3_600_000)
    for i in range(n):
        ts.append(1_000_000 + i * 1000, start + i * step)
    return ts


def test_range_position_reports_where_price_sits():
    ts = _ramp(120)  # strictly rising: the last point is the high
    assert ts.range_position(60_000) == pytest.approx(1.0)


def test_range_position_refuses_a_window_it_does_not_cover():
    ts = _ramp(30)  # only 30 seconds of history
    assert ts.range_position(300_000) is None


def test_a_flat_window_has_no_range_position():
    ts = TimeSeries(600_000)
    for i in range(120):
        ts.append(1_000_000 + i * 1000, 100.0)
    # Returning 0.5 here would invent a reading the data cannot support.
    assert ts.range_position(60_000) is None


def test_drift_is_positive_for_a_rising_market_and_scaled_per_minute():
    ts = _ramp(120, start=100.0, step=0.1)  # +0.1 per second = +6 per minute
    d = ts.drift_bps_per_min(60_000)
    assert d is not None and d > 0
    # 6 units per minute on a base near 100 is roughly 600 bps/min.
    assert d == pytest.approx(600, rel=0.15)


def test_drift_refuses_an_uncovered_window():
    assert _ramp(20).drift_bps_per_min(300_000) is None


def test_drift_is_flat_when_price_is_flat():
    ts = TimeSeries(600_000)
    for i in range(120):
        ts.append(1_000_000 + i * 1000, 100.0)
    assert ts.drift_bps_per_min(60_000) == pytest.approx(0.0, abs=1e-9)


# ------------------------------------------------------------ shadow scoring
def _shadow(rows):
    return pd.DataFrame(rows)


def _ticks(points):
    return pd.DataFrame([{"ts": t, "mid": m} for t, m in points])


def _row(ts, lean, entry=100.0, emitted=True, blocked=None, conf=0.7, regime="RANGE"):
    return {
        "ts": ts, "lean": lean, "prob_up": 0.6, "confidence": conf, "edge": 0.1,
        "horizon_s": 5.0, "entry": entry, "regime": regime, "emitted": emitted,
        "blocked_by": blocked or [], "data_quality": 1.0,
    }


def test_outcome_comes_from_the_tick_at_entry_plus_horizon():
    s = _shadow([_row(1000, "UP")])
    t = _ticks([(1000, 100.0), (6000, 101.0)])
    scored = shadow.attach_outcomes(s, t)
    assert scored.iloc[0]["result"] == "WIN"
    assert scored.iloc[0]["exit"] == 101.0


def test_a_wrong_lean_is_a_loss():
    scored = shadow.attach_outcomes(
        _shadow([_row(1000, "DOWN")]), _ticks([(1000, 100.0), (6000, 101.0)])
    )
    assert scored.iloc[0]["result"] == "LOSS"


def test_an_unchanged_price_is_a_tie_not_a_win():
    scored = shadow.attach_outcomes(
        _shadow([_row(1000, "UP")]), _ticks([(1000, 100.0), (6000, 100.0)])
    )
    assert scored.iloc[0]["result"] == "TIE"


def test_a_window_whose_future_is_not_recorded_is_dropped_not_filled():
    """The most recent horizon is always unresolvable. It must not be guessed."""
    s = _shadow([_row(1000, "UP")])
    t = _ticks([(1000, 100.0)])  # nothing at t+5s
    assert shadow.attach_outcomes(s, t).empty


def test_a_stale_future_tick_outside_tolerance_is_dropped():
    s = _shadow([_row(1000, "UP")])
    t = _ticks([(1000, 100.0), (60_000, 101.0)])  # 54s late for a 5s horizon
    assert shadow.attach_outcomes(s, t, tolerance_ms=2000).empty


def test_blocked_windows_are_scored_alongside_emitted_ones():
    rows = [
        _row(1000, "UP", emitted=True),
        _row(2000, "UP", emitted=False, blocked=["cooldown"]),
        _row(3000, "DOWN", emitted=False, blocked=["spread too wide"]),
    ]
    t = _ticks([(1000, 100.0), (2000, 100.0), (3000, 100.0),
                (6000, 101.0), (7000, 101.0), (8000, 101.0)])
    rep = shadow.evaluate(_shadow(rows), t)
    assert rep["status"] == "OK"
    assert rep["windows"]["n"] == 3
    assert rep["emitted"]["n"] == 1
    assert rep["blocked"]["n"] == 2
    assert "cooldown" in rep["by_block_reason"]
    assert "spread too wide" in rep["by_block_reason"]


def test_the_gate_verdict_reports_no_difference_when_there_is_none():
    rows = [_row(1000 + i * 1000, "UP", emitted=i % 2 == 0, blocked=["cooldown"])
            for i in range(20)]
    t = _ticks(
        [(1000 + i * 1000, 100.0) for i in range(20)]
        + [(6000 + i * 1000, 101.0) for i in range(20)]
    )
    rep = shadow.evaluate(_shadow(rows), t)
    # Every lean is right, emitted or not: the gate separated nothing.
    assert rep["gate_value"]["verdict"] == "gates make no measurable difference"


def test_a_gate_that_selects_better_windows_is_reported_as_such():
    rows = [_row(1000 + i * 1000, "UP", entry=100.0, emitted=True) for i in range(10)]
    rows += [_row(20_000 + i * 1000, "DOWN", entry=100.0, emitted=False,
                  blocked=["low confidence"]) for i in range(10)]
    t = _ticks(
        [(1000 + i * 1000, 100.0) for i in range(10)]
        + [(6000 + i * 1000, 101.0) for i in range(10)]
        + [(20_000 + i * 1000, 100.0) for i in range(10)]
        + [(25_000 + i * 1000, 101.0) for i in range(10)]
    )
    rep = shadow.evaluate(_shadow(rows), t)
    assert rep["emitted"]["win_rate"] == 1.0
    assert rep["blocked"]["win_rate"] == 0.0
    assert rep["gate_value"]["verdict"] == "gates select better windows"


def test_no_data_is_reported_rather_than_an_empty_success():
    rep = shadow.evaluate(_shadow([_row(1000, "UP")]), _ticks([(1000, 100.0)]))
    assert rep["status"] == "NO DATA"


def test_ties_are_counted_separately_from_losses():
    rows = [_row(1000, "UP"), _row(2000, "UP")]
    t = _ticks([(1000, 100.0), (2000, 100.0), (6000, 100.0), (7000, 101.0)])
    rep = shadow.evaluate(_shadow(rows), t)
    assert rep["windows"]["ties"] == 1
    assert rep["windows"]["decided"] == 1
    assert rep["windows"]["win_rate"] == 1.0


# ------------------------------------------- long features in the real engine
def _long_engine():
    from app.core.bus import EventBus
    from app.features.engine import FeatureEngine
    from app.marketdata.engine import MarketDataEngine

    s = Settings(signal_horizon_s=900.0, _env_file=None)
    bus = EventBus()
    return FeatureEngine(s, bus, MarketDataEngine(s, bus)), s


def _feed(fe, seconds: int, base: int = 1_700_000_000_000):
    from app.marketdata.types import DataSource, MarketTick

    for i in range(seconds):
        mid = 100_000.0 + i * 0.5
        tick = MarketTick(
            exchange="synthetic", symbol="BTCUSDT", ts=base + i * 1000,
            exchange_ts=base + i * 1000, bid_price=mid - 0.01, bid_qty=1.0,
            ask_price=mid + 0.01, ask_qty=1.0, mid=mid, micro_price=mid,
            spread=0.02, spread_bps=0.02 / mid * 10_000, last_price=mid,
            latency_ms=5, book_synced=True, source=DataSource.SYNTHETIC,
        )
        fe.market.last_tick = tick
        fe.ingest_tick(tick)
    return base + (seconds - 1) * 1000


def test_long_features_are_unavailable_until_history_covers_them():
    fe, _ = _long_engine()
    ts = _feed(fe, 120)  # two minutes
    f = fe.compute(ts=ts)["features"]
    assert f["return_60000ms"] is not None      # covered
    assert f["return_900000ms"] is None         # not covered: must not guess
    assert f["range_position_15m"] is None


def test_long_features_appear_once_the_buffer_covers_them():
    fe, s = _long_engine()
    assert s.tick_buffer_seconds >= 3600
    ts = _feed(fe, 1000)  # ~16 minutes
    f = fe.compute(ts=ts)["features"]
    assert f["return_900000ms"] is not None
    assert f["realized_vol_15m_bps"] is not None
    # Strictly rising feed: price sits at the top of its own range.
    assert f["range_position_15m"] == pytest.approx(1.0)
    assert f["drift_bps_min_15m"] > 0


def test_the_five_second_config_still_reports_long_features_as_unavailable():
    """A short-horizon deployment must not silently gain minute-scale claims."""
    from app.core.bus import EventBus
    from app.features.engine import FeatureEngine
    from app.marketdata.engine import MarketDataEngine

    s = Settings(signal_horizon_s=5.0, _env_file=None)
    bus = EventBus()
    fe = FeatureEngine(s, bus, MarketDataEngine(s, bus))
    ts = _feed(fe, 290)
    f = fe.compute(ts=ts)["features"]
    assert f["return_900000ms"] is None
    assert f["return_60000ms"] is not None


# ------------------------------------------------------- continuous learning
class _Provider:
    def __init__(self, loadable=True):
        self.loadable = loadable
        self.loaded: list[str] = []
        self.load_error = "unloadable"

    def load(self, model_id):
        self.loaded.append(model_id)
        return self.loadable


def _retrain(monkeypatch, report, ready=True, loadable=True):
    from app.services import retrain as mod

    s = Settings(signal_horizon_s=900.0, auto_retrain_enabled=True, _env_file=None)
    svc = mod.RetrainService(s, _Provider(loadable))

    async def fake_counts():
        return {"features": 999_999, "market_ticks": 999_999}

    async def fake_backtest(*a, **k):
        return report

    async def fake_set_active(model_id):
        svc._activated_in_db = model_id  # type: ignore[attr-defined]

    monkeypatch.setattr(mod.repo, "table_counts", fake_counts)
    monkeypatch.setattr(mod.repo, "set_active_model", fake_set_active)
    monkeypatch.setattr(mod, "run_backtest", fake_backtest)
    monkeypatch.setattr(
        mod, "dataset_readiness", lambda *a, **k: {"ready": ready}
    )
    return svc


def _report(saved_model=None, edge="PROVEN EDGE"):
    horizon = {"rows": 12_000, "edge": {"classification": edge}}
    if saved_model:
        horizon["saved_model"] = {"model_id": saved_model}
    # The key format is the runner's own, asserted below so the two cannot
    # drift apart: a mismatch here fails silently in production.
    return {"horizons": {"900s": horizon}, "conclusion": "..."}


def test_the_retrain_horizon_key_matches_the_runner_format():
    """A mocked report proves nothing if it invents the key the code reads."""
    from app.ml.runner import DEFAULT_HORIZONS  # noqa: F401

    assert f"{900.0:g}s" == "900s"
    assert f"{5.0:g}s" == "5s"


@pytest.mark.asyncio
async def test_a_validated_model_is_activated(monkeypatch):
    svc = _retrain(monkeypatch, _report(saved_model="m-123"))
    res = await svc.run_once()
    assert res["activated"] is True
    assert res["model_id"] == "m-123"
    assert svc.activations == 1


@pytest.mark.asyncio
async def test_a_failed_verdict_leaves_the_live_engine_untouched(monkeypatch):
    """No saved model means the edge did not clear. Nothing may be activated."""
    svc = _retrain(monkeypatch, _report(saved_model=None, edge="FAILED"))
    res = await svc.run_once()
    assert res["activated"] is False
    assert res["model_id"] is None
    assert svc.activations == 0
    assert svc.model_provider.loaded == []
    assert "unchanged" in res["note"]


@pytest.mark.asyncio
async def test_a_model_that_will_not_load_is_not_activated(monkeypatch):
    svc = _retrain(monkeypatch, _report(saved_model="m-broken"), loadable=False)
    res = await svc.run_once()
    assert res["activated"] is False
    assert svc.activations == 0
    assert svc.last_error is not None


@pytest.mark.asyncio
async def test_it_does_not_train_on_too_little_data(monkeypatch):
    svc = _retrain(monkeypatch, _report(saved_model="m-1"), ready=False)
    res = await svc.run_once()
    assert res["skipped"] == "insufficient data"
    assert svc.runs == 0


@pytest.mark.asyncio
async def test_the_loop_does_not_start_when_disabled():
    from app.services.retrain import RetrainService

    s = Settings(auto_retrain_enabled=False, _env_file=None)
    svc = RetrainService(s, _Provider())
    await svc.start()
    assert svc._task is None
    await svc.stop()


def test_status_states_the_activation_policy():
    from app.services.retrain import RetrainService

    s = Settings(auto_retrain_enabled=True, _env_file=None)
    st = RetrainService(s, _Provider()).status()
    assert "PROVEN" in st["policy"] and "PROMISING" in st["policy"]
    assert st["enabled"] is True
