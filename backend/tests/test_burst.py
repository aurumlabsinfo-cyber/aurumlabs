"""AURUM BURST-15: the trigger, the session, and the time-based entry.

The strategy is only worth having if its rules are actually enforced, so the
tests here are about the rules rather than about plumbing: does the tape filter
fire, does the flow have to agree, does the cooldown hold, and does a session
that hits its stop-loss really stop.
"""

from __future__ import annotations

import pandas as pd
import pytest

from app.agents.base import Direction
from app.config import Settings
from app.core.bus import EventBus
from app.core.clock import now_ms
from app.db import repository as repo
from app.db.repository import BatchWriter
from app.features.engine import FeatureEngine
from app.marketdata.engine import MarketDataEngine
from app.marketdata.types import DataSource, MarketTick
from app.ml.burst_backtest import simulate
from app.signals.burst import BurstStrategy
from app.signals.decision import DecisionEngine
from app.signals.lifecycle import SignalEngine, SignalStatus


@pytest.fixture(autouse=True)
def no_database(monkeypatch):
    async def _noop(*args, **kwargs):
        return None

    monkeypatch.setattr(repo, "upsert_paper_trade", _noop)
    monkeypatch.setattr(repo, "update_signal_status", _noop)


@pytest.fixture
def burst_settings() -> Settings:
    return Settings(
        env="test", exchanges="synthetic", allow_synthetic_source=True,
        min_warmup_seconds=0.0, signal_strategy="burst15",
        binary_payout=0.8, feature_interval_ms=100,
    )


def fv(ts: int | None = None, **over) -> dict:
    features = {
        "trade_count_5s": 60.0,
        "return_10000ms": 1.5,
        "ofi_notional_5s": 0.4,
        "mid": 100_000.0,
    }
    features.update(over)
    return {
        "ts": ts or now_ms(), "symbol": "BTCUSDT", "exchange": "synthetic",
        "source": "SYNTHETIC", "is_synthetic": True, "features": features,
    }


def market_state(price: float = 100_000.0) -> dict:
    return {"price": price, "symbol": "BTCUSDT"}


def health(quality: float = 1.0, feed_age_ms: float = 100.0) -> dict:
    return {
        "feed_age_ms": feed_age_ms,
        "data_quality": {
            "score": quality, "ok": True, "reasons": [], "warmup_complete": True
        },
    }


# --------------------------------------------------------------------- trigger
def test_fires_up_when_tape_burst_and_flow_agree(burst_settings):
    d = BurstStrategy(burst_settings).decide(fv(), market_state(), health())
    assert d.direction is Direction.UP, d.no_trade_reasons
    assert d.entry_mode == "DELAY"
    assert d.entry_delay_ms == burst_settings.burst_entry_delay_ms
    assert d.detail["strategy"] == "burst15"


def test_direction_is_the_sign_of_the_ten_second_move(burst_settings):
    d = BurstStrategy(burst_settings).decide(
        fv(return_10000ms=-1.5, ofi_notional_5s=-0.4), market_state(), health()
    )
    assert d.direction is Direction.DOWN


def test_thin_tape_blocks(burst_settings):
    d = BurstStrategy(burst_settings).decide(
        fv(trade_count_5s=5.0), market_state(), health()
    )
    assert d.direction is Direction.NO_TRADE
    assert any("tape too quiet" in r for r in d.no_trade_reasons)


def test_small_move_blocks(burst_settings):
    d = BurstStrategy(burst_settings).decide(
        fv(return_10000ms=0.1), market_state(), health()
    )
    assert d.direction is Direction.NO_TRADE
    assert any("bps" in r for r in d.no_trade_reasons)


def test_flow_must_agree_with_the_move(burst_settings):
    d = BurstStrategy(burst_settings).decide(
        fv(ofi_notional_5s=-0.6), market_state(), health()
    )
    assert d.direction is Direction.NO_TRADE
    assert any("flow disagrees" in r for r in d.no_trade_reasons)


def test_flow_agreement_can_be_switched_off(burst_settings):
    burst_settings.burst_require_ofi_agree = False
    d = BurstStrategy(burst_settings).decide(
        fv(ofi_notional_5s=-0.6), market_state(), health()
    )
    assert d.direction is Direction.UP


def test_stale_feed_blocks(burst_settings):
    d = BurstStrategy(burst_settings).decide(
        fv(), market_state(), health(feed_age_ms=9000)
    )
    assert d.direction is Direction.NO_TRADE
    assert any("stale" in r for r in d.no_trade_reasons)


def test_poor_data_quality_blocks(burst_settings):
    d = BurstStrategy(burst_settings).decide(fv(), market_state(), health(quality=0.4))
    assert d.direction is Direction.NO_TRADE
    assert any("data quality" in r for r in d.no_trade_reasons)


def test_missing_ten_second_return_blocks(burst_settings):
    d = BurstStrategy(burst_settings).decide(
        fv(return_10000ms=None), market_state(), health()
    )
    assert d.direction is Direction.NO_TRADE
    assert any("burst features unavailable" in r for r in d.no_trade_reasons)


# --------------------------------------------------------------------- session
def test_cooldown_holds_between_entries(burst_settings):
    strat = BurstStrategy(burst_settings)
    ts = now_ms()
    assert strat.decide(fv(ts), market_state(), health()).direction is Direction.UP
    strat.on_entry("sig-1", ts)
    strat.session.open_signals.clear()  # settle it so only the cooldown remains
    blocked = strat.decide(fv(ts + 1000), market_state(), health())
    assert blocked.direction is Direction.NO_TRADE
    assert any("cooldown" in r for r in blocked.no_trade_reasons)
    later = strat.decide(
        fv(ts + burst_settings.burst_cooldown_ms + 1), market_state(), health()
    )
    assert later.direction is Direction.UP


def test_only_one_open_trade_per_session(burst_settings):
    strat = BurstStrategy(burst_settings)
    ts = now_ms()
    strat.decide(fv(ts), market_state(), health())
    strat.on_entry("sig-1", ts)
    blocked = strat.decide(
        fv(ts + burst_settings.burst_cooldown_ms + 1), market_state(), health()
    )
    assert any("still open" in r for r in blocked.no_trade_reasons)


def test_stop_loss_closes_the_session(burst_settings):
    strat = BurstStrategy(burst_settings)
    ts = now_ms()
    strat.decide(fv(ts), market_state(), health())
    for i in range(6):
        strat.on_entry(f"sig-{i}", ts)
        strat.on_settled(f"sig-{i}", "LOSS", ts)
    assert strat.session.closed_reason == "session stop loss"
    blocked = strat.decide(fv(ts + 60_000), market_state(), health())
    assert blocked.direction is Direction.NO_TRADE
    assert any("no open session" in r for r in blocked.no_trade_reasons)


def test_take_profit_closes_the_session(burst_settings):
    strat = BurstStrategy(burst_settings)
    ts = now_ms()
    strat.decide(fv(ts), market_state(), health())
    for i in range(19):  # 19 wins x 0.8 = 15.2 units
        strat.on_entry(f"sig-{i}", ts)
        strat.on_settled(f"sig-{i}", "WIN", ts)
    assert strat.session.closed_reason == "session take profit"


def test_max_trades_closes_the_session(burst_settings):
    burst_settings.burst_max_trades_session = 2
    strat = BurstStrategy(burst_settings)
    ts = now_ms()
    strat.decide(fv(ts), market_state(), health())
    for i in range(2):
        strat.on_entry(f"sig-{i}", ts)
        strat.on_settled(f"sig-{i}", "TIE", ts)
    blocked = strat.decide(fv(ts + 60_000), market_state(), health())
    assert strat.session.closed_reason == "max trades reached"
    assert blocked.direction is Direction.NO_TRADE


def test_a_new_session_opens_after_the_window_elapses(burst_settings):
    strat = BurstStrategy(burst_settings)
    ts = now_ms()
    strat.decide(fv(ts), market_state(), health())
    first_start = strat.session.start_ts
    later = ts + burst_settings.burst_session_s * 1000 + 1
    d = strat.decide(fv(later), market_state(), health())
    assert strat.session.start_ts > first_start
    assert d.direction is Direction.UP
    assert strat.sessions_completed == 1


def test_a_stopped_session_does_not_reopen_early(burst_settings):
    strat = BurstStrategy(burst_settings)
    ts = now_ms()
    strat.decide(fv(ts), market_state(), health())
    strat.close_session("session stop loss", ts)
    mid_window = ts + burst_settings.burst_session_s * 1000 // 2
    d = strat.decide(fv(mid_window), market_state(), health())
    assert d.direction is Direction.NO_TRADE
    after = ts + burst_settings.burst_session_s * 1000 + 1
    assert strat.decide(fv(after), market_state(), health()).direction is Direction.UP


def test_session_pnl_uses_the_configured_payout(burst_settings):
    strat = BurstStrategy(burst_settings)
    ts = now_ms()
    strat.decide(fv(ts), market_state(), health())
    strat.on_entry("a", ts)
    strat.on_settled("a", "WIN", ts)
    assert strat.session.pnl_units == pytest.approx(0.8)
    assert strat.payout_is_assumed is False


def test_unknown_payout_is_flagged_as_assumed():
    s = Settings(
        env="test", exchanges="synthetic", allow_synthetic_source=True,
        signal_strategy="burst15", binary_payout=None,
    )
    strat = BurstStrategy(s)
    assert strat.payout_is_assumed is True
    assert strat.payout == s.burst_assumed_payout
    assert "PAYOUT UNKNOWN" in strat.status()["note"]


# ------------------------------------------------------------ live integration
def make_engine(settings) -> SignalEngine:
    bus = EventBus()
    market = MarketDataEngine(settings, bus)
    features = FeatureEngine(settings, bus, market)
    return SignalEngine(
        settings, bus, market, features, DecisionEngine(settings), BatchWriter(settings)
    )


def set_price(engine: SignalEngine, price: float) -> None:
    ts = now_ms()
    engine.market.last_tick = MarketTick(
        exchange="synthetic", symbol="BTCUSDT", ts=ts, exchange_ts=ts,
        bid_price=price - 0.01, bid_qty=1.0, ask_price=price + 0.01, ask_qty=1.0,
        mid=price, micro_price=price, spread=0.02,
        spread_bps=0.02 / price * 10_000, last_price=price, latency_ms=10,
        book_synced=True, source=DataSource.SYNTHETIC,
    )
    engine.market.started_at = ts - 120_000


async def test_engine_routes_to_burst_and_enters_on_time(burst_settings, monkeypatch):
    engine = make_engine(burst_settings)
    set_price(engine, 100_000.0)
    monkeypatch.setattr(engine.market.book, "synced", True, raising=False)

    decision = engine.evaluate(fv())
    assert decision.direction is Direction.UP, decision.no_trade_reasons
    assert len(engine.active) == 1
    sig = next(iter(engine.active.values()))
    assert sig.status is SignalStatus.WAITING
    assert sig.strategy == "burst15"

    # Before the delay elapses nothing happens; after it, entry is at market.
    engine._tick_check()
    assert sig.status is SignalStatus.WAITING
    sig.created_at -= burst_settings.burst_entry_delay_ms + 1
    set_price(engine, 100_010.0)
    engine._tick_check()
    assert sig.status is SignalStatus.ACTIVE
    assert sig.entry_price == pytest.approx(100_010.0)


async def test_diagnostics_rank_the_binding_gate(burst_settings):
    engine = make_engine(burst_settings)
    set_price(engine, 100_000.0)
    for i in range(5):
        engine.evaluate(fv(now_ms() + i, trade_count_5s=1.0))
    diag = engine.diagnostics()
    assert diag["strategy"] == "burst15"
    assert diag["signals_emitted"] == 0
    gates = {g["gate"]: g["count"] for g in diag["blocking_gates"]}
    tape = next(g for g in gates if "tape too quiet" in g)
    # Measurements are collapsed out of the key, so five windows are one gate.
    assert gates[tape] == 5
    assert diag["decisions_evaluated"] == 5


# ------------------------------------------------------------------- backtest
def _series(n: int, step_bps: float, start_ts: int = 1_700_000_000_000):
    """A tick series that moves `step_bps` per second, and matching features."""
    price = 100_000.0
    ticks = []
    for i in range(n * 10):  # 100ms ticks
        price *= 1 + step_bps / 10_000.0 / 10.0
        ticks.append({"ts": start_ts + i * 100, "mid": price})
    feats = [
        {
            "ts": start_ts + i * 1000,
            "mid": ticks[i * 10]["mid"],
            "trade_count_5s": 60.0,
            "return_10000ms": step_bps * 10.0,
            "ofi_notional_5s": 0.5 if step_bps > 0 else -0.5,
        }
        for i in range(10, n - 10)
    ]
    return pd.DataFrame(feats), pd.DataFrame(ticks)


def test_backtest_wins_on_a_rising_series(burst_settings):
    features, ticks = _series(600, step_bps=1.0)
    report = simulate(features, ticks, burst_settings)
    assert report["status"] == "COMPLETE"
    assert report["trades"] > 0
    # A monotonically rising series must be won by an UP-only rule.
    assert report["win_rate_decided"] == 1.0
    assert report["pnl_units"] > 0
    assert report["payout_status"] == "KNOWN"


def test_backtest_respects_the_cooldown(burst_settings):
    features, ticks = _series(600, step_bps=1.0)
    report = simulate(features, ticks, burst_settings)
    # One entry per cooldown at most, and the session cap on top of that.
    span_ms = int(features["ts"].iloc[-1] - features["ts"].iloc[0])
    assert report["trades"] <= span_ms // burst_settings.burst_cooldown_ms + 1


def test_backtest_reports_missing_features_instead_of_guessing(burst_settings):
    features, ticks = _series(200, step_bps=1.0)
    report = simulate(features.drop(columns=["return_10000ms"]), ticks, burst_settings)
    assert report["status"] == "MISSING_FEATURES"
    assert "return_10000ms" in report["missing"]


def test_backtest_says_so_when_the_rule_never_fires(burst_settings):
    features, ticks = _series(200, step_bps=1.0)
    features["trade_count_5s"] = 1.0
    report = simulate(features, ticks, burst_settings)
    assert report["status"] == "NO_TRADES"
    assert report["triggers"] == 0
