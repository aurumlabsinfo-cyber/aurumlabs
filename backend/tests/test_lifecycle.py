"""Signal lifecycle: the trigger, the countdown, and settlement.

This is the core promise of the product:

* the countdown does NOT start when the signal is created;
* it starts only when the live price touches the trigger, server-side;
* the outcome is decided by comparing the expiry price to the entry price.
"""

from __future__ import annotations

import asyncio

import pytest

from app.agents.base import Direction
from app.core.bus import EventBus, Topic
from app.core.clock import now_ms
from app.db import repository as repo
from app.db.repository import BatchWriter
from app.features.engine import FeatureEngine
from app.marketdata.engine import MarketDataEngine
from app.marketdata.types import DataSource, MarketTick
from app.signals.decision import Decision, DecisionEngine
from app.signals.lifecycle import SignalEngine, SignalStatus


@pytest.fixture(autouse=True)
def no_database(monkeypatch):
    async def _noop(*args, **kwargs):
        return None

    monkeypatch.setattr(repo, "upsert_paper_trade", _noop)
    monkeypatch.setattr(repo, "update_signal_status", _noop)


def make_engine(settings) -> SignalEngine:
    bus = EventBus()
    market = MarketDataEngine(settings, bus)
    features = FeatureEngine(settings, bus, market)
    return SignalEngine(
        settings, bus, market, features, DecisionEngine(settings), BatchWriter(settings)
    )


def set_price(engine: SignalEngine, price: float) -> None:
    engine.market.last_tick = MarketTick(
        exchange="synthetic", symbol="BTCUSDT", ts=now_ms(), exchange_ts=now_ms(),
        bid_price=price - 0.01, bid_qty=1.0, ask_price=price + 0.01, ask_qty=1.0,
        mid=price, micro_price=price, spread=0.02,
        spread_bps=0.02 / price * 10_000, last_price=price, latency_ms=10,
        book_synced=True, source=DataSource.SYNTHETIC,
    )


def make_decision(direction: Direction, ref: float, trigger: float) -> Decision:
    from app.agents.base import Regime

    return Decision(
        ts=now_ms(), symbol="BTCUSDT", exchange="synthetic", direction=direction,
        prob_up=0.7 if direction is Direction.UP else 0.2,
        prob_down=0.2 if direction is Direction.UP else 0.7,
        prob_neutral=0.1, confidence=0.78, edge=0.28, regime=Regime.TREND_UP,
        reference_price=ref, trigger_price=trigger, horizon_s=5.0,
        no_trade_reasons=[], agents=[], aggregate_score=0.5, data_quality=1.0,
    )


def create(engine: SignalEngine, direction: Direction, ref: float, trigger: float):
    return engine._create_signal(
        make_decision(direction, ref, trigger), {"features": {}}
    )


# ------------------------------------------------------------------- trigger
async def test_signal_starts_waiting_with_no_countdown(settings):
    engine = make_engine(settings)
    set_price(engine, 100_000.00)
    sig = create(engine, Direction.DOWN, 100_000.00, 99_995.00)

    assert sig.status is SignalStatus.WAITING
    assert sig.triggered_at is None
    assert sig.expires_at is None
    payload = sig.to_dict()
    # No countdown before the trigger - this is the whole point.
    assert payload["remaining_ms"] is None
    assert payload["wait_remaining_ms"] > 0


async def test_countdown_starts_only_when_price_touches_the_trigger(settings):
    engine = make_engine(settings)
    set_price(engine, 100_000.00)
    sig = create(engine, Direction.DOWN, 100_000.00, 99_995.00)

    # Price moves down but not far enough.
    set_price(engine, 99_996.00)
    engine._tick_check()
    assert sig.status is SignalStatus.WAITING
    assert sig.expires_at is None

    # Touch.
    set_price(engine, 99_995.00)
    engine._tick_check()
    assert sig.status is SignalStatus.ACTIVE
    assert sig.triggered_at is not None
    assert sig.expires_at == sig.triggered_at + 5000
    assert sig.entry_price == 99_995.00
    assert 4000 < sig.to_dict()["remaining_ms"] <= 5000


async def test_up_signal_triggers_from_below(settings):
    engine = make_engine(settings)
    set_price(engine, 100_000.00)
    sig = create(engine, Direction.UP, 100_000.00, 100_005.00)

    set_price(engine, 100_004.99)
    engine._tick_check()
    assert sig.status is SignalStatus.WAITING

    set_price(engine, 100_005.01)  # overshoot
    engine._tick_check()
    assert sig.status is SignalStatus.ACTIVE
    # Entry records the price actually observed, not the idealised trigger.
    assert sig.entry_price == 100_005.01
    assert sig.trigger_price == 100_005.00


async def test_wait_window_expiry_cancels_an_untriggered_signal(settings):
    settings.signal_wait_timeout_s = 0.05
    engine = make_engine(settings)
    set_price(engine, 100_000.00)
    sig = create(engine, Direction.DOWN, 100_000.00, 99_000.00)

    await asyncio.sleep(0.08)
    engine._tick_check()
    assert sig.status is SignalStatus.CANCELLED
    assert sig.result == "CANCELLED"
    assert engine.counters["cancelled"] == 1
    assert sig.signal_id not in engine.active


# ---------------------------------------------------------------- settlement
async def _run_to_expiry(engine, sig, entry_price, expiry_price):
    set_price(engine, entry_price)
    engine._tick_check()
    assert sig.status is SignalStatus.ACTIVE
    sig.expires_at = now_ms() - 1  # fast-forward the clock
    set_price(engine, expiry_price)
    engine._tick_check()


async def test_down_signal_wins_when_price_falls(settings):
    engine = make_engine(settings)
    set_price(engine, 100_000.00)
    sig = create(engine, Direction.DOWN, 100_000.00, 99_995.00)
    await _run_to_expiry(engine, sig, 99_995.00, 99_990.00)
    assert sig.result == "WIN"
    assert sig.status is SignalStatus.WIN
    assert engine.counters["wins"] == 1


async def test_down_signal_loses_when_price_rises(settings):
    engine = make_engine(settings)
    set_price(engine, 100_000.00)
    sig = create(engine, Direction.DOWN, 100_000.00, 99_995.00)
    await _run_to_expiry(engine, sig, 99_995.00, 99_999.00)
    assert sig.result == "LOSS"
    assert engine.counters["losses"] == 1


async def test_up_signal_wins_when_price_rises(settings):
    engine = make_engine(settings)
    set_price(engine, 100_000.00)
    sig = create(engine, Direction.UP, 100_000.00, 100_005.00)
    await _run_to_expiry(engine, sig, 100_005.00, 100_010.00)
    assert sig.result == "WIN"


async def test_unchanged_price_is_a_tie_not_a_win(settings):
    engine = make_engine(settings)
    set_price(engine, 100_000.00)
    sig = create(engine, Direction.UP, 100_000.00, 100_005.00)
    await _run_to_expiry(engine, sig, 100_005.00, 100_005.00)
    assert sig.result == "TIE"
    assert engine.counters["ties"] == 1
    assert engine.counters["wins"] == 0


async def test_pnl_is_none_when_the_payout_is_unknown(settings):
    settings.binary_payout = None
    engine = make_engine(settings)
    set_price(engine, 100_000.00)
    sig = create(engine, Direction.UP, 100_000.00, 100_005.00)
    await _run_to_expiry(engine, sig, 100_005.00, 100_010.00)
    assert sig.result == "WIN"
    assert sig.pnl_units is None  # never invent a monetary result


async def test_pnl_uses_the_configured_payout(settings):
    settings.binary_payout = 0.8
    settings.paper_stake = 1.0
    engine = make_engine(settings)
    set_price(engine, 100_000.00)
    win = create(engine, Direction.UP, 100_000.00, 100_005.00)
    await _run_to_expiry(engine, win, 100_005.00, 100_010.00)
    assert win.pnl_units == pytest.approx(0.8)

    loss = create(engine, Direction.UP, 100_000.00, 100_005.00)
    await _run_to_expiry(engine, loss, 100_005.00, 100_001.00)
    assert loss.pnl_units == pytest.approx(-1.0)


# ------------------------------------------------------------- broadcasting
async def test_lifecycle_events_are_broadcast_in_order(settings):
    engine = make_engine(settings)
    sub = engine.bus.subscribe(Topic.SIGNAL, maxsize=64)
    set_price(engine, 100_000.00)
    sig = create(engine, Direction.UP, 100_000.00, 100_005.00)
    await _run_to_expiry(engine, sig, 100_005.00, 100_010.00)

    events = []
    while not sub.queue.empty():
        events.append(sub.queue.get_nowait()["event"])
    assert events[0] == "signal_created"
    assert "trigger_hit" in events
    assert "trade_active" in events
    assert "trade_expired" in events
    assert events[-1] == "signal_settled"


async def test_every_broadcast_carries_a_server_timestamp(settings):
    engine = make_engine(settings)
    sub = engine.bus.subscribe(Topic.SIGNAL, maxsize=64)
    set_price(engine, 100_000.00)
    create(engine, Direction.UP, 100_000.00, 100_005.00)
    payload = sub.queue.get_nowait()
    # The client synchronises its countdown against this, not its own clock.
    assert payload["signal"]["server_ts"] > 0


async def test_concurrency_limit_is_respected(settings):
    settings.signal_max_concurrent = 1
    engine = make_engine(settings)
    set_price(engine, 100_000.00)
    create(engine, Direction.UP, 100_000.00, 100_005.00)
    fv = {
        "ts": now_ms(), "symbol": "BTCUSDT", "exchange": "synthetic",
        "source": "SYNTHETIC", "is_synthetic": True, "features": {},
    }
    engine.evaluate(fv)  # would create a second signal if allowed
    assert len(engine.active) == 1
