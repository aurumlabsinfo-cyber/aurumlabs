"""Agents and the decision engine, with emphasis on NO TRADE behaviour."""

from __future__ import annotations

from app.agents.base import AgentContext, Direction, Regime
from app.agents.catalog import (
    AnomalyDetector,
    MarketRegimeAgent,
    OrderBookAgent,
    OrderFlowAgent,
    build_agents,
)
from app.signals.decision import DecisionEngine


def base_features(**over) -> dict:
    f = {
        "return_100ms": 0.1, "return_250ms": 0.2, "return_500ms": 0.4,
        "return_1000ms": 0.8, "return_2000ms": 1.2, "return_3000ms": 1.4,
        "return_5000ms": 1.8, "momentum_consistency": 1.0,
        "acceleration_bps_s2": 0.2, "velocity_bps_s": 0.8,
        "mid": 100_000.0, "micro_price": 100_000.4, "micro_price_dev_bps": 0.4,
        "spread": 0.5, "spread_bps": 0.5, "relative_spread": 0.00005,
        "book_imbalance_l1": 0.5, "bid_qty_l1": 3.0, "ask_qty_l1": 1.0,
        "depth_imbalance_5": 0.45, "depth_imbalance_20": 0.35,
        "depth_notional_bid_20": 800_000.0, "depth_notional_ask_20": 600_000.0,
        "liquidity_concentration_bid": 0.2, "liquidity_concentration_ask": 0.2,
        "bid_wall_distance_bps": 12.0, "ask_wall_distance_bps": 14.0,
        "bid_wall_size": 5.0, "ask_wall_size": 5.0,
        "liquidity_removal_bid": 0.02, "liquidity_removal_ask": 0.03,
        "depth_within_5bps_bid": 12.0, "depth_within_5bps_ask": 10.0,
        "volume_imbalance_1s": 0.6, "volume_imbalance_5s": 0.5,
        "volume_imbalance_30s": 0.3, "buy_volume_1s": 4.0, "sell_volume_1s": 1.0,
        "trade_count_1s": 12.0, "trade_intensity_1s": 12.0,
        "trade_intensity_30s": 10.0, "avg_trade_size_1s": 0.3,
        "consecutive_buys": 5.0, "consecutive_sells": 0.0,
        "large_buy_notional": 250_000.0, "large_sell_notional": 50_000.0,
        "large_trade_count": 3.0,
        "realized_vol_1s_bps": 1.0, "realized_vol_5s_bps": 1.4,
        "realized_vol_30s_bps": 1.2, "vol_ratio_5s_30s": 1.17,
        "vol_acceleration": 0.2, "sigma_horizon_bps": 3.0,
        "zero_move_fraction": 0.10, "expected_move_ticks": 18.0,
        "l1_dust": False,
        "ema_9": 100_000.2, "ema_21": 99_999.8, "ema_spread_bps": 0.4,
        "rsi_14": 58.0, "vwap_60s": 99_999.5, "vwap_deviation_bps": 0.5,
        "bb_z": 0.6, "atr_14": 5.0,
        "latency_ms": 30, "book_synced": True, "data_quality": 1.0,
    }
    f.update(over)
    return f


def ctx(**over) -> AgentContext:
    f = base_features(**over)
    return AgentContext(
        ts=1_700_000_000_000, symbol="BTCUSDT", features=f,
        data_quality=f["data_quality"], book_synced=f["book_synced"],
    )


# ------------------------------------------------------------------- agents
def test_every_agent_returns_the_full_contract():
    for agent in build_agents():
        out = agent.evaluate(ctx())
        d = out.to_dict()
        for key in (
            "agent", "direction", "confidence", "score", "reason",
            "features_used", "timestamp", "data_quality",
        ):
            assert key in d, key
        assert d["direction"] in ("UP", "DOWN", "NO_TRADE")
        assert 0.0 <= d["confidence"] <= 1.0
        assert -1.0 <= d["score"] <= 1.0


def test_order_book_agent_abstains_when_book_is_desynced():
    out = OrderBookAgent().evaluate(ctx(book_synced=False))
    assert out.direction is Direction.NO_TRADE
    assert "not synchronised" in out.reason


def test_order_flow_agent_follows_aggression():
    up = OrderFlowAgent().evaluate(ctx())
    down = OrderFlowAgent().evaluate(
        ctx(volume_imbalance_1s=-0.7, volume_imbalance_5s=-0.6,
            consecutive_buys=0.0, consecutive_sells=6.0,
            large_buy_notional=10_000.0, large_sell_notional=300_000.0)
    )
    assert up.direction is Direction.UP
    assert down.direction is Direction.DOWN


def test_order_flow_abstains_on_thin_flow():
    out = OrderFlowAgent().evaluate(ctx(trade_count_1s=1.0))
    assert out.direction is Direction.NO_TRADE


def test_anomaly_detector_flags_a_price_spike():
    out = AnomalyDetector().evaluate(
        ctx(return_100ms=50.0, realized_vol_5s_bps=1.0)
    )
    assert out.extra["anomaly_detected"] is True
    assert any("spike" in a for a in out.extra["anomalies"])
    assert out.direction is Direction.NO_TRADE


def test_anomaly_detector_flags_liquidity_disappearance_and_wide_spread():
    out = AnomalyDetector().evaluate(
        ctx(liquidity_removal_bid=0.9, spread_bps=40.0, realized_vol_30s_bps=1.0)
    )
    anomalies = " ".join(out.extra["anomalies"])
    assert "bid liquidity disappeared" in anomalies
    assert "abnormal spread" in anomalies


def test_anomaly_detector_is_quiet_on_normal_data():
    out = AnomalyDetector().evaluate(ctx())
    assert out.extra["anomaly_detected"] is False


def test_regime_classification_covers_the_taxonomy():
    agent = MarketRegimeAgent()
    assert agent.classify(ctx())[0] in set(Regime)
    breakout, _, _ = agent.classify(
        ctx(realized_vol_5s_bps=6.0, realized_vol_30s_bps=2.0, return_5000ms=8.0)
    )
    assert breakout is Regime.BREAKOUT
    low, _, _ = agent.classify(
        ctx(realized_vol_5s_bps=0.3, realized_vol_30s_bps=2.0, return_5000ms=0.1)
    )
    assert low is Regime.LOW_VOLATILITY
    unknown, _, _ = agent.classify(
        ctx(realized_vol_30s_bps=None, return_5000ms=None)
    )
    assert unknown is Regime.UNKNOWN


# ---------------------------------------------------------------- decisions
def _health(quality=1.0, warm=True, reasons=None) -> dict:
    return {
        "data_quality": {
            "score": quality, "ok": True, "warmup_complete": warm,
            "reasons": reasons or [],
        }
    }


def _fv(**over) -> dict:
    return {
        "ts": 1_700_000_000_000, "symbol": "BTCUSDT", "exchange": "synthetic",
        "source": "SYNTHETIC", "is_synthetic": True, "features": base_features(**over),
    }


def _market(price=100_000.0) -> dict:
    return {"price": price}


def test_strong_bullish_state_produces_an_up_signal_with_a_trigger(settings):
    engine = DecisionEngine(settings)
    d = engine.decide(_fv(), _market(), _health())
    assert d.direction is Direction.UP
    assert d.trigger_price > d.reference_price  # must rise to the trigger
    assert 0.5 < d.confidence <= 1.0
    assert abs(d.prob_up + d.prob_down + d.prob_neutral - 1.0) < 1e-9


def test_bearish_state_places_the_trigger_below_the_price(settings):
    engine = DecisionEngine(settings)
    d = engine.decide(
        _fv(
            return_500ms=-0.5, return_1000ms=-0.9, return_2000ms=-1.3,
            return_5000ms=-2.0, momentum_consistency=-1.0,
            book_imbalance_l1=-0.5, depth_imbalance_5=-0.5, depth_imbalance_20=-0.4,
            micro_price_dev_bps=-0.4, volume_imbalance_1s=-0.7,
            volume_imbalance_5s=-0.6, consecutive_buys=0.0, consecutive_sells=6.0,
            large_buy_notional=20_000.0, large_sell_notional=300_000.0, bb_z=-0.6,
            rsi_14=42.0, ema_spread_bps=-0.4,
        ),
        _market(), _health(),
    )
    assert d.direction is Direction.DOWN
    assert d.trigger_price < d.reference_price


def test_no_trade_when_book_is_desynced(settings):
    d = DecisionEngine(settings).decide(
        _fv(book_synced=False), _market(), _health()
    )
    assert d.direction is Direction.NO_TRADE
    assert any("order book" in r for r in d.no_trade_reasons)


def test_no_trade_when_spread_is_too_wide(settings):
    d = DecisionEngine(settings).decide(
        _fv(spread_bps=25.0), _market(), _health()
    )
    assert d.direction is Direction.NO_TRADE
    assert any("spread" in r for r in d.no_trade_reasons)


def test_no_trade_when_latency_is_too_high(settings):
    d = DecisionEngine(settings).decide(
        _fv(latency_ms=5000), _market(), _health()
    )
    assert d.direction is Direction.NO_TRADE
    assert any("latency" in r for r in d.no_trade_reasons)


def test_no_trade_while_warming_up(settings):
    d = DecisionEngine(settings).decide(
        _fv(), _market(), _health(warm=False)
    )
    assert d.direction is Direction.NO_TRADE
    assert any("warming up" in r for r in d.no_trade_reasons)


def test_no_trade_on_low_data_quality(settings):
    d = DecisionEngine(settings).decide(
        _fv(data_quality=0.3), _market(),
        _health(quality=0.3, reasons=["feed slow"]),
    )
    assert d.direction is Direction.NO_TRADE


def test_no_trade_on_anomaly(settings):
    d = DecisionEngine(settings).decide(
        _fv(return_100ms=60.0, realized_vol_5s_bps=1.0), _market(), _health()
    )
    assert d.direction is Direction.NO_TRADE
    assert any("anomaly" in r for r in d.no_trade_reasons)


def test_no_trade_when_the_expected_move_is_too_small(settings):
    d = DecisionEngine(settings).decide(
        _fv(spread_bps=0.9, realized_vol_5s_bps=0.2, realized_vol_30s_bps=0.2),
        _market(), _health(),
    )
    assert d.direction is Direction.NO_TRADE
    assert any("too small" in r for r in d.no_trade_reasons)


def test_no_trade_when_price_often_does_not_move_at_all(settings):
    """The tie rate measured on real BTC at a 5s horizon was 32%."""
    d = DecisionEngine(settings).decide(
        _fv(zero_move_fraction=0.45), _market(), _health()
    )
    assert d.direction is Direction.NO_TRADE
    assert any("no price change at all" in r for r in d.no_trade_reasons)


def test_a_low_tie_rate_does_not_block_a_signal(settings):
    d = DecisionEngine(settings).decide(
        _fv(zero_move_fraction=0.05), _market(), _health()
    )
    assert d.direction is Direction.UP


def test_no_trade_when_the_expected_move_is_under_two_ticks(settings):
    d = DecisionEngine(settings).decide(
        _fv(expected_move_ticks=1.2), _market(), _health()
    )
    assert d.direction is Direction.NO_TRADE
    assert any("tick minimum" in r for r in d.no_trade_reasons)


def test_a_real_world_tight_spread_does_not_trip_the_spread_gate(settings):
    """A one-tick BTC spread is ~0.0016 bps: the gate must not fire on it."""
    d = DecisionEngine(settings).decide(
        _fv(spread_bps=0.0016), _market(), _health()
    )
    assert not any("spread" in r for r in d.no_trade_reasons)


def test_no_trade_when_agents_disagree(settings):
    """Bullish book against bearish flow must not be forced into a signal."""
    d = DecisionEngine(settings).decide(
        _fv(
            book_imbalance_l1=0.8, depth_imbalance_5=0.8,
            volume_imbalance_1s=-0.8, volume_imbalance_5s=-0.8,
            consecutive_buys=0.0, consecutive_sells=5.0,
            return_500ms=0.0, return_1000ms=0.0, return_2000ms=0.0,
            return_5000ms=0.0, momentum_consistency=0.0,
        ),
        _market(), _health(),
    )
    assert d.direction is Direction.NO_TRADE


def test_probabilities_always_form_a_distribution(settings):
    engine = DecisionEngine(settings)
    for over in ({}, {"book_synced": False}, {"spread_bps": 50.0},
                 {"volume_imbalance_1s": -0.9}):
        d = engine.decide(_fv(**over), _market(), _health())
        total = d.prob_up + d.prob_down + d.prob_neutral
        assert abs(total - 1.0) < 1e-9
        assert 0 <= d.prob_neutral <= 1


def test_trigger_offset_is_bounded_by_configuration(settings):
    settings.trigger_max_bps = 2.0
    d = DecisionEngine(settings).decide(
        _fv(sigma_horizon_bps=500.0), _market(), _health()
    )
    if d.trigger_price:
        offset_bps = abs(d.trigger_price - d.reference_price) / d.reference_price * 1e4
        assert offset_bps <= 2.0 + 1e-6
