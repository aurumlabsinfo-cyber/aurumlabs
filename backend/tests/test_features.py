"""Feature engine: causality, rolling windows, order flow, indicators."""

from __future__ import annotations

import math

import pytest

from app.core.bus import EventBus
from app.features.engine import FeatureEngine
from app.features.indicators import atr, bollinger, ema, rsi, vwap
from app.features.rolling import Bar, BarAggregator, TimeSeries, TradeRecord, TradeWindow
from app.marketdata.engine import MarketDataEngine
from app.marketdata.types import DataSource, MarketTick, Trade


# ------------------------------------------------------------------ rolling
def test_timeseries_lookup_never_extrapolates():
    ts = TimeSeries(10_000)
    ts.append(1000, 100.0)
    ts.append(2000, 101.0)
    # Asking for a point before the series starts must return None, not the
    # first value - inventing history is how look-ahead bias creeps in.
    assert ts.value_ago(5000) is None
    assert ts.value_ago(1000) == 100.0
    assert ts.returns_since(1000) == pytest.approx((101.0 - 100.0) / 100.0 * 10_000)


def test_timeseries_trims_to_window():
    ts = TimeSeries(1000)
    for i in range(100):
        ts.append(i * 100, float(i))
    assert ts.span_ms() <= 1000
    assert ts.last == 99.0


def test_realized_vol_is_zero_for_a_flat_series():
    ts = TimeSeries(10_000)
    for i in range(50):
        ts.append(i * 100, 100.0)
    assert ts.realized_vol_bps(5000) == 0.0


def test_realized_vol_rises_with_noise():
    calm = TimeSeries(10_000)
    wild = TimeSeries(10_000)
    for i in range(60):
        calm.append(i * 100, 100.0 + (i % 2) * 0.01)
        wild.append(i * 100, 100.0 + (i % 2) * 1.0)
    assert wild.realized_vol_bps(5000) > calm.realized_vol_bps(5000)


def test_zero_move_fraction_is_one_for_a_frozen_price():
    """The tie rate: measured at 32% on real BTC over 5 seconds."""
    ts = TimeSeries(120_000)
    for i in range(400):  # 40s of history, comfortably over the lookback
        ts.append(i * 100, 100.0)
    assert ts.zero_move_fraction(5000, 20_000, tolerance=0.005) == 1.0


def test_zero_move_fraction_is_zero_for_a_steadily_moving_price():
    ts = TimeSeries(120_000)
    for i in range(400):
        ts.append(i * 100, 100.0 + i * 0.5)
    assert ts.zero_move_fraction(5000, 20_000, tolerance=0.005) == 0.0


def test_zero_move_fraction_counts_a_partly_frozen_market():
    ts = TimeSeries(240_000)
    # First half frozen, second half moving.
    for i in range(400):
        ts.append(i * 100, 100.0)
    for i in range(400, 800):
        ts.append(i * 100, 100.0 + (i - 400) * 0.5)
    # 60s lookback over an 80s series: ~20s frozen, ~40s moving.
    frac = ts.zero_move_fraction(5000, 60_000, tolerance=0.005)
    assert 0.2 < frac < 0.8


def test_zero_move_fraction_is_none_without_enough_history():
    ts = TimeSeries(120_000)
    for i in range(3):
        ts.append(i * 100, 100.0)
    assert ts.zero_move_fraction(5000, 20_000) is None


def test_trade_window_flow_and_streaks():
    w = TradeWindow(10_000)
    for i in range(5):
        w.append(TradeRecord(1000 + i * 100, 100.0, 1.0, 100.0, is_buy=True))
    w.append(TradeRecord(1600, 100.0, 3.0, 300.0, is_buy=False))
    flow = w.flow(10_000)
    assert flow["buy_volume"] == 5.0
    assert flow["sell_volume"] == 3.0
    assert flow["volume_imbalance"] == pytest.approx(2.0 / 8.0)
    buys, sells = w.consecutive()
    assert (buys, sells) == (0, 1)


def test_bar_aggregator_builds_ohlc():
    agg = BarAggregator(interval_ms=1000)
    agg.add_price(1000, 100.0)
    agg.add_price(1500, 105.0)
    agg.add_price(1900, 95.0)
    agg.add_price(2000, 101.0)  # opens a new bar, closes the first
    bars = agg.closed_bars()
    assert len(bars) == 1
    assert (bars[0].open, bars[0].high, bars[0].low, bars[0].close) == (
        100.0, 105.0, 95.0, 95.0
    )


# --------------------------------------------------------------- indicators
def test_indicators_return_none_without_enough_history():
    assert ema([1.0, 2.0], 9) is None
    assert rsi([1.0] * 5, 14) is None
    assert bollinger([1.0] * 5, 20) is None
    assert atr([], 14) is None


def test_rsi_bounds():
    rising = [float(i) for i in range(40)]
    falling = list(reversed(rising))
    assert rsi(rising, 14) > 90
    assert rsi(falling, 14) < 10


def test_vwap_weights_by_volume():
    bars = [
        Bar(0, 10, 10, 10, 10, volume=1, notional=10),
        Bar(1, 20, 20, 20, 20, volume=3, notional=60),
    ]
    assert vwap(bars) == 17.5


# ------------------------------------------------------------ feature engine
def _engine(settings) -> FeatureEngine:
    bus = EventBus()
    market = MarketDataEngine(settings, bus)
    return FeatureEngine(settings, bus, market)


def _tick(ts: int, mid: float, spread: float = 0.02) -> MarketTick:
    bid, ask = mid - spread / 2, mid + spread / 2
    return MarketTick(
        exchange="synthetic", symbol="BTCUSDT", ts=ts, exchange_ts=ts,
        bid_price=bid, bid_qty=1.0, ask_price=ask, ask_qty=1.0, mid=mid,
        micro_price=mid, spread=spread, spread_bps=spread / mid * 10_000,
        last_price=mid, latency_ms=5, book_synced=True, source=DataSource.SYNTHETIC,
    )


def test_compute_returns_none_without_data(settings):
    fe = _engine(settings)
    assert fe.compute() is None


def test_feature_vector_is_causal_and_complete(settings):
    fe = _engine(settings)
    base = 1_700_000_000_000
    for i in range(120):  # 12 seconds at 100ms
        price = 100_000.0 + i * 0.5
        tick = _tick(base + i * 100, price)
        fe.market.last_tick = tick
        fe.ingest_tick(tick)
        fe.ingest_trade(
            Trade(
                exchange="synthetic", symbol="BTCUSDT", trade_id=i, price=price,
                quantity=0.1, is_buyer_maker=False, exchange_ts=base + i * 100,
                server_ts=base + i * 100, source=DataSource.SYNTHETIC,
            )
        )
    fv = fe.compute(ts=base + 119 * 100)
    assert fv is not None
    f = fv["features"]

    # Rising price => positive returns at every horizon.
    for ms in (100, 250, 500, 1000, 2000, 3000, 5000):
        assert f[f"return_{ms}ms"] > 0, ms
    # Only aggressive buys => flow imbalance pinned at +1.
    assert f["volume_imbalance_1s"] == 1.0
    assert f["consecutive_buys"] > 0
    assert f["consecutive_sells"] == 0
    assert f["spread_bps"] > 0
    assert f["realized_vol_5s_bps"] is not None
    assert fv["is_synthetic"] is True


def test_features_marked_unavailable_when_book_is_desynced(settings):
    fe = _engine(settings)
    base = 1_700_000_000_000
    for i in range(30):
        tick = _tick(base + i * 100, 100_000.0)
        fe.market.last_tick = tick
        fe.ingest_tick(tick)
    fv = fe.compute(ts=base + 3000)
    # The book was never synced, so depth features must be None - never zero,
    # which a model would read as "balanced book".
    assert fv["features"]["depth_imbalance_5"] is None
    assert fv["features"]["book_synced"] is False


def test_dust_at_the_touch_makes_l1_imbalance_unavailable(settings):
    """Real BTC book: $96 at the ask against $10,583 at the bid.

    Reporting +0.98 there would be a strong signal manufactured out of a dust
    order, so the feature is dropped instead.
    """
    fe = _engine(settings)
    base = 1_700_000_000_000
    mid = 63_000.0
    dust = MarketTick(
        exchange="binance_spot", symbol="BTCUSDT", ts=base, exchange_ts=base,
        bid_price=mid - 0.005, bid_qty=0.16802,   # ~$10,583
        ask_price=mid + 0.005, ask_qty=0.00153,   # ~$96
        mid=mid, micro_price=mid, spread=0.01, spread_bps=0.01 / mid * 10_000,
        last_price=mid, latency_ms=5, book_synced=True, source=DataSource.LIVE,
    )
    fe.market.last_tick = dust
    fe.ingest_tick(dust)
    f = fe.compute(ts=base)["features"]
    assert f["book_imbalance_l1"] is None
    assert f["l1_dust"] is True


def test_balanced_touch_keeps_l1_imbalance(settings):
    fe = _engine(settings)
    base = 1_700_000_000_000
    mid = 63_000.0
    healthy = MarketTick(
        exchange="binance_spot", symbol="BTCUSDT", ts=base, exchange_ts=base,
        bid_price=mid - 0.005, bid_qty=0.5, ask_price=mid + 0.005, ask_qty=0.25,
        mid=mid, micro_price=mid, spread=0.01, spread_bps=0.01 / mid * 10_000,
        last_price=mid, latency_ms=5, book_synced=True, source=DataSource.LIVE,
    )
    fe.market.last_tick = healthy
    fe.ingest_tick(healthy)
    f = fe.compute(ts=base)["features"]
    assert f["book_imbalance_l1"] == pytest.approx((0.5 - 0.25) / 0.75)
    assert f["l1_dust"] is False


def test_no_infinities_leak_into_the_vector(settings):
    fe = _engine(settings)
    base = 1_700_000_000_000
    for i in range(40):
        tick = _tick(base + i * 100, 100_000.0)
        fe.market.last_tick = tick
        fe.ingest_tick(tick)
        # Buys only: sell volume stays 0, so buy/sell ratio would be infinite.
        fe.ingest_trade(
            Trade("synthetic", "BTCUSDT", i, 100_000.0, 0.1, False,
                  base + i * 100, base + i * 100, DataSource.SYNTHETIC)
        )
    f = fe.compute(ts=base + 4000)["features"]
    for key, value in f.items():
        if isinstance(value, float):
            assert not math.isinf(value), key
            assert not math.isnan(value), key
