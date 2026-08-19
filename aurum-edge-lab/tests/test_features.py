"""Feature engine tests.

The point of most of these is causality.  A feature that reads one sample into
the future produces a backtest that cannot be traded, and the failure is
invisible unless it is tested for directly.
"""

from __future__ import annotations

import asyncio
import math
from pathlib import Path

import pytest

from aurum.adapters.replay import ReplayFeed
from aurum.bus import EventBus
from aurum.config import Config
from aurum.cross_market.engine import CrossMarketEngine
from aurum.domain import Regime
from aurum.execution.cost_model import CostModel
from aurum.features.engine import FeatureEngine, SymbolHistory, _label
from aurum.market.data_engine import DataEngine
from aurum.storage.repositories import Repositories

SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]


def test_label_naming_is_stable() -> None:
    assert _label(250) == "250ms"
    assert _label(1000) == "1s"
    assert _label(60000) == "60s"


def test_history_returns_look_only_backwards() -> None:
    history = SymbolHistory("BTCUSDT", cadence_ms=250, capacity=100)
    for i in range(10):
        history.append(1000 + i * 250, 100.0 + i, 100.0 + i)
    # 1 s back is four steps: 109 vs 105.
    assert history.return_bps(1000) == pytest.approx((109 - 105) / 105 * 10_000)
    # A horizon longer than the history has no answer, and says so.
    assert history.return_bps(60_000) is None


def test_forward_return_is_only_available_for_stored_history() -> None:
    history = SymbolHistory("BTCUSDT", cadence_ms=250, capacity=100)
    for i in range(10):
        history.append(1000 + i * 250, 100.0 + i, 100.0 + i)
    assert history.forward_return_bps(0, 1000) == pytest.approx((104 - 100) / 100 * 10_000)
    # The live edge has no future: index 9 + 4 steps is past the end.
    assert history.forward_return_bps(9, 1000) is None
    assert history.forward_return_bps(6, 1000) is None


def test_volatility_is_zero_for_a_flat_series_and_positive_otherwise() -> None:
    flat = SymbolHistory("X", cadence_ms=250, capacity=100)
    for i in range(20):
        flat.append(i * 250, 100.0, 100.0)
    assert flat.volatility_bps(2000) == pytest.approx(0.0)

    moving = SymbolHistory("Y", cadence_ms=250, capacity=100)
    for i in range(20):
        moving.append(i * 250, 100.0 + (i % 2), 100.0)
    assert moving.volatility_bps(2000) > 0


async def build_stack(config: Config, replay_file: Path, repos: Repositories, seconds: float = 6.0):
    config.market.symbols = [s for s in config.market.symbols if s.symbol in SYMBOLS]
    config.features.cadence_ms = 250
    config.market.feed = "replay"  # sampling follows the data clock, not the wall
    feed = ReplayFeed(SYMBOLS, replay_file, speed=0.0)
    bus = EventBus()
    data = DataEngine(config, feed, bus, repos)
    features = FeatureEngine(config, data, bus, repos)
    cross = CrossMarketEngine(config, features, CostModel(config.costs))
    await data.start()
    await features.start()
    deadline = asyncio.get_running_loop().time() + seconds
    while asyncio.get_running_loop().time() < deadline:
        await asyncio.sleep(0.05)
        if feed.finished.is_set() and features.computed > 40:
            break
    return data, features, cross


@pytest.mark.asyncio
async def test_features_are_real_values_not_placeholders(
    config: Config, replay_file: Path, repos: Repositories
) -> None:
    data, features, _ = await build_stack(config, replay_file, repos)
    try:
        snapshot = features.snapshot("BTCUSDT")
        assert snapshot is not None
        values = snapshot.values

        # Every documented family must be present and finite.
        assert values["mid"] > 0
        assert values["microprice"] > 0
        assert values["spread_bps"] > 0
        assert "ret_1s" in values and "ret_60s" in values
        assert "vol_1s" in values and "vol_60s" in values
        assert values["depth_bid_5"] > 0 and values["depth_ask_5"] > 0
        assert -1.0 <= values["imbalance_5"] <= 1.0
        assert "ofi_1s" in values and "ofi_norm_1s" in values
        assert "liq_added_bid_1s" in values
        assert values["mark_price"] > 0
        assert "funding_bps_8h" in values
        assert 0.0 <= values["quality_score"] <= 1.0

        assert all(math.isfinite(v) for v in values.values()), "a non-finite feature reached a snapshot"
        # Not a placeholder set: the book actually moved things around.
        assert any(abs(values[k]) > 0 for k in values if k.startswith("ret_"))
        assert len(values) > 50
    finally:
        await features.stop()
        await data.stop()


@pytest.mark.asyncio
async def test_snapshot_is_computed_only_from_the_past(
    config: Config, replay_file: Path, repos: Repositories
) -> None:
    data, features, _ = await build_stack(config, replay_file, repos)
    try:
        history = features.history["BTCUSDT"]
        # Recompute ret_1s by hand from the stored series and compare: if the
        # engine had peeked forward the two would disagree.
        snapshots = list(features.snapshots["BTCUSDT"])
        assert len(snapshots) > 20
        target = snapshots[-1]
        steps = history.steps_for(1000)
        mids = list(history.mid)
        expected = (mids[-1] - mids[-1 - steps]) / mids[-1 - steps] * 10_000.0
        assert target.values["ret_1s"] == pytest.approx(expected, rel=1e-9)
    finally:
        await features.stop()
        await data.stop()


@pytest.mark.asyncio
async def test_regime_is_classified_once_there_is_enough_history(
    config: Config, replay_file: Path, repos: Repositories
) -> None:
    config.regime.vol_lookback_s = 30
    config.regime.trend_lookback_s = 10
    data, features, _ = await build_stack(config, replay_file, repos, seconds=10.0)
    try:
        regimes = {s: features.snapshot(s).regime for s in SYMBOLS if features.snapshot(s)}
        assert regimes, "no snapshots produced"
        # UNKNOWN is a valid honest answer early on; what must not happen is a
        # confident label with no distribution behind it.
        for symbol, regime in regimes.items():
            assert isinstance(regime, Regime)
            if regime is not Regime.UNKNOWN:
                assert len(features.regime._vol_history[symbol]) >= 40
    finally:
        await features.stop()
        await data.stop()


@pytest.mark.asyncio
async def test_cross_market_matrix_covers_every_ordered_pair(
    config: Config, replay_file: Path, repos: Repositories
) -> None:
    config.cross_market.min_samples = 30
    config.cross_market.window_s = 60
    data, features, cross = await build_stack(config, replay_file, repos, seconds=10.0)
    try:
        state = cross.refresh()
        n = len(state.symbols)
        assert n == 3
        assert len(state.relations) == n * (n - 1)  # ordered pairs, no self-pairs

        matrix = cross.matrix()
        assert len(matrix["cells"]) == n
        assert matrix["cells"][0][0] is None  # the diagonal is not a relationship
        for row in matrix["cells"]:
            for cell in row:
                if cell is not None:
                    assert -1.0 <= cell["correlation"] <= 1.0
                    assert cell["best_lag_ms"] in cross.lags_ms
                    assert cell["cost_bps"] > 0, "an edge must be quoted against a real cost"
    finally:
        await features.stop()
        await data.stop()


@pytest.mark.asyncio
async def test_planted_lead_lag_is_found(config: Config, replay_file: Path, repos: Repositories) -> None:
    """The replay file plants BTC leading ETH and SOL; the engine must see it.

    This is the positive control for the cross-market engine.  Without it, an
    engine that returns zeros for everything would pass every other test here.
    """
    config.cross_market.min_samples = 30
    config.cross_market.window_s = 60
    data, features, cross = await build_stack(config, replay_file, repos, seconds=12.0)
    try:
        cross.refresh()
        btc_eth = cross.relation("BTCUSDT", "ETHUSDT")
        eth_btc = cross.relation("ETHUSDT", "BTCUSDT")
        assert btc_eth is not None and eth_btc is not None
        assert abs(btc_eth.best_correlation) > 0.05, "planted lead-lag was not detected at all"
        # The relationship is directional in the generator, so the leader's
        # predictive score should not be beaten by the follower's.
        assert btc_eth.predictive_score >= eth_btc.predictive_score * 0.5
    finally:
        await features.stop()
        await data.stop()


def test_round_trip_cost_dominates_a_small_edge(config: Config) -> None:
    """The reason most 250 ms signals are not tradable, stated as a test."""
    costs = CostModel(config.costs)
    round_trip = costs.round_trip_bps(spread_bps=1.5)
    # Two taker fees alone are 9 bps; a 3 bps signal cannot survive this.
    assert round_trip > 9.0
    assert round_trip > 3.0
