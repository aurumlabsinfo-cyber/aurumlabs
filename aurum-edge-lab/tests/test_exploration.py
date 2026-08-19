"""Exploration mode: trade without an edge, without lying about it.

The mode exists to exercise the execution path. These tests pin the two things
that make it safe to run: it waives exactly one gate and no others, and
everything it produces is marked so it can never be read as evidence of an
edge.
"""

from __future__ import annotations

import pytest

from aurum.config import Config
from aurum.domain import (
    BookLevel,
    BookSnapshot,
    Direction,
    FeatureSnapshot,
    Regime,
    RejectionReason,
)
from aurum.execution.cost_model import CostModel
from aurum.execution.exploration import STRATEGY_ID, ExplorationTrader
from aurum.risk.manager import RiskManager
from aurum.wallet.virtual_wallet import VirtualWallet


def book(mid: float = 60_000.0, spread: float = 1.0, qty: float = 50.0) -> BookSnapshot:
    half = spread / 2.0
    return BookSnapshot(
        symbol="BTCUSDT",
        ts_ms=1_000,
        recv_ms=1_005,
        bids=[BookLevel(mid - half - i, qty) for i in range(10)],
        asks=[BookLevel(mid + half + i, qty) for i in range(10)],
        last_update_id=1_000,
    )


def snapshot() -> FeatureSnapshot:
    return FeatureSnapshot(
        symbol="BTCUSDT",
        ts_ms=1_000,
        values={"volatility_bps_5000": 12.0, "return_bps_1000": 1.5},
        regime=Regime.NORMAL_RANGE,
        mid=60_000.0,
    )


@pytest.fixture
def risk(config: Config, repos) -> RiskManager:
    wallet = VirtualWallet(repos.wallet, starting_balance=100.0, currency="EUR")
    wallet.open_cycle(1)
    return RiskManager(config, wallet, CostModel(config.costs))


def evaluate(risk: RiskManager, config: Config, **overrides):
    kwargs = dict(
        symbol="BTCUSDT",
        direction=Direction.LONG,
        expected_edge_bps=0.0,
        horizon_ms=120_000,
        snapshot=snapshot(),
        book=book(),
        open_positions={},
        cycle_active=True,
        quality_ok=True,
        quality_detail="",
        usdt_per_eur=1.08,
        at_ms=10_000,
    )
    kwargs.update(overrides)
    return risk.evaluate(**kwargs)


def test_an_edgeless_entry_is_refused_by_default(risk: RiskManager, config: Config) -> None:
    """Without exploration, zero expected edge must never reach the book."""
    decision = evaluate(risk, config)
    assert not decision.allowed
    assert decision.reason is RejectionReason.EDGE_BELOW_COSTS


def test_exploration_waives_only_the_edge_gate(risk: RiskManager, config: Config) -> None:
    decision = evaluate(risk, config, require_edge=False)
    assert decision.allowed, decision.detail
    assert decision.plan is not None and decision.plan.qty > 0


@pytest.mark.parametrize(
    ("overrides", "expected"),
    [
        ({"quality_ok": False, "quality_detail": "stale"}, RejectionReason.DATA_QUALITY),
        ({"cycle_active": False}, RejectionReason.CYCLE_NOT_ACTIVE),
        ({"book": book(spread=400.0)}, RejectionReason.SPREAD_TOO_WIDE),
        ({"open_positions": {"BTCUSDT": object()}}, RejectionReason.POSITION_ALREADY_OPEN),
    ],
)
def test_exploration_still_obeys_every_other_gate(
    risk: RiskManager, config: Config, overrides: dict, expected: RejectionReason
) -> None:
    """Waiving the edge gate must not open a hole in the protective ones.

    These are the gates that stop trading through a broken feed or a failing
    cycle. An exploration entry has no edge to defend, but it has exactly the
    same power to do damage as any other, so it is held to the same rules.
    """
    decision = evaluate(risk, config, require_edge=False, **overrides)
    assert not decision.allowed
    assert decision.reason is expected


def test_exploration_sizes_smaller_than_a_real_signal(risk: RiskManager, config: Config) -> None:
    """250 round trips a day at full risk would end the cycle on costs alone."""
    full = evaluate(risk, config, require_edge=False)
    small = evaluate(risk, config, require_edge=False, risk_pct_override=0.10)
    assert full.plan is not None and small.plan is not None
    assert small.plan.notional_eur < full.plan.notional_eur


def test_the_schedule_paces_to_the_target_rate(config: Config) -> None:
    config.exploration.enabled = True
    config.exploration.trades_per_day = 288          # one every 300 s exactly
    trader = ExplorationTrader(config)
    assert trader.settings.interval_s == pytest.approx(300.0)

    # The first call arms rather than fires: enabling exploration must not open
    # a position on the very first snapshot, before the books have settled.
    assert trader.due(1_000_000) is False
    assert trader.due(1_000_000 + 299_000) is False
    assert trader.due(1_000_000 + 300_000) is True

    trader.arm_next(1_000_000 + 300_000)
    assert trader.due(1_000_000 + 300_000) is False


def test_disabled_exploration_never_fires(config: Config) -> None:
    config.exploration.enabled = False
    trader = ExplorationTrader(config)
    assert trader.enabled is False
    for ts in range(0, 10_000_000, 250_000):
        assert trader.due(ts) is False


def test_symbols_are_taken_round_robin_and_skip_open_ones(config: Config) -> None:
    """Clustering on one symbol would leave the others just as untested."""
    config.exploration.enabled = True
    trader = ExplorationTrader(config)
    picked = [trader.next_symbol(set()) for _ in range(len(trader.symbols) * 2)]
    assert picked[: len(trader.symbols)] == trader.symbols
    assert picked[len(trader.symbols) :] == trader.symbols, "did not wrap around"

    everything_open = set(trader.symbols)
    assert trader.next_symbol(everything_open) is None


def test_the_signal_admits_it_has_no_edge(config: Config) -> None:
    """The stored record is what a post-mortem will believe, so it must be honest."""
    config.exploration.enabled = True
    trader = ExplorationTrader(config)
    signal = trader.build_signal("BTCUSDT", snapshot(), cost_bps=11.4)

    assert signal.exploration is True
    assert signal.strategy_id == STRATEGY_ID
    assert signal.expected_edge_bps == 0.0
    assert signal.confidence == 0.0
    assert signal.net_edge_bps < 0, "an edgeless trade must not record a positive net edge"
    assert signal.hypothesis_id == "", "exploration must not claim a hypothesis"
    assert signal.to_dict()["exploration"] is True


def test_the_direction_is_a_coin_flip_and_is_reproducible(config: Config) -> None:
    """Anything cleverer would make the P&L look like a claim about the market."""
    config.exploration.enabled = True
    first = ExplorationTrader(config)
    second = ExplorationTrader(config)
    draws = [first.direction() for _ in range(400)]
    assert draws == [second.direction() for _ in range(400)], "seed did not reproduce the run"

    longs = sum(1 for d in draws if d is Direction.LONG)
    assert 150 < longs < 250, f"{longs}/400 long — not a coin flip"


def test_the_report_warns_about_what_the_numbers_mean(config: Config) -> None:
    config.exploration.enabled = True
    report = ExplorationTrader(config).to_dict()
    assert report["enabled"] is True
    assert report["trades_per_day_target"] == config.exploration.trades_per_day
    assert "no validated edge" in str(report["warning"]).lower()
