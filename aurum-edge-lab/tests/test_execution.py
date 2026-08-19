"""Wallet, risk and PaperBroker.

Several of these are blueprint acceptance checks stated as tests: the wallet
initialises at exactly €100.00, a controlled trade moves the ledger with
explicit fees and slippage, and no reachable path can place a live order.
"""

from __future__ import annotations

import pytest

from aurum.config import Config
from aurum.domain import (
    BookLevel,
    BookSnapshot,
    Direction,
    ExitReason,
    FeatureSnapshot,
    Regime,
    RejectionReason,
    Side,
    Signal,
)
from aurum.execution.cost_model import CostModel
from aurum.execution.paper_broker import PaperBroker
from aurum.risk.manager import RiskManager
from aurum.storage.repositories import Repositories
from aurum.wallet.virtual_wallet import InsufficientFunds, VirtualWallet


def book(bid: float = 60000.0, ask: float = 60001.0, qty: float = 5.0, levels: int = 10) -> BookSnapshot:
    return BookSnapshot(
        symbol="BTCUSDT",
        ts_ms=1_000_000,
        recv_ms=1_000_002,
        bids=[BookLevel(bid - i, qty) for i in range(levels)],
        asks=[BookLevel(ask + i, qty) for i in range(levels)],
        last_update_id=1,
        is_crossed=bid >= ask,
    )


def features(**overrides: float) -> FeatureSnapshot:
    values = {
        "mid": 60000.5, "spread_bps": 0.167, "vol_1s": 3.0, "vol_5s": 6.0, "vol_10s": 8.0,
        "vol_30s": 12.0, "vol_60s": 18.0, "ofi_1s": 0.5, "imbalance_5": 0.2, "quality_score": 0.95,
    }
    values.update(overrides)
    return FeatureSnapshot(
        symbol="BTCUSDT", ts_ms=1_000_000, values=values, regime=Regime.NORMAL_RANGE,
        quality_score=0.95, tradable=True, mid=60000.5, microprice=60000.6, spread_bps=0.167,
    )


def signal(edge_bps: float = 40.0, direction: Direction = Direction.LONG) -> Signal:
    return Signal(
        signal_id="sig-1", ts_ms=1_000_000, strategy_id="strat-1", hypothesis_id="hyp-1",
        symbol="BTCUSDT", direction=direction, confidence=0.7, expected_edge_bps=edge_bps,
        expected_cost_bps=11.0, horizon_ms=5_000, regime=Regime.NORMAL_RANGE, accepted=True,
    )


@pytest.fixture
def stack(config: Config, repos: Repositories):
    wallet = VirtualWallet(repos.wallet, starting_balance=config.wallet.starting_balance_eur)
    wallet.open_cycle(1)
    costs = CostModel(config.costs)
    risk = RiskManager(config, wallet, costs)
    broker = PaperBroker(config, wallet, costs, repos.execution)
    return config, wallet, costs, risk, broker, repos


# --------------------------------------------------------------------- wallet


def test_wallet_initialises_at_exactly_one_hundred_euro(stack) -> None:
    _, wallet, *_ = stack
    assert wallet.state.balance == 100.00
    assert wallet.state.equity == 100.00
    assert wallet.state.available == 100.00
    assert wallet.state.currency == "EUR"


def test_every_movement_is_written_to_the_ledger(stack) -> None:
    _, wallet, _, _, _, repos = stack
    wallet.reserve(20.0, "pos-x")
    wallet.charge_fee(0.05, "pos-x")
    wallet.settle(1.25, "pos-x")
    wallet.release(20.0, "pos-x")

    entries = repos.wallet.list_ledger(cycle_id=1)
    kinds = [e["kind"] for e in entries]
    assert "CYCLE_START" in kinds
    assert "RESERVE" in kinds and "RELEASE" in kinds
    assert "ENTRY_FEE" in kinds and "REALIZED_PNL" in kinds
    # The ledger's final balance must agree with the wallet's.
    latest = entries[0]
    assert latest["balance_after"] == pytest.approx(wallet.state.balance, abs=1e-8)


def test_reserving_more_than_available_raises_rather_than_clamping(stack) -> None:
    _, wallet, *_ = stack
    with pytest.raises(InsufficientFunds):
        wallet.reserve(150.0, "pos-y")
    assert wallet.state.reserved == 0.0


def test_drawdown_is_measured_against_peak_equity(stack) -> None:
    _, wallet, *_ = stack
    wallet.mark_unrealized(40.0)          # equity 140
    assert wallet.state.peak_equity == pytest.approx(140.0)
    wallet.mark_unrealized(0.0)
    wallet.settle(-5.0, "pos-z")          # balance 95
    assert wallet.state.drawdown_pct == pytest.approx((140 - 95) / 140 * 100, rel=1e-6)


def test_a_new_cycle_starts_at_one_hundred_and_keeps_history(stack) -> None:
    _, wallet, _, _, _, repos = stack
    wallet.settle(-60.0, "pos-a")
    assert wallet.state.balance == pytest.approx(40.0)
    wallet.open_cycle(2)
    assert wallet.state.balance == 100.00
    assert wallet.state.cycle_id == 2
    # Cycle 1's ledger is untouched.
    assert len(repos.wallet.list_ledger(cycle_id=1)) >= 2


# ----------------------------------------------------------------- cost model


def test_market_order_walks_the_book_instead_of_taking_the_touch(config: Config) -> None:
    costs = CostModel(config.costs)
    thin = book(qty=1.0)
    fill = costs.simulate_market_order(thin, Side.BUY, qty=3.5)
    assert fill.fully_filled
    assert fill.levels_consumed == 4          # 1.0 + 1.0 + 1.0 + 0.5
    assert fill.fill_price > thin.best_ask    # worse than the touch, as it must be
    assert fill.slippage_bps > 0


def test_an_order_larger_than_the_book_is_not_filled(config: Config) -> None:
    costs = CostModel(config.costs)
    fill = costs.simulate_market_order(book(qty=1.0, levels=3), Side.BUY, qty=100.0)
    assert not fill.fully_filled


# ------------------------------------------------------------------ risk gates


def _evaluate(risk: RiskManager, broker: PaperBroker, config: Config, **kwargs):
    defaults = dict(
        symbol="BTCUSDT", direction=Direction.LONG, expected_edge_bps=40.0, horizon_ms=5_000,
        snapshot=features(), book=book(), open_positions=broker.open_positions(),
        cycle_active=True, quality_ok=True, quality_detail="ok",
        usdt_per_eur=config.fx.usdt_per_eur, at_ms=2_000_000,
    )
    defaults.update(kwargs)
    return risk.evaluate(**defaults)


def test_a_clean_signal_is_authorised_with_risk_first_sizing(stack) -> None:
    config, wallet, _, risk, broker, _ = stack
    decision = _evaluate(risk, broker, config)
    assert decision.allowed, decision.detail
    plan = decision.plan
    assert plan is not None
    # 1% of 100 EUR is 1 EUR of risk; notional = risk / stop distance.
    assert plan.risk_eur_target == pytest.approx(1.0)
    round_trip = CostModel(config.costs).round_trip_bps(book().spread_bps())
    assert plan.stop_bps >= 2 * round_trip - 1e-9, "a stop tighter than the round trip is not a stop"
    # The caps bind at these horizons, so the position is sized down and says so.
    assert plan.capped_by in {"", "max_leverage", "max_symbol_exposure_pct", "max_exposure_pct",
                              "available_margin"}
    assert plan.risk_eur_effective == pytest.approx(plan.notional_eur * plan.stop_bps / 10_000, rel=1e-6)
    assert plan.risk_eur_effective <= plan.risk_eur_target + 1e-9


def test_data_quality_blocks_regardless_of_confidence(stack) -> None:
    config, _, _, risk, broker, _ = stack
    decision = _evaluate(risk, broker, config, expected_edge_bps=500.0, quality_ok=False,
                         quality_detail="feed state is STALE")
    assert not decision.allowed
    assert decision.reason is RejectionReason.DATA_QUALITY
    assert "STALE" in decision.detail


def test_edge_below_the_round_trip_is_rejected(stack) -> None:
    config, _, _, risk, broker, _ = stack
    decision = _evaluate(risk, broker, config, expected_edge_bps=3.0)
    assert not decision.allowed
    assert decision.reason is RejectionReason.EDGE_BELOW_COSTS
    assert "round trip" in decision.detail


def test_crossed_and_wide_books_are_rejected(stack) -> None:
    config, _, _, risk, broker, _ = stack
    crossed = book(bid=60002.0, ask=60001.0)
    assert _evaluate(risk, broker, config, book=crossed).reason is RejectionReason.CROSSED_BOOK
    wide = book(bid=60000.0, ask=60400.0)
    assert _evaluate(risk, broker, config, book=wide).reason is RejectionReason.SPREAD_TOO_WIDE


def test_circuit_breakers_fire_before_anything_else(stack) -> None:
    config, wallet, _, risk, broker, _ = stack
    wallet.mark_unrealized(0.0)
    wallet.settle(-30.0, "pos-loss")   # 30% drawdown, limit is 25%
    decision = _evaluate(risk, broker, config)
    assert decision.reason is RejectionReason.MAX_DRAWDOWN

    risk.wallet.open_cycle(2)
    risk.block_entries("post-mortem in progress")
    assert _evaluate(risk, broker, config).reason is RejectionReason.ENTRIES_BLOCKED


def test_cooldown_and_duplicate_suppression(stack) -> None:
    config, _, _, risk, broker, _ = stack
    risk.record_signal("BTCUSDT", Direction.LONG, at_ms=2_000_000)
    soon = _evaluate(risk, broker, config, at_ms=2_001_000)
    assert soon.reason is RejectionReason.COOLDOWN
    # After the cooldown but inside the duplicate window, same direction only.
    config.risk.cooldown_s = 1.0
    dup = _evaluate(risk, broker, config, at_ms=2_010_000, direction=Direction.LONG)
    assert dup.reason is RejectionReason.DUPLICATE_SIGNAL
    other = _evaluate(risk, broker, config, at_ms=2_010_000, direction=Direction.SHORT)
    assert other.allowed, other.detail


def test_position_limits_and_exposure(stack) -> None:
    config, _, _, risk, broker, _ = stack
    config.risk.max_concurrent_positions = 1
    decision = _evaluate(risk, broker, config)
    assert decision.allowed
    broker.open(signal(), decision.plan, book(), features(), cycle_id=1, at_ms=2_000_000)

    same = _evaluate(risk, broker, config, open_positions=broker.open_positions())
    assert same.reason is RejectionReason.POSITION_ALREADY_OPEN

    others = dict(broker.open_positions())
    others["ETHUSDT"] = others.pop("BTCUSDT")
    assert _evaluate(risk, broker, config, open_positions=others).reason is RejectionReason.MAX_POSITIONS


def test_thin_book_is_rejected_for_liquidity(stack) -> None:
    config, _, _, risk, broker, _ = stack
    tiny = book(qty=0.00001)
    decision = _evaluate(risk, broker, config, book=tiny)
    assert decision.reason is RejectionReason.INSUFFICIENT_LIQUIDITY


# --------------------------------------------------------------- paper broker


def test_paper_broker_cannot_place_live_orders() -> None:
    assert PaperBroker.can_place_live_orders is False
    source = __import__("inspect").getsource(PaperBroker)
    for forbidden in ("api_key", "signature", "hmac", "/order", "POST"):
        assert forbidden not in source, f"PaperBroker mentions {forbidden!r}"


def test_a_controlled_trade_moves_the_ledger_with_explicit_costs(stack) -> None:
    config, wallet, _, risk, broker, repos = stack
    decision = _evaluate(risk, broker, config)
    assert decision.allowed
    start_balance = wallet.state.balance

    position = broker.open(signal(), decision.plan, book(), features(), cycle_id=1, at_ms=2_000_000)
    assert position is not None
    # Requested and filled prices are stored separately, and differ.
    assert position.requested_entry_price == 60001.0
    assert position.entry_price > position.requested_entry_price
    assert position.entry_slippage_bps > 0
    assert position.entry_fee_eur > 0
    assert wallet.state.reserved == pytest.approx(position.margin_eur)
    assert wallet.state.balance == pytest.approx(start_balance - position.entry_fee_eur)

    # Price moves up 50 bps; close into the new book.
    higher = book(bid=60300.0, ask=60301.0)
    trade = broker.close(position, higher, ExitReason.TAKE_PROFIT, at_ms=2_005_000)

    assert trade.gross_pnl_eur > 0
    assert trade.fees_eur > 0
    assert trade.net_pnl_eur == pytest.approx(trade.gross_pnl_eur - trade.fees_eur)
    assert trade.net_return_bps < trade.return_bps  # costs are not optional
    assert trade.cost_bps > 0
    assert trade.exit_reason is ExitReason.TAKE_PROFIT
    assert trade.features, "the decision-time feature snapshot must travel with the trade"
    assert trade.cost_model_version == config.costs.version

    assert wallet.state.reserved == pytest.approx(0.0)
    assert wallet.state.trades == 1 and wallet.state.wins == 1

    stored = repos.execution.load_trade(trade.trade_id)
    assert stored is not None
    assert stored["net_pnl_eur"] == pytest.approx(trade.net_pnl_eur, abs=1e-6)
    assert stored["features"], "features must be persisted, not just held in memory"


def test_a_losing_trade_is_recorded_as_a_loss(stack) -> None:
    config, wallet, _, risk, broker, _ = stack
    decision = _evaluate(risk, broker, config)
    position = broker.open(signal(), decision.plan, book(), features(), cycle_id=1, at_ms=2_000_000)
    lower = book(bid=59700.0, ask=59701.0)
    trade = broker.close(position, lower, ExitReason.STOP_LOSS, at_ms=2_003_000)
    assert trade.net_pnl_eur < 0
    assert wallet.state.losses == 1
    assert wallet.state.balance < 100.0


def test_exit_conditions_are_checked_against_the_closing_side(stack) -> None:
    config, _, _, risk, broker, _ = stack
    decision = _evaluate(risk, broker, config)
    position = broker.open(signal(), decision.plan, book(), features(), cycle_id=1, at_ms=2_000_000)

    assert not broker.check_exit(position, book(), at_ms=2_000_100).should_exit

    stop_price = position.entry_price * (1 - (position.stop_bps + 1) / 10_000)
    hit = broker.check_exit(position, book(bid=stop_price, ask=stop_price + 1), at_ms=2_000_200)
    assert hit.should_exit and hit.reason is ExitReason.STOP_LOSS

    horizon = broker.check_exit(position, book(), at_ms=2_000_000 + position.horizon_ms + 1)
    assert horizon.should_exit and horizon.reason is ExitReason.HORIZON

    quality = broker.check_exit(position, book(), at_ms=2_000_100, quality_ok=False,
                                quality_detail="feed went stale")
    assert quality.should_exit and quality.reason is ExitReason.DATA_QUALITY


def test_mark_to_market_uses_the_price_the_position_could_close_at(stack) -> None:
    config, wallet, _, risk, broker, _ = stack
    decision = _evaluate(risk, broker, config)
    position = broker.open(signal(), decision.plan, book(), features(), cycle_id=1, at_ms=2_000_000)
    # A long is worth what the bid will pay, not the mid.
    unrealized = broker.mark_to_market({"BTCUSDT": book(bid=60100.0, ask=60101.0)})
    expected = (60100.0 - position.entry_price) * position.qty / config.fx.usdt_per_eur
    assert unrealized == pytest.approx(expected, rel=1e-9)
    assert wallet.state.unrealized_pnl == pytest.approx(expected, abs=1e-8)
