"""The Decision Core: one answer, always explained, always sized inside the limits."""

from __future__ import annotations

import dataclasses

import pytest

from aurum_edge.config import Config
from aurum_edge.decide.decision_core import DecisionCore
from aurum_edge.decide.model import champion_v1
from aurum_edge.decide.risk import AccountState, RiskEngine
from aurum_edge.scan.market_core import DEPTH_BANDS_BPS
from aurum_edge.scan.scanner import Opportunity, Scanner
from aurum_edge.scan.snapshot import MarketSnapshot


def make_snapshot(**overrides) -> MarketSnapshot:
    """A strong, clean LONG setup unless overridden."""
    base = dict(
        symbol="BTCUSDT", source="bybit", seq=1,
        ts_exchange_ms=1_000_000.0, ts_local_ms=1_000_050.0, latency_ms=50.0,
        book_age_ms=40.0, trade_age_ms=100.0,
        bid=99.99, ask=100.01, bid_size=50.0, ask_size=20.0,
        spread=0.02, spread_bps=2.0, mid=100.0, last_price=100.0,
        microprice=100.005, microprice_edge_bps=0.5,
        ret_250ms_bps=3.0, ret_1s_bps=12.0, ret_3s_bps=28.0, ret_5s_bps=40.0,
        ret_15s_bps=55.0, ret_60s_bps=70.0,
        volatility_bps=9.0, range_60s_bps=90.0,
        trades_5s=40, trades_60s=300, trade_rate_hz=8.0,
        volume_5s_usd=250_000.0, volume_60s_usd=1_200_000.0, volume_acceleration=2.5,
        buy_volume_5s_usd=200_000.0, sell_volume_5s_usd=50_000.0,
        aggression_5s=0.6, aggression_60s=0.35,
        ofi_1s=0.55, ofi_5s=0.65, imbalance_top=0.42, imbalance_depth=0.30,
        depth_bid_usd=900_000.0, depth_ask_usd=700_000.0,
        book_state="OK", book_levels_bid=50, book_levels_ask=50,
        depth_topic="orderbook.50", book_top=((99.99, 50.0), (100.01, 20.0)),
        depth_curve_bps=DEPTH_BANDS_BPS,
        depth_curve_bid_usd=tuple(50_000.0 * (i + 1) for i in range(len(DEPTH_BANDS_BPS))),
        depth_curve_ask_usd=tuple(50_000.0 * (i + 1) for i in range(len(DEPTH_BANDS_BPS))),
        open_interest=1_000_000.0, open_interest_change_bps=15.0,
        funding_rate=0.0001, turnover_24h_usd=5e8,
        tick_size=0.01, qty_step=0.001, min_qty=0.001, min_notional_usd=5.0,
        max_leverage=25.0,
        quality="OK", quality_score=1.0, quality_reasons=(), tradable=True,
    )
    base.update(overrides)
    return MarketSnapshot(**base)


def opportunity(snap: MarketSnapshot, side: str = "LONG") -> Opportunity:
    score, components = Scanner().score_side(snap, side)
    return Opportunity(snap.symbol, side, score, snap, components)


def rich_account() -> AccountState:
    return AccountState(
        source="paper", equity_eur=1000.0, available_eur=1000.0, used_margin_eur=0.0,
        exposure_eur=0.0, open_positions=0, confirmed=True,
    )


def core(cfg: Config) -> tuple[DecisionCore, RiskEngine]:
    risk = RiskEngine(cfg)
    return DecisionCore(cfg, risk), risk


# ------------------------------------------------------------------ the happy path

def test_a_strong_setup_produces_a_trade_with_every_number(cfg: Config) -> None:
    decision_core, _ = core(cfg)
    snap = make_snapshot()
    decision = decision_core.evaluate(
        opportunity(snap), champion_v1(), rich_account(), now_mono=100.0
    )
    assert decision.action == "LONG"
    assert decision.side == "LONG"
    assert 0.5 < decision.probability <= 1.0
    assert decision.quality >= cfg.decide.min_quality
    assert decision.expected_move_bps > decision.required_move_bps > 0
    assert decision.expected_cost_eur > 0
    assert cfg.decide.margin_min_eur * 0.5 <= decision.margin_eur <= cfg.decide.margin_max_eur
    assert 0 < decision.leverage <= cfg.decide.leverage_default
    assert decision.notional_eur == pytest.approx(decision.margin_eur * decision.leverage, rel=1e-6)
    assert decision.qty > 0
    assert decision.target_eur > 0
    assert decision.max_loss_eur <= cfg.decide.max_loss_per_trade_eur * 1.25
    assert 0 < decision.max_hold_s <= cfg.decide.max_hold_s
    assert decision.expectancy_eur >= cfg.decide.min_expectancy_eur
    assert decision.reason and decision.reasons
    assert decision.model_version == champion_v1().version


def test_a_short_setup_produces_a_short(cfg: Config) -> None:
    decision_core, _ = core(cfg)
    snap = make_snapshot(
        ret_250ms_bps=-3.0, ret_1s_bps=-12.0, ret_3s_bps=-28.0, ret_5s_bps=-40.0,
        ret_15s_bps=-55.0, ret_60s_bps=-70.0,
        ofi_1s=-0.55, ofi_5s=-0.65, imbalance_top=-0.42, imbalance_depth=-0.30,
        aggression_5s=-0.6, aggression_60s=-0.35, microprice_edge_bps=-0.5,
        bid_size=20.0, ask_size=50.0,
    )
    decision = decision_core.evaluate(
        opportunity(snap, "SHORT"), champion_v1(), rich_account(), 100.0
    )
    assert decision.action == "SHORT"
    assert decision.qty > 0


def test_a_flat_market_is_no_trade_and_says_why(cfg: Config) -> None:
    decision_core, _ = core(cfg)
    snap = make_snapshot(
        ret_250ms_bps=0.0, ret_1s_bps=0.2, ret_3s_bps=-0.1, ret_5s_bps=0.0,
        ret_15s_bps=0.1, ret_60s_bps=0.0, ofi_1s=0.01, ofi_5s=-0.02,
        imbalance_top=0.0, imbalance_depth=0.0, aggression_5s=0.0, aggression_60s=0.0,
        volume_acceleration=1.0, microprice_edge_bps=0.0,
    )
    decision = decision_core.evaluate(opportunity(snap), champion_v1(), rich_account(), 100.0)
    assert decision.action == "NO_TRADE"
    assert decision.reason
    assert decision.reasons
    assert decision.probability < cfg.decide.min_probability or decision.expectancy_eur < 0


# ------------------------------------------------------------------ the gates

@pytest.mark.parametrize(
    "overrides, expected_fragment",
    [
        ({"quality": "BAD", "tradable": False,
          "quality_reasons": ("book RESYNC: sequence gap",)}, "not usable"),
        ({"quality": "DEGRADED", "tradable": False,
          "quality_reasons": ("top-of-book only (not in focus set)",)}, "degraded"),
        ({"spread_bps": 40.0, "quality": "DEGRADED", "tradable": False,
          "quality_reasons": ("spread 40.00bps > 8.00bps",)}, "spread"),
    ],
)
def test_bad_data_never_becomes_a_trade(cfg: Config, overrides, expected_fragment) -> None:
    decision_core, _ = core(cfg)
    snap = make_snapshot(**overrides)
    decision = decision_core.evaluate(opportunity(snap), champion_v1(), rich_account(), 100.0)
    assert decision.action == "NO_TRADE"
    assert any(expected_fragment in reason for reason in decision.reasons)


def test_reserve_capital_is_never_committed(cfg: Config) -> None:
    decision_core, _ = core(cfg)
    account = AccountState(
        source="paper", equity_eur=1000.0, available_eur=340.0, confirmed=True
    )
    # reserve is 35% of 1000 = 350, so 340 available leaves nothing free
    assert account.free_after_reserve(cfg.decide.reserve_fraction) == 0.0
    decision = decision_core.evaluate(opportunity(make_snapshot()), champion_v1(), account, 100.0)
    assert decision.action == "NO_TRADE"
    assert any("reserve" in reason for reason in decision.reasons)


def test_position_limits_block_new_entries(cfg: Config) -> None:
    decision_core, _ = core(cfg)
    account = rich_account()
    account.open_positions = cfg.decide.max_concurrent_positions
    decision = decision_core.evaluate(opportunity(make_snapshot()), champion_v1(), account, 100.0)
    assert decision.action == "NO_TRADE"
    assert any("max concurrent positions" in reason for reason in decision.reasons)


def test_the_same_symbol_is_not_doubled(cfg: Config) -> None:
    decision_core, _ = core(cfg)
    account = rich_account()
    account.symbols_open = {"BTCUSDT"}
    decision = decision_core.evaluate(opportunity(make_snapshot()), champion_v1(), account, 100.0)
    assert decision.action == "NO_TRADE"
    assert any("already holding BTCUSDT" in reason for reason in decision.reasons)


def test_unconfirmed_account_blocks_trading(cfg: Config) -> None:
    decision_core, _ = core(cfg)
    account = rich_account()
    account.confirmed = False
    decision = decision_core.evaluate(opportunity(make_snapshot()), champion_v1(), account, 100.0)
    assert decision.action == "NO_TRADE"
    assert any("not confirmed" in reason for reason in decision.reasons)


def test_kill_switch_blocks_everything(cfg: Config) -> None:
    decision_core, risk = core(cfg)
    risk.engage_kill_switch("manual test")
    decision = decision_core.evaluate(
        opportunity(make_snapshot()), champion_v1(), rich_account(), 100.0
    )
    assert decision.action == "NO_TRADE"
    assert any("kill switch" in reason for reason in decision.reasons)


def test_daily_loss_limit_stops_the_day(cfg: Config) -> None:
    decision_core, risk = core(cfg)
    risk.realized_today_eur = -cfg.decide.daily_max_loss_eur
    decision = decision_core.evaluate(
        opportunity(make_snapshot()), champion_v1(), rich_account(), 100.0
    )
    assert decision.action == "NO_TRADE"
    assert any("daily loss limit" in reason for reason in decision.reasons)


def test_cooldown_after_a_loss(cfg: Config) -> None:
    decision_core, risk = core(cfg)
    risk.record_realized(-1.0, "BTCUSDT", now_mono=100.0)
    decision = decision_core.evaluate(
        opportunity(make_snapshot()), champion_v1(), rich_account(), now_mono=110.0
    )
    assert decision.action == "NO_TRADE"
    assert any("cooldown" in reason for reason in decision.reasons)


# ------------------------------------------------------------------ economics

def test_costs_must_be_covered_by_the_expected_move(cfg: Config) -> None:
    """Fees dominate a small move: that trade must be refused."""
    expensive = dataclasses.replace(
        cfg, decide=dataclasses.replace(cfg.decide, taker_fee_rate=0.005)  # 50bps a side
    )
    decision_core, _ = core(expensive)
    decision = decision_core.evaluate(
        opportunity(make_snapshot()), champion_v1(), rich_account(), 100.0
    )
    assert decision.action == "NO_TRADE"
    assert any("clear costs" in r or "expectancy" in r for r in decision.reasons)


def test_high_volatility_reduces_leverage_rather_than_the_loss_cap(cfg: Config) -> None:
    decision_core, _ = core(cfg)
    calm = decision_core.evaluate(
        opportunity(make_snapshot(volatility_bps=4.0)), champion_v1(), rich_account(), 100.0
    )
    wild = decision_core.evaluate(
        opportunity(make_snapshot(volatility_bps=45.0, ret_3s_bps=140.0, ret_5s_bps=190.0)),
        champion_v1(), rich_account(), 100.0,
    )
    assert calm.leverage >= wild.leverage
    for decision in (calm, wild):
        if decision.is_trade:
            assert decision.max_loss_eur <= cfg.decide.max_loss_per_trade_eur * 1.25
            assert decision.leverage <= cfg.decide.leverage_default


def test_leverage_is_never_raised_to_reach_the_target(cfg: Config) -> None:
    decision_core, _ = core(cfg)
    for volatility in (3.0, 8.0, 20.0, 60.0):
        decision = decision_core.evaluate(
            opportunity(make_snapshot(volatility_bps=volatility)),
            champion_v1(), rich_account(), 100.0,
        )
        assert decision.leverage <= cfg.decide.leverage_default


def test_expectancy_uses_probability_win_and_loss(cfg: Config) -> None:
    decision_core, _ = core(cfg)
    decision = decision_core.evaluate(
        opportunity(make_snapshot()), champion_v1(), rich_account(), 100.0
    )
    if decision.is_trade:
        expected = (
            decision.probability * decision.target_eur
            - (1 - decision.probability) * decision.max_loss_eur
        )
        assert decision.expectancy_eur == pytest.approx(expected, rel=1e-9)


def test_target_is_a_reference_not_an_obligation(cfg: Config) -> None:
    """When the move on offer is smaller than €2, the target shrinks to fit it."""
    decision_core, _ = core(cfg)
    modest = make_snapshot(ret_3s_bps=18.0, ret_5s_bps=22.0, volatility_bps=7.0)
    decision = decision_core.evaluate(opportunity(modest), champion_v1(), rich_account(), 100.0)
    reference_bps = (
        (cfg.decide.target_net_eur + decision.expected_cost_eur)
        / max(decision.notional_eur, 1e-9) * 10_000.0
    )
    assert decision.target_move_bps <= reference_bps + 1e-9
    assert decision.target_move_bps <= decision.expected_move_bps


def test_size_respects_the_exchange_minimums(cfg: Config) -> None:
    decision_core, _ = core(cfg)
    snap = make_snapshot(min_qty=1_000.0, min_notional_usd=1_000_000.0)
    decision = decision_core.evaluate(opportunity(snap), champion_v1(), rich_account(), 100.0)
    assert decision.action == "NO_TRADE"
    assert any("minimum" in reason for reason in decision.reasons)


def test_every_no_trade_carries_a_reason(cfg: Config) -> None:
    """The property that makes 'why 0 signals?' answerable."""
    decision_core, risk = core(cfg)
    variants = [
        make_snapshot(),
        make_snapshot(quality="BAD", tradable=False, quality_reasons=("book RESYNC",)),
        make_snapshot(ret_1s_bps=0.0, ret_3s_bps=0.0, ret_5s_bps=0.0, ofi_5s=0.0,
                      aggression_5s=0.0, imbalance_top=0.0),
        make_snapshot(spread_bps=30.0),
        make_snapshot(volatility_bps=200.0),
        make_snapshot(min_notional_usd=10_000_000.0),
    ]
    for snap in variants:
        for side in ("LONG", "SHORT"):
            decision = decision_core.evaluate(
                opportunity(snap, side), champion_v1(), rich_account(), 100.0
            )
            assert decision.reason.strip(), "a decision without a reason is a bug"
            assert decision.to_dict()["reasons"]


def test_shadow_decisions_ignore_portfolio_limits(cfg: Config) -> None:
    """A challenger must be judged on the market, not on the account's state."""
    decision_core, _ = core(cfg)
    account = rich_account()
    account.open_positions = cfg.decide.max_concurrent_positions
    shadow = decision_core.evaluate(
        opportunity(make_snapshot()), champion_v1(), account, 100.0, shadow=True
    )
    assert shadow.shadow is True
    assert not any("max concurrent" in reason for reason in shadow.reasons)
