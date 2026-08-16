"""Paper-trading statistics, break-even logic, calibration, Monte Carlo."""

from __future__ import annotations

from app.ml import montecarlo
from app.signals import statistics as st


def trade(result: str, confidence: float = 0.7, regime: str = "TREND_UP", **over):
    d = {
        "result": result, "confidence": confidence, "direction": "UP",
        "market_regime": regime, "ts": 1_700_000_000_000, "triggered_at": 1_700_000_000_000,
        "features": {"realized_vol_5s_bps": 1.5},
    }
    d.update(over)
    return d


def test_break_even_win_rate():
    assert st.break_even_win_rate(0.8) == 1 / 1.8
    assert round(st.break_even_win_rate(0.8), 4) == 0.5556
    assert st.break_even_win_rate(1.0) == 0.5
    assert st.break_even_win_rate(None) is None


def test_a_54_percent_win_rate_loses_money_at_an_80_percent_payout():
    """The single most important sanity check in binary options."""
    ev = st.expected_value_per_trade(0.54, 0.8)
    assert ev < 0
    assert st.expected_value_per_trade(0.60, 0.8) > 0


def test_summary_refuses_monetary_pnl_without_a_payout():
    trades = [trade("WIN")] * 30 + [trade("LOSS")] * 20
    s = st.summarise(trades, payout=None)
    assert s["win_rate"] == 0.6
    assert s["payout_status"] == "PAYOUT UNKNOWN"
    assert s["pnl_units"] is None
    assert s["expected_value_per_trade"] is None
    assert s["break_even_win_rate"] is None
    assert any("PAYOUT UNKNOWN" in w for w in s["warnings"])


def test_summary_computes_pnl_when_the_payout_is_known():
    trades = (
        [trade("WIN", pnl_units=0.8) for _ in range(30)]
        + [trade("LOSS", pnl_units=-1.0) for _ in range(20)]
    )
    s = st.summarise(trades, payout=0.8)
    assert s["pnl_units"] == round(30 * 0.8 - 20, 4)
    assert s["beats_break_even"] is True
    assert s["profit_factor"] == round((30 * 0.8) / 20, 4)
    assert s["max_drawdown_units"] is not None


def test_small_samples_are_flagged_not_celebrated():
    s = st.summarise([trade("WIN")] * 5, payout=0.8)
    assert s["win_rate"] == 1.0
    assert s["sufficient_sample"] is False
    assert s["statistically_significant"] is False
    assert any("settled trades" in w for w in s["warnings"])


def test_ties_are_excluded_from_the_win_rate_but_reported():
    trades = [trade("WIN")] * 10 + [trade("LOSS")] * 10 + [trade("TIE")] * 5
    s = st.summarise(trades, payout=0.8)
    assert s["decided"] == 20
    assert s["ties"] == 5
    assert s["win_rate"] == 0.5


def test_synthetic_trades_taint_the_report():
    trades = [trade("WIN", is_synthetic=True)] * 40
    s = st.summarise(trades, payout=0.8)
    assert any("SYNTHETIC" in w for w in s["warnings"])


def test_wilson_interval_is_wide_for_tiny_samples():
    lo, hi = st.wilson_interval(3, 3)
    assert lo < 0.5 < hi or lo > 0.3  # never claims certainty from 3 samples
    lo2, hi2 = st.wilson_interval(600, 1000)
    assert (hi2 - lo2) < (hi - lo)


def test_streaks():
    seq = ["WIN", "WIN", "LOSS", "LOSS", "LOSS", "WIN"]
    s = st.summarise([trade(r) for r in seq], payout=0.8)
    assert s["max_losing_streak"] == 3
    assert s["max_winning_streak"] == 2


def test_calibration_detects_overconfidence():
    # Claims 90%, delivers 50%.
    trades = [trade("WIN", 0.92) for _ in range(50)] + [
        trade("LOSS", 0.92) for _ in range(50)
    ]
    cal = st.calibration(trades)
    bucket = next(b for b in cal["buckets"] if b["n"] == 100)
    assert bucket["calibration_error"] < -0.35
    assert bucket["within_ci"] is False
    assert cal["brier_score"] > 0.25


def test_calibration_rewards_honest_confidence():
    trades = [trade("WIN", 0.75) for _ in range(75)] + [
        trade("LOSS", 0.75) for _ in range(25)
    ]
    cal = st.calibration(trades)
    bucket = next(b for b in cal["buckets"] if b["n"] == 100)
    assert abs(bucket["calibration_error"]) < 0.02
    assert bucket["within_ci"] is True


def test_breakdowns_group_correctly():
    trades = [trade("WIN", regime="RANGE") for _ in range(10)] + [
        trade("LOSS", regime="TREND_UP") for _ in range(10)
    ]
    by_regime = st.by_bucket(trades, "market_regime", 0.8)
    assert by_regime["RANGE"]["win_rate"] == 1.0
    assert by_regime["TREND_UP"]["win_rate"] == 0.0
    assert by_regime["RANGE"]["sufficient_sample"] is False


def test_full_report_shape():
    trades = [trade("WIN")] * 40 + [trade("LOSS")] * 40
    report = st.full_report(trades, payout=0.8, no_trade_count=1000)
    for key in ("overall", "by_regime", "by_direction", "by_confidence",
                "by_volatility", "by_hour", "calibration"):
        assert key in report
    assert report["overall"]["no_trade"] == 1000


# ------------------------------------------------------------- monte carlo
def test_monte_carlo_needs_a_real_sample():
    out = montecarlo.from_trades(["WIN"] * 5, payout=0.8)
    assert "error" in out


def test_monte_carlo_reports_streaks_without_a_payout():
    out = montecarlo.from_trades(
        ["WIN", "LOSS"] * 100, payout=None, n_simulations=500
    )
    assert out["payout_known"] is False
    assert "max_losing_streak" in out
    assert "final_pnl_units" not in out
    assert "PAYOUT UNKNOWN" in out["note"]


def test_monte_carlo_drawdown_and_ruin_with_a_payout():
    out = montecarlo.simulate(
        win_rate=0.55, n_trades=200, payout=0.8, n_simulations=500,
        starting_bankroll=10.0,
    )
    assert out["max_drawdown_units"]["p95"] > 0
    assert 0.0 <= out["risk_of_ruin"] <= 1.0
    assert out["break_even_win_rate"] == round(1 / 1.8, 5)
    # 55% at an 80% payout is below break-even: EV must be negative.
    assert out["expected_value_per_trade"] < 0


def test_kelly_is_zero_below_break_even():
    out = montecarlo.simulate(0.50, 100, 0.8, 200, starting_bankroll=10.0)
    assert out["kelly_fraction"] == 0.0
