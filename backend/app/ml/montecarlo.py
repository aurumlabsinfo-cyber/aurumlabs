"""Monte Carlo risk analysis.

A win rate is a point estimate. What kills an account is the *path*: the
drawdown you have to sit through and the losing streak you have to survive.
These simulations resample the observed outcome sequence to show the
distribution of paths consistent with the same edge.
"""

from __future__ import annotations

import math
from typing import Any, Sequence

import numpy as np


def simulate(
    win_rate: float,
    n_trades: int,
    payout: float | None,
    n_simulations: int = 10_000,
    stake: float = 1.0,
    starting_bankroll: float | None = None,
    seed: int = 42,
) -> dict[str, Any]:
    """Bootstrap `n_simulations` independent sequences of `n_trades`.

    With an unknown payout the P&L path is undefined, so only the streak and
    win-count distributions are reported.
    """
    rng = np.random.default_rng(seed)
    wins = rng.random((n_simulations, n_trades)) < win_rate

    out: dict[str, Any] = {
        "win_rate_assumed": round(win_rate, 5),
        "n_trades": n_trades,
        "n_simulations": n_simulations,
        "payout": payout,
        "payout_known": payout is not None,
        "win_count": _dist(wins.sum(axis=1)),
        "max_losing_streak": _dist(_max_streak(~wins)),
        "max_winning_streak": _dist(_max_streak(wins)),
    }

    if payout is None:
        out["note"] = (
            "PAYOUT UNKNOWN: monetary drawdown and risk of ruin cannot be "
            "computed. Set BINARY_PAYOUT to enable them."
        )
        return out

    pnl = np.where(wins, stake * payout, -stake)
    equity = np.cumsum(pnl, axis=1)
    running_peak = np.maximum.accumulate(equity, axis=1)
    drawdown = running_peak - equity
    max_dd = drawdown.max(axis=1)
    final = equity[:, -1]

    out.update(
        {
            "final_pnl_units": _dist(final),
            "max_drawdown_units": _dist(max_dd),
            "probability_of_loss": round(float((final < 0).mean()), 5),
            "expected_value_per_trade": round(
                win_rate * payout - (1 - win_rate), 5
            ),
            "break_even_win_rate": round(1.0 / (1.0 + payout), 5),
        }
    )
    if starting_bankroll:
        ruin = (equity.min(axis=1) <= -starting_bankroll).mean()
        out["risk_of_ruin"] = round(float(ruin), 5)
        out["starting_bankroll_units"] = starting_bankroll
        out["kelly_fraction"] = round(_kelly(win_rate, payout), 5)
    return out


def from_trades(
    results: Sequence[str],
    payout: float | None,
    n_simulations: int = 10_000,
    stake: float = 1.0,
    starting_bankroll: float | None = None,
) -> dict[str, Any]:
    decided = [r for r in results if r in ("WIN", "LOSS")]
    n = len(decided)
    if n < 20:
        return {
            "error": f"only {n} settled trades; Monte Carlo on this is theatre, "
                     "not analysis (need >= 20, realistically several hundred)",
            "n": n,
        }
    wr = sum(1 for r in decided if r == "WIN") / n
    sim = simulate(wr, n, payout, n_simulations, stake, starting_bankroll)
    sim["observed"] = {
        "n": n,
        "win_rate": round(wr, 5),
        "max_losing_streak": int(_observed_streak(decided, "LOSS")),
        "max_winning_streak": int(_observed_streak(decided, "WIN")),
    }
    return sim


def _dist(values: np.ndarray) -> dict[str, float]:
    v = np.asarray(values, dtype=float)
    return {
        "mean": round(float(v.mean()), 4),
        "std": round(float(v.std()), 4),
        "p05": round(float(np.percentile(v, 5)), 4),
        "p25": round(float(np.percentile(v, 25)), 4),
        "median": round(float(np.percentile(v, 50)), 4),
        "p75": round(float(np.percentile(v, 75)), 4),
        "p95": round(float(np.percentile(v, 95)), 4),
        "worst": round(float(v.min()), 4),
        "best": round(float(v.max()), 4),
    }


def _max_streak(flags: np.ndarray) -> np.ndarray:
    """Longest run of True per row, vectorised over simulations."""
    n_sims, n = flags.shape
    best = np.zeros(n_sims, dtype=int)
    cur = np.zeros(n_sims, dtype=int)
    for i in range(n):
        col = flags[:, i]
        cur = np.where(col, cur + 1, 0)
        best = np.maximum(best, cur)
    return best


def _observed_streak(results: Sequence[str], target: str) -> int:
    best = cur = 0
    for r in results:
        cur = cur + 1 if r == target else 0
        best = max(best, cur)
    return best


def _kelly(win_rate: float, payout: float) -> float:
    """Kelly fraction for a binary payout; negative means do not bet."""
    b = payout
    if b <= 0:
        return 0.0
    f = (win_rate * (b + 1) - 1) / b
    return max(0.0, f) if not math.isnan(f) else 0.0
