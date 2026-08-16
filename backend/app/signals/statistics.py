"""Paper-trading statistics.

Everything here is computed from recorded paper trades. Nothing is assumed.

Two rules drive the design:

1. **A win rate above 50% does not imply profit.** For a binary option the
   break-even win rate is ``1 / (1 + payout)``. With a typical 80% payout you
   need 55.6% just to break even. If the payout is not configured, this module
   reports ``PAYOUT UNKNOWN`` and refuses to state a monetary P&L.
2. **Small samples say nothing.** Every win rate is reported with a Wilson
   confidence interval and a binomial p-value against the 50% null, plus an
   explicit "insufficient sample" flag.
"""

from __future__ import annotations

import math
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Iterable, Sequence

MIN_SAMPLE = 30


def wilson_interval(wins: int, n: int, z: float = 1.96) -> tuple[float, float]:
    """Wilson score interval - well behaved for small n, unlike normal approx."""
    if n == 0:
        return (0.0, 0.0)
    p = wins / n
    denom = 1 + z * z / n
    centre = (p + z * z / (2 * n)) / denom
    margin = (z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n))) / denom
    return (max(0.0, centre - margin), min(1.0, centre + margin))


def binomial_p_value(wins: int, n: int, p0: float = 0.5) -> float | None:
    """Two-sided exact binomial test against `p0`."""
    if n == 0:
        return None
    try:
        from scipy.stats import binomtest

        return float(binomtest(wins, n, p0, alternative="two-sided").pvalue)
    except Exception:  # noqa: BLE001 - scipy optional at runtime
        # Normal approximation fallback.
        sd = math.sqrt(p0 * (1 - p0) * n)
        if sd == 0:
            return None
        z = abs(wins - p0 * n) / sd
        return float(math.erfc(z / math.sqrt(2)))


def break_even_win_rate(payout: float | None) -> float | None:
    """1 / (1 + payout). A 0.8 payout needs 55.56%."""
    if payout is None or payout <= 0:
        return None
    return 1.0 / (1.0 + payout)


def expected_value_per_trade(win_rate: float, payout: float | None) -> float | None:
    """EV in stake units: p*payout - (1-p). Undefined without a payout."""
    if payout is None:
        return None
    return win_rate * payout - (1.0 - win_rate)


def _settled(trades: Iterable[dict]) -> list[dict]:
    return [t for t in trades if t.get("result") in ("WIN", "LOSS", "TIE")]


def _decided(trades: Iterable[dict]) -> list[dict]:
    return [t for t in trades if t.get("result") in ("WIN", "LOSS")]


def summarise(
    trades: Sequence[dict],
    payout: float | None,
    stake: float = 1.0,
    no_trade_count: int | None = None,
) -> dict[str, Any]:
    settled = _settled(trades)
    decided = _decided(trades)
    wins = [t for t in decided if t["result"] == "WIN"]
    losses = [t for t in decided if t["result"] == "LOSS"]
    ties = [t for t in settled if t["result"] == "TIE"]
    cancelled = [t for t in trades if t.get("result") == "CANCELLED"]

    n = len(decided)
    n_wins = len(wins)
    win_rate = (n_wins / n) if n else 0.0
    lo, hi = wilson_interval(n_wins, n)
    p_value = binomial_p_value(n_wins, n)
    be = break_even_win_rate(payout)
    ev = expected_value_per_trade(win_rate, payout)

    pnl_values = [t.get("pnl_units") for t in decided if t.get("pnl_units") is not None]
    pnl_known = payout is not None and len(pnl_values) == n and n > 0

    equity, max_dd, dd_curve = _drawdown(decided, payout, stake)
    win_streak, loss_streak = _streaks(decided)

    gross_win = sum(v for v in pnl_values if v > 0) if pnl_known else None
    gross_loss = -sum(v for v in pnl_values if v < 0) if pnl_known else None
    profit_factor = (
        (gross_win / gross_loss) if (pnl_known and gross_loss and gross_loss > 0)
        else None
    )

    directions = defaultdict(int)
    for t in trades:
        directions[t.get("direction", "?")] += 1

    return {
        "total_signals": len(trades),
        "call_up": directions.get("UP", 0),
        "put_down": directions.get("DOWN", 0),
        "no_trade": no_trade_count,
        "cancelled": len(cancelled),
        "settled": len(settled),
        "decided": n,
        "wins": n_wins,
        "losses": len(losses),
        "ties": len(ties),
        "win_rate": round(win_rate, 4) if n else None,
        "win_rate_ci95": [round(lo, 4), round(hi, 4)] if n else None,
        "win_rate_p_value_vs_50": round(p_value, 5) if p_value is not None else None,
        "statistically_significant": bool(p_value is not None and p_value < 0.05 and n >= MIN_SAMPLE),
        "sufficient_sample": n >= MIN_SAMPLE,
        "min_sample_required": MIN_SAMPLE,
        "payout": payout,
        "payout_known": payout is not None,
        "payout_status": "KNOWN" if payout is not None else "PAYOUT UNKNOWN",
        "break_even_win_rate": round(be, 4) if be is not None else None,
        "beats_break_even": (
            None if (be is None or not n) else bool(win_rate > be)
        ),
        "expected_value_per_trade": round(ev, 5) if ev is not None else None,
        "expected_value_note": (
            "Expected value requires the broker payout. Set BINARY_PAYOUT to "
            "compute it; without it no monetary P&L is reported."
            if payout is None else
            f"EV = win_rate*{payout} - (1-win_rate), in stake units."
        ),
        "pnl_units": round(sum(pnl_values), 4) if pnl_known else None,
        "profit_factor": round(profit_factor, 4) if profit_factor else None,
        "max_drawdown_units": round(max_dd, 4) if pnl_known else None,
        "equity_curve": dd_curve if pnl_known else None,
        "final_equity_units": round(equity, 4) if pnl_known else None,
        "max_winning_streak": win_streak,
        "max_losing_streak": loss_streak,
        "average_confidence": (
            round(sum(t.get("confidence", 0) for t in decided) / n, 4) if n else None
        ),
        "average_confidence_wins": (
            round(sum(t.get("confidence", 0) for t in wins) / len(wins), 4)
            if wins else None
        ),
        "average_confidence_losses": (
            round(sum(t.get("confidence", 0) for t in losses) / len(losses), 4)
            if losses else None
        ),
        "trigger_hit_rate": (
            round(len(settled) / len(trades), 4) if trades else None
        ),
        "warnings": _warnings(n, payout, trades),
    }


def _warnings(n: int, payout: float | None, trades: Sequence[dict]) -> list[str]:
    out: list[str] = []
    if n < MIN_SAMPLE:
        out.append(
            f"Only {n} settled trades. At least {MIN_SAMPLE} are needed before a "
            "win rate means anything, and several hundred before it is stable."
        )
    if payout is None:
        out.append(
            "PAYOUT UNKNOWN - no monetary P&L is reported. A win rate above 50% "
            "does not imply profit; break-even is 1/(1+payout)."
        )
    if any(t.get("is_synthetic") for t in trades):
        out.append(
            "This sample contains SYNTHETIC (simulator) trades. They describe the "
            "simulator, not the market, and must not be used to claim an edge."
        )
    return out


def _drawdown(
    decided: Sequence[dict], payout: float | None, stake: float
) -> tuple[float, float, list[float]]:
    if payout is None:
        return (0.0, 0.0, [])
    equity = 0.0
    peak = 0.0
    max_dd = 0.0
    curve: list[float] = []
    for t in decided:
        pnl = t.get("pnl_units")
        if pnl is None:
            pnl = stake * payout if t["result"] == "WIN" else -stake
        equity += pnl
        peak = max(peak, equity)
        max_dd = max(max_dd, peak - equity)
        curve.append(round(equity, 4))
    return (equity, max_dd, curve[-500:])


def _streaks(decided: Sequence[dict]) -> tuple[int, int]:
    best_w = best_l = cur_w = cur_l = 0
    for t in decided:
        if t["result"] == "WIN":
            cur_w += 1
            cur_l = 0
        else:
            cur_l += 1
            cur_w = 0
        best_w = max(best_w, cur_w)
        best_l = max(best_l, cur_l)
    return (best_w, best_l)


# ------------------------------------------------------------------ breakdowns
def by_bucket(
    trades: Sequence[dict], key: str, payout: float | None
) -> dict[str, Any]:
    groups: dict[str, list[dict]] = defaultdict(list)
    for t in _decided(trades):
        groups[str(t.get(key) or "UNKNOWN")].append(t)
    return {
        k: _bucket_stats(v, payout) for k, v in sorted(groups.items())
    }


def by_confidence(trades: Sequence[dict], payout: float | None) -> dict[str, Any]:
    groups: dict[str, list[dict]] = defaultdict(list)
    for t in _decided(trades):
        c = float(t.get("confidence") or 0.0)
        lo = int(c * 10) * 10
        groups[f"{lo}-{lo + 10}%"].append(t)
    return {k: _bucket_stats(v, payout) for k, v in sorted(groups.items())}


def by_hour(trades: Sequence[dict], payout: float | None) -> dict[str, Any]:
    groups: dict[str, list[dict]] = defaultdict(list)
    for t in _decided(trades):
        ts = t.get("triggered_at") or t.get("ts")
        if not ts:
            continue
        hour = datetime.fromtimestamp(ts / 1000, tz=timezone.utc).hour
        groups[f"{hour:02d}:00 UTC"].append(t)
    return {k: _bucket_stats(v, payout) for k, v in sorted(groups.items())}


def by_volatility(trades: Sequence[dict], payout: float | None) -> dict[str, Any]:
    groups: dict[str, list[dict]] = defaultdict(list)
    for t in _decided(trades):
        feats = t.get("features") or {}
        vol = feats.get("realized_vol_5s_bps")
        if vol is None:
            bucket = "unknown"
        elif vol < 1:
            bucket = "<1bps"
        elif vol < 2:
            bucket = "1-2bps"
        elif vol < 4:
            bucket = "2-4bps"
        else:
            bucket = ">4bps"
        groups[bucket].append(t)
    return {k: _bucket_stats(v, payout) for k, v in sorted(groups.items())}


def _bucket_stats(trades: Sequence[dict], payout: float | None) -> dict[str, Any]:
    n = len(trades)
    wins = sum(1 for t in trades if t["result"] == "WIN")
    wr = wins / n if n else 0.0
    lo, hi = wilson_interval(wins, n)
    return {
        "n": n,
        "wins": wins,
        "win_rate": round(wr, 4) if n else None,
        "ci95": [round(lo, 4), round(hi, 4)] if n else None,
        "expected_value": (
            round(expected_value_per_trade(wr, payout), 5)
            if payout is not None and n else None
        ),
        "sufficient_sample": n >= MIN_SAMPLE,
    }


# ---------------------------------------------------------------- calibration
def calibration(trades: Sequence[dict], bins: int = 10) -> dict[str, Any]:
    """Do the stated confidences match observed frequencies?

    A model claiming 80% should win ~80% of those trades. Deviation is reported
    per bucket along with the Brier score.
    """
    decided = _decided(trades)
    buckets: list[dict[str, Any]] = []
    brier_terms: list[float] = []
    for i in range(bins):
        lo = i / bins
        hi = (i + 1) / bins
        group = [
            t for t in decided
            if lo <= float(t.get("confidence") or 0.0) < hi
            or (i == bins - 1 and float(t.get("confidence") or 0.0) == 1.0)
        ]
        if not group:
            continue
        wins = sum(1 for t in group if t["result"] == "WIN")
        observed = wins / len(group)
        stated = sum(float(t.get("confidence") or 0) for t in group) / len(group)
        ci = wilson_interval(wins, len(group))
        buckets.append(
            {
                "bucket": f"{int(lo * 100)}-{int(hi * 100)}%",
                "n": len(group),
                "stated_confidence": round(stated, 4),
                "observed_win_rate": round(observed, 4),
                "ci95": [round(ci[0], 4), round(ci[1], 4)],
                "calibration_error": round(observed - stated, 4),
                "within_ci": bool(ci[0] <= stated <= ci[1]),
                "sufficient_sample": len(group) >= MIN_SAMPLE,
            }
        )
    for t in decided:
        p = float(t.get("confidence") or 0.0)
        outcome = 1.0 if t["result"] == "WIN" else 0.0
        brier_terms.append((p - outcome) ** 2)

    ece = (
        sum(abs(b["calibration_error"]) * b["n"] for b in buckets)
        / sum(b["n"] for b in buckets)
        if buckets else None
    )
    return {
        "buckets": buckets,
        "brier_score": round(sum(brier_terms) / len(brier_terms), 5) if brier_terms else None,
        "expected_calibration_error": round(ece, 5) if ece is not None else None,
        "n": len(decided),
        "sufficient_sample": len(decided) >= MIN_SAMPLE,
        "note": (
            "Brier score: lower is better; 0.25 is what you get from always "
            "saying 50%. Confidence is only meaningful once these buckets line up."
        ),
    }


def full_report(
    trades: Sequence[dict],
    payout: float | None,
    stake: float = 1.0,
    no_trade_count: int | None = None,
) -> dict[str, Any]:
    return {
        "overall": summarise(trades, payout, stake, no_trade_count),
        "by_regime": by_bucket(trades, "market_regime", payout),
        "by_direction": by_bucket(trades, "direction", payout),
        "by_confidence": by_confidence(trades, payout),
        "by_volatility": by_volatility(trades, payout),
        "by_hour": by_hour(trades, payout),
        "calibration": calibration(trades),
    }
