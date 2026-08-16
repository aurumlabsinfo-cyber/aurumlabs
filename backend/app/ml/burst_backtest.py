"""Offline simulation of AURUM BURST-15, sessions and all.

`app.ml.strategies.burst15` scores the entry rule on the same walk-forward
folds as every other candidate, which answers "does this rule pick direction
better than a coin". This module answers the other half: run the whole thing -
15-minute windows, cooldown, trade cap, stop-loss and take-profit - over
recorded data and report what the session P&L actually looks like.

Two things are honest about it and worth stating plainly:

* entries and expiries are resolved from the recorded tick series at
  `ts + entry_delay` and `entry + horizon`. No fill is assumed that the
  recorded data does not support: a row whose future is not recorded is
  dropped, never forward-filled.
* it is still a backtest on paper. It applies the configured payout to a
  binary outcome; slippage, broker rejection and the fact that your broker's
  settlement price is not this venue's mid are not modelled, and no backtest
  of a 5-second binary should be read as a promise.
"""

from __future__ import annotations

import bisect
from dataclasses import dataclass, field
from typing import Any

import pandas as pd

from app.config import Settings
from app.signals.burst import N5_FEATURE, OFI_FEATURE, R10_FEATURE


@dataclass
class BurstTrade:
    ts: int
    direction: str
    entry_ts: int
    entry: float
    expiry_ts: int
    expiry: float
    result: str
    pnl_units: float
    n5: float
    r10: float
    ofi5: float


@dataclass
class SimSession:
    start_ts: int
    end_ts: int
    pnl_units: float = 0.0
    trades: list[BurstTrade] = field(default_factory=list)
    closed_reason: str | None = None

    def summary(self) -> dict[str, Any]:
        results = [t.result for t in self.trades]
        return {
            "start_ts": self.start_ts,
            "trades": len(self.trades),
            "wins": results.count("WIN"),
            "losses": results.count("LOSS"),
            "ties": results.count("TIE"),
            "pnl_units": round(self.pnl_units, 4),
            "closed_reason": self.closed_reason,
        }


def _price_at(tick_ts: list[int], tick_mid: list[float], target: int,
              tolerance_ms: int) -> tuple[int, float] | None:
    """First recorded tick at or after `target`, within tolerance."""
    idx = bisect.bisect_left(tick_ts, target)
    if idx >= len(tick_ts) or tick_ts[idx] - target > tolerance_ms:
        return None
    return tick_ts[idx], tick_mid[idx]


def simulate(
    features: pd.DataFrame,
    ticks: pd.DataFrame,
    settings: Settings,
    payout: float | None = None,
    tolerance_ms: int = 750,
) -> dict[str, Any]:
    """Replay BURST-15 over recorded feature rows and ticks."""
    s = settings
    payout = payout if payout is not None else s.binary_payout
    payout_known = payout is not None
    effective_payout = float(payout) if payout_known else float(s.burst_assumed_payout)

    required = {"ts", N5_FEATURE, R10_FEATURE}
    missing = sorted(required - set(features.columns))
    if features.empty or ticks.empty:
        return {"status": "NO_DATA", "conclusion": "no recorded data to replay"}
    if missing:
        return {
            "status": "MISSING_FEATURES",
            "missing": missing,
            "conclusion": (
                "the recorded rows predate these features. Record more data "
                "with the current build, or re-import the archive."
            ),
        }

    features = features.sort_values("ts").reset_index(drop=True)
    ticks = ticks.sort_values("ts").reset_index(drop=True)
    tick_ts: list[int] = ticks["ts"].tolist()
    tick_mid: list[float] = ticks["mid"].tolist()

    horizon_ms = int(s.signal_horizon_s * 1000)
    sessions: list[SimSession] = []
    current: SimSession | None = None
    last_entry_ts = -10**18
    trades: list[BurstTrade] = []
    triggers = 0
    unfilled = 0

    n5_col = features[N5_FEATURE].to_numpy()
    r10_col = features[R10_FEATURE].to_numpy()
    ofi_col = (
        features[OFI_FEATURE].to_numpy()
        if OFI_FEATURE in features.columns
        else [None] * len(features)
    )
    ts_col = features["ts"].to_numpy()

    for i in range(len(features)):
        ts = int(ts_col[i])
        n5 = n5_col[i]
        r10 = r10_col[i]
        ofi = ofi_col[i]

        if current is None or ts >= current.end_ts or current.closed_reason:
            if current is not None and current.trades:
                sessions.append(current)
            if current is not None and current.closed_reason and ts < current.end_ts:
                continue  # session stopped out: stand down until its window ends
            current = SimSession(start_ts=ts, end_ts=ts + s.burst_session_s * 1000)

        # ---------------------------------------------------------- trigger
        if pd.isna(n5) or pd.isna(r10) or r10 == 0:
            continue
        if n5 < s.burst_n5_min or abs(r10) < s.burst_r10_min_bps:
            continue
        if s.burst_require_ofi_agree and ofi is not None and not pd.isna(ofi):
            if (ofi > 0) != (r10 > 0):
                continue
        triggers += 1

        # ------------------------------------------------------- session rules
        if len(current.trades) >= s.burst_max_trades_session:
            current.closed_reason = "max trades reached"
            continue
        if ts - last_entry_ts < s.burst_cooldown_ms:
            continue

        entry = _price_at(tick_ts, tick_mid, ts + s.burst_entry_delay_ms, tolerance_ms)
        if entry is None:
            unfilled += 1
            continue
        entry_ts, entry_px = entry
        exit_ = _price_at(tick_ts, tick_mid, entry_ts + horizon_ms, tolerance_ms)
        if exit_ is None:
            unfilled += 1
            continue
        exit_ts, exit_px = exit_

        direction = "UP" if r10 > 0 else "DOWN"
        if exit_px == entry_px:
            result, pnl = "TIE", 0.0
        elif (exit_px > entry_px) == (direction == "UP"):
            result, pnl = "WIN", effective_payout * s.paper_stake
        else:
            result, pnl = "LOSS", -s.paper_stake

        trade = BurstTrade(
            ts=ts, direction=direction, entry_ts=entry_ts, entry=entry_px,
            expiry_ts=exit_ts, expiry=exit_px, result=result, pnl_units=pnl,
            n5=float(n5), r10=float(r10),
            ofi5=float(ofi) if ofi is not None and not pd.isna(ofi) else 0.0,
        )
        trades.append(trade)
        current.trades.append(trade)
        current.pnl_units += pnl
        last_entry_ts = ts

        if current.pnl_units <= s.burst_stop_loss_units:
            current.closed_reason = "session stop loss"
        elif current.pnl_units >= s.burst_take_profit_units:
            current.closed_reason = "session take profit"

    if current is not None and current.trades:
        sessions.append(current)

    return _report(
        trades, sessions, triggers, unfilled, len(features),
        effective_payout, payout_known, s,
    )


def _report(
    trades: list[BurstTrade],
    sessions: list[SimSession],
    triggers: int,
    unfilled: int,
    rows: int,
    payout: float,
    payout_known: bool,
    s: Settings,
) -> dict[str, Any]:
    from app.signals.statistics import binomial_p_value, wilson_interval

    n = len(trades)
    if n == 0:
        return {
            "status": "NO_TRADES",
            "rows_scanned": rows,
            "triggers": triggers,
            "conclusion": (
                "the rule never fired on this data. Lower BURST_N5_MIN / "
                "BURST_R10_MIN_BPS, or check that trade_count_5s and "
                "return_10000ms are populated (the archive importer has no "
                "book depth, but it does have both of these)."
            ),
        }

    results = [t.result for t in trades]
    wins = results.count("WIN")
    losses = results.count("LOSS")
    ties = results.count("TIE")
    decided = wins + losses
    pnl = sum(t.pnl_units for t in trades)
    equity, peak, max_dd = 0.0, 0.0, 0.0
    for t in trades:
        equity += t.pnl_units
        peak = max(peak, equity)
        max_dd = min(max_dd, equity - peak)

    lo, hi = wilson_interval(wins, decided) if decided else (None, None)
    breakeven = 1.0 / (1.0 + payout)
    session_pnl = [ses.pnl_units for ses in sessions]

    return {
        "status": "COMPLETE",
        "rows_scanned": rows,
        "triggers": triggers,
        "unfilled_triggers": unfilled,
        "trades": n,
        "wins": wins,
        "losses": losses,
        "ties": ties,
        "tie_fraction": round(ties / n, 4),
        "win_rate_decided": round(wins / decided, 4) if decided else None,
        "win_rate_ci95": [round(lo, 4), round(hi, 4)] if decided else None,
        "p_value_vs_coinflip": (
            round(binomial_p_value(wins, decided) or 1.0, 6) if decided else None
        ),
        "payout_used": payout,
        "payout_status": "KNOWN" if payout_known else "ASSUMED (BINARY_PAYOUT unset)",
        "breakeven_win_rate": round(breakeven, 4),
        "ev_per_trade_units": round(pnl / n, 5),
        "pnl_units": round(pnl, 3),
        "max_drawdown_units": round(max_dd, 3),
        "sessions": {
            "count": len(sessions),
            "green_fraction": (
                round(sum(1 for p in session_pnl if p > 0) / len(sessions), 4)
                if sessions else None
            ),
            "mean_pnl_units": (
                round(sum(session_pnl) / len(sessions), 4) if sessions else None
            ),
            "worst": round(min(session_pnl), 3) if sessions else None,
            "best": round(max(session_pnl), 3) if sessions else None,
            "mean_trades": (
                round(sum(len(x.trades) for x in sessions) / len(sessions), 2)
                if sessions else None
            ),
            "stopped_out": sum(
                1 for x in sessions if x.closed_reason == "session stop loss"
            ),
            "took_profit": sum(
                1 for x in sessions if x.closed_reason == "session take profit"
            ),
            "detail": [x.summary() for x in sessions[-20:]],
        },
        "config": {
            "n5_min": s.burst_n5_min,
            "r10_min_bps": s.burst_r10_min_bps,
            "require_ofi_agree": s.burst_require_ofi_agree,
            "horizon_s": s.signal_horizon_s,
            "entry_delay_ms": s.burst_entry_delay_ms,
            "session_s": s.burst_session_s,
            "cooldown_ms": s.burst_cooldown_ms,
            "max_trades_session": s.burst_max_trades_session,
            "stop_loss_units": s.burst_stop_loss_units,
            "take_profit_units": s.burst_take_profit_units,
        },
        "conclusion": _conclusion(wins, decided, lo, breakeven, payout_known),
    }


def _conclusion(
    wins: int, decided: int, lo: float | None, breakeven: float, payout_known: bool
) -> str:
    if not decided or lo is None:
        return "Every window was a tie: nothing to conclude."
    wr = wins / decided
    payout_note = (
        "" if payout_known
        else " The payout was ASSUMED (BINARY_PAYOUT is unset), so treat the "
             "P&L as an illustration, not a result."
    )
    if lo > breakeven:
        return (
            f"Win rate {wr:.4f}, whose 95% lower bound ({lo:.4f}) is above the "
            f"{breakeven:.4f} break-even for this payout. That is a result on "
            "recorded data, not a promise: overlapping 5s windows are "
            "correlated, so re-validate on data this run never saw before "
            "acting on it." + payout_note
        )
    return (
        f"Win rate {wr:.4f}; the 95% lower bound is {lo:.4f}, below the "
        f"{breakeven:.4f} break-even for this payout. NESSUN EDGE ROBUSTO "
        "IDENTIFICATO / no robust edge identified on this data." + payout_note
    )


async def run_from_db(
    settings: Settings,
    include_synthetic: bool = False,
    payout: float | None = None,
) -> dict[str, Any]:
    """Load recorded rows and simulate. Reads only; touches no exchange."""
    from app.ml.dataset import load_raw

    features, ticks = await load_raw(
        settings.symbol, include_synthetic=include_synthetic
    )
    report = simulate(features, ticks, settings, payout=payout)
    report["include_synthetic"] = include_synthetic
    if include_synthetic:
        report["warning"] = (
            "SYNTHETIC ROWS INCLUDED - this describes the simulator, not the "
            "market, and cannot establish anything."
        )
    return report
