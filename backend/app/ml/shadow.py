"""Scoring the windows the engine did NOT trade.

`signals` only contains what passed the gates. Judging the engine on that
sample answers the wrong question: it measures how the engine did on the
moments it already liked, which is exactly the sample its filter selected.

Every evaluated window is recorded in `shadow_decisions` with the direction the
aggregate leaned and the reasons it was gated out, and no outcome. The outcome
is attached here, offline, by looking up the tick at `ts + horizon` - the same
causal path the training labels use. A shadow row therefore cannot smuggle in
anything that was unknowable when it was written.

What this buys:

* the gates can be judged. If blocked windows would have won as often as the
  emitted ones, a gate is discarding information rather than noise.
* the sample for learning grows by orders of magnitude, since a 15-minute
  horizon emits a handful of signals an hour but evaluates thousands of
  windows.
"""

from __future__ import annotations

import bisect
from typing import Any

import pandas as pd
from sqlalchemy import select

from app.db.engine import session_scope
from app.db.models import MarketTickRow, ShadowDecisionRow
from app.signals.statistics import wilson_interval


async def load_shadow(
    symbol: str,
    include_synthetic: bool = False,
    limit: int | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Return (shadow decisions, ticks) ordered by timestamp."""
    sstmt = select(
        ShadowDecisionRow.ts, ShadowDecisionRow.lean, ShadowDecisionRow.prob_up,
        ShadowDecisionRow.confidence, ShadowDecisionRow.edge,
        ShadowDecisionRow.horizon_s, ShadowDecisionRow.reference_price,
        ShadowDecisionRow.market_regime, ShadowDecisionRow.emitted,
        ShadowDecisionRow.blocked_by, ShadowDecisionRow.data_quality,
        ShadowDecisionRow.is_synthetic,
    ).where(ShadowDecisionRow.symbol == symbol).order_by(ShadowDecisionRow.ts.asc())
    tstmt = select(MarketTickRow.ts, MarketTickRow.mid).where(
        MarketTickRow.symbol == symbol
    ).order_by(MarketTickRow.ts.asc())
    if not include_synthetic:
        sstmt = sstmt.where(ShadowDecisionRow.is_synthetic.is_(False))
        tstmt = tstmt.where(MarketTickRow.is_synthetic.is_(False))
    if limit:
        sstmt = sstmt.limit(limit)

    async with session_scope() as s:
        srows = (await s.execute(sstmt)).all()
        trows = (await s.execute(tstmt)).all()

    shadow = pd.DataFrame(
        [
            {
                "ts": r.ts, "lean": r.lean, "prob_up": r.prob_up,
                "confidence": r.confidence, "edge": r.edge,
                "horizon_s": r.horizon_s, "entry": r.reference_price,
                "regime": r.market_regime, "emitted": r.emitted,
                "blocked_by": r.blocked_by or [], "data_quality": r.data_quality,
            }
            for r in srows
        ]
    )
    ticks = pd.DataFrame([{"ts": r.ts, "mid": r.mid} for r in trows])
    return shadow, ticks


def attach_outcomes(
    shadow: pd.DataFrame, ticks: pd.DataFrame, tolerance_ms: int = 2000
) -> pd.DataFrame:
    """Resolve each row's outcome from the tick at `ts + horizon`.

    Rows whose future is not yet recorded - the most recent horizon of data,
    always - are dropped rather than forward-filled. A forward-filled outcome
    is a fabricated one.
    """
    if shadow.empty or ticks.empty:
        return pd.DataFrame()

    ticks = ticks.sort_values("ts").reset_index(drop=True)
    tick_ts: list[int] = ticks["ts"].tolist()
    tick_mid: list[float] = ticks["mid"].tolist()

    out: list[dict[str, Any]] = []
    for row in shadow.sort_values("ts").itertuples(index=False):
        target = int(row.ts) + int(row.horizon_s * 1000)
        idx = bisect.bisect_left(tick_ts, target)
        if idx >= len(tick_ts) or tick_ts[idx] - target > tolerance_ms:
            continue
        exit_price = tick_mid[idx]
        entry = float(row.entry)
        if entry <= 0:
            continue
        move = exit_price - entry
        if move == 0:
            result = "TIE"
        elif (move > 0) == (row.lean == "UP"):
            result = "WIN"
        else:
            result = "LOSS"
        out.append(
            {
                "ts": row.ts, "lean": row.lean, "confidence": row.confidence,
                "edge": row.edge, "regime": row.regime, "emitted": bool(row.emitted),
                "blocked_by": list(row.blocked_by), "entry": entry,
                "exit": exit_price, "move_bps": move / entry * 10_000.0,
                "result": result,
            }
        )
    return pd.DataFrame(out)


def _rate(frame: pd.DataFrame) -> dict[str, Any]:
    n = len(frame)
    if n == 0:
        return {"n": 0, "win_rate": None, "ci95": None, "ties": 0}
    wins = int((frame["result"] == "WIN").sum())
    ties = int((frame["result"] == "TIE").sum())
    decided = n - ties
    if decided == 0:
        return {"n": n, "win_rate": None, "ci95": None, "ties": ties,
                "note": "every window was a tie"}
    lo, hi = wilson_interval(wins, decided)
    return {
        "n": n,
        "decided": decided,
        "ties": ties,
        "tie_fraction": round(ties / n, 4),
        "win_rate": round(wins / decided, 4),
        "ci95": [round(lo, 4), round(hi, 4)],
    }


def evaluate(shadow: pd.DataFrame, ticks: pd.DataFrame) -> dict[str, Any]:
    """Hit rate of the engine's lean across every window, split by gate."""
    scored = attach_outcomes(shadow, ticks)
    if scored.empty:
        return {
            "status": "NO DATA",
            "note": (
                "no shadow window has a resolved future yet. The most recent "
                "horizon of data is always unresolvable - that is correct."
            ),
        }

    emitted = scored[scored["emitted"]]
    blocked = scored[~scored["emitted"]]

    by_reason: dict[str, Any] = {}
    for reason in sorted({r for rs in blocked["blocked_by"] for r in rs}):
        sel = blocked[blocked["blocked_by"].apply(lambda rs, r=reason: r in rs)]
        by_reason[reason] = _rate(sel)

    buckets: dict[str, Any] = {}
    for lo, hi in ((0.5, 0.6), (0.6, 0.7), (0.7, 0.8), (0.8, 1.01)):
        sel = scored[(scored["confidence"] >= lo) & (scored["confidence"] < hi)]
        buckets[f"{lo:.2f}-{hi:.2f}"] = _rate(sel)

    overall = _rate(scored)
    report = {
        "status": "OK",
        "windows": overall,
        "emitted": _rate(emitted),
        "blocked": _rate(blocked),
        "by_block_reason": by_reason,
        "by_confidence": buckets,
        "by_regime": {
            str(reg): _rate(sel) for reg, sel in scored.groupby("regime")
        },
        "note": (
            "A win rate here is the accuracy of the LEAN, not a tradable "
            "result: no trigger had to be reached and no payout is applied. "
            "Compare `emitted` against `blocked` to judge the gates."
        ),
    }

    e, b = report["emitted"], report["blocked"]
    if e.get("win_rate") is not None and b.get("win_rate") is not None:
        delta = e["win_rate"] - b["win_rate"]
        report["gate_value"] = {
            "emitted_minus_blocked": round(delta, 4),
            "verdict": (
                "gates select better windows" if delta > 0.02
                else "gates select worse windows" if delta < -0.02
                else "gates make no measurable difference"
            ),
            "caveat": (
                "Overlapping windows are not independent, so the intervals "
                "above are narrower than the truth. Treat a small delta as no "
                "delta."
            ),
        }
    return report
