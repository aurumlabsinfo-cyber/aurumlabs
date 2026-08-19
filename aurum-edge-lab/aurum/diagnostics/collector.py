"""Diagnostics: making "0 signals" an explanation instead of a shrug.

Every gate that can turn a would-be trade into no trade increments a counter
here, with the first reason that actually applied.  ``/diagnostics`` then reports
counts *and* percentages, so the answer to "why has nothing traded today?" is a
ranked list with numbers rather than a guess.

The blueprint's rule that this exists to serve: do not tune gates downward
because the signal count is zero — diagnose the reason quantitatively.  A gate
that blocks 94% of candidates is telling you something specific, and which gate
it is changes what you should do about it.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from typing import Any

from ..domain import RejectionReason, now_ms

#: What each rejection means, and what a human should do about it.  Served with
#: the counts so a number is never presented without its interpretation.
EXPLANATIONS: dict[str, tuple[str, str]] = {
    RejectionReason.NO_CHAMPION.value: (
        "No strategy has been promoted to champion, so nothing is authorised to trade.",
        "Check /research for what the validation gates are rejecting.",
    ),
    RejectionReason.NO_VALIDATED_EDGE.value: (
        "Research has run and found no hypothesis that survives costs and validation.",
        "This is a valid outcome. Do not lower the gates to produce trades.",
    ),
    RejectionReason.WARMUP.value: (
        "Not enough history yet to compute the features a decision needs.",
        "Wait. The warmup window is research.min_warmup_s.",
    ),
    RejectionReason.DATA_QUALITY.value: (
        "The symbol's data-quality score or flags make its state untrustworthy.",
        "Check /data-quality for the flag; a desynced book or stale feed blocks entry.",
    ),
    RejectionReason.STALE_FEED.value: (
        "No market event arrived for this symbol within the staleness window.",
        "Check the feed connection and /health.",
    ),
    RejectionReason.CROSSED_BOOK.value: (
        "The best bid is at or above the best ask, so the book is not usable.",
        "Usually transient; a persistent crossed book means a desync.",
    ),
    RejectionReason.SPREAD_TOO_WIDE.value: (
        "The spread at decision time exceeded quality.max_spread_bps.",
        "Widening this makes trades possible and edges smaller. Diagnose first.",
    ),
    RejectionReason.INSUFFICIENT_LIQUIDITY.value: (
        "The visible book could not absorb the intended size.",
        "Either the size is too large or the symbol is too thin to trade.",
    ),
    RejectionReason.CONDITIONS_NOT_MET.value: (
        "The champion's feature conditions did not hold. This is the normal case.",
        "A high count here is expected: a selective strategy is mostly not firing.",
    ),
    RejectionReason.EDGE_BELOW_COSTS.value: (
        "The expected edge did not exceed the round trip it would have to pay.",
        "The most common honest reason for no trades at these horizons.",
    ),
    RejectionReason.LOW_CONFIDENCE.value: (
        "The signal fired but its calibrated confidence was too low to act on.",
        "Check calibration on /strategies before changing the threshold.",
    ),
    RejectionReason.REGIME_FILTER.value: (
        "The strategy is validated for a regime that is not the current one.",
        "Expected: a conditional edge should not trade outside its condition.",
    ),
    RejectionReason.COOLDOWN.value: (
        "A signal fired for this symbol within the cooldown window.",
        "Prevents one condition from producing a burst of correlated positions.",
    ),
    RejectionReason.DUPLICATE_SIGNAL.value: (
        "Same symbol, same direction, inside the duplicate-suppression window.",
        "Prevents double-counting one market event as two independent signals.",
    ),
    RejectionReason.MAX_POSITIONS.value: (
        "The concurrent-position limit is already reached.",
        "Raise risk.max_concurrent_positions only if the edge justifies it.",
    ),
    RejectionReason.MAX_EXPOSURE.value: (
        "Total notional exposure is at its cap.",
        "Positions must close before new ones open.",
    ),
    RejectionReason.SYMBOL_EXPOSURE.value: (
        "This symbol is already at its individual exposure cap.",
        "Prevents one market from becoming the whole book.",
    ),
    RejectionReason.POSITION_ALREADY_OPEN.value: (
        "A position in this symbol is already open.",
        "The system does not add to positions in v1.",
    ),
    RejectionReason.NOTIONAL_TOO_SMALL.value: (
        "After the exposure caps, the position would be below the minimum notional.",
        "Common on a small wallet with a wide stop.",
    ),
    RejectionReason.INSUFFICIENT_BALANCE.value: (
        "Not enough available margin for the position.",
        "Check /wallet: margin is reserved by open positions.",
    ),
    RejectionReason.DAILY_LOSS_LIMIT.value: (
        "The daily loss circuit breaker has fired.",
        "Entries stay blocked until the next day.",
    ),
    RejectionReason.MAX_DRAWDOWN.value: (
        "The cycle drawdown circuit breaker has fired.",
        "Entries stay blocked; the cycle may be heading for a post-mortem.",
    ),
    RejectionReason.CYCLE_NOT_ACTIVE.value: (
        "The cycle is in post-mortem or awaiting a validated edge.",
        "Check /cycles for the post-mortem and what it is waiting for.",
    ),
    RejectionReason.ENTRIES_BLOCKED.value: (
        "Entries are blocked administratively or by a circuit breaker.",
        "The block reason is on /health under risk.",
    ),
    RejectionReason.SHADOW_ONLY.value: (
        "The signal came from a shadow strategy, which never touches the wallet.",
        "Working as designed: shadow signals are measured, not traded.",
    ),
}


@dataclass
class _Event:
    ts_ms: int
    reason: str


@dataclass
class DiagnosticsCollector:
    window_s: float = 3600.0
    totals: dict[str, int] = field(default_factory=dict)
    accepted: int = 0
    evaluated: int = 0
    shadow_signals: int = 0
    _events: deque[_Event] = field(default_factory=lambda: deque(maxlen=200_000))
    started_ms: int = field(default_factory=now_ms)

    def record_rejection(self, reason: RejectionReason | str, *, at_ms: int | None = None) -> None:
        value = reason.value if isinstance(reason, RejectionReason) else str(reason)
        stamp = at_ms if at_ms is not None else now_ms()
        self.totals[value] = self.totals.get(value, 0) + 1
        self.evaluated += 1
        self._events.append(_Event(stamp, value))

    def record_acceptance(self, *, at_ms: int | None = None) -> None:
        self.accepted += 1
        self.evaluated += 1
        self._events.append(_Event(at_ms if at_ms is not None else now_ms(), "ACCEPTED"))

    def record_shadow(self) -> None:
        self.shadow_signals += 1

    # --------------------------------------------------------------- report

    def _windowed(self, at_ms: int | None = None) -> dict[str, int]:
        stamp = at_ms if at_ms is not None else now_ms()
        cutoff = stamp - int(self.window_s * 1000)
        counts: dict[str, int] = {}
        for event in reversed(self._events):
            if event.ts_ms < cutoff:
                break
            counts[event.reason] = counts.get(event.reason, 0) + 1
        return counts

    def report(self, *, at_ms: int | None = None) -> dict[str, Any]:
        windowed = self._windowed(at_ms)
        window_total = sum(windowed.values())
        rows = []
        for reason, count in sorted(windowed.items(), key=lambda kv: kv[1], reverse=True):
            what, action = EXPLANATIONS.get(reason, ("", ""))
            rows.append(
                {
                    "reason": reason,
                    "count": count,
                    "percent": round(count / window_total * 100.0, 2) if window_total else 0.0,
                    "total_since_start": self.totals.get(reason, self.accepted if reason == "ACCEPTED" else 0),
                    "explanation": what,
                    "action": action,
                }
            )
        return {
            "window_s": self.window_s,
            "evaluated_total": self.evaluated,
            "accepted_total": self.accepted,
            "shadow_signals_total": self.shadow_signals,
            "acceptance_rate": round(self.accepted / self.evaluated, 6) if self.evaluated else 0.0,
            "window_evaluated": window_total,
            "window_accepted": windowed.get("ACCEPTED", 0),
            "rejections": rows,
            "top_reason": rows[0]["reason"] if rows and rows[0]["reason"] != "ACCEPTED" else (
                rows[1]["reason"] if len(rows) > 1 else None
            ),
            "uptime_s": round((now_ms() - self.started_ms) / 1000.0, 1),
        }

    def summary_sentence(self, *, at_ms: int | None = None) -> str:
        """One line a human can read without opening the JSON."""
        report = self.report(at_ms=at_ms)
        if report["window_evaluated"] == 0:
            return "No decisions have been evaluated in the reporting window."
        if report["window_accepted"]:
            return (
                f"{report['window_accepted']} of {report['window_evaluated']} decisions were "
                f"accepted in the last {int(self.window_s)}s."
            )
        rows = [r for r in report["rejections"] if r["reason"] != "ACCEPTED"]
        if not rows:
            return "No decisions were rejected, and none were accepted."
        top = rows[0]
        return (
            f"No trades in the last {int(self.window_s)}s: {top['percent']:.0f}% of "
            f"{report['window_evaluated']} decisions were blocked by {top['reason']}. "
            f"{top['explanation']}"
        )


def reason_catalogue() -> list[dict[str, str]]:
    """Every gate the system can report, whether or not it has fired.

    Served by ``/diagnostics`` so the UI can show gates at zero — a gate that
    has never fired is information too.
    """
    return [
        {
            "reason": reason.value,
            "explanation": EXPLANATIONS.get(reason.value, ("", ""))[0],
            "action": EXPLANATIONS.get(reason.value, ("", ""))[1],
        }
        for reason in RejectionReason
    ]
