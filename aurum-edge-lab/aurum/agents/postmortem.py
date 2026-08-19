"""Cycle post-mortem agent.

Runs only on failure or material degradation.  Its job is to answer "what
actually killed this cycle?" with evidence, before the system is allowed to
propose anything new — because a reset without a diagnosis is just a retry, and
a system that retries without learning will make the same loss repeatedly and
call it research.

It attributes losses across the causes the blueprint names, and each attribution
is computed from the cycle's own trades rather than asserted:

``EDGE_DECAY``        the champion's live edge fell far below its validated one.
``EXECUTION_COST``    the trades were directionally right and lost to costs.
``SLIPPAGE``          fills were materially worse than the prices requested.
``REGIME_SHIFT``      the losses concentrate in a regime the edge was not
                      validated in.
``CONCENTRATION``     one symbol or a handful of trades dominates the loss.
``POOR_CALIBRATION``  realised edge is systematically below what was expected.
``SIZING``            per-trade risk was too large for the edge's hit rate.
``INSUFFICIENT_EVIDENCE`` too few trades to attribute anything, which is itself
                      a finding and must not be dressed up as one of the above.
"""

from __future__ import annotations

import statistics
from dataclasses import dataclass, field
from typing import Any, Sequence

from ..domain import PaperTrade, Regime, now_ms
from ..logging_setup import get_logger
from ..storage.repositories import ResearchRepository

log = get_logger("agents.postmortem")


class Cause:
    EDGE_DECAY = "EDGE_DECAY"
    EXECUTION_COST = "EXECUTION_COST"
    SLIPPAGE = "SLIPPAGE"
    REGIME_SHIFT = "REGIME_SHIFT"
    CONCENTRATION = "CONCENTRATION"
    POOR_CALIBRATION = "POOR_CALIBRATION"
    SIZING = "SIZING"
    LATENCY = "LATENCY"
    INSUFFICIENT_EVIDENCE = "INSUFFICIENT_EVIDENCE"


@dataclass
class CauseFinding:
    cause: str
    weight: float
    detail: str
    evidence: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "cause": self.cause,
            "weight": round(self.weight, 4),
            "detail": self.detail,
            "evidence": self.evidence,
        }


@dataclass
class PostMortem:
    cycle_id: int
    created_ms: int
    verdict: str
    primary_cause: str
    causes: list[CauseFinding] = field(default_factory=list)
    evidence: dict[str, Any] = field(default_factory=dict)
    recommendations: list[str] = field(default_factory=list)
    trades_analyzed: int = 0
    net_pnl: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        return {
            "cycle_id": self.cycle_id,
            "created_ms": self.created_ms,
            "verdict": self.verdict,
            "primary_cause": self.primary_cause,
            "causes": [c.to_dict() for c in self.causes],
            "evidence": self.evidence,
            "recommendations": self.recommendations,
            "trades_analyzed": self.trades_analyzed,
            "net_pnl": round(self.net_pnl, 6),
        }

    def to_row(self) -> dict[str, Any]:
        return {
            "cycle_id": self.cycle_id,
            "created_ms": self.created_ms,
            "verdict": self.verdict,
            "primary_cause": self.primary_cause,
            "causes": [c.to_dict() for c in self.causes],
            "evidence": self.evidence,
            "recommendations": self.recommendations,
            "trades_analyzed": self.trades_analyzed,
            "net_pnl": self.net_pnl,
        }


class CyclePostMortemAgent:
    name = "cycle_postmortem"
    family = "diagnostics"
    description = "Attributes a failed or degraded cycle to root causes, with evidence"

    #: Below this many trades, attribution is storytelling.
    min_trades_for_attribution = 8

    def __init__(self, repo: ResearchRepository) -> None:
        self.repo = repo
        self.runs = 0
        self.last_run_ms = 0

    def analyse(
        self,
        *,
        cycle_id: int,
        trades: Sequence[PaperTrade],
        starting_balance: float,
        final_equity: float,
        validated_edge_bps: float = 0.0,
        end_reason: str = "",
        at_ms: int | None = None,
    ) -> PostMortem:
        stamp = at_ms if at_ms is not None else now_ms()
        self.runs += 1
        self.last_run_ms = stamp

        net_pnl = final_equity - starting_balance
        post = PostMortem(
            cycle_id=cycle_id,
            created_ms=stamp,
            verdict="",
            primary_cause="",
            trades_analyzed=len(trades),
            net_pnl=net_pnl,
        )

        if len(trades) < self.min_trades_for_attribution:
            post.verdict = "INSUFFICIENT_EVIDENCE"
            post.primary_cause = Cause.INSUFFICIENT_EVIDENCE
            post.causes = [
                CauseFinding(
                    Cause.INSUFFICIENT_EVIDENCE, 1.0,
                    f"{len(trades)} trades is too few to attribute a cause; "
                    f"anything more specific would be invented",
                    {"trades": len(trades), "minimum": self.min_trades_for_attribution},
                )
            ]
            post.evidence = {"net_pnl": net_pnl, "end_reason": end_reason}
            post.recommendations = [
                "Collect more live evidence before drawing conclusions",
                "Check /diagnostics for which gate is suppressing signals",
            ]
            self._persist(post)
            return post

        gross_bps = [t.return_bps for t in trades]
        net_bps = [t.net_return_bps for t in trades]
        cost_bps = [t.cost_bps for t in trades]
        slippage = [t.entry_slippage_bps + t.exit_slippage_bps for t in trades]
        expected = [t.expected_edge_bps for t in trades if t.expected_edge_bps]

        mean_gross = statistics.fmean(gross_bps)
        mean_net = statistics.fmean(net_bps)
        mean_cost = statistics.fmean(cost_bps)
        mean_slip = statistics.fmean(slippage)

        findings: list[CauseFinding] = []

        # --- costs ate a real directional edge ---------------------------
        if mean_gross > 0 and mean_net <= 0:
            findings.append(
                CauseFinding(
                    Cause.EXECUTION_COST,
                    weight=min(1.0, mean_cost / max(1e-9, abs(mean_gross))),
                    detail=(
                        f"direction was right on average ({mean_gross:+.2f} bps gross) but "
                        f"{mean_cost:.2f} bps of costs turned it into {mean_net:+.2f} bps net"
                    ),
                    evidence={"mean_gross_bps": round(mean_gross, 3),
                              "mean_cost_bps": round(mean_cost, 3),
                              "mean_net_bps": round(mean_net, 3)},
                )
            )

        # --- slippage specifically ----------------------------------------
        if mean_slip > mean_cost * 0.5 and mean_slip > 1.0:
            findings.append(
                CauseFinding(
                    Cause.SLIPPAGE,
                    weight=min(1.0, mean_slip / max(1e-9, mean_cost)),
                    detail=(
                        f"fills averaged {mean_slip:.2f} bps worse than the prices requested, "
                        f"which is {mean_slip / max(1e-9, mean_cost):.0%} of total costs"
                    ),
                    evidence={"mean_slippage_bps": round(mean_slip, 3),
                              "worst_bps": round(max(slippage), 3)},
                )
            )

        # --- the edge simply was not there --------------------------------
        if validated_edge_bps > 0 and mean_net < validated_edge_bps * 0.4:
            findings.append(
                CauseFinding(
                    Cause.EDGE_DECAY,
                    weight=min(1.0, 1.0 - mean_net / max(1e-9, validated_edge_bps)),
                    detail=(
                        f"live net {mean_net:+.2f} bps against a validated {validated_edge_bps:+.2f} "
                        f"bps: the edge did not survive contact with the live feed"
                    ),
                    evidence={"validated_bps": round(validated_edge_bps, 3),
                              "live_bps": round(mean_net, 3)},
                )
            )

        # --- calibration: was the expectation itself wrong? ---------------
        if expected and len(expected) >= self.min_trades_for_attribution:
            mean_expected = statistics.fmean(expected)
            if mean_expected > 0 and mean_gross < mean_expected * 0.3:
                findings.append(
                    CauseFinding(
                        Cause.POOR_CALIBRATION,
                        weight=min(1.0, 1.0 - mean_gross / mean_expected),
                        detail=(
                            f"predicted {mean_expected:+.2f} bps, realised {mean_gross:+.2f} bps "
                            f"gross: the confidence attached to these signals was overstated"
                        ),
                        evidence={"mean_expected_bps": round(mean_expected, 3),
                                  "mean_realised_bps": round(mean_gross, 3)},
                    )
                )

        # --- regime ---------------------------------------------------------
        by_regime = self._group_pnl(trades, key=lambda t: t.regime.value)
        losing_regimes = {k: v for k, v in by_regime.items() if v["net_pnl"] < 0}
        if losing_regimes and len(by_regime) > 1:
            worst = min(losing_regimes.items(), key=lambda kv: kv[1]["net_pnl"])
            share = worst[1]["net_pnl"] / net_pnl if net_pnl < 0 else 0.0
            if share > 0.6:
                findings.append(
                    CauseFinding(
                        Cause.REGIME_SHIFT,
                        weight=min(1.0, share),
                        detail=(
                            f"{share:.0%} of the loss came from the {worst[0]} regime "
                            f"({worst[1]['trades']} trades)"
                        ),
                        evidence={"by_regime": by_regime},
                    )
                )

        # --- concentration ---------------------------------------------------
        by_symbol = self._group_pnl(trades, key=lambda t: t.symbol)
        if net_pnl < 0 and by_symbol:
            worst_symbol = min(by_symbol.items(), key=lambda kv: kv[1]["net_pnl"])
            share = worst_symbol[1]["net_pnl"] / net_pnl
            if share > 0.7 and len(by_symbol) > 1:
                findings.append(
                    CauseFinding(
                        Cause.CONCENTRATION,
                        weight=min(1.0, share),
                        detail=(
                            f"{share:.0%} of the loss is one symbol ({worst_symbol[0]}, "
                            f"{worst_symbol[1]['trades']} trades)"
                        ),
                        evidence={"by_symbol": by_symbol},
                    )
                )
        worst_trade = min(trades, key=lambda t: t.net_pnl_eur)
        if net_pnl < 0 and worst_trade.net_pnl_eur / net_pnl > 0.4:
            findings.append(
                CauseFinding(
                    Cause.SIZING,
                    weight=min(1.0, worst_trade.net_pnl_eur / net_pnl),
                    detail=(
                        f"a single trade ({worst_trade.symbol}, {worst_trade.net_pnl_eur:+.2f} EUR) "
                        f"is {worst_trade.net_pnl_eur / net_pnl:.0%} of the cycle's loss"
                    ),
                    evidence={"trade_id": worst_trade.trade_id,
                              "net_pnl_eur": round(worst_trade.net_pnl_eur, 4),
                              "return_bps": round(worst_trade.net_return_bps, 3)},
                )
            )

        if not findings:
            findings.append(
                CauseFinding(
                    Cause.INSUFFICIENT_EVIDENCE, 0.5,
                    "no single cause dominates: the losses look like ordinary variance "
                    "around a small or absent edge",
                    {"mean_net_bps": round(mean_net, 3), "trades": len(trades)},
                )
            )

        findings.sort(key=lambda f: f.weight, reverse=True)
        post.causes = findings
        post.primary_cause = findings[0].cause
        post.verdict = "FAILED" if net_pnl < 0 else "DEGRADED"
        post.evidence = {
            "trades": len(trades),
            "net_pnl": round(net_pnl, 6),
            "mean_gross_bps": round(mean_gross, 3),
            "mean_net_bps": round(mean_net, 3),
            "mean_cost_bps": round(mean_cost, 3),
            "mean_slippage_bps": round(mean_slip, 3),
            "win_rate": round(sum(1 for t in trades if t.net_pnl_eur > 0) / len(trades), 4),
            "by_symbol": by_symbol,
            "by_regime": by_regime,
            "by_exit_reason": self._group_pnl(trades, key=lambda t: t.exit_reason.value),
            "end_reason": end_reason,
        }
        post.recommendations = self._recommend(findings, post.evidence)
        self._persist(post)
        return post

    @staticmethod
    def _group_pnl(trades: Sequence[PaperTrade], key) -> dict[str, dict[str, Any]]:
        grouped: dict[str, dict[str, Any]] = {}
        for trade in trades:
            bucket = grouped.setdefault(key(trade), {"trades": 0, "net_pnl": 0.0, "net_bps": 0.0})
            bucket["trades"] += 1
            bucket["net_pnl"] = round(bucket["net_pnl"] + trade.net_pnl_eur, 6)
            bucket["net_bps"] = round(bucket["net_bps"] + trade.net_return_bps, 4)
        return grouped

    @staticmethod
    def _recommend(findings: list[CauseFinding], evidence: dict[str, Any]) -> list[str]:
        """Recommendations are about what to *research*, never about loosening a
        gate.  "Lower the threshold until something trades" is the one
        conclusion this system is not allowed to reach."""
        primary = findings[0].cause
        common = [
            "Record the failed parameter region in research memory so it is not re-proposed",
        ]
        specific = {
            Cause.EXECUTION_COST: [
                "Require a larger validated edge relative to the round trip before promotion",
                "Prefer longer horizons, where the same cost is a smaller share of the move",
                "Restrict to symbols and hours whose spread is consistently tighter",
            ],
            Cause.SLIPPAGE: [
                "Cap position size against resting depth at the touch rather than the top ten levels",
                "Re-measure the cost model's extra_slippage_bps against realised fills",
            ],
            Cause.EDGE_DECAY: [
                "Shorten the shadow-to-champion interval so decay is caught before the wallet is",
                "Investigate whether the edge was ever present out of sample or only in the fit",
            ],
            Cause.POOR_CALIBRATION: [
                "Compare predicted against realised edge per strategy and recalibrate confidence",
                "Raise the minimum independent sample count before a hypothesis may be promoted",
            ],
            Cause.REGIME_SHIFT: [
                "Add an explicit regime filter to the hypothesis rather than trading it everywhere",
                "Require validation stability across regimes, not only in aggregate",
            ],
            Cause.CONCENTRATION: [
                "Tighten the per-symbol exposure cap",
                "Require the edge to be present on more than one symbol before promotion",
            ],
            Cause.SIZING: [
                "Reduce risk per trade until the hit rate justifies the position size",
                "Widen stops relative to volatility so a single move cannot dominate the cycle",
            ],
            Cause.INSUFFICIENT_EVIDENCE: [
                "Keep researching: a cycle that ends without a diagnosable cause has not "
                "demonstrated the absence of an edge, only the absence of evidence",
            ],
        }
        return specific.get(primary, []) + common

    def _persist(self, post: PostMortem) -> None:
        self.repo.log_agent_event(
            self.name,
            "postmortem",
            f"cycle {post.cycle_id}: {post.verdict} ({post.primary_cause})",
            severity="WARNING",
            detail=post.to_dict(),
        )
        log.warning(
            "post-mortem complete",
            extra={"cycle": post.cycle_id, "verdict": post.verdict, "cause": post.primary_cause},
        )

    def state(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "family": self.family,
            "description": self.description,
            "enabled": True,
            "runs": self.runs,
            "last_run_ms": self.last_run_ms,
            "min_trades_for_attribution": self.min_trades_for_attribution,
            "note": "runs only on cycle failure or champion degradation",
        }
