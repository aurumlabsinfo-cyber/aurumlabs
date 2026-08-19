"""Mean reversion agent.

The mirror of momentum, and deliberately built as its own agent rather than as
momentum with a sign flip.  The two disagree about the same features, and
letting them compete on the same evidence is the point: whichever survives
validation does so against a rival that had the same data.

What it tests: exhaustion after an extended move, temporary dislocation from the
microprice, reversal after a liquidity sweep, and overextension relative to
recent volatility.  It proposes from the *tails* of a feature's distribution
rather than the shoulders, because reversion is a tail phenomenon — the claim is
about the unusual move, not the typical one.
"""

from __future__ import annotations

from ..domain import Direction
from ..research.hypotheses import (
    Hypothesis,
    PercentileCondition,
    new_hypothesis_id,
    resolve_thresholds,
)
from .base import AgentContext, ResearchAgent


class MeanReversionAgent(ResearchAgent):
    name = "mean_reversion"
    family = "mean_reversion"
    description = (
        "Exhaustion, temporary dislocation, post-sweep reversal, overextension and "
        "microprice divergence"
    )
    inputs = ("ret_", "micro_dev_bps", "liq_removed_", "liq_net_", "vol_ratio", "imbalance_", "trend_z")
    horizons_ms = (1000, 2000, 5000, 10000)

    #: Reversion lives in the tails, so only extreme buckets are proposed.
    tails = ((95.0, "<="), (5.0, ">="))

    def candidate_features(self, view, symbol: str) -> list[str]:
        names = view.feature_names(symbol)
        return [name for name in names if any(name.startswith(prefix) for prefix in self.inputs)]

    def build(self, context: AgentContext) -> list[Hypothesis]:
        discovery = self.discovery_view(context.view)
        proposals: list[Hypothesis] = []
        symbols = sorted(discovery.symbols, key=lambda s: discovery.length(s), reverse=True)

        for symbol in symbols:
            if len(proposals) >= context.max_proposals:
                break
            features = self.candidate_features(discovery, symbol)
            if not features:
                continue
            for horizon in self.horizons_ms:
                if len(proposals) >= context.max_proposals:
                    break
                ranks = self.rank_features(discovery, symbol, features, horizon)
                if not ranks:
                    continue
                proposals.extend(self._tail_hypotheses(context, discovery, ranks, symbol, horizon))
        return proposals[: context.max_proposals]

    def _tail_hypotheses(self, context, discovery, ranks, symbol: str, horizon_ms: int):
        """Propose the reversion reading of the strongest candidate.

        Where the momentum agent would take the upper tail long, this takes it
        short: the same feature, the same tail, the opposite claim about what
        follows.  Both go to the validation lab and at most one survives.
        """
        proposals: list[Hypothesis] = []
        for rank in ranks[:3]:
            if len(proposals) >= 2:
                break
            if abs(rank.correlation) < self.min_abs_correlation:
                self.metrics.dropped_weak += 1
                continue
            for percentile, _ in self.tails:
                op = ">=" if percentile >= 50 else "<="
                # High feature value that momentum reads as continuation, this
                # agent reads as exhaustion.
                direction = Direction.SHORT if percentile >= 50 else Direction.LONG
                condition = PercentileCondition(feature=rank.feature, op=op, percentile=percentile)
                distributions = discovery.distributions_for(symbol, [rank.feature])
                if not resolve_thresholds([condition], distributions):
                    self.metrics.dropped_unresolvable += 1
                    continue
                start_ms, end_ms = discovery.range_ms()
                proposals.append(
                    Hypothesis(
                        hypothesis_id=new_hypothesis_id(self.name),
                        agent=self.name,
                        family=self.family,
                        signal_symbol=symbol,
                        execution_symbol=symbol,
                        direction=direction,
                        conditions=[condition],
                        horizon_ms=horizon_ms,
                        cost_model_version=context.costs.version,
                        dataset_start_ms=start_ms,
                        dataset_end_ms=end_ms,
                        cycle_id=context.cycle_id,
                    )
                )
                if len(proposals) >= 2:
                    break
        return proposals
