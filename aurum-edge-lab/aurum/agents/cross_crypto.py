"""Cross-crypto agent.

Trades one market on another market's signal.  Its input is the cross-market
engine's ranked relations — all 90 ordered pairs at 11 lags — and its output is
a hypothesis per relation worth testing: *when the leader moves, the follower
follows, this far behind*.

Two things keep it honest.

The **lag becomes the entry delay**, not a fudge factor.  If BTC is measured to
lead DOGE by 750 ms, the hypothesis enters DOGE 750 ms after the BTC condition
fires, and is measured from there.  Entering immediately would be measuring a
different claim.

It **only proposes relations the cross-market engine already found**, and the
engine already charges each one a round trip.  A pair whose conditional net edge
is deeply negative is not proposed at all: it has been measured, it lost, and
spending a validation slot on it would only add to the multiple-testing burden.
"""

from __future__ import annotations

from typing import Any

from ..research.hypotheses import Hypothesis
from .base import AgentContext, ResearchAgent


class CrossCryptoAgent(ResearchAgent):
    name = "cross_crypto"
    family = "cross_market"
    description = (
        "All 90 directional relationships among the 10 markets, at lead-lag horizons from "
        "100 ms to 60 s"
    )
    inputs = ("ret_", "ofi_norm_", "flow_imbalance_", "micro_dev_bps")
    horizons_ms = (1000, 2000, 5000)

    #: A relation the cross-market engine scored below this is not worth a test.
    min_predictive_score = 0.02
    #: How far below break-even a relation may sit and still be worth measuring
    #: properly. Relations that lose by more than this are already answered.
    max_negative_net_edge_bps = -25.0

    def candidate_features(self, view, symbol: str) -> list[str]:
        names = view.feature_names(symbol)
        return [name for name in names if any(name.startswith(prefix) for prefix in self.inputs)]

    def build(self, context: AgentContext) -> list[Hypothesis]:
        discovery = self.discovery_view(context.view)
        relations = self._usable_relations(context)
        if not relations:
            return []

        proposals: list[Hypothesis] = []
        for relation in relations:
            if len(proposals) >= context.max_proposals:
                break
            leader = getattr(relation, "leader", None)
            follower = getattr(relation, "follower", None)
            lag_ms = int(getattr(relation, "best_lag_ms", 0) or 0)
            if not leader or not follower or lag_ms <= 0:
                continue
            if leader not in discovery.series or follower not in discovery.series:
                continue

            features = self.candidate_features(discovery, leader)
            if not features:
                continue
            # Rank the leader's features against the *follower's* forward
            # return: that is the relationship being claimed.
            horizon = max(context.view.cadence_ms, lag_ms)
            ranks = self.rank_features(
                discovery, leader, features, horizon, execution_symbol=follower
            )
            if not ranks:
                continue
            proposals.extend(
                self.propose_from_ranks(
                    context,
                    ranks,
                    signal_symbol=leader,
                    execution_symbol=follower,
                    horizon_ms=horizon,
                    entry_delay_ms=lag_ms,
                    limit=1,
                )
            )
        return proposals[: context.max_proposals]

    def _usable_relations(self, context: AgentContext) -> list[Any]:
        usable = []
        for relation in context.relations:
            score = float(getattr(relation, "predictive_score", 0.0) or 0.0)
            net = float(getattr(relation, "conditional_net_edge_bps", 0.0) or 0.0)
            if score < self.min_predictive_score:
                continue
            if net < self.max_negative_net_edge_bps:
                # Already measured, already lost, by a wide margin.
                self.metrics.dropped_weak += 1
                continue
            usable.append(relation)
        usable.sort(key=lambda r: float(getattr(r, "predictive_score", 0.0) or 0.0), reverse=True)
        return usable
