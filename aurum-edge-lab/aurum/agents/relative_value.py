"""Relative value agent.

Everything here is perpetual-specific: the instruments have a mark price, an
index price and a funding rate, and the relationships between those three and
the traded mid are where a perp differs from a spot pair.

What it tests:

* **mark/mid deviation** — the mark is what liquidations are priced against, so
  a mid that has run away from it tends to be pulled back.
* **mark/index deviation (basis)** — the perp trading away from its underlying.
* **funding dislocation** — a funding rate far from zero is the market paying
  one side to hold; the payment and the price pressure that created it are not
  independent.
* **cross-market spreads** — one market rich against another it normally tracks.

Funding deserves a caution the agent encodes: the rate is quoted per settlement
interval, so a rate that looks enormous in annualised terms is small over a
five-second horizon.  ``funding_bps_8h`` is stored in the same units as an edge
so the comparison is direct instead of eyeballed.
"""

from __future__ import annotations

from ..research.hypotheses import Hypothesis
from .base import AgentContext, ResearchAgent


class RelativeValueAgent(ResearchAgent):
    name = "relative_value"
    family = "relative_value"
    description = (
        "Cross-market spreads, mark/index deviations, funding and basis dislocations, "
        "convergence and divergence"
    )
    inputs = (
        "mark_dev_bps", "index_dev_bps", "funding_bps_8h", "funding_rate", "funding_countdown_s",
        "micro_dev_bps", "spread_bps",
    )
    horizons_ms = (2000, 5000, 10000, 30000)

    def candidate_features(self, view, symbol: str) -> list[str]:
        names = view.feature_names(symbol)
        return [name for name in names if any(name.startswith(prefix) for prefix in self.inputs)]

    def build(self, context: AgentContext) -> list[Hypothesis]:
        discovery = self.discovery_view(context.view)
        proposals: list[Hypothesis] = []
        symbols = sorted(discovery.symbols, key=lambda s: discovery.length(s), reverse=True)

        # Own-symbol relative value: the perp against its own mark and index.
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
                proposals.extend(
                    self.propose_from_ranks(
                        context, ranks, signal_symbol=symbol, execution_symbol=symbol,
                        horizon_ms=horizon, limit=1,
                    )
                )

        # Cross-symbol relative value: one market's dislocation read on another.
        if len(proposals) < context.max_proposals and len(symbols) >= 2:
            proposals.extend(self._pair_hypotheses(context, discovery, symbols))
        return proposals[: context.max_proposals]

    def _pair_hypotheses(self, context: AgentContext, discovery, symbols: list[str]) -> list[Hypothesis]:
        """Read one market's basis dislocation against another market's move."""
        proposals: list[Hypothesis] = []
        anchor = symbols[0]
        for follower in symbols[1:3]:
            if len(proposals) >= 2:
                break
            features = [f for f in self.candidate_features(discovery, anchor) if "dev_bps" in f]
            if not features:
                continue
            ranks = self.rank_features(
                discovery, anchor, features, 5000, execution_symbol=follower
            )
            if not ranks:
                continue
            proposals.extend(
                self.propose_from_ranks(
                    context, ranks, signal_symbol=anchor, execution_symbol=follower,
                    horizon_ms=5000, limit=1,
                )
            )
        return proposals
