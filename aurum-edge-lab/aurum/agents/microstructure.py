"""Microstructure agent.

Reads the shape and flow of the book itself rather than the price path: order
flow imbalance, resting-depth imbalance, microprice divergence, spread
compression and expansion, liquidity removal and replenishment, and clusters of
aggressive trades.

The claim it tests is the oldest one in market microstructure — that pressure
visible in the book precedes the price move it causes.  At the horizons here
(half a second to five seconds) that pressure is genuinely measurable, which is
exactly why it is also almost entirely arbitraged away: the agent's usual honest
output is a set of hypotheses that are statistically real and economically dead.
"""

from __future__ import annotations

from ..research.hypotheses import Hypothesis
from .base import AgentContext, ResearchAgent


class MicrostructureAgent(ResearchAgent):
    name = "microstructure"
    family = "microstructure"
    description = (
        "Order flow imbalance, book imbalance, microprice divergence, spread dynamics, "
        "liquidity removal and aggressive-trade clusters"
    )
    inputs = (
        "ofi_norm_", "ofi_", "imbalance_", "micro_dev_bps", "flow_imbalance_",
        "liq_pressure_", "liq_net_", "liq_removed_", "spread_bps", "trades_",
    )
    horizons_ms = (500, 1000, 2000, 5000)

    def candidate_features(self, view, symbol: str) -> list[str]:
        names = view.feature_names(symbol)
        return [name for name in names if any(name.startswith(prefix) for prefix in self.inputs)]

    def build(self, context: AgentContext) -> list[Hypothesis]:
        discovery = self.discovery_view(context.view)
        proposals: list[Hypothesis] = []

        # Rank symbols by how much history they have: a symbol that joined late
        # gives a weaker read, and spending the budget there is wasteful.
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
                proposals.extend(
                    self.propose_from_ranks(
                        context,
                        ranks,
                        signal_symbol=symbol,
                        execution_symbol=symbol,
                        horizon_ms=horizon,
                        limit=1,
                    )
                )
        return proposals[: context.max_proposals]
