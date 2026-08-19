"""Momentum agent.

Tests continuation: that a market which has just moved keeps moving.  Its inputs
are the return ladder, realized volatility and its expansion, aggressive volume
and trade counts — the observable signatures of a move still in progress rather
than one already finished.

It proposes in both regimes it can see.  Continuation is strongly
regime-dependent — the same 30 bps burst means "trend" in a quiet book and
"noise" in a fast one — so a subset of proposals carry an explicit regime
filter, and the validation lab reports stability across regimes rather than
averaging them.
"""

from __future__ import annotations

from ..domain import Regime
from ..research.hypotheses import Hypothesis
from .base import AgentContext, ResearchAgent


class MomentumAgent(ResearchAgent):
    name = "momentum"
    family = "momentum"
    description = (
        "Continuation, breakout, acceleration, volume burst, volatility expansion and persistence"
    )
    inputs = ("ret_", "vol_", "vol_ratio", "trades_", "buy_vol_", "sell_vol_", "trend_z")
    horizons_ms = (1000, 2000, 5000, 10000)

    #: Regimes worth conditioning on. UNKNOWN is excluded: filtering on "we do
    #: not know" is not a hypothesis.
    regimes = (None, Regime.TRENDING_UP, Regime.TRENDING_DOWN, Regime.HIGH_VOL)

    def candidate_features(self, view, symbol: str) -> list[str]:
        names = view.feature_names(symbol)
        return [
            name
            for name in names
            if any(name.startswith(prefix) for prefix in self.inputs)
            # A return measured over the same window as the horizon is close to
            # the horizon's own definition; the shorter ones carry the signal.
            and not name.startswith("ret_60s")
        ]

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
                proposals.extend(
                    self.propose_from_ranks(
                        context, ranks, signal_symbol=symbol, execution_symbol=symbol,
                        horizon_ms=horizon, limit=1,
                    )
                )
                # One conditional variant per symbol, on the regime actually
                # present in the discovery slice.
                regime = self._dominant_regime(discovery, symbol)
                if regime is not None and len(proposals) < context.max_proposals:
                    proposals.extend(
                        self.propose_from_ranks(
                            context, ranks, signal_symbol=symbol, execution_symbol=symbol,
                            horizon_ms=horizon, limit=1, regime_filter=regime,
                        )
                    )
        return proposals[: context.max_proposals]

    @staticmethod
    def _dominant_regime(view, symbol: str) -> Regime | None:
        series = view.series.get(symbol)
        if not series or not series.regime:
            return None
        counts: dict[Regime, int] = {}
        for regime in series.regime:
            if regime is Regime.UNKNOWN:
                continue
            counts[regime] = counts.get(regime, 0) + 1
        if not counts:
            return None
        regime, count = max(counts.items(), key=lambda kv: kv[1])
        # Below a quarter of the slice there is not enough of it to condition on.
        return regime if count >= len(series.regime) * 0.25 else None
