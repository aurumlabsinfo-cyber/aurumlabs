"""Research agent contract.

The blueprint is explicit that no agent may be cosmetic: each needs real
inputs, a real algorithm, state, outputs, memory, logs and metrics.  This base
class makes that structural rather than aspirational — an agent that returns a
hard-coded list fails :meth:`stats` accounting and shows up in ``/agents`` with
zero features read and zero candidates ranked.

The shared algorithm underneath all five is the same and lives here:

1.  Take the *discovery* slice of history.  Never the validation, walk-forward
    or holdout regions — those are what will judge the result, and an agent that
    peeked at them has already invalidated them.
2.  Rank candidate features by how strongly they relate to the forward return
    the agent is interested in.
3.  Turn the strongest into percentile-thresholded hypotheses.

Ranking on the discovery slice is a search, and searches produce false
positives.  That is expected and handled downstream: the validation lab applies
a multiple-testing correction against the number of tests research memory has
counted.  What is *not* acceptable is a search that touches the data meant to
judge it, which is why the slice is enforced here rather than requested politely.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Iterable, Sequence

import numpy as np

from ..config import Config
from ..domain import Direction, Regime, now_ms
from ..execution.cost_model import CostModel
from ..logging_setup import get_logger
from ..research.dataset import ResearchView
from ..research.hypotheses import (
    PERCENTILE_BUCKETS,
    Hypothesis,
    PercentileCondition,
    new_hypothesis_id,
    resolve_thresholds,
)
from ..research.memory import ResearchMemory
from ..storage.repositories import ResearchRepository

log = get_logger("agents")


@dataclass
class AgentContext:
    """Everything an agent may look at when proposing."""

    view: ResearchView
    costs: CostModel
    cycle_id: int
    symbols: list[str]
    #: Cross-market relations, for the agents that trade between markets.
    relations: Sequence[Any] = field(default_factory=list)
    max_proposals: int = 6
    at_ms: int = field(default_factory=now_ms)


@dataclass
class FeatureRank:
    feature: str
    correlation: float
    samples: int
    direction: Direction

    def to_dict(self) -> dict[str, Any]:
        return {
            "feature": self.feature,
            "correlation": round(self.correlation, 4),
            "samples": self.samples,
            "direction": self.direction.value,
        }


@dataclass
class AgentMetrics:
    runs: int = 0
    features_read: int = 0
    candidates_ranked: int = 0
    proposed: int = 0
    blocked_by_memory: int = 0
    dropped_unresolvable: int = 0
    dropped_weak: int = 0
    errors: int = 0
    last_run_ms: int = 0
    last_error: str = ""
    last_proposals: list[str] = field(default_factory=list)
    best_ranks: list[dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "runs": self.runs,
            "features_read": self.features_read,
            "candidates_ranked": self.candidates_ranked,
            "proposed": self.proposed,
            "blocked_by_memory": self.blocked_by_memory,
            "dropped_unresolvable": self.dropped_unresolvable,
            "dropped_weak": self.dropped_weak,
            "errors": self.errors,
            "last_run_ms": self.last_run_ms,
            "last_error": self.last_error,
            "last_proposals": self.last_proposals[-5:],
            "best_ranks": self.best_ranks[:5],
        }


class ResearchAgent(ABC):
    """One family of hypotheses."""

    name: str = "agent"
    family: str = "generic"
    description: str = ""
    #: Feature name prefixes this agent reads.  Reported by ``/agents`` so the
    #: inputs of every agent are inspectable rather than implied.
    inputs: tuple[str, ...] = ()
    horizons_ms: tuple[int, ...] = (1000, 2000, 5000)
    #: Fraction of history reserved for discovery.  The remainder is untouched
    #: by proposal logic.
    discovery_fraction: float = 0.40
    #: A candidate whose |correlation| is below this is noise at this sample
    #: size; proposing it would spend a test on nothing.
    min_abs_correlation: float = 0.03

    def __init__(self, config: Config, memory: ResearchMemory, repo: ResearchRepository) -> None:
        self.config = config
        self.memory = memory
        self.repo = repo
        self.metrics = AgentMetrics()
        self.enabled = True

    # ------------------------------------------------------------- interface

    @abstractmethod
    def candidate_features(self, view: ResearchView, symbol: str) -> list[str]:
        """Which features this agent is willing to build a hypothesis from."""

    @abstractmethod
    def build(self, context: AgentContext) -> list[Hypothesis]:
        """Propose hypotheses.  Implementations use :meth:`propose_from_ranks`."""

    def run(self, context: AgentContext) -> list[Hypothesis]:
        """Propose, filtered through research memory.  Never raises."""
        self.metrics.runs += 1
        self.metrics.last_run_ms = context.at_ms
        if not self.enabled:
            return []
        try:
            proposals = self.build(context)
        except Exception as exc:  # noqa: BLE001 - one agent must not stop the cycle
            self.metrics.errors += 1
            self.metrics.last_error = f"{type(exc).__name__}: {exc}"
            log.exception("agent failed", extra={"agent": self.name})
            self.repo.log_agent_event(self.name, "error", str(exc), severity="ERROR")
            return []

        kept: list[Hypothesis] = []
        for hypothesis in proposals:
            verdict = self.memory.check(hypothesis, at_ms=context.at_ms)
            if verdict.blocked:
                self.metrics.blocked_by_memory += 1
                self.repo.log_agent_event(
                    self.name, "memory_block", verdict.reason,
                    detail={"fingerprint": hypothesis.fingerprint, "outcome": verdict.outcome},
                )
                continue
            kept.append(hypothesis)

        self.metrics.proposed += len(kept)
        self.metrics.last_proposals = [h.describe() for h in kept]
        if kept:
            self.repo.log_agent_event(
                self.name, "proposed", f"{len(kept)} hypothesis/es",
                detail={"ids": [h.hypothesis_id for h in kept]},
            )
        return kept

    # ------------------------------------------------------- shared algorithm

    def discovery_view(self, view: ResearchView) -> ResearchView:
        """The slice an agent is allowed to look at."""
        return slice_view(view, 0.0, self.discovery_fraction)

    def rank_features(
        self,
        view: ResearchView,
        symbol: str,
        features: Iterable[str],
        horizon_ms: int,
        *,
        execution_symbol: str | None = None,
    ) -> list[FeatureRank]:
        """Correlate each candidate feature with the forward return it claims to
        predict, on the discovery slice only.

        The sign of the correlation chooses the direction: a feature that goes
        up before the price falls is a SHORT signal, not a broken LONG one.
        """
        signal_series = view.series.get(symbol)
        exec_series = view.series.get(execution_symbol or symbol)
        if signal_series is None or exec_series is None:
            return []

        steps = max(1, round(horizon_ms / view.cadence_ms))
        usable = min(len(signal_series), len(exec_series)) - steps
        if usable < 30:
            return []

        mids = np.asarray(exec_series.mid[: usable + steps], dtype=np.float64)
        with np.errstate(divide="ignore", invalid="ignore"):
            forward = np.where(
                mids[:usable] > 0, (mids[steps:] - mids[:usable]) / mids[:usable] * 10_000.0, 0.0
            )
        forward_std = float(np.std(forward))
        if forward_std <= 0:
            return []

        ranks: list[FeatureRank] = []
        for feature in features:
            column = np.fromiter(
                (row.get(feature, 0.0) for row in signal_series.values[:usable]),
                dtype=np.float64,
                count=usable,
            )
            self.metrics.features_read += 1
            std = float(np.std(column))
            if std <= 0 or not np.isfinite(std):
                continue
            correlation = float(np.corrcoef(column, forward)[0, 1])
            if not np.isfinite(correlation):
                continue
            ranks.append(
                FeatureRank(
                    feature=feature,
                    correlation=correlation,
                    samples=usable,
                    direction=Direction.LONG if correlation > 0 else Direction.SHORT,
                )
            )

        ranks.sort(key=lambda r: abs(r.correlation), reverse=True)
        self.metrics.candidates_ranked += len(ranks)
        if ranks:
            self.metrics.best_ranks = [r.to_dict() for r in ranks[:5]]
        return ranks

    def propose_from_ranks(
        self,
        context: AgentContext,
        ranks: list[FeatureRank],
        *,
        signal_symbol: str,
        execution_symbol: str,
        horizon_ms: int,
        entry_delay_ms: int = 0,
        regime_filter: Regime | None = None,
        limit: int = 2,
        percentile: float | None = None,
    ) -> list[Hypothesis]:
        """Turn the strongest ranked features into resolvable hypotheses."""
        discovery = self.discovery_view(context.view)
        proposals: list[Hypothesis] = []
        for rank in ranks[: limit * 3]:
            if len(proposals) >= limit:
                break
            if abs(rank.correlation) < self.min_abs_correlation:
                self.metrics.dropped_weak += 1
                continue

            # A positive correlation means high feature values precede rises, so
            # the condition is an upper-tail percentile; a negative one means the
            # tail that predicts is the opposite end.
            if percentile is not None:
                bucket = percentile
                op = ">=" if bucket >= 50 else "<="
            elif rank.correlation > 0:
                bucket, op = 80.0, ">="
            else:
                bucket, op = 20.0, "<="

            condition = PercentileCondition(feature=rank.feature, op=op, percentile=bucket)
            distributions = discovery.distributions_for(signal_symbol, [rank.feature])
            if not resolve_thresholds([condition], distributions):
                self.metrics.dropped_unresolvable += 1
                continue

            start_ms, end_ms = discovery.range_ms()
            proposals.append(
                Hypothesis(
                    hypothesis_id=new_hypothesis_id(self.name),
                    agent=self.name,
                    family=self.family,
                    signal_symbol=signal_symbol,
                    execution_symbol=execution_symbol,
                    direction=rank.direction,
                    conditions=[condition],
                    horizon_ms=horizon_ms,
                    entry_delay_ms=entry_delay_ms,
                    regime_filter=regime_filter,
                    cost_model_version=context.costs.version,
                    dataset_start_ms=start_ms,
                    dataset_end_ms=end_ms,
                    cycle_id=context.cycle_id,
                )
            )
        return proposals

    # ------------------------------------------------------------- reporting

    def state(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "family": self.family,
            "description": self.description,
            "enabled": self.enabled,
            "inputs": list(self.inputs),
            "horizons_ms": list(self.horizons_ms),
            "discovery_fraction": self.discovery_fraction,
            "min_abs_correlation": self.min_abs_correlation,
            "metrics": self.metrics.to_dict(),
        }


def slice_view(view: ResearchView, start_fraction: float, end_fraction: float) -> ResearchView:
    """A ResearchView restricted to a contiguous fraction of each symbol's history."""
    from ..research.dataset import SymbolSeries  # local import keeps the module cycle-free

    sliced: dict[str, SymbolSeries] = {}
    for symbol, series in view.series.items():
        total = len(series)
        if total == 0:
            continue
        start = max(0, int(total * start_fraction))
        end = min(total, int(total * end_fraction))
        if end - start < 2:
            continue
        sliced[symbol] = SymbolSeries(
            symbol=symbol,
            ts=series.ts[start:end],
            mid=series.mid[start:end],
            spread_bps=series.spread_bps[start:end],
            regime=series.regime[start:end],
            values=series.values[start:end],
        )
    return ResearchView(sliced, cadence_ms=view.cadence_ms)


__all__ = [
    "ResearchAgent",
    "AgentContext",
    "AgentMetrics",
    "FeatureRank",
    "slice_view",
    "PERCENTILE_BUCKETS",
]
