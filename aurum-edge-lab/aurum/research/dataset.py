"""Turning a hypothesis into observations.

A hypothesis says "when *these* conditions hold on market A, market B moves
*this* way over *this* horizon".  Testing it means finding every instant the
conditions held and measuring what actually happened next — net of what
capturing it would have cost.

Two rules are enforced here rather than trusted to callers:

**No lookahead.**  A trigger at index *i* enters at ``i + delay`` and exits at
``i + delay + horizon``.  Triggers too close to the end of the recorded history
have no measurable outcome and are dropped, not truncated to whatever data
happens to exist.  A dataset that quietly shortens the horizon of its last
observations reports an edge the strategy could never have taken.

**Costs at the observation, not at the summary.**  The round trip is priced
from the spread that was actually quoted at that entry, so a signal that only
fires when the book is wide is charged for it.
"""

from __future__ import annotations

import bisect
from dataclasses import dataclass, field
from typing import Any, Iterable

from ..domain import Direction, FeatureSnapshot, Regime
from ..execution.cost_model import CostModel
from .hypotheses import Hypothesis


@dataclass(slots=True)
class Observation:
    """One triggered instance of a hypothesis, with its realised outcome."""

    ts_ms: int
    index: int
    gross_bps: float
    cost_bps: float
    net_bps: float
    regime: Regime
    spread_bps: float


@dataclass
class SymbolSeries:
    """Cadence-gridded history for one symbol, frozen for a research pass."""

    symbol: str
    ts: list[int] = field(default_factory=list)
    mid: list[float] = field(default_factory=list)
    spread_bps: list[float] = field(default_factory=list)
    regime: list[Regime] = field(default_factory=list)
    values: list[dict[str, float]] = field(default_factory=list)

    def index_at(self, ts_ms: int) -> int:
        """First index at or after ``ts_ms``; ``len(self)`` when past the end."""
        return bisect.bisect_left(self.ts, ts_ms)

    def __len__(self) -> int:
        return len(self.ts)


class ResearchView:
    """An immutable snapshot of every symbol's feature history.

    Research runs against a frozen copy rather than the live deques.  Otherwise
    a hypothesis evaluated at the start of a cycle and one evaluated at the end
    would be measured over different data, and their scores would not be
    comparable — which is exactly the comparison the Research Director makes.
    """

    def __init__(self, series: dict[str, SymbolSeries], *, cadence_ms: int) -> None:
        self.series = series
        self.cadence_ms = cadence_ms
        self._distributions: dict[tuple[str, str], list[float]] = {}

    @classmethod
    def capture(
        cls, snapshots: dict[str, Iterable[FeatureSnapshot]], *, cadence_ms: int
    ) -> "ResearchView":
        series: dict[str, SymbolSeries] = {}
        for symbol, history in snapshots.items():
            entry = SymbolSeries(symbol=symbol)
            for snapshot in history:
                if snapshot.mid is None:
                    continue
                entry.ts.append(snapshot.ts_ms)
                entry.mid.append(snapshot.mid)
                entry.spread_bps.append(snapshot.spread_bps or 0.0)
                entry.regime.append(snapshot.regime)
                entry.values.append(snapshot.values)
            if entry.ts:
                series[symbol] = entry
        return cls(series, cadence_ms=cadence_ms)

    # ------------------------------------------------------------ accessors

    @property
    def symbols(self) -> list[str]:
        return list(self.series)

    def range_ms(self) -> tuple[int, int]:
        starts = [s.ts[0] for s in self.series.values() if s.ts]
        ends = [s.ts[-1] for s in self.series.values() if s.ts]
        return (min(starts) if starts else 0, max(ends) if ends else 0)

    def length(self, symbol: str) -> int:
        entry = self.series.get(symbol)
        return len(entry) if entry else 0

    def distribution(self, symbol: str, feature: str) -> list[float]:
        """Observed values of one feature, cached for the pass."""
        key = (symbol, feature)
        cached = self._distributions.get(key)
        if cached is not None:
            return cached
        entry = self.series.get(symbol)
        values: list[float] = []
        if entry is not None:
            for row in entry.values:
                value = row.get(feature)
                if value is not None:
                    values.append(value)
        self._distributions[key] = values
        return values

    def distributions_for(self, symbol: str, features: Iterable[str]) -> dict[str, list[float]]:
        return {feature: self.distribution(symbol, feature) for feature in features}

    def feature_names(self, symbol: str) -> list[str]:
        entry = self.series.get(symbol)
        if not entry or not entry.values:
            return []
        return sorted(entry.values[-1])


def build_observations(
    hypothesis: Hypothesis, view: ResearchView, costs: CostModel, *, max_observations: int = 20_000
) -> list[Observation]:
    """Every instant the hypothesis fired, with what happened next."""
    signal_series = view.series.get(hypothesis.signal_symbol)
    exec_series = view.series.get(hypothesis.execution_symbol)
    if signal_series is None or exec_series is None:
        return []

    horizon_steps = max(1, round(hypothesis.horizon_ms / view.cadence_ms))
    delay_steps = max(0, round(hypothesis.entry_delay_ms / view.cadence_ms))
    observations: list[Observation] = []
    direction_sign = hypothesis.direction.sign

    for index in range(len(signal_series)):
        row = signal_series.values[index]
        if not hypothesis.matches(row):
            continue
        if hypothesis.regime_filter is not None and signal_series.regime[index] is not hypothesis.regime_filter:
            continue

        # Map the trigger's timestamp onto the execution symbol's own grid: the
        # two symbols share a cadence but not necessarily a start.
        entry_ts = signal_series.ts[index] + hypothesis.entry_delay_ms
        entry_index = exec_series.index_at(entry_ts)
        exit_index = entry_index + horizon_steps
        if exit_index >= len(exec_series):
            # No measurable outcome yet. Dropping it is the point: shortening
            # the horizon here would report an edge nothing could have taken.
            continue

        entry_price = exec_series.mid[entry_index]
        exit_price = exec_series.mid[exit_index]
        if entry_price <= 0:
            continue

        gross = (exit_price - entry_price) / entry_price * 10_000.0 * direction_sign
        spread = exec_series.spread_bps[entry_index]
        cost = costs.round_trip_bps(spread)
        observations.append(
            Observation(
                ts_ms=exec_series.ts[entry_index],
                index=entry_index,
                gross_bps=gross,
                cost_bps=cost,
                net_bps=gross - cost,
                regime=exec_series.regime[entry_index],
                spread_bps=spread,
            )
        )
        if len(observations) >= max_observations:
            break

    _ = delay_steps  # the delay is applied in milliseconds above, not in steps
    return observations


def summarise(observations: list[Observation]) -> dict[str, Any]:
    if not observations:
        return {"count": 0}
    nets = [o.net_bps for o in observations]
    return {
        "count": len(observations),
        "first_ms": observations[0].ts_ms,
        "last_ms": observations[-1].ts_ms,
        "mean_net_bps": round(sum(nets) / len(nets), 4),
        "mean_cost_bps": round(sum(o.cost_bps for o in observations) / len(observations), 4),
        "wins": sum(1 for n in nets if n > 0),
    }
