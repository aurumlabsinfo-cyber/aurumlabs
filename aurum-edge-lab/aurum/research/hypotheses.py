"""Hypotheses: the unit of research.

Every hypothesis is explicit and reproducible.  It names the market whose
features trigger it, the market it would trade, the exact feature conditions,
the direction, the delay before entry, the horizon it is measured over, the cost
model it was priced under, the data range it was fitted on and the regime it is
restricted to.  Nothing about it is implied by code — replaying a hypothesis
means reading these fields, not re-running the agent that produced it.

**Thresholds are percentiles, not raw numbers.**  ``ofi_norm_1s >= 0.42`` means
nothing a week later when the book is twice as deep; ``ofi_norm_1s >= p80``
means the same thing in both worlds.  Each condition therefore carries the
percentile it was drawn from *and* the concrete value that percentile resolved
to at fitting time.  The fingerprint uses the percentile.

That is what makes research memory work.  Two agents proposing "strong buy flow
predicts a rise in one second" with thresholds of 0.42 and 0.44 are proposing
the *same* hypothesis, and a memory keyed on raw floats would let them rediscover
the same failure forever.
"""

from __future__ import annotations

import hashlib
import uuid
from dataclasses import dataclass, field
from typing import Any

from ..domain import Condition, Direction, MetricSet, Regime, ValidationStatus, now_ms

#: Percentile buckets agents may draw thresholds from.  Coarse on purpose: the
#: difference between p78 and p80 is not a different idea, and treating it as
#: one is how a search re-tests the same failure a thousand times.
PERCENTILE_BUCKETS: tuple[float, ...] = (5.0, 10.0, 20.0, 30.0, 70.0, 80.0, 90.0, 95.0)


@dataclass(slots=True)
class PercentileCondition:
    """A condition expressed as a percentile of the feature's own distribution."""

    feature: str
    op: str
    percentile: float
    threshold: float = 0.0  # resolved at fitting time from observed data
    samples: int = 0

    def to_condition(self) -> Condition:
        return Condition(feature=self.feature, op=self.op, threshold=self.threshold)

    def to_dict(self) -> dict[str, Any]:
        return {
            "feature": self.feature,
            "op": self.op,
            "percentile": self.percentile,
            "threshold": self.threshold,
            "samples": self.samples,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> PercentileCondition:
        return cls(
            feature=data["feature"],
            op=data["op"],
            percentile=float(data.get("percentile", 50.0)),
            threshold=float(data.get("threshold", 0.0)),
            samples=int(data.get("samples", 0)),
        )

    def describe(self) -> str:
        return f"{self.feature} {self.op} p{self.percentile:g} ({self.threshold:.6g})"


@dataclass
class Hypothesis:
    """One testable claim, with everything needed to test it again."""

    hypothesis_id: str
    agent: str
    family: str
    signal_symbol: str
    execution_symbol: str
    direction: Direction
    conditions: list[PercentileCondition]
    horizon_ms: int
    entry_delay_ms: int = 0
    regime_filter: Regime | None = None
    parent_id: str | None = None
    cost_model_version: str = ""
    dataset_start_ms: int = 0
    dataset_end_ms: int = 0
    sample_count: int = 0
    validation_status: ValidationStatus = ValidationStatus.UNTESTED
    created_ms: int = field(default_factory=now_ms)
    updated_ms: int = field(default_factory=now_ms)
    cycle_id: int = 0
    metrics: MetricSet = field(default_factory=MetricSet)
    rejection_reason: str = ""

    # ------------------------------------------------------------ identity

    @property
    def fingerprint(self) -> str:
        """Stable identity of the *idea*, independent of fitted numbers.

        Deliberately excludes the resolved thresholds, the dataset range and
        every metric: those are what testing produces, not what is being tested.
        """
        parts = [
            self.agent,
            self.family,
            self.signal_symbol,
            self.execution_symbol,
            self.direction.value,
            str(self.horizon_ms),
            str(self.entry_delay_ms),
            self.regime_filter.value if self.regime_filter else "ANY",
        ]
        for condition in sorted(self.conditions, key=lambda c: (c.feature, c.op)):
            parts.append(f"{condition.feature}{condition.op}p{condition.percentile:g}")
        digest = hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()
        return digest[:32]

    def describe(self) -> str:
        where = " AND ".join(c.describe() for c in self.conditions)
        delay = f" after {self.entry_delay_ms} ms" if self.entry_delay_ms else ""
        regime = f" [{self.regime_filter.value}]" if self.regime_filter else ""
        return (
            f"When {self.signal_symbol} has {where}, go {self.direction.value} "
            f"{self.execution_symbol}{delay} for {self.horizon_ms} ms{regime}"
        )

    def matches(self, values: dict[str, float]) -> bool:
        """Do the current features satisfy every condition?"""
        for condition in self.conditions:
            value = values.get(condition.feature)
            if value is None:
                return False
            threshold = condition.threshold
            if condition.op == ">=" and not value >= threshold:
                return False
            if condition.op == "<=" and not value <= threshold:
                return False
            if condition.op == ">" and not value > threshold:
                return False
            if condition.op == "<" and not value < threshold:
                return False
        return True

    # --------------------------------------------------------- persistence

    def to_row(self) -> dict[str, Any]:
        return {
            "hypothesis_id": self.hypothesis_id,
            "parent_id": self.parent_id,
            "agent": self.agent,
            "family": self.family,
            "signal_symbol": self.signal_symbol,
            "execution_symbol": self.execution_symbol,
            "direction": self.direction.value,
            "conditions": [c.to_dict() for c in self.conditions],
            "entry_delay_ms": self.entry_delay_ms,
            "horizon_ms": self.horizon_ms,
            "regime_filter": self.regime_filter.value if self.regime_filter else None,
            "cost_model_version": self.cost_model_version,
            "dataset_start_ms": self.dataset_start_ms,
            "dataset_end_ms": self.dataset_end_ms,
            "sample_count": self.sample_count,
            "validation_status": self.validation_status.value,
            "fingerprint": self.fingerprint,
            "description": self.describe(),
            "created_ms": self.created_ms,
            "updated_ms": self.updated_ms,
            "cycle_id": self.cycle_id,
        }

    @classmethod
    def from_row(cls, row: dict[str, Any]) -> Hypothesis:
        regime = row.get("regime_filter")
        return cls(
            hypothesis_id=row["hypothesis_id"],
            agent=row["agent"],
            family=row.get("family") or "",
            signal_symbol=row["signal_symbol"],
            execution_symbol=row["execution_symbol"],
            direction=Direction(row["direction"]),
            conditions=[PercentileCondition.from_dict(c) for c in (row.get("conditions") or [])],
            horizon_ms=int(row["horizon_ms"]),
            entry_delay_ms=int(row.get("entry_delay_ms") or 0),
            regime_filter=Regime(regime) if regime else None,
            parent_id=row.get("parent_id"),
            cost_model_version=row.get("cost_model_version") or "",
            dataset_start_ms=int(row.get("dataset_start_ms") or 0),
            dataset_end_ms=int(row.get("dataset_end_ms") or 0),
            sample_count=int(row.get("sample_count") or 0),
            validation_status=ValidationStatus(row.get("validation_status") or "UNTESTED"),
            created_ms=int(row.get("created_ms") or now_ms()),
            updated_ms=int(row.get("updated_ms") or now_ms()),
            cycle_id=int(row.get("cycle_id") or 0),
        )

    def to_dict(self) -> dict[str, Any]:
        data = self.to_row()
        data["metrics"] = self.metrics.to_dict()
        data["rejection_reason"] = self.rejection_reason
        return data


def new_hypothesis_id(agent: str) -> str:
    return f"hyp-{agent[:4].lower()}-{uuid.uuid4().hex[:10]}"


def resolve_thresholds(
    conditions: list[PercentileCondition], distributions: dict[str, list[float]]
) -> bool:
    """Turn percentile conditions into concrete thresholds.

    Returns False when any feature lacks a usable distribution — a hypothesis
    with an unresolvable threshold is not a weak hypothesis, it is not a
    hypothesis, and is dropped rather than fitted to a default.
    """
    for condition in conditions:
        values = distributions.get(condition.feature)
        if not values or len(values) < 20:
            return False
        ordered = sorted(values)
        index = min(len(ordered) - 1, max(0, round(condition.percentile / 100.0 * (len(ordered) - 1))))
        condition.threshold = ordered[index]
        condition.samples = len(ordered)
    return True
