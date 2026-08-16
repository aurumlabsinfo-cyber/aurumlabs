"""Agent contract.

Every agent is a pure function of the current `AgentContext`: it never reads the
future, never mutates shared state, and always reports which features it used so
a decision can be audited after the fact.
"""

from __future__ import annotations

import abc
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from app.core.clock import now_ms


class Direction(str, Enum):
    UP = "UP"
    DOWN = "DOWN"
    NO_TRADE = "NO_TRADE"


class Regime(str, Enum):
    TREND_UP = "TREND_UP"
    TREND_DOWN = "TREND_DOWN"
    RANGE = "RANGE"
    BREAKOUT = "BREAKOUT"
    HIGH_VOLATILITY = "HIGH_VOLATILITY"
    LOW_VOLATILITY = "LOW_VOLATILITY"
    EXHAUSTION = "EXHAUSTION"
    UNKNOWN = "UNKNOWN"


@dataclass
class AgentContext:
    ts: int
    symbol: str
    features: dict[str, Any]
    data_quality: float
    book_synced: bool
    regime: Regime = Regime.UNKNOWN
    extra: dict[str, Any] = field(default_factory=dict)

    def f(self, name: str, default: Any = None) -> Any:
        v = self.features.get(name, default)
        return default if v is None else v

    def has(self, *names: str) -> bool:
        return all(self.features.get(n) is not None for n in names)


@dataclass
class AgentOutput:
    agent: str
    direction: Direction
    confidence: float  # 0..1 - how sure this agent is
    score: float  # -1..+1 - signed strength (negative = down)
    reason: str
    features_used: list[str]
    timestamp: int
    data_quality: float
    extra: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "agent": self.agent,
            "direction": self.direction.value,
            "confidence": round(self.confidence, 4),
            "score": round(self.score, 4),
            "reason": self.reason,
            "features_used": self.features_used,
            "timestamp": self.timestamp,
            "data_quality": round(self.data_quality, 3),
            "extra": self.extra,
        }


class Agent(abc.ABC):
    name: str = "abstract"
    #: Prior weight in the ensemble. Microstructure agents outrank indicators.
    weight: float = 1.0

    @abc.abstractmethod
    def _evaluate(self, ctx: AgentContext) -> AgentOutput:
        ...

    def evaluate(self, ctx: AgentContext) -> AgentOutput:
        try:
            return self._evaluate(ctx)
        except Exception as exc:  # noqa: BLE001 - one bad agent must not stop the rest
            return self.abstain(ctx, f"agent error: {type(exc).__name__}: {exc}")

    def abstain(self, ctx: AgentContext, reason: str) -> AgentOutput:
        return AgentOutput(
            agent=self.name,
            direction=Direction.NO_TRADE,
            confidence=0.0,
            score=0.0,
            reason=reason,
            features_used=[],
            timestamp=ctx.ts or now_ms(),
            data_quality=ctx.data_quality,
        )

    def emit(
        self,
        ctx: AgentContext,
        score: float,
        confidence: float,
        reason: str,
        used: list[str],
        min_confidence: float = 0.5,
        extra: dict[str, Any] | None = None,
    ) -> AgentOutput:
        score = max(-1.0, min(1.0, score))
        confidence = max(0.0, min(1.0, confidence))
        if confidence < min_confidence or score == 0:
            direction = Direction.NO_TRADE
        else:
            direction = Direction.UP if score > 0 else Direction.DOWN
        return AgentOutput(
            agent=self.name,
            direction=direction,
            confidence=confidence,
            score=score,
            reason=reason,
            features_used=used,
            timestamp=ctx.ts,
            data_quality=ctx.data_quality,
            extra=extra or {},
        )


def squash(value: float, scale: float) -> float:
    """Map an unbounded quantity to (-1, 1) with a soft knee at `scale`."""
    import math

    if scale <= 0:
        return 0.0
    return math.tanh(value / scale)
