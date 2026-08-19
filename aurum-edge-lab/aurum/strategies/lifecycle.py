"""Strategy lifecycle.

    RESEARCH → CANDIDATE → CHALLENGER → SHADOW → CHAMPION
                                           ↓        ↓
                                        REJECTED  DEGRADED → RETIRED

Every transition is checked against the table below, persisted with its reason
and its evidence, and given a new version number.  Nothing is edited in place:
a strategy's history is append-only, so "why is this the champion?" is answered
by reading its versions rather than by trusting the current row.

The SHADOW stage is the one that catches what backtests cannot.  A hypothesis
that passed holdout is still only a claim about recorded history; SHADOW runs it
against the live feed, emitting real signals with real timestamps and real
spreads, and measures what they would have made — with no wallet impact.  Most
of what dies here dies because the spread at the moment the signal fired was not
the spread the backtest averaged over.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from typing import Any, Iterable

from ..domain import Direction, MetricSet, StrategyState, now_ms
from ..logging_setup import get_logger
from ..research.hypotheses import Hypothesis
from ..storage.repositories import StrategyRepository

log = get_logger("strategies.lifecycle")

#: Legal transitions.  Anything absent here is a bug, not a policy decision.
TRANSITIONS: dict[StrategyState, frozenset[StrategyState]] = {
    StrategyState.RESEARCH: frozenset({StrategyState.CANDIDATE, StrategyState.REJECTED}),
    StrategyState.CANDIDATE: frozenset({StrategyState.CHALLENGER, StrategyState.REJECTED}),
    StrategyState.CHALLENGER: frozenset({StrategyState.SHADOW, StrategyState.REJECTED}),
    StrategyState.SHADOW: frozenset(
        {StrategyState.CHAMPION, StrategyState.REJECTED, StrategyState.CHALLENGER}
    ),
    StrategyState.CHAMPION: frozenset({StrategyState.DEGRADED, StrategyState.RETIRED}),
    StrategyState.DEGRADED: frozenset(
        {StrategyState.CHAMPION, StrategyState.RETIRED, StrategyState.SHADOW}
    ),
    StrategyState.RETIRED: frozenset({StrategyState.SHADOW}),
    StrategyState.REJECTED: frozenset(),
}


class IllegalTransition(ValueError):
    pass


@dataclass
class ShadowRecord:
    """One live signal from a shadow strategy, awaiting its outcome."""

    strategy_id: str
    symbol: str
    direction: Direction
    ts_ms: int
    entry_price: float
    horizon_ms: int
    cost_bps: float
    resolved: bool = False
    net_bps: float = 0.0

    def resolve(self, exit_price: float) -> float:
        if self.entry_price <= 0:
            self.resolved = True
            return 0.0
        gross = (exit_price - self.entry_price) / self.entry_price * 10_000.0 * self.direction.sign
        self.net_bps = gross - self.cost_bps
        self.resolved = True
        return self.net_bps


@dataclass
class Strategy:
    strategy_id: str
    hypothesis: Hypothesis
    name: str
    state: StrategyState = StrategyState.RESEARCH
    version: int = 1
    score: float = 0.0
    validated_metrics: MetricSet = field(default_factory=MetricSet)
    shadow_metrics: MetricSet = field(default_factory=MetricSet)
    live_metrics: MetricSet = field(default_factory=MetricSet)
    created_ms: int = field(default_factory=now_ms)
    updated_ms: int = field(default_factory=now_ms)
    promoted_ms: int = 0
    retired_ms: int = 0
    cycle_id: int = 0
    shadow_started_ms: int = 0
    shadow_signals: int = 0
    last_reason: str = ""
    live_trades: int = 0
    live_net_pnl_eur: float = 0.0

    @property
    def can_trade(self) -> bool:
        return self.state.can_trade

    @property
    def symbol(self) -> str:
        return self.hypothesis.execution_symbol

    def to_dict(self) -> dict[str, Any]:
        return {
            "strategy_id": self.strategy_id,
            "name": self.name,
            "state": self.state.value,
            "version": self.version,
            "score": round(self.score, 4),
            "can_trade": self.can_trade,
            "symbol": self.symbol,
            "signal_symbol": self.hypothesis.signal_symbol,
            "direction": self.hypothesis.direction.value,
            "horizon_ms": self.hypothesis.horizon_ms,
            "entry_delay_ms": self.hypothesis.entry_delay_ms,
            "description": self.hypothesis.describe(),
            "hypothesis_id": self.hypothesis.hypothesis_id,
            "agent": self.hypothesis.agent,
            "conditions": [c.to_dict() for c in self.hypothesis.conditions],
            "validated_metrics": self.validated_metrics.to_dict(),
            "shadow_metrics": self.shadow_metrics.to_dict(),
            "live_metrics": self.live_metrics.to_dict(),
            "shadow_signals": self.shadow_signals,
            "shadow_started_ms": self.shadow_started_ms,
            "created_ms": self.created_ms,
            "updated_ms": self.updated_ms,
            "promoted_ms": self.promoted_ms,
            "retired_ms": self.retired_ms,
            "cycle_id": self.cycle_id,
            "last_reason": self.last_reason,
            "live_trades": self.live_trades,
            "live_net_pnl_eur": round(self.live_net_pnl_eur, 6),
        }


class StrategyLifecycle:
    def __init__(self, repo: StrategyRepository) -> None:
        self.repo = repo
        self.strategies: dict[str, Strategy] = {}
        self.transitions = 0

    # ------------------------------------------------------------- creation

    def create(
        self, hypothesis: Hypothesis, metrics: MetricSet, score: float, *, cycle_id: int
    ) -> Strategy:
        strategy = Strategy(
            strategy_id=f"str-{uuid.uuid4().hex[:12]}",
            hypothesis=hypothesis,
            name=f"{hypothesis.agent}:{hypothesis.execution_symbol}:{hypothesis.horizon_ms}ms",
            state=StrategyState.RESEARCH,
            validated_metrics=metrics,
            score=score,
            cycle_id=cycle_id,
        )
        self.strategies[strategy.strategy_id] = strategy
        self._persist(strategy)
        self._version(strategy, previous=None, reason="created from validated hypothesis",
                      evidence={"metrics": metrics.to_dict(), "score": score})
        return strategy

    # ----------------------------------------------------------- transitions

    def transition(
        self,
        strategy: Strategy,
        target: StrategyState,
        reason: str,
        *,
        evidence: dict[str, Any] | None = None,
        at_ms: int | None = None,
    ) -> Strategy:
        allowed = TRANSITIONS.get(strategy.state, frozenset())
        if target not in allowed:
            raise IllegalTransition(
                f"{strategy.strategy_id}: {strategy.state.value} -> {target.value} is not a legal "
                f"transition (allowed: {sorted(s.value for s in allowed) or 'none'})"
            )
        stamp = at_ms if at_ms is not None else now_ms()
        previous = strategy.state
        strategy.state = target
        strategy.version += 1
        strategy.updated_ms = stamp
        strategy.last_reason = reason
        if target is StrategyState.CHAMPION:
            strategy.promoted_ms = stamp
        if target is StrategyState.SHADOW:
            strategy.shadow_started_ms = stamp
        if target in (StrategyState.RETIRED, StrategyState.REJECTED):
            strategy.retired_ms = stamp

        self.transitions += 1
        self._persist(strategy)
        self._version(strategy, previous=previous, reason=reason, evidence=evidence or {}, at_ms=stamp)
        log.info(
            "strategy transition",
            extra={
                "strategy": strategy.strategy_id,
                "from": previous.value,
                "to": target.value,
                "reason": reason,
            },
        )
        return strategy

    def can_transition(self, strategy: Strategy, target: StrategyState) -> bool:
        return target in TRANSITIONS.get(strategy.state, frozenset())

    # -------------------------------------------------------------- queries

    def champion(self) -> Strategy | None:
        for strategy in self.strategies.values():
            if strategy.state is StrategyState.CHAMPION:
                return strategy
        return None

    def in_state(self, state: StrategyState) -> list[Strategy]:
        return [s for s in self.strategies.values() if s.state is state]

    def tradable(self) -> list[Strategy]:
        return [s for s in self.strategies.values() if s.can_trade]

    def shadows(self) -> list[Strategy]:
        return self.in_state(StrategyState.SHADOW)

    def ranked(self, states: Iterable[StrategyState] | None = None) -> list[Strategy]:
        pool = (
            [s for s in self.strategies.values() if s.state in set(states)]
            if states
            else list(self.strategies.values())
        )
        return sorted(pool, key=lambda s: s.score, reverse=True)

    def counts(self) -> dict[str, int]:
        counts = {state.value: 0 for state in StrategyState}
        for strategy in self.strategies.values():
            counts[strategy.state.value] += 1
        return counts

    # ---------------------------------------------------------- persistence

    def _persist(self, strategy: Strategy) -> None:
        self.repo.save_strategy(
            {
                "strategy_id": strategy.strategy_id,
                "hypothesis_id": strategy.hypothesis.hypothesis_id,
                "name": strategy.name,
                "state": strategy.state.value,
                "version": strategy.version,
                "score": strategy.score,
                "created_ms": strategy.created_ms,
                "updated_ms": strategy.updated_ms,
                "promoted_ms": strategy.promoted_ms,
                "retired_ms": strategy.retired_ms,
                "cycle_id": strategy.cycle_id,
                "metrics": {
                    "validated": strategy.validated_metrics.to_dict(),
                    "shadow": strategy.shadow_metrics.to_dict(),
                    "live": strategy.live_metrics.to_dict(),
                },
            }
        )

    def _version(
        self,
        strategy: Strategy,
        *,
        previous: StrategyState | None,
        reason: str,
        evidence: dict[str, Any],
        at_ms: int | None = None,
    ) -> None:
        self.repo.add_version(
            {
                "strategy_id": strategy.strategy_id,
                "version": strategy.version,
                "state": strategy.state.value,
                "previous_state": previous.value if previous else None,
                "reason": reason,
                "evidence": evidence,
                "definition": strategy.hypothesis.to_row(),
                "created_ms": at_ms if at_ms is not None else now_ms(),
            }
        )

    def restore(self, rows: Iterable[dict[str, Any]], hypotheses: dict[str, Hypothesis]) -> int:
        """Rebuild strategies from the database so a restart keeps its champion."""
        restored = 0
        for row in rows:
            hypothesis = hypotheses.get(row["hypothesis_id"])
            if hypothesis is None:
                continue
            metrics = row.get("metrics") or {}
            strategy = Strategy(
                strategy_id=row["strategy_id"],
                hypothesis=hypothesis,
                name=row.get("name") or "",
                state=StrategyState(row["state"]),
                version=int(row.get("version") or 1),
                score=float(row.get("score") or 0.0),
                validated_metrics=MetricSet.from_dict(metrics.get("validated", {})),
                shadow_metrics=MetricSet.from_dict(metrics.get("shadow", {})),
                live_metrics=MetricSet.from_dict(metrics.get("live", {})),
                created_ms=int(row.get("created_ms") or now_ms()),
                updated_ms=int(row.get("updated_ms") or now_ms()),
                promoted_ms=int(row.get("promoted_ms") or 0),
                retired_ms=int(row.get("retired_ms") or 0),
                cycle_id=int(row.get("cycle_id") or 0),
            )
            self.strategies[strategy.strategy_id] = strategy
            restored += 1
        return restored
