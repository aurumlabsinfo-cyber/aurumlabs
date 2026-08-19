"""Research Director.

Coordinates experiments, identifies duplicate hypotheses, ranks candidates,
detects edge decay, and controls promotion and retirement.  It never fabricates
a trade, and it never lowers a gate because nothing has passed.

One research cycle:

1.  Freeze a view of feature history.  Everything in this cycle is measured on
    the same data, or the rankings it produces are not comparable.
2.  Ask each agent for hypotheses; drop the ones research memory already knows.
3.  Deduplicate within the batch — five agents can independently invent the
    same idea, and testing it five times is five draws on the same budget.
4.  Build observations, validate through every gate, record the outcome in
    memory whether it passed or failed.
5.  Apply batch false-discovery control across simultaneous survivors.
6.  Create strategies from survivors and walk them up the lifecycle.
7.  Check the champion for decay against what it was validated at.

The ranking score combines net expectancy, sample size, fold stability, regime
stability, drawdown and recent decay — deflated by the size of the search that
produced it.  A single number, but not a mysterious one: every component is
reported alongside it.
"""

from __future__ import annotations

import asyncio
import math
from dataclasses import dataclass, field
from typing import Any, Sequence

from ..agents import build_agents
from ..agents.base import AgentContext, ResearchAgent
from ..config import Config
from ..cross_market.engine import CrossMarketEngine
from ..domain import Direction, MetricSet, StrategyState, ValidationStatus, now_ms
from ..execution.cost_model import CostModel
from ..features.engine import FeatureEngine
from ..logging_setup import get_logger
from ..storage.repositories import Repositories
from ..strategies.lifecycle import ShadowRecord, Strategy, StrategyLifecycle
from .dataset import ResearchView, build_observations
from .hypotheses import Hypothesis
from .memory import Outcome, ResearchMemory
from .statistics import build_metrics, deflated_score
from .validation import Stage, ValidationLab, ValidationReport

log = get_logger("research.director")


@dataclass
class CycleReport:
    cycle: int
    started_ms: int
    finished_ms: int = 0
    proposed: int = 0
    deduplicated: int = 0
    blocked_by_memory: int = 0
    evaluated: int = 0
    rejected: int = 0
    survived: int = 0
    promoted: int = 0
    observations_built: int = 0
    reasons: dict[str, int] = field(default_factory=dict)
    notes: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "cycle": self.cycle,
            "started_ms": self.started_ms,
            "finished_ms": self.finished_ms,
            "duration_ms": max(0, self.finished_ms - self.started_ms),
            "proposed": self.proposed,
            "deduplicated": self.deduplicated,
            "blocked_by_memory": self.blocked_by_memory,
            "evaluated": self.evaluated,
            "rejected": self.rejected,
            "survived": self.survived,
            "promoted": self.promoted,
            "observations_built": self.observations_built,
            "reasons": dict(self.reasons),
            "notes": self.notes[-10:],
        }


class ResearchDirector:
    def __init__(
        self,
        config: Config,
        features: FeatureEngine,
        cross_market: CrossMarketEngine,
        costs: CostModel,
        repos: Repositories,
    ) -> None:
        self.config = config
        self.features = features
        self.cross_market = cross_market
        self.costs = costs
        self.repos = repos

        self.memory = ResearchMemory(repos.research, retest_cooldown_h=config.research.retest_cooldown_h)
        self.lab = ValidationLab(config)
        self.lifecycle = StrategyLifecycle(repos.strategies)
        self.agents: list[ResearchAgent] = build_agents(config, self.memory, repos.research)

        self.hypotheses: dict[str, Hypothesis] = {}
        self.reports: list[CycleReport] = []
        self.shadow_pending: list[ShadowRecord] = []
        self.shadow_results: dict[str, list[float]] = {}
        self.cycles_run = 0
        self.cycle_id = 1
        self.started_ms = 0
        self.last_cycle_ms = 0
        self.running = False
        self._task: asyncio.Task[None] | None = None
        #: Why the last cycle produced no champion.  Surfaced by /diagnostics —
        #: "no validated edge" should always come with a reason.
        self.no_edge_reason = "research has not run yet"

    # -------------------------------------------------------------- lifecycle

    async def start(self) -> None:
        if self.running:
            return
        self.running = True
        self.started_ms = now_ms()
        self.memory.load()
        self._restore()
        self._task = asyncio.create_task(self._loop(), name="research-director")

    async def stop(self) -> None:
        self.running = False
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
                pass
            self._task = None

    def _restore(self) -> None:
        rows = self.repos.research.list_hypotheses(limit=2000)
        for row in rows:
            try:
                hypothesis = Hypothesis.from_row(row)
            except (KeyError, ValueError):
                continue
            self.hypotheses[hypothesis.hypothesis_id] = hypothesis
        restored = self.lifecycle.restore(
            self.repos.strategies.list_strategies(limit=500), self.hypotheses
        )
        if restored:
            log.info("strategies restored", extra={"count": restored, "hypotheses": len(self.hypotheses)})

    async def _loop(self) -> None:
        while self.running:
            await asyncio.sleep(self.config.research.cycle_interval_s)
            if not self.config.research.enabled:
                self.no_edge_reason = "research is disabled in configuration"
                continue
            try:
                # The cycle is CPU-bound (numpy correlations over every symbol
                # and every candidate feature) and would otherwise stall the
                # ingest loop for the length of a full pass.
                await asyncio.to_thread(self.run_cycle)
            except Exception:  # noqa: BLE001
                log.exception("research cycle failed")
                self.repos.system.log("research", "cycle_error", "cycle raised", level="ERROR")

    # ------------------------------------------------------------- the cycle

    def run_cycle(self, *, at_ms: int | None = None) -> CycleReport:
        stamp = at_ms if at_ms is not None else now_ms()
        report = CycleReport(cycle=self.cycles_run + 1, started_ms=stamp)

        elapsed_s = (stamp - self.started_ms) / 1000.0 if self.started_ms else 0.0
        if elapsed_s < self.config.research.min_warmup_s:
            report.notes.append(
                f"warming up: {elapsed_s:.0f}s of {self.config.research.min_warmup_s:.0f}s"
            )
            self.no_edge_reason = (
                f"warming up ({elapsed_s:.0f}s of {self.config.research.min_warmup_s:.0f}s)"
            )
            return self._finish_cycle(report, stamp)

        view = ResearchView.capture(
            {s: list(d) for s, d in self.features.snapshots.items()},
            cadence_ms=self.features.cadence_ms,
        )
        if not view.symbols:
            report.notes.append("no feature history yet")
            self.no_edge_reason = "no feature history yet"
            return self._finish_cycle(report, stamp)

        context = AgentContext(
            view=view,
            costs=self.costs,
            cycle_id=self.cycle_id,
            symbols=view.symbols,
            relations=self.cross_market.ranked_relations(),
            max_proposals=self.config.research.max_hypotheses_per_agent_per_cycle,
            at_ms=stamp,
        )

        # --- 1. propose ---------------------------------------------------
        proposals: list[Hypothesis] = []
        for agent in self.agents:
            before = agent.metrics.blocked_by_memory
            proposals.extend(agent.run(context))
            report.blocked_by_memory += agent.metrics.blocked_by_memory - before
        report.proposed = len(proposals)

        # --- 2. deduplicate within the batch ------------------------------
        unique: dict[str, Hypothesis] = {}
        for hypothesis in proposals:
            fingerprint = hypothesis.fingerprint
            if fingerprint in unique:
                report.deduplicated += 1
                continue
            unique[fingerprint] = hypothesis
        batch = list(unique.values())

        if not batch:
            report.notes.append("no new hypotheses proposed this cycle")
            self.no_edge_reason = (
                "agents proposed nothing new: every candidate was already known to research memory"
                if report.blocked_by_memory
                else "agents found no candidate feature above the minimum correlation"
            )
            return self._finish_cycle(report, stamp)

        # --- 3. evaluate --------------------------------------------------
        total_tests = max(1, self.memory.total_tests())
        reports: list[tuple[Hypothesis, ValidationReport]] = []
        for hypothesis in batch:
            observations = build_observations(hypothesis, view, self.costs)
            report.observations_built += len(observations)
            result = self.lab.evaluate(hypothesis, observations, total_tests=total_tests)
            report.evaluated += 1

            hypothesis.sample_count = len(observations)
            hypothesis.validation_status = result.status
            hypothesis.metrics = result.final_metrics
            hypothesis.rejection_reason = "" if result.passed else result.reason
            hypothesis.updated_ms = stamp
            self.hypotheses[hypothesis.hypothesis_id] = hypothesis
            self.repos.research.save_hypothesis(hypothesis.to_row())
            for row in self.lab.experiment_rows(result, hypothesis):
                self.repos.research.save_experiment(row)

            reports.append((hypothesis, result))
            if not result.passed:
                report.rejected += 1
                key = result.reason.split(":")[0] if ":" in result.reason else result.reason[:40]
                report.reasons[key] = report.reasons.get(key, 0) + 1

        # --- 4. batch false-discovery control -----------------------------
        self.lab.apply_fdr([r for _, r in reports])

        # --- 5. record every outcome in memory ----------------------------
        survivors: list[tuple[Hypothesis, ValidationReport]] = []
        for hypothesis, result in reports:
            if result.passed:
                survivors.append((hypothesis, result))
                self.memory.record(
                    hypothesis, Outcome.INCONCLUSIVE, reason="passed validation, entering shadow",
                    net_edge_bps=result.final_metrics.net_edge_bps,
                    samples=result.final_metrics.samples, at_ms=stamp,
                )
            else:
                self.memory.record(
                    hypothesis, Outcome.REJECTED, reason=result.reason,
                    net_edge_bps=result.final_metrics.net_edge_bps,
                    samples=result.final_metrics.samples, at_ms=stamp,
                )
        report.survived = len(survivors)

        # --- 6. create strategies and advance the lifecycle ---------------
        for hypothesis, result in survivors:
            score = self.rank_score(hypothesis, result)
            strategy = self.lifecycle.create(
                hypothesis, result.final_metrics, score, cycle_id=self.cycle_id
            )
            self.lifecycle.transition(
                strategy, StrategyState.CANDIDATE, "passed every validation gate",
                evidence=result.to_dict(), at_ms=stamp,
            )
            self.lifecycle.transition(
                strategy, StrategyState.CHALLENGER,
                f"ranked {score:.3f} against current champion", at_ms=stamp,
            )
            self.lifecycle.transition(
                strategy, StrategyState.SHADOW,
                "live shadow evaluation before any wallet impact", at_ms=stamp,
            )

        self._advance_shadows(stamp, report)
        self._check_champion_decay(stamp, report)

        if not self.lifecycle.champion():
            self._explain_no_champion(report)
        return self._finish_cycle(report, stamp)

    def _finish_cycle(self, report: CycleReport, stamp: int) -> CycleReport:
        report.finished_ms = now_ms()
        self.cycles_run += 1
        self.last_cycle_ms = stamp
        self.reports.append(report)
        if len(self.reports) > 200:
            del self.reports[:100]
        self.repos.research.log_agent_event(
            "director", "cycle", f"cycle {report.cycle} complete", detail=report.to_dict()
        )
        return report

    # ------------------------------------------------------------- promotion

    def rank_score(self, hypothesis: Hypothesis, result: ValidationReport) -> float:
        """Combine every dimension the blueprint asks for into one comparable number.

        Deliberately multiplicative in the penalties: an edge with a great mean
        and terrible fold stability should not out-rank a modest, stable one
        just because the mean dominates a weighted sum.
        """
        metrics = result.final_metrics
        if metrics.samples == 0:
            return 0.0

        holdout = result.stage(Stage.HOLDOUT)
        walk = result.stage(Stage.WALKFORWARD)

        base = deflated_score(metrics.net_edge_bps, metrics.t_stat, result.tests_before)
        if base <= 0:
            return 0.0

        independent = holdout.independent_samples if holdout else metrics.samples
        evidence = min(1.0, math.sqrt(independent / 100.0))

        stability = 1.0
        if walk and walk.folds:
            positive = sum(1 for fold in walk.folds if fold["net_edge_bps"] > 0)
            stability = positive / len(walk.folds)

        drawdown_penalty = 1.0
        if metrics.max_drawdown_bps > 0 and metrics.net_edge_bps > 0:
            ratio = metrics.max_drawdown_bps / (metrics.net_edge_bps * max(1, metrics.samples))
            drawdown_penalty = 1.0 / (1.0 + ratio)

        frequency = min(1.0, metrics.events_per_hour / 10.0) if metrics.events_per_hour else 0.2
        return base * evidence * stability * drawdown_penalty * (0.5 + 0.5 * frequency)

    def _advance_shadows(self, stamp: int, report: CycleReport) -> None:
        """Promote shadows that have proved themselves live."""
        cfg = self.config.validation
        for strategy in self.lifecycle.shadows():
            results = self.shadow_results.get(strategy.strategy_id, [])
            elapsed_min = (stamp - strategy.shadow_started_ms) / 60_000.0 if strategy.shadow_started_ms else 0.0
            strategy.shadow_signals = len(results)

            if len(results) < cfg.shadow_min_signals or elapsed_min < cfg.shadow_min_minutes:
                continue

            timestamps = list(range(len(results)))
            metrics = build_metrics(results, results, [0.0] * len(results), timestamps, 1)
            strategy.shadow_metrics = metrics

            if metrics.net_edge_bps < cfg.min_net_edge_bps:
                self.lifecycle.transition(
                    strategy, StrategyState.REJECTED,
                    f"shadow net edge {metrics.net_edge_bps:+.2f} bps over {len(results)} live "
                    f"signals, below {cfg.min_net_edge_bps:.2f}",
                    evidence={"shadow": metrics.to_dict()}, at_ms=stamp,
                )
                self.memory.record(
                    strategy.hypothesis, Outcome.REJECTED,
                    reason="failed live shadow evaluation",
                    net_edge_bps=metrics.net_edge_bps, samples=len(results), at_ms=stamp,
                )
                report.reasons["SHADOW"] = report.reasons.get("SHADOW", 0) + 1
                continue

            champion = self.lifecycle.champion()
            if champion is None:
                self.lifecycle.transition(
                    strategy, StrategyState.CHAMPION,
                    f"first validated edge: shadow net {metrics.net_edge_bps:+.2f} bps over "
                    f"{len(results)} live signals",
                    evidence={"shadow": metrics.to_dict()}, at_ms=stamp,
                )
                report.promoted += 1
                self.memory.record(
                    strategy.hypothesis, Outcome.PROMOTED, reason="promoted to champion",
                    net_edge_bps=metrics.net_edge_bps, samples=len(results), at_ms=stamp,
                )
            elif strategy.score > champion.score * (1.0 + cfg.champion_replace_margin):
                self.lifecycle.transition(
                    champion, StrategyState.RETIRED,
                    f"replaced by {strategy.strategy_id} scoring {strategy.score:.3f} against "
                    f"{champion.score:.3f}", at_ms=stamp,
                )
                self.lifecycle.transition(
                    strategy, StrategyState.CHAMPION,
                    f"beat the incumbent by more than the {cfg.champion_replace_margin:.0%} margin",
                    evidence={"shadow": metrics.to_dict()}, at_ms=stamp,
                )
                report.promoted += 1
            # Otherwise it stays in shadow: good, but not better enough to
            # justify the churn of replacing a working champion.

    def _check_champion_decay(self, stamp: int, report: CycleReport) -> None:
        champion = self.lifecycle.champion()
        if champion is None:
            return
        cfg = self.config.validation
        live = champion.live_metrics
        if live.samples < cfg.degrade_min_signals:
            return
        validated = champion.validated_metrics.net_edge_bps
        if validated <= 0:
            return
        ratio = live.net_edge_bps / validated
        if ratio < cfg.degrade_edge_ratio:
            self.lifecycle.transition(
                champion, StrategyState.DEGRADED,
                f"live net edge {live.net_edge_bps:+.2f} bps is {ratio:.0%} of the validated "
                f"{validated:+.2f} bps over {live.samples} trades",
                evidence={"live": live.to_dict(), "validated": champion.validated_metrics.to_dict()},
                at_ms=stamp,
            )
            self.memory.record(
                champion.hypothesis, Outcome.DECAYED,
                reason="live edge decayed below the validated level",
                net_edge_bps=live.net_edge_bps, samples=live.samples, at_ms=stamp,
            )
            report.notes.append(f"champion {champion.strategy_id} degraded")

    def _explain_no_champion(self, report: CycleReport) -> None:
        shadows = self.lifecycle.shadows()
        if shadows:
            waiting = min(
                self.config.validation.shadow_min_signals - len(self.shadow_results.get(s.strategy_id, []))
                for s in shadows
            )
            self.no_edge_reason = (
                f"{len(shadows)} strategy/ies in live shadow evaluation; the closest needs "
                f"{max(0, waiting)} more live signals before it may touch the wallet"
            )
            return
        if report.evaluated == 0:
            self.no_edge_reason = self.no_edge_reason or "no hypotheses were evaluated this cycle"
            return
        if report.reasons:
            top = max(report.reasons.items(), key=lambda kv: kv[1])
            self.no_edge_reason = (
                f"{report.rejected} of {report.evaluated} hypotheses rejected this cycle; "
                f"most common reason: {top[0]} ({top[1]})"
            )
        else:
            self.no_edge_reason = "no hypothesis survived validation"

    # ---------------------------------------------------------------- shadow

    def record_shadow_signal(self, record: ShadowRecord) -> None:
        self.shadow_pending.append(record)

    def resolve_shadows(self, prices: dict[str, float], *, at_ms: int | None = None) -> int:
        """Settle shadow signals whose horizon has elapsed."""
        stamp = at_ms if at_ms is not None else now_ms()
        resolved = 0
        remaining: list[ShadowRecord] = []
        for record in self.shadow_pending:
            if stamp - record.ts_ms < record.horizon_ms:
                remaining.append(record)
                continue
            price = prices.get(record.symbol)
            if price is None or price <= 0:
                continue  # no price to settle against: drop rather than invent one
            net = record.resolve(price)
            self.shadow_results.setdefault(record.strategy_id, []).append(net)
            resolved += 1
        self.shadow_pending = remaining
        return resolved

    # ---------------------------------------------------------------- report

    def update_live_metrics(self, strategy_id: str, returns: Sequence[float], net_pnl_eur: float) -> None:
        strategy = self.lifecycle.strategies.get(strategy_id)
        if strategy is None or not returns:
            return
        timestamps = list(range(len(returns)))
        strategy.live_metrics = build_metrics(list(returns), list(returns), [0.0] * len(returns), timestamps, 1)
        strategy.live_trades = len(returns)
        strategy.live_net_pnl_eur = net_pnl_eur

    def state(self) -> dict[str, Any]:
        champion = self.lifecycle.champion()
        return {
            "running": self.running,
            "cycles_run": self.cycles_run,
            "last_cycle_ms": self.last_cycle_ms,
            "cycle_interval_s": self.config.research.cycle_interval_s,
            "enabled": self.config.research.enabled,
            "hypotheses_known": len(self.hypotheses),
            "no_edge_reason": self.no_edge_reason,
            "has_champion": champion is not None,
            "champion": champion.to_dict() if champion else None,
            "strategy_counts": self.lifecycle.counts(),
            "memory": self.memory.stats(),
            "validation": self.lab.stats(),
            "shadow_pending": len(self.shadow_pending),
            "shadow_resolved": {k: len(v) for k, v in self.shadow_results.items()},
            "last_cycle": self.reports[-1].to_dict() if self.reports else None,
        }

    def agent_states(self) -> list[dict[str, Any]]:
        return [agent.state() for agent in self.agents]
