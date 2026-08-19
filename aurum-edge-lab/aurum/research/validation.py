"""Validation lab.

A hypothesis passes through five gates in order and stops at the first failure:

1. **Train** — does it do anything at all on the discovery slice?
2. **Validation** — does it survive on data the agent did not rank features on?
3. **Purged walk-forward** — is it stable across time, or was it one lucky hour?
4. **Holdout** — a slice touched by nothing until this moment.
5. **Shadow** — live signals, no wallet impact, measured in real conditions.

Each gate is cost-aware; a hypothesis is never measured gross.

The purging matters more than it looks.  Observations overlap: a 5-second
horizon sampled every 250 ms produces triggers whose outcome windows share most
of their data.  Without an embargo around each fold boundary, a fold's "test"
observations contain the same price moves as its neighbour's, and walk-forward
degenerates into evaluating the same data five times.  The embargo is
``embargo_multiple × horizon`` on each side of every boundary.

The holdout is used **once**, at the end, and never by an agent.  A holdout
consulted during hypothesis creation is not a holdout; it is training data with
a reassuring name.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from typing import Any, Sequence

from ..config import Config
from ..domain import MetricSet, ValidationStatus, now_ms
from ..logging_setup import get_logger
from .dataset import Observation
from .hypotheses import Hypothesis
from .statistics import (
    adjusted_p_value,
    benjamini_hochberg,
    build_metrics,
    deflated_score,
    effective_sample_size,
)

log = get_logger("research.validation")


class Stage:
    TRAIN = "TRAIN"
    VALIDATION = "VALIDATION"
    WALKFORWARD = "WALKFORWARD"
    HOLDOUT = "HOLDOUT"
    SHADOW = "SHADOW"


@dataclass
class StageResult:
    stage: str
    passed: bool
    reason: str
    metrics: MetricSet = field(default_factory=MetricSet)
    independent_samples: int = 0
    folds: list[dict[str, Any]] = field(default_factory=list)
    adjusted_p_value: float = 1.0
    score: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        return {
            "stage": self.stage,
            "passed": self.passed,
            "reason": self.reason,
            "metrics": self.metrics.to_dict(),
            "independent_samples": self.independent_samples,
            "folds": self.folds,
            "adjusted_p_value": round(self.adjusted_p_value, 6),
            "score": round(self.score, 4),
        }


@dataclass
class ValidationReport:
    hypothesis_id: str
    status: ValidationStatus
    passed: bool
    stages: list[StageResult] = field(default_factory=list)
    reason: str = ""
    score: float = 0.0
    total_observations: int = 0
    tests_before: int = 0

    @property
    def final_metrics(self) -> MetricSet:
        for stage in reversed(self.stages):
            if stage.metrics.samples:
                return stage.metrics
        return MetricSet()

    def stage(self, name: str) -> StageResult | None:
        return next((s for s in self.stages if s.stage == name), None)

    def to_dict(self) -> dict[str, Any]:
        return {
            "hypothesis_id": self.hypothesis_id,
            "status": self.status.value,
            "passed": self.passed,
            "reason": self.reason,
            "score": round(self.score, 4),
            "total_observations": self.total_observations,
            "tests_before": self.tests_before,
            "stages": [s.to_dict() for s in self.stages],
            "final_metrics": self.final_metrics.to_dict(),
        }


class ValidationLab:
    def __init__(self, config: Config) -> None:
        self.config = config
        self.validated = 0
        self.rejected = 0
        self.stage_rejections: dict[str, int] = {}

    # ------------------------------------------------------------- splitting

    def split(self, observations: Sequence[Observation], horizon_ms: int) -> dict[str, list[Observation]]:
        """Chronological split with an embargo at every boundary.

        Chronological, never shuffled: shuffling a time series lets a fold learn
        from its own future.
        """
        cfg = self.config.validation
        total = len(observations)
        embargo_ms = int(horizon_ms * cfg.embargo_multiple)

        train_end = int(total * cfg.train_frac)
        validation_end = train_end + int(total * cfg.validation_frac)
        holdout_start = total - int(total * cfg.holdout_frac)

        train = list(observations[:train_end])
        validation = list(observations[train_end:validation_end])
        walkforward = list(observations[validation_end:holdout_start])
        holdout = list(observations[holdout_start:])

        return {
            Stage.TRAIN: _purge_tail(train, embargo_ms),
            Stage.VALIDATION: _purge_head(_purge_tail(validation, embargo_ms), embargo_ms),
            Stage.WALKFORWARD: _purge_head(_purge_tail(walkforward, embargo_ms), embargo_ms),
            Stage.HOLDOUT: _purge_head(holdout, embargo_ms),
        }

    # ------------------------------------------------------------ evaluation

    def evaluate(
        self,
        hypothesis: Hypothesis,
        observations: Sequence[Observation],
        *,
        total_tests: int = 1,
    ) -> ValidationReport:
        report = ValidationReport(
            hypothesis_id=hypothesis.hypothesis_id,
            status=ValidationStatus.UNTESTED,
            passed=False,
            total_observations=len(observations),
            tests_before=total_tests,
        )
        cfg = self.config.validation
        minimum = self.config.research.min_samples_to_evaluate

        if len(observations) < minimum:
            report.reason = f"{len(observations)} observations, need {minimum}"
            report.status = ValidationStatus.REJECTED
            self._count_rejection("INSUFFICIENT_SAMPLES")
            self.rejected += 1
            return report

        splits = self.split(observations, hypothesis.horizon_ms)

        # --- gate 1: train -----------------------------------------------
        train = self._measure(Stage.TRAIN, splits[Stage.TRAIN], hypothesis, total_tests)
        report.stages.append(train)
        if not train.passed:
            return self._finish(report, ValidationStatus.REJECTED, train.reason, Stage.TRAIN)
        report.status = ValidationStatus.TRAIN_PASS

        # --- gate 2: validation ------------------------------------------
        validation = self._measure(Stage.VALIDATION, splits[Stage.VALIDATION], hypothesis, total_tests)
        report.stages.append(validation)
        if not validation.passed:
            return self._finish(report, ValidationStatus.REJECTED, validation.reason, Stage.VALIDATION)
        report.status = ValidationStatus.VALIDATION_PASS

        # --- gate 3: purged walk-forward ----------------------------------
        walk = self._walk_forward(splits[Stage.WALKFORWARD], hypothesis, total_tests)
        report.stages.append(walk)
        if not walk.passed:
            return self._finish(report, ValidationStatus.REJECTED, walk.reason, Stage.WALKFORWARD)
        report.status = ValidationStatus.WALKFORWARD_PASS

        # --- gate 4: holdout, touched for the first time ------------------
        holdout = self._measure(Stage.HOLDOUT, splits[Stage.HOLDOUT], hypothesis, total_tests)
        report.stages.append(holdout)
        if not holdout.passed:
            return self._finish(report, ValidationStatus.REJECTED, holdout.reason, Stage.HOLDOUT)

        report.status = ValidationStatus.HOLDOUT_PASS
        report.passed = True
        report.score = holdout.score
        report.reason = (
            f"passed all gates: holdout net {holdout.metrics.net_edge_bps:+.2f} bps over "
            f"{holdout.independent_samples} independent samples "
            f"(adjusted p={holdout.adjusted_p_value:.4f})"
        )
        self.validated += 1
        _ = cfg
        return report

    def _measure(
        self, stage: str, observations: Sequence[Observation], hypothesis: Hypothesis, total_tests: int
    ) -> StageResult:
        cfg = self.config.validation
        if len(observations) < cfg.min_fold_samples:
            return StageResult(
                stage=stage,
                passed=False,
                reason=f"{len(observations)} observations after purging, need {cfg.min_fold_samples}",
            )

        timestamps = [o.ts_ms for o in observations]
        metrics = build_metrics(
            [o.gross_bps for o in observations],
            [o.net_bps for o in observations],
            [o.cost_bps for o in observations],
            timestamps,
            hypothesis.horizon_ms,
        )
        independent = effective_sample_size(timestamps, hypothesis.horizon_ms)
        adjusted = adjusted_p_value(metrics.p_value, total_tests)
        score = deflated_score(metrics.net_edge_bps, metrics.t_stat, total_tests)

        result = StageResult(
            stage=stage,
            passed=False,
            reason="",
            metrics=metrics,
            independent_samples=independent,
            adjusted_p_value=adjusted,
            score=score,
        )

        if independent < cfg.min_fold_samples:
            result.reason = (
                f"{independent} independent observations after de-overlapping "
                f"{len(observations)}, need {cfg.min_fold_samples}"
            )
            return result
        if metrics.net_edge_bps < cfg.min_net_edge_bps:
            result.reason = (
                f"net edge {metrics.net_edge_bps:+.2f} bps below minimum {cfg.min_net_edge_bps:.2f}"
            )
            return result
        if metrics.profit_factor < cfg.min_profit_factor:
            result.reason = (
                f"profit factor {metrics.profit_factor:.3f} below minimum {cfg.min_profit_factor:.3f}"
            )
            return result
        if metrics.max_drawdown_bps > cfg.max_drawdown_bps:
            result.reason = (
                f"drawdown {metrics.max_drawdown_bps:.1f} bps beyond limit {cfg.max_drawdown_bps:.1f}"
            )
            return result
        if adjusted > cfg.fdr_alpha:
            result.reason = (
                f"adjusted p={adjusted:.4f} (raw {metrics.p_value:.4f} over {total_tests} tests) "
                f"above alpha {cfg.fdr_alpha:.3f}"
            )
            return result

        result.passed = True
        result.reason = (
            f"net {metrics.net_edge_bps:+.2f} bps, PF {metrics.profit_factor:.2f}, "
            f"{independent} independent samples, adjusted p={adjusted:.4f}"
        )
        return result

    def _walk_forward(
        self, observations: Sequence[Observation], hypothesis: Hypothesis, total_tests: int
    ) -> StageResult:
        """Split into folds with an embargo between them, then require most to hold.

        Requiring *every* fold to be profitable would reject a genuine edge for
        one bad hour; requiring the average would accept an edge that only ever
        worked once.  The rule is a majority of folds positive plus a positive
        aggregate — stability, not perfection.
        """
        cfg = self.config.validation
        folds = _make_folds(observations, cfg.walkforward_folds, int(hypothesis.horizon_ms * cfg.embargo_multiple))
        usable = [f for f in folds if len(f) >= cfg.min_fold_samples]
        if len(usable) < 2:
            return StageResult(
                stage=Stage.WALKFORWARD,
                passed=False,
                reason=(
                    f"only {len(usable)} of {cfg.walkforward_folds} folds reached "
                    f"{cfg.min_fold_samples} observations after purging"
                ),
            )

        fold_rows: list[dict[str, Any]] = []
        positive = 0
        for index, fold in enumerate(usable):
            metrics = build_metrics(
                [o.gross_bps for o in fold],
                [o.net_bps for o in fold],
                [o.cost_bps for o in fold],
                [o.ts_ms for o in fold],
                hypothesis.horizon_ms,
            )
            if metrics.net_edge_bps > 0:
                positive += 1
            fold_rows.append(
                {
                    "fold": index,
                    "samples": metrics.samples,
                    "independent": effective_sample_size([o.ts_ms for o in fold], hypothesis.horizon_ms),
                    "net_edge_bps": round(metrics.net_edge_bps, 4),
                    "profit_factor": round(metrics.profit_factor, 4),
                    "win_rate": round(metrics.win_rate, 4),
                    "from_ms": fold[0].ts_ms,
                    "to_ms": fold[-1].ts_ms,
                }
            )

        combined = list(observations)
        aggregate = build_metrics(
            [o.gross_bps for o in combined],
            [o.net_bps for o in combined],
            [o.cost_bps for o in combined],
            [o.ts_ms for o in combined],
            hypothesis.horizon_ms,
        )
        independent = effective_sample_size([o.ts_ms for o in combined], hypothesis.horizon_ms)
        adjusted = adjusted_p_value(aggregate.p_value, total_tests)
        result = StageResult(
            stage=Stage.WALKFORWARD,
            passed=False,
            reason="",
            metrics=aggregate,
            independent_samples=independent,
            folds=fold_rows,
            adjusted_p_value=adjusted,
            score=deflated_score(aggregate.net_edge_bps, aggregate.t_stat, total_tests),
        )

        needed = len(usable) // 2 + 1
        if positive < needed:
            result.reason = f"only {positive} of {len(usable)} folds positive, need {needed}"
            return result
        if aggregate.net_edge_bps < cfg.min_net_edge_bps:
            result.reason = (
                f"aggregate net edge {aggregate.net_edge_bps:+.2f} bps below "
                f"{cfg.min_net_edge_bps:.2f}"
            )
            return result
        if adjusted > cfg.fdr_alpha:
            result.reason = f"adjusted p={adjusted:.4f} above alpha {cfg.fdr_alpha:.3f}"
            return result

        result.passed = True
        result.reason = (
            f"{positive}/{len(usable)} folds positive, aggregate net "
            f"{aggregate.net_edge_bps:+.2f} bps"
        )
        return result

    def _finish(
        self, report: ValidationReport, status: ValidationStatus, reason: str, stage: str
    ) -> ValidationReport:
        report.status = status
        report.reason = f"{stage}: {reason}"
        report.passed = False
        self._count_rejection(stage)
        self.rejected += 1
        return report

    def _count_rejection(self, stage: str) -> None:
        self.stage_rejections[stage] = self.stage_rejections.get(stage, 0) + 1

    # ------------------------------------------------------- batch FDR pass

    def apply_fdr(self, reports: Sequence[ValidationReport]) -> list[ValidationReport]:
        """Second line of defence over a batch of survivors.

        Each report already carries a per-hypothesis adjustment for the tests
        spent so far.  Benjamini-Hochberg over the batch catches the case that
        adjustment cannot see: many hypotheses passing *together* by chance.
        """
        survivors = [r for r in reports if r.passed]
        if len(survivors) < 2:
            return reports
        holdouts = [r.stage(Stage.HOLDOUT) for r in survivors]
        p_values = [(s.metrics.p_value if s else 1.0) for s in holdouts]
        verdicts = benjamini_hochberg(p_values, self.config.validation.fdr_alpha)
        for report, survived in zip(survivors, verdicts, strict=True):
            if not survived:
                report.passed = False
                report.status = ValidationStatus.REJECTED
                report.reason = (
                    f"rejected by false-discovery control across {len(survivors)} simultaneous "
                    f"survivors (alpha {self.config.validation.fdr_alpha})"
                )
                self.validated -= 1
                self.rejected += 1
                self._count_rejection("FDR_BATCH")
        return reports

    def experiment_rows(self, report: ValidationReport, hypothesis: Hypothesis) -> list[dict[str, Any]]:
        rows = []
        for stage in report.stages:
            rows.append(
                {
                    "experiment_id": f"exp-{uuid.uuid4().hex[:12]}",
                    "hypothesis_id": hypothesis.hypothesis_id,
                    "stage": stage.stage,
                    "passed": stage.passed,
                    "reason": stage.reason,
                    "metrics": stage.metrics.to_dict(),
                    "folds": stage.folds,
                    "samples": stage.metrics.samples,
                    "p_value": stage.metrics.p_value,
                    "adjusted_p_value": stage.adjusted_p_value,
                    "score": stage.score,
                    "dataset_start_ms": hypothesis.dataset_start_ms,
                    "dataset_end_ms": hypothesis.dataset_end_ms,
                    "created_ms": now_ms(),
                }
            )
        return rows

    def stats(self) -> dict[str, Any]:
        return {
            "validated": self.validated,
            "rejected": self.rejected,
            "rejections_by_stage": dict(self.stage_rejections),
            "gates": [Stage.TRAIN, Stage.VALIDATION, Stage.WALKFORWARD, Stage.HOLDOUT, Stage.SHADOW],
            "config": {
                "train_frac": self.config.validation.train_frac,
                "validation_frac": self.config.validation.validation_frac,
                "holdout_frac": self.config.validation.holdout_frac,
                "walkforward_folds": self.config.validation.walkforward_folds,
                "embargo_multiple": self.config.validation.embargo_multiple,
                "fdr_alpha": self.config.validation.fdr_alpha,
                "min_net_edge_bps": self.config.validation.min_net_edge_bps,
            },
        }


# ---------------------------------------------------------------------------
# Purging helpers
# ---------------------------------------------------------------------------


def _purge_tail(observations: list[Observation], embargo_ms: int) -> list[Observation]:
    """Drop observations whose outcome window reaches past the split point."""
    if not observations or embargo_ms <= 0:
        return observations
    cutoff = observations[-1].ts_ms - embargo_ms
    return [o for o in observations if o.ts_ms <= cutoff] or []


def _purge_head(observations: list[Observation], embargo_ms: int) -> list[Observation]:
    """Drop observations that begin inside the previous split's outcome windows."""
    if not observations or embargo_ms <= 0:
        return observations
    cutoff = observations[0].ts_ms + embargo_ms
    return [o for o in observations if o.ts_ms >= cutoff] or []


def _make_folds(
    observations: Sequence[Observation], count: int, embargo_ms: int
) -> list[list[Observation]]:
    if not observations or count < 1:
        return []
    size = max(1, len(observations) // count)
    folds: list[list[Observation]] = []
    for index in range(count):
        start = index * size
        end = len(observations) if index == count - 1 else (index + 1) * size
        chunk = list(observations[start:end])
        if not chunk:
            continue
        # Embargo the head of every fold after the first: those observations
        # share outcome windows with the fold before them.
        folds.append(_purge_head(chunk, embargo_ms) if index else chunk)
    return folds
