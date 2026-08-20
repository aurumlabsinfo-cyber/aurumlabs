"""Continuous learning: Champion / Challenger, with a gate that is hard to game.

    historical data -> purged walk-forward -> untouched holdout -> shadow -> paper

Rules this module enforces:

* the Champion is **never** modified.  Training writes a new row; promotion is a
  status change with the old version kept for instant rollback;
* the holdout is the most recent slice of data and is never used for fitting or
  for choosing hyper-parameters;
* folds are **purged**: samples within ``purge_seconds`` of a fold boundary are
  dropped, so a label cannot leak backwards into training through overlapping
  windows;
* a higher win rate alone never promotes anything.  Net profit, expectancy,
  drawdown and average loss decide.

Training data is not only closed trades: every decision - including NO TRADE and
shadow decisions - is labelled afterwards with what the market actually did, so
the model learns from what the system *declined* as well.
"""

from __future__ import annotations

import json
import math
import time
from dataclasses import dataclass, field
from typing import Any, Sequence

from ..config import Config
from ..scan.snapshot import FEATURE_NAMES
from ..storage.repo import Repo
from ..util.logging_setup import get_logger
from ..decide.model import Model, sigmoid

log = get_logger("learn")


@dataclass
class Sample:
    ts_ms: float
    x: list[float]
    y: int
    symbol: str
    net_eur: float = 0.0          # realised, when the sample is a closed trade
    move_bps: float = 0.0
    cost_bps: float = 0.0
    is_trade: bool = False


@dataclass
class FitResult:
    weights: dict[str, float]
    bias: float
    mean: dict[str, float]
    std: dict[str, float]
    epochs: int
    n: int


@dataclass
class EvalResult:
    n: int = 0
    taken: int = 0
    accuracy: float = 0.0
    logloss: float = 0.0
    auc: float = 0.0
    precision: float = 0.0
    net_bps: float = 0.0
    net_eur: float = 0.0
    expectancy_eur: float = 0.0
    max_drawdown_eur: float = 0.0
    avg_loss_eur: float = 0.0
    win_rate: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        return {k: round(v, 6) if isinstance(v, float) else v for k, v in self.__dict__.items()}


# ---------------------------------------------------------------------------
# maths (pure python: the datasets are small and the result must be auditable)
# ---------------------------------------------------------------------------

def standardise(samples: Sequence[Sample]) -> tuple[dict[str, float], dict[str, float]]:
    n = len(samples)
    mean: dict[str, float] = {}
    std: dict[str, float] = {}
    for i, name in enumerate(FEATURE_NAMES):
        column = [s.x[i] for s in samples]
        m = sum(column) / n
        var = sum((v - m) ** 2 for v in column) / max(n - 1, 1)
        mean[name] = m
        std[name] = math.sqrt(var) if var > 1e-12 else 1.0
    return mean, std


def fit_logistic(
    samples: Sequence[Sample],
    l2: float,
    learning_rate: float,
    epochs: int,
) -> FitResult:
    """Batch gradient descent with L2.  Deterministic: same data, same model."""
    n = len(samples)
    d = len(FEATURE_NAMES)
    mean, std = standardise(samples)
    xs = [
        [(s.x[i] - mean[name]) / std[name] for i, name in enumerate(FEATURE_NAMES)]
        for s in samples
    ]
    ys = [float(s.y) for s in samples]
    # class weights: rare positives must not be drowned out
    positives = sum(ys) or 1.0
    negatives = n - positives or 1.0
    w_pos = n / (2.0 * positives)
    w_neg = n / (2.0 * negatives)
    weights = [0.0] * d
    bias = 0.0

    for _ in range(epochs):
        grad = [0.0] * d
        grad_b = 0.0
        for xi, yi in zip(xs, ys):
            z = bias + sum(w * x for w, x in zip(weights, xi))
            p = sigmoid(z)
            weight = w_pos if yi > 0.5 else w_neg
            err = (p - yi) * weight
            grad_b += err
            for j in range(d):
                grad[j] += err * xi[j]
        bias -= learning_rate * grad_b / n
        for j in range(d):
            weights[j] -= learning_rate * (grad[j] / n + l2 * weights[j] / n)

    return FitResult(
        weights={name: weights[i] for i, name in enumerate(FEATURE_NAMES)},
        bias=bias,
        mean=mean,
        std=std,
        epochs=epochs,
        n=n,
    )


def auc_score(scores: Sequence[float], labels: Sequence[int]) -> float:
    pairs = sorted(zip(scores, labels))
    positives = sum(labels)
    negatives = len(labels) - positives
    if positives == 0 or negatives == 0:
        return 0.5
    rank_sum = 0.0
    i = 0
    rank = 1
    while i < len(pairs):
        j = i
        while j + 1 < len(pairs) and pairs[j + 1][0] == pairs[i][0]:
            j += 1
        average_rank = (rank + (rank + (j - i))) / 2.0
        for k in range(i, j + 1):
            if pairs[k][1] == 1:
                rank_sum += average_rank
        rank += j - i + 1
        i = j + 1
    return (rank_sum - positives * (positives + 1) / 2.0) / (positives * negatives)


def evaluate(model: Model, samples: Sequence[Sample], threshold: float) -> EvalResult:
    """Replay a model over samples it never saw.

    For samples that were real trades the realised net P&L is used.  For labelled
    decisions the outcome is expressed in basis points net of the costs that were
    estimated at the time, so a model cannot look good by picking moves that pay
    less than they cost.
    """
    result = EvalResult(n=len(samples))
    if not samples:
        return result

    scores: list[float] = []
    labels: list[int] = []
    losses: list[float] = []
    taken_net: list[float] = []
    correct = 0
    logloss = 0.0

    for sample in samples:
        features = {name: sample.x[i] for i, name in enumerate(FEATURE_NAMES)}
        p = model.probability(features)
        scores.append(p)
        labels.append(sample.y)
        p_clip = min(max(p, 1e-9), 1 - 1e-9)
        logloss -= sample.y * math.log(p_clip) + (1 - sample.y) * math.log(1 - p_clip)
        if (p >= 0.5) == (sample.y == 1):
            correct += 1
        if p >= threshold:
            result.taken += 1
            net_bps = sample.move_bps - sample.cost_bps
            result.net_bps += net_bps
            net_eur = sample.net_eur if sample.is_trade else net_bps / 10_000.0 * 500.0
            taken_net.append(net_eur)
            if net_eur <= 0:
                losses.append(net_eur)

    result.accuracy = correct / len(samples)
    result.logloss = logloss / len(samples)
    result.auc = auc_score(scores, labels)
    if result.taken:
        wins = sum(1 for v in taken_net if v > 0)
        result.precision = wins / result.taken
        result.win_rate = result.precision
        result.net_eur = sum(taken_net)
        result.expectancy_eur = result.net_eur / result.taken
        result.avg_loss_eur = sum(losses) / len(losses) if losses else 0.0
        running = peak = 0.0
        for value in taken_net:
            running += value
            peak = max(peak, running)
            result.max_drawdown_eur = max(result.max_drawdown_eur, peak - running)
    return result


def purged_walk_forward(
    samples: Sequence[Sample],
    folds: int,
    purge_ms: float,
    l2: float,
    learning_rate: float,
    epochs: int,
    threshold: float,
) -> tuple[list[EvalResult], list[FitResult]]:
    """Expanding-window walk forward with a purge gap between train and test."""
    results: list[EvalResult] = []
    fits: list[FitResult] = []
    if len(samples) < folds * 20:
        return results, fits
    ordered = sorted(samples, key=lambda s: s.ts_ms)
    size = len(ordered) // (folds + 1)
    for fold in range(1, folds + 1):
        train_end = size * fold
        test_start = train_end
        test_end = min(size * (fold + 1), len(ordered))
        boundary_ts = ordered[train_end - 1].ts_ms
        train = [s for s in ordered[:train_end] if s.ts_ms <= boundary_ts - purge_ms]
        test = ordered[test_start:test_end]
        if len(train) < 40 or not test:
            continue
        fit = fit_logistic(train, l2, learning_rate, epochs)
        candidate = Model(
            version=f"fold-{fold}",
            kind="challenger",
            weights=fit.weights,
            bias=fit.bias,
            mean=fit.mean,
            std=fit.std,
        )
        results.append(evaluate(candidate, test, threshold))
        fits.append(fit)
    return results, fits


# ---------------------------------------------------------------------------
# pipeline
# ---------------------------------------------------------------------------

@dataclass
class PipelineReport:
    status: str
    detail: str
    challenger: str | None = None
    champion: str | None = None
    cv: list[dict[str, Any]] = field(default_factory=list)
    holdout_champion: dict[str, Any] = field(default_factory=dict)
    holdout_challenger: dict[str, Any] = field(default_factory=dict)
    samples: int = 0
    holdout_samples: int = 0
    gates: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "detail": self.detail,
            "challenger": self.challenger,
            "champion": self.champion,
            "cv": self.cv,
            "holdout_champion": self.holdout_champion,
            "holdout_challenger": self.holdout_challenger,
            "samples": self.samples,
            "holdout_samples": self.holdout_samples,
            "gates": self.gates,
        }


class LearningPipeline:
    def __init__(self, cfg: Config, repo: Repo) -> None:
        self.cfg = cfg
        self.repo = repo
        self.last_report: PipelineReport | None = None
        self.runs = 0

    # ---------------------------------------------------------------- data
    def load_samples(self) -> list[Sample]:
        samples: list[Sample] = []
        for row in self.repo.labelled_decisions():
            try:
                features = json.loads(row["features_json"])
            except (TypeError, json.JSONDecodeError):
                continue
            if not all(name in features for name in FEATURE_NAMES):
                continue
            samples.append(
                Sample(
                    ts_ms=row["ts_ms"],
                    x=[float(features[name]) for name in FEATURE_NAMES],
                    y=int(row["outcome_label"]),
                    symbol=row["symbol"],
                    move_bps=float(row["outcome_move_bps"] or 0.0),
                    cost_bps=float(row["outcome_cost_bps"] or 0.0),
                )
            )
        for row in self.repo.all_trades_for_training():
            try:
                features = json.loads(row["features_json"])
            except (TypeError, json.JSONDecodeError):
                continue
            if not all(name in features for name in FEATURE_NAMES):
                continue
            notional = row["notional_eur"] or 1.0
            samples.append(
                Sample(
                    ts_ms=row["entry_ts"],
                    x=[float(features[name]) for name in FEATURE_NAMES],
                    y=int(row["label"]),
                    symbol=row["symbol"],
                    net_eur=row["net_pnl_eur"],
                    move_bps=(row["gross_pnl_eur"] / notional) * 10_000.0,
                    cost_bps=((row["fees_eur"] + row["slippage_eur"]) / notional) * 10_000.0,
                    is_trade=True,
                )
            )
        samples.sort(key=lambda s: s.ts_ms)
        return samples

    # ---------------------------------------------------------------- run
    def run(self, champion: Model) -> PipelineReport:
        self.runs += 1
        cfg = self.cfg.learn
        samples = self.load_samples()
        report = PipelineReport(
            status="skipped", detail="", champion=champion.version, samples=len(samples)
        )
        if len(samples) < cfg.min_trades_for_training:
            report.detail = (
                f"{len(samples)} labelled samples, {cfg.min_trades_for_training} needed "
                f"before a challenger is trained"
            )
            self.last_report = report
            return report

        # untouched holdout: the most recent slice, never fitted, never tuned on
        split = int(len(samples) * (1.0 - cfg.holdout_fraction))
        train_pool = samples[:split]
        holdout = samples[split:]
        report.holdout_samples = len(holdout)
        if len(train_pool) < 60 or len(holdout) < 20:
            report.detail = "not enough data on either side of the holdout split"
            self.last_report = report
            return report

        threshold = self.cfg.decide.min_probability
        purge_ms = cfg.purge_seconds * 1000.0
        cv_results, _ = purged_walk_forward(
            train_pool, cfg.walk_forward_folds, purge_ms,
            cfg.l2, cfg.learning_rate, cfg.epochs, threshold,
        )
        report.cv = [r.to_dict() for r in cv_results]
        if not cv_results:
            report.detail = "walk-forward produced no usable fold"
            self.last_report = report
            return report

        fit = fit_logistic(train_pool, cfg.l2, cfg.learning_rate, cfg.epochs)
        version = f"challenger-{time.strftime('%Y%m%dT%H%M%S')}-n{len(train_pool)}"
        challenger = Model(
            version=version,
            kind="challenger",
            weights=fit.weights,
            bias=fit.bias,
            mean=fit.mean,
            std=fit.std,
            move_scale=self._fit_move_scale(train_pool, champion),
            parent=champion.version,
            notes=(
                f"trained on {len(train_pool)} samples, "
                f"{cfg.walk_forward_folds}-fold purged walk-forward, "
                f"holdout of {len(holdout)} samples untouched"
            ),
        )

        champion_eval = evaluate(champion, holdout, threshold)
        challenger_eval = evaluate(challenger, holdout, threshold)
        report.holdout_champion = champion_eval.to_dict()
        report.holdout_challenger = challenger_eval.to_dict()
        challenger.metrics = {
            "cv": report.cv,
            "holdout_challenger": report.holdout_challenger,
            "holdout_champion": report.holdout_champion,
            "trained_on": len(train_pool),
            "holdout_samples": len(holdout),
            "mean_cv_auc": sum(r.auc for r in cv_results) / len(cv_results),
        }

        gates = self._gates(champion_eval, challenger_eval)
        report.gates = gates
        report.challenger = version
        # A challenger that fails the holdout is still kept: the history matters.
        status = "shadow" if not gates else "rejected"
        self.repo.save_model_version(challenger.to_row(status))
        self.repo.log_model_event(
            version,
            "trained",
            {
                "status": status,
                "gates_failed": gates,
                "holdout_challenger": report.holdout_challenger,
                "holdout_champion": report.holdout_champion,
                "champion": champion.version,
            },
        )
        report.status = status
        report.detail = (
            "challenger passed the holdout and moved to shadow"
            if not gates
            else "challenger rejected: " + "; ".join(gates)
        )
        log.info("learning run: %s (%s)", report.status, report.detail)
        self.last_report = report
        return report

    def _fit_move_scale(self, samples: Sequence[Sample], champion: Model) -> float:
        """Calibrate the residual-move estimate against what actually happened."""
        moves = [abs(s.move_bps) for s in samples if s.move_bps]
        if len(moves) < 30:
            return champion.move_scale
        moves.sort()
        median = moves[len(moves) // 2]
        # keep it in a sane band: this multiplies a volatility-based estimate
        return max(0.4, min(2.5, champion.move_scale * (0.5 + median / 20.0)))

    def _gates(self, champion: EvalResult, challenger: EvalResult) -> list[str]:
        """Every reason the challenger must not be promoted."""
        cfg = self.cfg.learn
        failures: list[str] = []
        if challenger.taken < 20:
            failures.append(
                f"challenger would have taken only {challenger.taken} trades out of sample"
            )
        required = champion.net_eur * (1.0 + cfg.promote_min_net_improvement)
        if champion.net_eur <= 0:
            required = max(0.0, champion.net_eur) + 0.01
        if challenger.net_eur < required:
            failures.append(
                f"net profit out of sample {challenger.net_eur:.2f} EUR < required "
                f"{required:.2f} EUR (champion {champion.net_eur:.2f})"
            )
        if challenger.expectancy_eur <= 0:
            failures.append(f"expectancy {challenger.expectancy_eur:.4f} EUR is not positive")
        if champion.max_drawdown_eur > 0 and challenger.max_drawdown_eur > (
            champion.max_drawdown_eur * cfg.promote_max_drawdown_ratio
        ):
            failures.append(
                f"drawdown {challenger.max_drawdown_eur:.2f} EUR worse than "
                f"{cfg.promote_max_drawdown_ratio:.2f}x champion "
                f"({champion.max_drawdown_eur:.2f})"
            )
        if champion.avg_loss_eur < 0 and challenger.avg_loss_eur < champion.avg_loss_eur * 1.15:
            failures.append(
                f"average loss {challenger.avg_loss_eur:.3f} EUR worse than champion "
                f"{champion.avg_loss_eur:.3f} EUR"
            )
        if challenger.auc <= 0.5:
            failures.append(f"out-of-sample AUC {challenger.auc:.3f} is no better than chance")
        return failures

    # ---------------------------------------------------------------- promotion
    def shadow_evidence(self, version: str) -> dict[str, Any]:
        """How the shadow challenger did on decisions it took live."""
        rows = self.repo.db.query(
            "SELECT probability, outcome_move_bps, outcome_cost_bps, outcome_label "
            "FROM decisions WHERE model_version=? AND shadow=1 AND outcome_label IS NOT NULL",
            (version,),
        )
        taken = [r for r in rows if r["probability"] >= self.cfg.decide.min_probability]
        net_bps = sum(
            (r["outcome_move_bps"] or 0.0) - (r["outcome_cost_bps"] or 0.0) for r in taken
        )
        wins = sum(1 for r in taken if r["outcome_label"] == 1)
        return {
            "version": version,
            "decisions": len(rows),
            "taken": len(taken),
            "net_bps": net_bps,
            "win_rate": wins / len(taken) if taken else 0.0,
            "expectancy_bps": net_bps / len(taken) if taken else 0.0,
        }

    def maybe_promote(self, champion: Model) -> tuple[Model | None, str]:
        """Promote a shadow challenger only on live, out-of-sample evidence."""
        rows = [
            row for row in self.repo.model_versions() if row["status"] == "shadow"
        ]
        if not rows:
            return None, "no challenger in shadow"
        row = rows[0]
        evidence = self.shadow_evidence(row["version"])
        champion_evidence = self.shadow_evidence(champion.version)
        if evidence["taken"] < self.cfg.learn.shadow_min_decisions:
            return None, (
                f"shadow evidence not sufficient yet: {evidence['taken']}/"
                f"{self.cfg.learn.shadow_min_decisions} decisions"
            )
        if evidence["expectancy_bps"] <= max(champion_evidence["expectancy_bps"], 0.0):
            self.repo.log_model_event(
                row["version"], "shadow_rejected",
                {"challenger": evidence, "champion": champion_evidence},
            )
            self.repo.save_model_version({**row, "status": "rejected", "retired_ts": time.time()})
            return None, (
                f"shadow expectancy {evidence['expectancy_bps']:.2f}bps did not beat the "
                f"champion's {champion_evidence['expectancy_bps']:.2f}bps"
            )
        challenger = Model.from_row(row)
        return self.promote(challenger, champion, evidence), "promoted"

    def promote(self, challenger: Model, champion: Model, evidence: dict[str, Any]) -> Model:
        """Swap champions.  The old one is retired, never deleted."""
        champion_row = self.repo.get_model(champion.version)
        if champion_row:
            self.repo.save_model_version(
                {**champion_row, "status": "retired", "retired_ts": time.time()}
            )
        challenger.kind = "champion"
        self.repo.save_model_version(challenger.to_row("champion"))
        self.repo.log_model_event(
            challenger.version,
            "promoted",
            {"replaced": champion.version, "evidence": evidence},
        )
        self.repo.db.kv_set("champion_version", challenger.version)
        log.warning("CHAMPION PROMOTED: %s replaces %s", challenger.version, champion.version)
        return challenger

    def rollback(self) -> tuple[Model | None, str]:
        """Instant return to the previous champion."""
        rows = self.repo.model_versions()
        current = next((r for r in rows if r["status"] == "champion"), None)
        previous = next((r for r in rows if r["status"] == "retired"), None)
        if not previous:
            return None, "no retired version to roll back to"
        if current:
            self.repo.save_model_version(
                {**current, "status": "rolled_back", "retired_ts": time.time()}
            )
        self.repo.save_model_version({**previous, "status": "champion", "promoted_ts": time.time()})
        self.repo.log_model_event(
            previous["version"], "rollback", {"from": current["version"] if current else None}
        )
        self.repo.db.kv_set("champion_version", previous["version"])
        return Model.from_row(previous), f"rolled back to {previous['version']}"
