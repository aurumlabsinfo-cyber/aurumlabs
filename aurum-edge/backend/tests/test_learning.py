"""Champion / Challenger: training never touches the Champion, and the gate holds."""

from __future__ import annotations

import dataclasses
import json
import math
import random

import pytest

from aurum_edge.config import Config
from aurum_edge.decide.model import Model, champion_v1
from aurum_edge.learn.pipeline import (
    EvalResult, LearningPipeline, Sample, auc_score, evaluate, fit_logistic,
    purged_walk_forward,
)
from aurum_edge.scan.snapshot import FEATURE_NAMES
from aurum_edge.storage.repo import Repo


def synthetic_samples(n: int = 600, seed: int = 3, signal: float = 1.4) -> list[Sample]:
    """A learnable dataset: the label follows two of the features plus noise."""
    rng = random.Random(seed)
    samples: list[Sample] = []
    for i in range(n):
        x = [rng.gauss(0.0, 1.0) for _ in FEATURE_NAMES]
        score = signal * x[1] + 0.9 * x[7] - 0.4
        probability = 1.0 / (1.0 + math.exp(-score))
        label = 1 if rng.random() < probability else 0
        move = 30.0 if label else -22.0
        samples.append(
            Sample(
                ts_ms=1_000_000.0 + i * 1_000.0,
                x=x,
                y=label,
                symbol="BTCUSDT",
                move_bps=move + rng.gauss(0, 4),
                cost_bps=15.0,
                net_eur=(move - 15.0) / 10_000.0 * 500.0,
                is_trade=True,
            )
        )
    return samples


# ------------------------------------------------------------------- maths

def test_logistic_fit_recovers_the_signal() -> None:
    samples = synthetic_samples()
    fit = fit_logistic(samples, l2=1.0, learning_rate=0.3, epochs=400)
    ordered = sorted(fit.weights.items(), key=lambda kv: -abs(kv[1]))
    assert ordered[0][0] in (FEATURE_NAMES[1], FEATURE_NAMES[7])
    assert fit.weights[FEATURE_NAMES[1]] > 0
    assert fit.n == len(samples)


def test_fit_is_deterministic() -> None:
    samples = synthetic_samples()
    a = fit_logistic(samples, 1.0, 0.2, 60)
    b = fit_logistic(samples, 1.0, 0.2, 60)
    assert a.weights == b.weights and a.bias == b.bias


def test_auc_is_sane() -> None:
    assert auc_score([0.1, 0.2, 0.8, 0.9], [0, 0, 1, 1]) == pytest.approx(1.0)
    assert auc_score([0.9, 0.8, 0.2, 0.1], [0, 0, 1, 1]) == pytest.approx(0.0)
    assert auc_score([0.5, 0.5, 0.5, 0.5], [0, 1, 0, 1]) == pytest.approx(0.5)


def test_walk_forward_is_purged() -> None:
    samples = synthetic_samples(400)
    results, fits = purged_walk_forward(
        samples, folds=4, purge_ms=10_000.0, l2=1.0, learning_rate=0.2, epochs=60,
        threshold=0.55,
    )
    assert len(results) >= 3
    assert all(isinstance(r, EvalResult) for r in results)
    assert sum(r.auc for r in results) / len(results) > 0.5

    # with an enormous purge the training window shrinks, proving the gap is real
    tiny, _ = purged_walk_forward(
        samples, folds=4, purge_ms=1e12, l2=1.0, learning_rate=0.2, epochs=20, threshold=0.55
    )
    assert len(tiny) < len(results)


def test_evaluate_reports_selectivity_and_money() -> None:
    samples = synthetic_samples(300)
    fit = fit_logistic(samples, 1.0, 0.3, 300)
    model = Model(version="t", weights=fit.weights, bias=fit.bias, mean=fit.mean, std=fit.std)
    result = evaluate(model, samples, threshold=0.6)
    assert 0 < result.taken <= result.n
    assert result.auc > 0.5
    assert result.expectancy_eur == pytest.approx(
        result.net_eur / result.taken, rel=1e-9
    )


# ---------------------------------------------------------------- pipeline

def seed_decisions(repo: Repo, count: int = 400, seed: int = 11) -> None:
    """Labelled decisions, exactly as the engine records them."""
    rng = random.Random(seed)
    for i in range(count):
        features = {name: rng.gauss(0, 1) for name in FEATURE_NAMES}
        score = 1.5 * features["mom_1s"] + 1.0 * features["ofi_5s"] - 0.3
        probability = 1.0 / (1.0 + math.exp(-score))
        label = 1 if rng.random() < probability else 0
        repo.db.insert(
            "decisions",
            {
                "run_id": repo.db.run_id,
                # spaced a minute apart: the 300s purge must still leave a
                # usable training window, as it does on real data
                "ts_ms": 1_000_000.0 + i * 60_000.0,
                "symbol": "BTCUSDT",
                "action": "NO_TRADE" if i % 3 else "LONG",
                "side": "LONG",
                "quality": 0.6, "probability": probability,
                "expected_move_bps": 30.0, "expected_cost_eur": 0.5,
                "margin_eur": 50.0, "leverage": 10.0, "notional_eur": 500.0,
                "target_eur": 2.0, "max_loss_eur": 1.5, "max_hold_s": 60.0,
                "expectancy_eur": 0.4, "reason": "test", "reasons_json": "[]",
                "features_json": json.dumps(features),
                "snapshot_id": None, "model_version": "champion-1.0.0-momentum-of",
                "shadow": 0, "executed": 0,
                "outcome_move_bps": 32.0 if label else -20.0,
                "outcome_cost_bps": 15.0,
                "outcome_label": label,
                "outcome_ts": 1_000_000.0 + i * 60_000.0 + 30_000,
                "outcome_horizon_s": 30.0,
            },
        )


def test_not_enough_data_is_reported_not_guessed(cfg: Config, repo: Repo) -> None:
    pipeline = LearningPipeline(cfg, repo)
    report = pipeline.run(champion_v1())
    assert report.status == "skipped"
    assert "labelled samples" in report.detail
    assert repo.model_versions() == []


def test_training_creates_a_challenger_and_leaves_the_champion_alone(
    cfg: Config, repo: Repo
) -> None:
    champion = champion_v1()
    repo.save_model_version(champion.to_row("champion"))
    before = repo.get_model(champion.version)
    seed_decisions(repo, 400)

    pipeline = LearningPipeline(cfg, repo)
    report = pipeline.run(champion)

    assert report.status in ("shadow", "rejected")
    assert report.challenger and report.challenger.startswith("challenger-")
    challenger_row = repo.get_model(report.challenger)
    assert challenger_row["status"] in ("shadow", "rejected")
    assert challenger_row["parent"] == champion.version

    after = repo.get_model(champion.version)
    assert after == before, "the champion row must be untouched by training"
    assert repo.champion()["version"] == champion.version
    # the challenger is trained, scored out of sample, and recorded
    assert report.cv and report.holdout_challenger and report.holdout_champion
    assert report.holdout_samples > 0
    events = [e for e in repo.model_events() if e["version"] == report.challenger]
    assert events and events[0]["event"] == "trained"


def test_the_holdout_is_never_used_for_fitting(cfg: Config, repo: Repo) -> None:
    """The holdout is the most recent slice, and it is not in the training pool."""
    seed_decisions(repo, 400)
    pipeline = LearningPipeline(cfg, repo)
    samples = pipeline.load_samples()
    split = int(len(samples) * (1.0 - cfg.learn.holdout_fraction))
    train, holdout = samples[:split], samples[split:]
    assert holdout[0].ts_ms > train[-1].ts_ms
    assert len(holdout) == len(samples) - split
    assert not {id(s) for s in train} & {id(s) for s in holdout}


def test_a_worse_challenger_is_rejected(cfg: Config, repo: Repo) -> None:
    pipeline = LearningPipeline(cfg, repo)
    strong = EvalResult(n=200, taken=100, auc=0.7, net_eur=50.0, expectancy_eur=0.5,
                        max_drawdown_eur=5.0, avg_loss_eur=-1.0, win_rate=0.6)
    weak = EvalResult(n=200, taken=100, auc=0.52, net_eur=10.0, expectancy_eur=0.1,
                      max_drawdown_eur=20.0, avg_loss_eur=-2.0, win_rate=0.9)
    failures = pipeline._gates(strong, weak)
    assert failures, "a challenger that earns less must not pass"
    assert any("net profit" in f for f in failures)
    assert any("drawdown" in f for f in failures)
    assert any("average loss" in f for f in failures)


def test_a_higher_win_rate_alone_does_not_promote(cfg: Config, repo: Repo) -> None:
    pipeline = LearningPipeline(cfg, repo)
    champion = EvalResult(n=200, taken=100, auc=0.65, net_eur=40.0, expectancy_eur=0.4,
                          max_drawdown_eur=6.0, avg_loss_eur=-1.0, win_rate=0.55)
    # wins far more often, earns far less: the classic overfit challenger
    challenger = EvalResult(n=200, taken=30, auc=0.66, net_eur=12.0, expectancy_eur=0.4,
                            max_drawdown_eur=6.0, avg_loss_eur=-1.0, win_rate=0.90)
    failures = pipeline._gates(champion, challenger)
    assert any("net profit" in f for f in failures)


def test_a_better_challenger_passes_the_holdout_gate(cfg: Config, repo: Repo) -> None:
    pipeline = LearningPipeline(cfg, repo)
    champion = EvalResult(n=200, taken=100, auc=0.60, net_eur=20.0, expectancy_eur=0.2,
                          max_drawdown_eur=10.0, avg_loss_eur=-1.0, win_rate=0.55)
    challenger = EvalResult(n=200, taken=110, auc=0.68, net_eur=40.0, expectancy_eur=0.36,
                            max_drawdown_eur=9.0, avg_loss_eur=-0.9, win_rate=0.57)
    assert pipeline._gates(champion, challenger) == []


def test_promotion_requires_live_shadow_evidence(cfg: Config, repo: Repo) -> None:
    champion = champion_v1()
    repo.save_model_version(champion.to_row("champion"))
    challenger = dataclasses.replace(champion, version="challenger-x", kind="challenger",
                                     parent=champion.version)
    repo.save_model_version(challenger.to_row("shadow"))

    pipeline = LearningPipeline(cfg, repo)
    promoted, detail = pipeline.maybe_promote(champion)
    assert promoted is None
    assert "shadow evidence not sufficient" in detail
    assert repo.champion()["version"] == champion.version


def test_promotion_and_rollback(cfg: Config, repo: Repo) -> None:
    small = dataclasses.replace(
        cfg, learn=dataclasses.replace(cfg.learn, shadow_min_decisions=5)
    )
    champion = champion_v1()
    repo.save_model_version(champion.to_row("champion"))
    challenger = dataclasses.replace(champion, version="challenger-x", kind="challenger",
                                     parent=champion.version)
    repo.save_model_version(challenger.to_row("shadow"))

    # live shadow decisions that actually paid
    for i in range(10):
        repo.db.insert("decisions", {
            "run_id": repo.db.run_id, "ts_ms": 1e6 + i, "symbol": "BTCUSDT",
            "action": "LONG", "side": "LONG", "quality": 0.7, "probability": 0.75,
            "expected_move_bps": 30.0, "expected_cost_eur": 0.5, "margin_eur": 50.0,
            "leverage": 10.0, "notional_eur": 500.0, "target_eur": 2.0,
            "max_loss_eur": 1.5, "max_hold_s": 60.0, "expectancy_eur": 0.5,
            "reason": "shadow", "reasons_json": "[]", "features_json": "{}",
            "snapshot_id": None, "model_version": "challenger-x", "shadow": 1,
            "executed": 0, "outcome_move_bps": 40.0, "outcome_cost_bps": 15.0,
            "outcome_label": 1, "outcome_ts": 1e6 + i + 30_000, "outcome_horizon_s": 30.0,
        })

    pipeline = LearningPipeline(small, repo)
    promoted, detail = pipeline.maybe_promote(champion)
    assert promoted is not None and detail == "promoted"
    assert promoted.version == "challenger-x"
    assert repo.champion()["version"] == "challenger-x"
    assert repo.get_model(champion.version)["status"] == "retired"
    assert repo.db.kv_get("champion_version") == "challenger-x"

    restored, detail = pipeline.rollback()
    assert restored is not None
    assert restored.version == champion.version
    assert repo.champion()["version"] == champion.version
    assert repo.get_model("challenger-x")["status"] == "rolled_back"
    # every version is kept: history is never destroyed
    versions = {row["version"] for row in repo.model_versions()}
    assert {champion.version, "challenger-x"} <= versions


def test_a_model_trained_on_other_features_is_refused(repo: Repo) -> None:
    row = champion_v1().to_row("champion")
    params = json.loads(row["params_json"])
    params["features"] = ["something", "else"]
    row["params_json"] = json.dumps(params)
    with pytest.raises(ValueError) as exc:
        Model.from_row(row)
    assert "different feature set" in str(exc.value)


def test_model_round_trips_through_the_database(repo: Repo) -> None:
    champion = champion_v1()
    repo.save_model_version(champion.to_row("champion"))
    loaded = Model.from_row(repo.champion())
    assert loaded.version == champion.version
    assert loaded.weights == champion.weights
    assert loaded.bias == champion.bias
