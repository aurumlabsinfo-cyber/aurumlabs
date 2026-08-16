"""Walk-forward validation, leakage detection and edge classification.

The most valuable test in this file is `test_pure_noise_is_never_declared_an_edge`:
a system that cannot say "no edge here" is worse than useless.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from app.ml.dataset import Dataset, build_dataset
from app.ml.strategies import evaluate_strategy
from app.ml.validation import (
    EDGE_FAILED,
    EDGE_INCONCLUSIVE,
    EDGE_OVERFIT,
    EDGE_PROVEN,
    classify_edge,
    leakage_checks,
    run_walk_forward,
    walk_forward_splits,
)


def make_dataset(n=3000, informative=False, leak=False, seed=3) -> Dataset:
    rng = np.random.default_rng(seed)
    ts = pd.Series(np.arange(n, dtype=np.int64) * 100 + 1_700_000_000_000)
    y = pd.Series(rng.integers(0, 2, n))
    data = {f"feat_{i}": rng.normal(size=n) for i in range(8)}
    if informative:
        # A genuinely predictive feature (a luxury real markets rarely offer).
        data["feat_0"] = y.to_numpy() * 1.2 + rng.normal(scale=1.0, size=n)
    if leak:
        data["feat_leak"] = y.to_numpy().astype(float)  # the label itself
    X = pd.DataFrame(data)
    entry = pd.Series(np.full(n, 100_000.0))
    exit_ = entry + np.where(y == 1, 1.0, -1.0)
    return Dataset(X=X, y=y, ts=ts, entry=entry, exit=exit_, horizon_s=5.0)


# ------------------------------------------------------------------- splits
def test_splits_are_chronological_and_purged():
    ds = make_dataset(4000)
    splits = list(
        walk_forward_splits(ds.ts.to_numpy(), n_splits=4, horizon_s=5.0, embargo_s=30.0)
    )
    assert splits
    for s in splits:
        assert s.train_end_ts < s.test_start_ts
        # Purge gap must cover horizon + embargo (35s = 35_000ms).
        assert s.test_start_ts - s.train_end_ts >= 35_000
        assert s.train_idx.max() < s.test_idx.min()


def test_splits_move_forward_in_time():
    ds = make_dataset(6000)
    splits = list(walk_forward_splits(ds.ts.to_numpy(), n_splits=5, horizon_s=5.0))
    starts = [s.test_start_ts for s in splits]
    assert starts == sorted(starts)
    assert len(set(starts)) == len(starts)


def test_no_splits_when_data_is_too_short():
    ds = make_dataset(100)
    assert list(walk_forward_splits(ds.ts.to_numpy(), n_splits=5)) == []


# ---------------------------------------------------------------- leakage
def test_leakage_checks_pass_on_clean_data():
    checks = leakage_checks(make_dataset(1500))
    assert checks["timestamps_monotonic"]["pass"] is True
    assert checks["duplicate_timestamps"]["pass"] is True
    assert checks["single_feature_separation"]["pass"] is True
    assert checks["shuffled_label_control"]["pass"] is True


def test_leakage_checks_catch_a_leaked_label():
    checks = leakage_checks(make_dataset(1500, leak=True))
    assert checks["single_feature_separation"]["pass"] is False
    assert any(
        s["feature"] == "feat_leak"
        for s in checks["single_feature_separation"]["suspicious"]
    )
    assert checks["all_passed"] is False


def test_leakage_checks_flag_forward_looking_feature_names():
    ds = make_dataset(600)
    ds.X["future_return"] = 0.0
    checks = leakage_checks(ds)
    assert checks["no_future_named_features"]["pass"] is False


# ---------------------------------------------------------- walk-forward
def test_walk_forward_on_noise_lands_near_chance():
    res = run_walk_forward(make_dataset(4000), "logistic_regression", n_splits=4)
    assert res.error is None
    acc = res.oos_metrics["accuracy"]
    assert 0.4 < acc < 0.6
    ci = res.oos_metrics["accuracy_ci95"]
    assert ci[0] <= 0.5 <= ci[1]


def test_walk_forward_finds_a_genuinely_informative_feature():
    res = run_walk_forward(
        make_dataset(4000, informative=True), "logistic_regression", n_splits=4
    )
    assert res.oos_metrics["accuracy"] > 0.6
    assert res.oos_metrics["accuracy_ci95"][0] > 0.5


def test_walk_forward_reports_insufficient_data_instead_of_guessing():
    res = run_walk_forward(make_dataset(200), "logistic_regression")
    assert res.error is not None
    assert "not enough data" in res.error


# ------------------------------------------------------------------- edge
def test_pure_noise_is_never_declared_an_edge():
    ds = make_dataset(6000)
    res = run_walk_forward(ds, "logistic_regression", n_splits=4)
    verdict = classify_edge(res, leakage_checks(ds), payout=0.8, min_rows=500)
    assert verdict["classification"] in (
        EDGE_INCONCLUSIVE, EDGE_FAILED, EDGE_OVERFIT
    )
    assert "NO ROBUST EDGE" in verdict["conclusion"]


def test_leaking_data_is_classified_as_failed_not_proven():
    ds = make_dataset(4000, leak=True)
    res = run_walk_forward(ds, "logistic_regression", n_splits=4)
    # The model scores ~100% - and the verdict must still refuse it.
    assert res.oos_metrics["accuracy"] > 0.95
    verdict = classify_edge(res, leakage_checks(ds), payout=0.8, min_rows=500)
    assert verdict["classification"] == EDGE_FAILED
    assert "leakage" in " ".join(verdict["notes"]).lower()


def test_insufficient_rows_gives_inconclusive_not_a_verdict():
    ds = make_dataset(4000, informative=True)
    res = run_walk_forward(ds, "logistic_regression", n_splits=4)
    verdict = classify_edge(res, leakage_checks(ds), payout=0.8, min_rows=1_000_000)
    assert verdict["classification"] == EDGE_INCONCLUSIVE


def test_a_real_signal_can_be_proven_when_everything_lines_up():
    ds = make_dataset(8000, informative=True)
    res = run_walk_forward(ds, "logistic_regression", n_splits=5)
    verdict = classify_edge(res, leakage_checks(ds), payout=0.8, min_rows=500)
    assert verdict["classification"] == EDGE_PROVEN


# -------------------------------------------------------------- strategies
def test_rule_strategy_evaluates_out_of_sample():
    ds = make_dataset(4000)
    ds.X["depth_imbalance_5"] = np.random.default_rng(1).normal(size=len(ds))
    ds.X["book_imbalance_l1"] = ds.X["depth_imbalance_5"]
    out = evaluate_strategy(ds, "order_book_imbalance", n_splits=4, payout=0.8)
    assert out["n"] > 0
    assert 0.3 < out["win_rate"] < 0.7  # noise in, noise out
    assert out["beats_chance"] is False


def test_strategy_declines_when_its_inputs_are_missing():
    ds = make_dataset(4000)
    out = evaluate_strategy(ds, "liquidation_pressure", n_splits=4)
    # No futures feed => no liquidation features => the strategy must not trade.
    assert out.get("n", 0) == 0


# --------------------------------------------------------------- dataset
def test_build_dataset_labels_come_from_the_future_only():
    base = 1_700_000_000_000
    features = pd.DataFrame(
        [
            {"ts": base + i * 100, "mid": 100.0, "book_synced": True,
             "data_quality": 1.0, "is_synthetic": False, "feat_a": float(i)}
            for i in range(100)
        ]
    )
    # Price steps up permanently after 5 seconds.
    ticks = pd.DataFrame(
        [
            {"ts": base + i * 100, "mid": 100.0 if i < 50 else 101.0}
            for i in range(200)
        ]
    )
    ds = build_dataset(features, ticks, horizon_s=5.0)
    assert len(ds) > 0
    # Every row whose t+5s lands after the step must be labelled UP.
    assert ds.y.sum() > 0
    assert (ds.exit >= ds.entry).all()
    assert "ts" not in ds.X.columns
    assert "mid" not in ds.X.columns


def test_build_dataset_drops_rows_without_a_future_tick():
    base = 1_700_000_000_000
    features = pd.DataFrame(
        [{"ts": base + i * 100, "mid": 100.0, "book_synced": True,
          "data_quality": 1.0, "is_synthetic": False, "feat_a": 1.0}
         for i in range(50)]
    )
    ticks = pd.DataFrame([{"ts": base + i * 100, "mid": 100.0} for i in range(50)])
    ds = build_dataset(features, ticks, horizon_s=5.0)
    # No tick exists 5 seconds after any feature row: nothing is labelled.
    assert len(ds) == 0
    assert ds.meta["dropped_no_future_tick"] == 50


def test_build_dataset_excludes_desynced_book_rows():
    base = 1_700_000_000_000
    features = pd.DataFrame(
        [{"ts": base + i * 100, "mid": 100.0 + i * 0.01, "book_synced": i % 2 == 0,
          "data_quality": 1.0, "is_synthetic": False, "feat_a": float(i)}
         for i in range(200)]
    )
    ticks = pd.DataFrame(
        [{"ts": base + i * 100, "mid": 100.0 + i * 0.01} for i in range(300)]
    )
    ds = build_dataset(features, ticks, horizon_s=5.0, require_book_synced=True)
    assert len(ds) > 0
    assert len(ds) <= 100
