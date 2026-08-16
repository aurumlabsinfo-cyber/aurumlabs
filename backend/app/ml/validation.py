"""Time-series validation, leakage detection and edge classification.

No random shuffling anywhere. Splits are strictly chronological, with a purge
gap between train and test equal to the label horizon plus an embargo, so a
training row's *label window* can never overlap a test row's features.

    |........ train ........|== purge ==|.... test ....|
                             ^ horizon + embargo
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Any, Iterator, Sequence

import numpy as np
import pandas as pd
from sklearn.metrics import (
    accuracy_score,
    brier_score_loss,
    log_loss,
    roc_auc_score,
)

from app.ml.dataset import Dataset, impute
from app.ml.models import build, feature_importance
from app.signals.statistics import (
    binomial_p_value,
    break_even_win_rate,
    expected_value_per_trade,
    wilson_interval,
)


# --------------------------------------------------------------------- splits
@dataclass
class Split:
    fold: int
    train_idx: np.ndarray
    test_idx: np.ndarray
    train_start_ts: int
    train_end_ts: int
    test_start_ts: int
    test_end_ts: int
    purged: int


def walk_forward_splits(
    ts: Sequence[int],
    n_splits: int = 5,
    horizon_s: float = 5.0,
    embargo_s: float = 30.0,
    expanding: bool = True,
    min_train: int = 500,
) -> Iterator[Split]:
    """Chronological folds with a purge gap. Train always precedes test."""
    ts_arr = np.asarray(ts, dtype=np.int64)
    n = len(ts_arr)
    if n < min_train + n_splits * 50:
        return
    purge_ms = int((horizon_s + embargo_s) * 1000)

    test_size = (n - min_train) // n_splits
    if test_size < 30:
        return

    for fold in range(n_splits):
        test_start = min_train + fold * test_size
        test_end = test_start + test_size if fold < n_splits - 1 else n
        if test_end - test_start < 30:
            continue
        test_start_ts = int(ts_arr[test_start])
        # Purge: drop training rows whose label window reaches into the test set.
        train_cutoff_ts = test_start_ts - purge_ms
        train_mask = ts_arr < train_cutoff_ts
        train_idx = np.flatnonzero(train_mask)
        if not expanding:
            window = test_size * 3
            train_idx = train_idx[-window:] if len(train_idx) > window else train_idx
        if len(train_idx) < min_train:
            continue
        test_idx = np.arange(test_start, test_end)
        yield Split(
            fold=fold,
            train_idx=train_idx,
            test_idx=test_idx,
            train_start_ts=int(ts_arr[train_idx[0]]),
            train_end_ts=int(ts_arr[train_idx[-1]]),
            test_start_ts=test_start_ts,
            test_end_ts=int(ts_arr[test_end - 1]),
            purged=int(test_start - len(train_idx)),
        )


# -------------------------------------------------------------------- metrics
def classification_metrics(
    y_true: np.ndarray, proba: np.ndarray, threshold: float = 0.5
) -> dict[str, Any]:
    pred = (proba >= threshold).astype(int)
    n = len(y_true)
    out: dict[str, Any] = {
        "n": int(n),
        "accuracy": round(float(accuracy_score(y_true, pred)), 5) if n else None,
        "base_rate_up": round(float(np.mean(y_true)), 5) if n else None,
    }
    try:
        out["auc"] = round(float(roc_auc_score(y_true, proba)), 5)
    except ValueError:
        out["auc"] = None
    try:
        out["log_loss"] = round(float(log_loss(y_true, np.clip(proba, 1e-6, 1 - 1e-6))), 5)
    except ValueError:
        out["log_loss"] = None
    try:
        out["brier"] = round(float(brier_score_loss(y_true, proba)), 5)
    except ValueError:
        out["brier"] = None
    correct = int((pred == y_true).sum())
    lo, hi = wilson_interval(correct, n)
    out["accuracy_ci95"] = [round(lo, 5), round(hi, 5)] if n else None
    out["p_value_vs_coinflip"] = (
        round(binomial_p_value(correct, n) or 1.0, 6) if n else None
    )
    return out


def selective_metrics(
    y_true: np.ndarray,
    proba: np.ndarray,
    thresholds: Sequence[float] = (0.5, 0.55, 0.6, 0.65, 0.7, 0.8),
    payout: float | None = None,
) -> list[dict[str, Any]]:
    """Accuracy when the model is only allowed to act above a confidence level.

    This is what actually matters for a NO-TRADE-capable system: it can decline.
    """
    out = []
    for th in thresholds:
        conf = np.maximum(proba, 1 - proba)
        mask = conf >= th
        n = int(mask.sum())
        if n == 0:
            out.append({"threshold": th, "n": 0, "coverage": 0.0})
            continue
        pred = (proba[mask] >= 0.5).astype(int)
        correct = int((pred == y_true[mask]).sum())
        wr = correct / n
        lo, hi = wilson_interval(correct, n)
        out.append(
            {
                "threshold": th,
                "n": n,
                "coverage": round(n / len(proba), 4),
                "win_rate": round(wr, 5),
                "ci95": [round(lo, 5), round(hi, 5)],
                "p_value": round(binomial_p_value(correct, n) or 1.0, 6),
                "expected_value": (
                    round(expected_value_per_trade(wr, payout), 5)
                    if payout is not None else None
                ),
                "beats_break_even": (
                    bool(wr > (break_even_win_rate(payout) or 1.0))
                    if payout is not None else None
                ),
            }
        )
    return out


# ------------------------------------------------------------- walk-forward
@dataclass
class WalkForwardResult:
    model: str
    horizon_s: float
    folds: list[dict[str, Any]] = field(default_factory=list)
    oos_metrics: dict[str, Any] = field(default_factory=dict)
    selective: list[dict[str, Any]] = field(default_factory=list)
    train_metrics: dict[str, Any] = field(default_factory=dict)
    importance: dict[str, float] = field(default_factory=dict)
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "model": self.model,
            "horizon_s": self.horizon_s,
            "folds": self.folds,
            "out_of_sample": self.oos_metrics,
            "in_sample": self.train_metrics,
            "selective_thresholds": self.selective,
            "feature_importance": self.importance,
            "error": self.error,
        }


def run_walk_forward(
    ds: Dataset,
    model_name: str,
    n_splits: int = 5,
    embargo_s: float = 30.0,
    payout: float | None = None,
    expanding: bool = True,
) -> WalkForwardResult:
    res = WalkForwardResult(model=model_name, horizon_s=ds.horizon_s)
    X = impute(ds.X)
    y = ds.y.to_numpy()
    ts = ds.ts.to_numpy()

    splits = list(
        walk_forward_splits(
            ts, n_splits=n_splits, horizon_s=ds.horizon_s, embargo_s=embargo_s,
            expanding=expanding,
        )
    )
    if not splits:
        res.error = (
            f"not enough data for walk-forward validation "
            f"({len(ds)} rows). Collect more before drawing conclusions."
        )
        return res

    all_true: list[np.ndarray] = []
    all_proba: list[np.ndarray] = []
    train_true: list[np.ndarray] = []
    train_proba: list[np.ndarray] = []
    last_model = None

    for split in splits:
        Xtr = X.iloc[split.train_idx]
        ytr = y[split.train_idx]
        Xte = X.iloc[split.test_idx]
        yte = y[split.test_idx]
        if len(np.unique(ytr)) < 2:
            continue
        model = build(model_name)
        model.fit(Xtr, ytr)
        last_model = model
        p_te = _proba(model, Xte)
        p_tr = _proba(model, Xtr)
        all_true.append(yte)
        all_proba.append(p_te)
        train_true.append(ytr)
        train_proba.append(p_tr)
        fold_metrics = classification_metrics(yte, p_te)
        fold_metrics.update(
            {
                "fold": split.fold,
                "train_rows": int(len(split.train_idx)),
                "test_rows": int(len(split.test_idx)),
                "train_start_ts": split.train_start_ts,
                "train_end_ts": split.train_end_ts,
                "test_start_ts": split.test_start_ts,
                "test_end_ts": split.test_end_ts,
                "purge_gap_ms": split.test_start_ts - split.train_end_ts,
                "train_accuracy": round(
                    float(accuracy_score(ytr, (p_tr >= 0.5).astype(int))), 5
                ),
            }
        )
        res.folds.append(fold_metrics)

    if not all_true:
        res.error = "every fold was degenerate (single-class training window)"
        return res

    y_oos = np.concatenate(all_true)
    p_oos = np.concatenate(all_proba)
    res.oos_metrics = classification_metrics(y_oos, p_oos)
    res.oos_metrics["folds"] = len(res.folds)
    res.oos_metrics["folds_above_50"] = sum(
        1 for f in res.folds if (f.get("accuracy") or 0) > 0.5
    )
    res.train_metrics = classification_metrics(
        np.concatenate(train_true), np.concatenate(train_proba)
    )
    res.selective = selective_metrics(y_oos, p_oos, payout=payout)
    if last_model is not None:
        res.importance = feature_importance(last_model, list(X.columns))
    return res


def _proba(model: Any, X: pd.DataFrame) -> np.ndarray:
    p = model.predict_proba(X)
    return np.asarray(p[:, 1], dtype=float)


# -------------------------------------------------------------- leakage tests
def leakage_checks(ds: Dataset, model_name: str = "logistic_regression") -> dict[str, Any]:
    """Structural and empirical checks for look-ahead and leakage.

    A model that scores well here on *shuffled labels* is reading the future.
    """
    checks: dict[str, Any] = {}
    ts = ds.ts.to_numpy()

    checks["timestamps_monotonic"] = {
        "pass": bool(np.all(np.diff(ts) >= 0)),
        "detail": "feature rows must be chronologically ordered",
    }
    dupes = int(len(ts) - len(np.unique(ts)))
    checks["duplicate_timestamps"] = {
        "pass": dupes == 0, "count": dupes,
        "detail": "duplicate timestamps can place the same instant on both sides "
                  "of a split",
    }
    checks["label_uses_future_only"] = {
        "pass": bool((ds.exit.to_numpy() != ds.entry.to_numpy()).all()),
        "detail": (
            "labels are built from mid(t+h) resolved from the tick table; ties "
            "are dropped, so entry == exit must never appear"
        ),
    }
    checks["no_future_named_features"] = {
        "pass": not [
            c for c in ds.X.columns
            if any(k in c.lower() for k in ("future", "next", "label", "target", "exit"))
        ],
        "detail": "no feature name suggests forward information",
    }

    # Empirical control: shuffled labels must score at chance.
    shuffled = None
    if len(ds) >= 800:
        rng = np.random.default_rng(11)
        y_shuf = rng.permutation(ds.y.to_numpy())
        ds_shuf = Dataset(
            X=ds.X, y=pd.Series(y_shuf), ts=ds.ts, entry=ds.entry, exit=ds.exit,
            horizon_s=ds.horizon_s,
        )
        r = run_walk_forward(ds_shuf, model_name, n_splits=3)
        acc = (r.oos_metrics or {}).get("accuracy")
        ci = (r.oos_metrics or {}).get("accuracy_ci95") or [0, 1]
        shuffled = {
            "accuracy": acc,
            "ci95": ci,
            # Chance must sit inside the interval.
            "pass": bool(acc is None or (ci[0] <= 0.5 <= ci[1])),
            "detail": (
                "a model trained on randomly permuted labels must not beat a "
                "coin flip out of sample; if it does, information is leaking"
            ),
        }
    checks["shuffled_label_control"] = shuffled or {
        "pass": None, "detail": "needs >= 800 rows to run"
    }

    # Any single feature that alone separates the label is almost certainly a leak.
    suspicious: list[dict[str, Any]] = []
    if len(ds) >= 200:
        y = ds.y.to_numpy()
        for col in ds.X.columns:
            v = ds.X[col].to_numpy(dtype=float)
            if not np.isfinite(v).all() or np.nanstd(v) == 0:
                continue
            try:
                auc = float(roc_auc_score(y, v))
            except ValueError:
                continue
            if auc > 0.9 or auc < 0.1:
                suspicious.append({"feature": col, "univariate_auc": round(auc, 4)})
    checks["single_feature_separation"] = {
        "pass": not suspicious,
        "suspicious": suspicious,
        "detail": "a lone feature with AUC > 0.9 at a 5s horizon is a leak, not alpha",
    }

    checks["all_passed"] = all(
        c.get("pass") is not False for c in checks.values() if isinstance(c, dict)
    )
    return checks


# ------------------------------------------------------------ edge verdict
EDGE_PROVEN = "PROVEN EDGE"
EDGE_PROMISING = "PROMISING"
EDGE_INCONCLUSIVE = "INCONCLUSIVE"
EDGE_OVERFIT = "OVERFIT"
EDGE_FAILED = "FAILED"


def classify_edge(
    result: WalkForwardResult,
    leakage: dict[str, Any],
    payout: float | None = None,
    min_rows: int = 5000,
) -> dict[str, Any]:
    """Turn validation output into one of five honest verdicts."""
    oos = result.oos_metrics or {}
    train = result.train_metrics or {}
    n = int(oos.get("n") or 0)
    acc = oos.get("accuracy")
    ci = oos.get("accuracy_ci95") or [0.0, 1.0]
    p = oos.get("p_value_vs_coinflip")
    folds = result.folds or []
    above = sum(1 for f in folds if (f.get("accuracy") or 0) > 0.5)
    train_acc = train.get("accuracy")
    notes: list[str] = []

    if result.error:
        return _verdict(EDGE_INCONCLUSIVE, [result.error], oos)
    if leakage.get("all_passed") is False:
        failed = [k for k, v in leakage.items()
                  if isinstance(v, dict) and v.get("pass") is False]
        return _verdict(
            EDGE_FAILED,
            [f"leakage checks failed: {', '.join(failed)} - results are not valid"],
            oos,
        )
    if n < min_rows:
        notes.append(
            f"only {n} out-of-sample rows (want >= {min_rows}); no verdict is "
            "reliable at this size"
        )
        return _verdict(EDGE_INCONCLUSIVE, notes, oos)
    if acc is None:
        return _verdict(EDGE_INCONCLUSIVE, ["no out-of-sample accuracy"], oos)

    overfit_gap = (train_acc - acc) if (train_acc is not None) else 0.0
    if overfit_gap > 0.08 and ci[0] <= 0.5:
        notes.append(
            f"in-sample {train_acc:.3f} vs out-of-sample {acc:.3f} "
            f"(gap {overfit_gap:.3f}) with chance inside the confidence interval"
        )
        return _verdict(EDGE_OVERFIT, notes, oos)
    if ci[1] < 0.5:
        notes.append(f"out-of-sample accuracy {acc:.4f} is significantly *below* chance")
        return _verdict(EDGE_FAILED, notes, oos)
    if ci[0] <= 0.5:
        notes.append(
            f"out-of-sample accuracy {acc:.4f}, 95% CI [{ci[0]:.4f}, {ci[1]:.4f}] "
            "includes 0.5 - indistinguishable from a coin flip"
        )
        return _verdict(EDGE_INCONCLUSIVE, notes, oos)

    consistent = len(folds) >= 3 and above >= math.ceil(0.75 * len(folds))
    significant = p is not None and p < 0.01
    be = break_even_win_rate(payout)
    best_selective = max(
        (s for s in result.selective if s.get("n", 0) >= 100),
        key=lambda s: s.get("win_rate", 0),
        default=None,
    )
    profitable = None
    if be is not None and best_selective:
        profitable = best_selective.get("win_rate", 0) > be
        notes.append(
            f"break-even win rate at payout {payout}: {be:.4f}; best selective "
            f"win rate {best_selective['win_rate']:.4f} "
            f"({'above' if profitable else 'below'} break-even)"
        )
    elif be is None:
        notes.append(
            "PAYOUT UNKNOWN - statistical edge can be assessed, profitability "
            "cannot. Set BINARY_PAYOUT to evaluate expected value."
        )

    if consistent and significant and overfit_gap <= 0.06 and profitable is not False:
        notes.append(
            f"out-of-sample accuracy {acc:.4f} (CI [{ci[0]:.4f}, {ci[1]:.4f}]), "
            f"p={p:.2g}, {above}/{len(folds)} folds above chance"
        )
        return _verdict(EDGE_PROVEN, notes, oos)

    notes.append(
        f"out-of-sample accuracy {acc:.4f} above chance but "
        f"{'inconsistent across folds' if not consistent else ''}"
        f"{' / weak significance' if not significant else ''}"
        f"{' / fails break-even' if profitable is False else ''}".strip(" /")
    )
    return _verdict(EDGE_PROMISING, notes, oos)


def _verdict(label: str, notes: list[str], oos: dict[str, Any]) -> dict[str, Any]:
    return {
        "classification": label,
        "notes": notes,
        "out_of_sample": oos,
        "conclusion": (
            "NESSUN EDGE ROBUSTO IDENTIFICATO / NO ROBUST EDGE IDENTIFIED"
            if label in (EDGE_FAILED, EDGE_INCONCLUSIVE, EDGE_OVERFIT)
            else f"Edge classification: {label}"
        ),
    }
