"""Model zoo.

Deliberately shallow: at a five second horizon the signal-to-noise ratio is
brutal, and a deep network trained on a few hours of ticks will memorise the
session, not the market. A neural network is only justified if the tree
ensembles show a real, stable out-of-sample edge first.
"""

from __future__ import annotations

from typing import Any, Callable

import numpy as np
from sklearn.ensemble import GradientBoostingClassifier, RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

ModelFactory = Callable[[], Any]


def logistic() -> Pipeline:
    return Pipeline(
        [
            ("scaler", StandardScaler()),
            (
                "clf",
                LogisticRegression(
                    max_iter=2000, C=0.1, solver="lbfgs", class_weight="balanced"
                ),
            ),
        ]
    )


def random_forest() -> RandomForestClassifier:
    return RandomForestClassifier(
        n_estimators=300,
        max_depth=6,  # shallow on purpose: depth is where overfitting lives
        min_samples_leaf=50,
        max_features="sqrt",
        class_weight="balanced_subsample",
        n_jobs=-1,
        random_state=7,
    )


def gradient_boosting() -> GradientBoostingClassifier:
    return GradientBoostingClassifier(
        n_estimators=150, learning_rate=0.03, max_depth=3, subsample=0.8,
        random_state=7,
    )


def xgboost_model() -> Any:
    from xgboost import XGBClassifier

    return XGBClassifier(
        n_estimators=300,
        max_depth=4,
        learning_rate=0.03,
        subsample=0.8,
        colsample_bytree=0.8,
        reg_lambda=2.0,
        min_child_weight=20,
        eval_metric="logloss",
        tree_method="hist",
        n_jobs=-1,
        random_state=7,
    )


def lightgbm_model() -> Any:
    from lightgbm import LGBMClassifier

    return LGBMClassifier(
        n_estimators=300,
        max_depth=5,
        num_leaves=15,
        learning_rate=0.03,
        subsample=0.8,
        colsample_bytree=0.8,
        min_child_samples=50,
        reg_lambda=2.0,
        n_jobs=-1,
        random_state=7,
        verbose=-1,
    )


REGISTRY: dict[str, ModelFactory] = {
    "logistic_regression": logistic,
    "random_forest": random_forest,
    "gradient_boosting": gradient_boosting,
    "xgboost": xgboost_model,
    "lightgbm": lightgbm_model,
}

DEFAULT_MODELS = [
    "logistic_regression", "random_forest", "gradient_boosting", "xgboost", "lightgbm",
]


def build(name: str) -> Any:
    if name not in REGISTRY:
        raise ValueError(f"unknown model '{name}'. Available: {sorted(REGISTRY)}")
    return REGISTRY[name]()


def available() -> list[str]:
    out = []
    for name in REGISTRY:
        try:
            build(name)
            out.append(name)
        except Exception:  # noqa: BLE001 - optional dependency missing
            continue
    return out


def feature_importance(model: Any, names: list[str]) -> dict[str, float]:
    """Best-effort importance, normalised to sum to 1."""
    values: np.ndarray | None = None
    if hasattr(model, "feature_importances_"):
        values = np.asarray(model.feature_importances_, dtype=float)
    elif isinstance(model, Pipeline) and hasattr(model[-1], "coef_"):
        values = np.abs(np.asarray(model[-1].coef_, dtype=float)).ravel()
    elif hasattr(model, "coef_"):
        values = np.abs(np.asarray(model.coef_, dtype=float)).ravel()
    if values is None or len(values) != len(names):
        return {}
    total = float(values.sum())
    if total <= 0:
        return {}
    pairs = sorted(zip(names, values / total), key=lambda kv: -kv[1])
    return {k: round(float(v), 5) for k, v in pairs[:25]}
