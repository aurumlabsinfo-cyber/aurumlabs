"""Live model inference with an out-of-distribution guard.

A model is only allowed to speak about market states that resemble the ones it
was fitted on. When the current feature vector sits far outside the training
distribution, `predict` reports `out_of_distribution` and the decision engine
turns that into a NO TRADE - which is the correct answer, not a limitation.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import numpy as np

from app.core.logging_conf import get_logger

log = get_logger(__name__)


class ModelProvider:
    def __init__(self, model_dir: str, model_id: str | None = None) -> None:
        self.model_dir = Path(model_dir)
        self.model_id = model_id
        self.model: Any = None
        self.feature_names: list[str] = []
        self.stats: dict[str, dict[str, float]] = {}
        self.calibrated = False
        self.horizon_s: float | None = None
        self.metadata: dict[str, Any] = {}
        self.load_error: str | None = None
        self.ood_z_threshold = 6.0
        self.ood_max_features = 3

        if model_id:
            self.load(model_id)

    # ------------------------------------------------------------------ load
    def load(self, model_id: str) -> bool:
        path = self.model_dir / f"{model_id}.joblib"
        try:
            import joblib

            bundle = joblib.load(path)
            self.model = bundle["model"]
            self.feature_names = list(bundle["feature_names"])
            self.stats = bundle.get("feature_stats", {})
            self.calibrated = bool(bundle.get("calibrated", False))
            self.horizon_s = bundle.get("horizon_s")
            self.metadata = bundle.get("metadata", {})
            self.model_id = model_id
            self.load_error = None
            log.info(
                "model.loaded", model_id=model_id, features=len(self.feature_names),
                calibrated=self.calibrated,
            )
            return True
        except Exception as exc:  # noqa: BLE001
            self.load_error = f"{type(exc).__name__}: {exc}"
            self.model = None
            log.warning("model.load_failed", model_id=model_id, error=self.load_error)
            return False

    def unload(self) -> None:
        self.model = None
        self.model_id = None
        self.feature_names = []

    def is_ready(self) -> bool:
        return self.model is not None and bool(self.feature_names)

    # --------------------------------------------------------------- predict
    def predict(self, features: dict[str, Any]) -> dict[str, Any] | None:
        if not self.is_ready():
            return None
        row = []
        missing: list[str] = []
        for name in self.feature_names:
            v = features.get(name)
            if v is None or (isinstance(v, float) and not np.isfinite(v)):
                missing.append(name)
                row.append(0.0)
            else:
                row.append(float(v))
        # Too many blanks means the engine has not warmed up for this model.
        if len(missing) > max(2, len(self.feature_names) // 10):
            return {
                "out_of_distribution": True,
                "ood_reason": f"{len(missing)} features unavailable",
                "model_id": self.model_id,
            }

        ood, reason = self._check_distribution(features)
        if ood:
            return {
                "out_of_distribution": True,
                "ood_reason": reason,
                "model_id": self.model_id,
            }
        try:
            X = np.asarray([row], dtype=float)
            prob = float(self.model.predict_proba(X)[0, 1])
        except Exception as exc:  # noqa: BLE001
            log.warning("model.predict_failed", error=str(exc))
            return None
        return {
            "prob_up": prob,
            "model_id": self.model_id,
            "calibrated": self.calibrated,
            "out_of_distribution": False,
            "missing_features": missing,
        }

    def _check_distribution(self, features: dict[str, Any]) -> tuple[bool, str]:
        if not self.stats:
            return (False, "")
        offenders: list[str] = []
        for name, st in self.stats.items():
            v = features.get(name)
            if v is None:
                continue
            sd = st.get("std") or 0.0
            if sd <= 0:
                continue
            z = abs((float(v) - st.get("mean", 0.0)) / sd)
            if z > self.ood_z_threshold:
                offenders.append(f"{name} z={z:.1f}")
        if len(offenders) > self.ood_max_features:
            return (True, "; ".join(offenders[:5]))
        return (False, "")

    def info(self) -> dict[str, Any]:
        return {
            "model_id": self.model_id,
            "ready": self.is_ready(),
            "calibrated": self.calibrated,
            "horizon_s": self.horizon_s,
            "feature_count": len(self.feature_names),
            "load_error": self.load_error,
            "ood_z_threshold": self.ood_z_threshold,
            "metadata": self.metadata,
        }


def save_model(
    model_dir: str,
    model_id: str,
    model: Any,
    feature_names: list[str],
    feature_stats: dict[str, dict[str, float]],
    horizon_s: float,
    calibrated: bool,
    metadata: dict[str, Any],
) -> str:
    import joblib

    os.makedirs(model_dir, exist_ok=True)
    path = Path(model_dir) / f"{model_id}.joblib"
    joblib.dump(
        {
            "model": model,
            "feature_names": feature_names,
            "feature_stats": feature_stats,
            "horizon_s": horizon_s,
            "calibrated": calibrated,
            "metadata": metadata,
        },
        path,
    )
    return str(path)
