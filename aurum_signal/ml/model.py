"""Il modello: interfaccia unica, implementazioni intercambiabili.

Scelta deliberata: si comincia da una regressione logistica. Non perche' sia
elegante, ma perche' su un problema con poche migliaia di righe, rumore
dominante e una probabilita' vera vicina al 50%, un modello semplice e
regolarizzato perde meno di uno complesso — e soprattutto si puo' capire
perche' sbaglia. Alberi e boosting sono disponibili quando scikit-learn c'e';
le reti profonde non sono previste, perche' aggiungerebbero capacita' dove il
problema non ha bisogno di capacita' ma di dati puliti.

Il modello vive senza scikit-learn: l'implementazione di riserva e' pura
libreria standard, cosi' il sistema si installa e si prova ovunque.
"""

from __future__ import annotations

import json
import math
import random
import time
from dataclasses import dataclass, field
from typing import Any, Sequence

try:                                            # pragma: no cover - opzionale
    import numpy as np
    from sklearn.ensemble import HistGradientBoostingClassifier, RandomForestClassifier
    from sklearn.linear_model import LogisticRegression, SGDClassifier
    SKLEARN = True
except Exception:                               # pragma: no cover - opzionale
    SKLEARN = False

ALGORITHMS = ("logistic", "sgd", "random_forest", "hist_gradient_boosting")


def _standardise(rows: list[list[float]]) -> tuple[list[float], list[float]]:
    n = len(rows)
    cols = len(rows[0]) if rows else 0
    mean = [0.0] * cols
    std = [1.0] * cols
    for j in range(cols):
        vals = [r[j] for r in rows if math.isfinite(r[j])]
        if not vals:
            continue
        m = sum(vals) / len(vals)
        var = sum((v - m) ** 2 for v in vals) / max(1, len(vals) - 1)
        mean[j] = m
        std[j] = math.sqrt(var) if var > 1e-12 else 1.0
    return mean, std


class PureLogistic:
    """Regressione logistica a discesa stocastica, senza dipendenze.

    I valori mancanti sono imputati con la MEDIA DI ADDESTRAMENTO, la stessa
    in addestramento e in produzione. Usare zero in un caso e la media
    nell'altro e' un errore silenzioso: il modello imparerebbe una regolarita'
    che al momento della predizione non esiste piu'.
    """

    def __init__(self, epochs: int = 24, lr: float = 0.08, l2: float = 1e-4,
                 seed: int = 7) -> None:
        self.epochs, self.lr, self.l2, self.seed = epochs, lr, l2, seed
        self.w: list[float] = []
        self.b: float = 0.0
        self.mean: list[float] = []
        self.std: list[float] = []

    def _prep(self, row: Sequence[float]) -> list[float]:
        return [((row[j] if math.isfinite(row[j]) else self.mean[j]) - self.mean[j])
                / self.std[j] for j in range(len(self.w))]

    def fit(self, X: list[list[float]], y: list[int]) -> "PureLogistic":
        if not X:
            raise ValueError("dataset vuoto")
        self.mean, self.std = _standardise(X)
        self.w = [0.0] * len(X[0])
        self.b = 0.0
        rng = random.Random(self.seed)
        idx = list(range(len(X)))
        # Bilanciamento: se le classi sono sbilanciate il modello imparerebbe
        # a rispondere sempre la maggioritaria e sembrerebbe accurato.
        pos = sum(y) or 1
        neg = len(y) - pos or 1
        w_pos, w_neg = len(y) / (2.0 * pos), len(y) / (2.0 * neg)
        for _ in range(self.epochs):
            rng.shuffle(idx)
            for i in idx:
                x = self._prep(X[i])
                z = self.b + sum(wi * xi for wi, xi in zip(self.w, x))
                p = 1.0 / (1.0 + math.exp(-max(-35.0, min(35.0, z))))
                err = (p - y[i]) * (w_pos if y[i] else w_neg)
                self.b -= self.lr * err
                for j in range(len(self.w)):
                    self.w[j] -= self.lr * (err * x[j] + self.l2 * self.w[j])
        return self

    def decision(self, row: Sequence[float]) -> float:
        x = self._prep(row)
        return self.b + sum(wi * xi for wi, xi in zip(self.w, x))

    def predict_proba_one(self, row: Sequence[float]) -> float:
        z = self.decision(row)
        return 1.0 / (1.0 + math.exp(-max(-35.0, min(35.0, z))))


@dataclass
class Model:
    """Un modello addestrato, con tutto cio' che serve a giudicarlo."""

    model_id: str
    algorithm: str
    feature_names: list[str]
    horizon_s: int
    estimator: Any = None
    calibrator: Any = None
    calibrated: bool = False
    metrics: dict[str, Any] = field(default_factory=dict)
    validation: dict[str, Any] = field(default_factory=dict)
    n_train: int = 0
    created_ts: int = 0

    @property
    def ready(self) -> bool:
        return self.estimator is not None and bool(self.feature_names)

    def _vector(self, features: dict[str, float]) -> list[float] | None:
        if not self.feature_names:
            return None
        return [float(features.get(n, float("nan"))) for n in self.feature_names]

    def predict(self, features: dict[str, float]) -> dict[str, Any] | None:
        """Probabilita' che il prezzo sia PIU ALTO alla scadenza."""
        if not self.ready:
            return None
        vec = self._vector(features)
        if vec is None:
            return None
        try:
            if SKLEARN and not isinstance(self.estimator, PureLogistic):
                arr = np.array([[0.0 if not math.isfinite(v) else v for v in vec]])
                p = float(self.estimator.predict_proba(arr)[0][1])
            else:
                p = self.estimator.predict_proba_one(vec)
        except Exception:                       # noqa: BLE001
            return None
        raw = p
        if self.calibrator is not None:
            p = self.calibrator.transform(p)
        return {"probability": max(0.001, min(0.999, p)),
                "raw_probability": raw,
                "model_id": self.model_id, "calibrated": self.calibrated}

    def to_row(self) -> dict[str, Any]:
        return {
            "model_id": self.model_id, "ts": self.created_ts,
            "algorithm": self.algorithm, "horizon_s": self.horizon_s,
            "n_train": self.n_train,
            "feature_names": json.dumps(self.feature_names),
            "metrics": json.dumps(self.metrics, default=str),
            "calibration": json.dumps(
                self.calibrator.to_dict() if self.calibrator else None, default=str),
            "validation": json.dumps(self.validation, default=str),
            "artifact": json.dumps(self.artifact(), default=str),
            "is_champion": 0,
        }

    def artifact(self) -> dict[str, Any]:
        """Serializzazione del solo modello puro: quelli sklearn si riaddestrano.

        Salvare un oggetto sklearn con pickle legherebbe il database alla
        versione della libreria installata quel giorno. Un modello che non si
        puo' ricaricare fra sei mesi non e' archiviato: e' perso.
        """
        if isinstance(self.estimator, PureLogistic):
            return {"kind": "pure_logistic", "w": self.estimator.w,
                    "b": self.estimator.b, "mean": self.estimator.mean,
                    "std": self.estimator.std,
                    "features": self.feature_names,
                    "calibrator": self.calibrator.to_dict() if self.calibrator else None}
        return {"kind": self.algorithm, "features": self.feature_names,
                "note": "modello sklearn: va riaddestrato, non ricaricato"}

    @classmethod
    def from_artifact(cls, model_id: str, artifact: dict, horizon_s: int) -> "Model | None":
        if artifact.get("kind") != "pure_logistic":
            return None
        est = PureLogistic()
        est.w = list(artifact["w"])
        est.b = float(artifact["b"])
        est.mean = list(artifact["mean"])
        est.std = list(artifact["std"])
        from .calibration import Calibrator
        cal_raw = artifact.get("calibrator")
        cal = Calibrator.from_dict(cal_raw) if cal_raw else None
        return cls(model_id=model_id, algorithm="logistic",
                   feature_names=list(artifact["features"]),
                   horizon_s=horizon_s, estimator=est, calibrator=cal,
                   calibrated=cal is not None)


def build_estimator(algorithm: str, seed: int = 7):
    """Crea l'algoritmo richiesto, con la riserva pura se sklearn non c'e'."""
    if not SKLEARN or algorithm == "logistic_pure":
        return PureLogistic(seed=seed)
    if algorithm == "logistic":
        return LogisticRegression(max_iter=800, C=0.5, class_weight="balanced")
    if algorithm == "sgd":
        return SGDClassifier(loss="log_loss", alpha=1e-4, max_iter=1500,
                             class_weight="balanced", random_state=seed)
    if algorithm == "random_forest":
        return RandomForestClassifier(
            n_estimators=180, max_depth=6, min_samples_leaf=25,
            class_weight="balanced", random_state=seed, n_jobs=-1)
    if algorithm == "hist_gradient_boosting":
        return HistGradientBoostingClassifier(
            max_iter=180, max_depth=4, learning_rate=0.06,
            min_samples_leaf=40, random_state=seed)
    return PureLogistic(seed=seed)


def fit_estimator(estimator, X: list[list[float]], y: list[int]):
    if isinstance(estimator, PureLogistic):
        return estimator.fit(X, y)
    arr = np.array([[0.0 if not math.isfinite(v) else v for v in row] for row in X])
    estimator.fit(arr, np.array(y))
    return estimator


def predict_proba(estimator, X: list[list[float]]) -> list[float]:
    if isinstance(estimator, PureLogistic):
        return [estimator.predict_proba_one(r) for r in X]
    arr = np.array([[0.0 if not math.isfinite(v) else v for v in row] for row in X])
    return [float(p) for p in estimator.predict_proba(arr)[:, 1]]
