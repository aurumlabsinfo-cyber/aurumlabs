"""The decision model - one object, versioned, serialisable, replaceable.

A model is a logistic score over the snapshot's feature vector plus an estimate
of how much residual move is left.  Champion and challenger are the *same
class*: promoting a challenger is swapping one instance for another, which is
what makes rollback instant and comparison honest.

There is no committee of agents voting.  One model, one probability, one number
for the expected move.
"""

from __future__ import annotations

import json
import math
import time
from dataclasses import dataclass, field
from typing import Any

from ..scan.snapshot import FEATURE_NAMES, MarketSnapshot

CHAMPION_V1 = "champion-1.0.0-momentum-of"


def sigmoid(z: float) -> float:
    if z >= 0:
        return 1.0 / (1.0 + math.exp(-z))
    exp_z = math.exp(z)
    return exp_z / (1.0 + exp_z)


@dataclass
class Model:
    version: str
    kind: str = "champion"                  # champion | challenger
    weights: dict[str, float] = field(default_factory=dict)
    bias: float = 0.0
    mean: dict[str, float] = field(default_factory=dict)
    std: dict[str, float] = field(default_factory=dict)
    move_scale: float = 1.0
    created_ts: float = field(default_factory=time.time)
    parent: str | None = None
    metrics: dict[str, Any] = field(default_factory=dict)
    notes: str = ""

    # ---------------------------------------------------------------- scoring
    def score(self, features: dict[str, float]) -> float:
        z = self.bias
        for name in FEATURE_NAMES:
            value = features.get(name, 0.0)
            mean = self.mean.get(name, 0.0)
            std = self.std.get(name, 1.0) or 1.0
            z += self.weights.get(name, 0.0) * ((value - mean) / std)
        return z

    def probability(self, features: dict[str, float]) -> float:
        return sigmoid(self.score(features))

    def expected_move_bps(self, snap: MarketSnapshot, probability: float) -> float:
        """How much of the move is plausibly *left*, in basis points.

        Deliberately modest: this system captures the tail of a move already in
        progress, it does not forecast a new one.  The estimate scales with the
        symbol's own volatility and with the strength of the ongoing push, and is
        cut down when the model is barely above a coin flip.
        """
        edge = max(0.0, (probability - 0.5) * 2.0)
        push = 0.55 * abs(snap.ret_3s_bps) + 0.90 * max(snap.volatility_bps, 0.5)
        return self.move_scale * push * (0.35 + 0.85 * edge)

    # ---------------------------------------------------------------- io
    def to_params(self) -> dict[str, Any]:
        return {
            "weights": self.weights,
            "bias": self.bias,
            "mean": self.mean,
            "std": self.std,
            "move_scale": self.move_scale,
            "features": list(FEATURE_NAMES),
        }

    def to_row(self, status: str) -> dict[str, Any]:
        return {
            "version": self.version,
            "created_ts": self.created_ts,
            "kind": self.kind,
            "status": status,
            "parent": self.parent,
            "params_json": json.dumps(self.to_params()),
            "metrics_json": json.dumps(self.metrics, default=str),
            "promoted_ts": time.time() if status == "champion" else None,
            "retired_ts": None,
            "notes": self.notes,
        }

    @classmethod
    def from_row(cls, row: dict[str, Any]) -> "Model":
        params = json.loads(row["params_json"])
        stored_features = params.get("features", list(FEATURE_NAMES))
        if list(stored_features) != list(FEATURE_NAMES):
            raise ValueError(
                f"model {row['version']} was trained on a different feature set "
                f"({len(stored_features)} features) - refusing to load it"
            )
        return cls(
            version=row["version"],
            kind=row["kind"],
            weights=params["weights"],
            bias=params["bias"],
            mean=params.get("mean", {}),
            std=params.get("std", {}),
            move_scale=params.get("move_scale", 1.0),
            created_ts=row["created_ts"],
            parent=row.get("parent"),
            metrics=json.loads(row.get("metrics_json") or "{}"),
            notes=row.get("notes") or "",
        )


def champion_v1() -> Model:
    """The first Champion: momentum + order-flow continuation.

    Hand-set, explainable and intentionally strict - a flat market scores below
    0.5 and produces NO TRADE.  Everything after this version is learned from
    the data this one collects.
    """
    return Model(
        version=CHAMPION_V1,
        kind="champion",
        bias=-0.35,
        weights={
            "mom_250ms": 0.10,
            "mom_1s": 0.35,
            "mom_3s": 0.30,
            "mom_5s": 0.18,
            "mom_15s": 0.06,
            "mom_60s": -0.05,      # already extended: less left to capture
            "ofi_1s": 0.30,
            "ofi_5s": 0.45,
            "imbalance_top": 0.20,
            "imbalance_depth": 0.15,
            "aggression_5s": 0.35,
            "aggression_60s": 0.10,
            "microprice_edge": 0.25,
            "volume_accel": 0.22,
            "vol_ratio": 0.05,
            "spread_over_vol": -0.45,
            "depth_ratio": 0.10,
            "trade_rate": 0.08,
            "oi_change": 0.05,
        },
        mean={name: 0.0 for name in FEATURE_NAMES},
        std={name: 1.0 for name in FEATURE_NAMES},
        move_scale=1.0,
        notes="initial champion: momentum + order-flow continuation, hand-specified",
        metrics={"origin": "hand-specified", "trained_on": 0},
    )
