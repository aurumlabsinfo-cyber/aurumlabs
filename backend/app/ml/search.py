"""Strategy search with an honest correction for having searched.

The naive way to "find a winning strategy" is to try a few hundred variants and
keep the best. That procedure finds a winner on pure noise essentially every
time: with 200 candidates, the best one clears p < 0.05 by luck alone with
probability ~1. Any search that does not correct for its own breadth is a
machine for manufacturing false confidence.

This module therefore does three things:

1. enumerates a parameter grid over the rule families;
2. scores every candidate on the **same purged walk-forward out-of-sample
   folds**, so no candidate sees its own training data;
3. tests the best candidate against a null distribution of *the maximum
   statistic across all candidates* - White's Reality Check, implemented as a
   circular-shift (rotation) test.

The rotation test works because circularly shifting the label series against
the features destroys any real predictive relationship while preserving the
labels' own autocorrelation exactly. That matters here: 5-second labels sampled
every 100 ms overlap heavily, and a naive i.i.d. bootstrap would understate the
null and hand back a spurious "edge".

The cross-correlation for every possible shift is obtained in one FFT per
candidate, which is what makes an exhaustive rotation test affordable.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Sequence

import numpy as np
import pandas as pd

from app.core.logging_conf import get_logger
from app.ml.dataset import Dataset
from app.ml.validation import walk_forward_splits
from app.signals.statistics import (
    break_even_win_rate,
    expected_value_per_trade,
    wilson_interval,
)

log = get_logger(__name__)

#: A candidate must take at least this many out-of-sample trades to be ranked.
#: Without it the search would always crown some ultra-selective variant that
#: went 9-1 by luck.
MIN_TRADES = 200


@dataclass
class Candidate:
    """One concrete strategy: a family plus its parameters."""

    family: str
    params: dict[str, Any]
    #: features -> (signed score, where sign is the direction)
    score_fn: Callable[[pd.DataFrame], np.ndarray] = field(repr=False)
    threshold: float = 0.0

    @property
    def name(self) -> str:
        parts = ",".join(f"{k}={v}" for k, v in self.params.items())
        return f"{self.family}({parts})"

    def signals(self, df: pd.DataFrame) -> tuple[np.ndarray, np.ndarray]:
        """Return (take_mask, predicted_up) for every row."""
        score = np.nan_to_num(self.score_fn(df), nan=0.0, posinf=0.0, neginf=0.0)
        take = np.abs(score) >= self.threshold
        return take, (score > 0)


def _col(df: pd.DataFrame, name: str) -> np.ndarray:
    if name not in df.columns:
        return np.full(len(df), np.nan)
    return df[name].to_numpy(dtype=float)


def _norm(df: pd.DataFrame, name: str, by: str) -> np.ndarray:
    """Value normalised by a volatility column, so thresholds mean something."""
    v = _col(df, name)
    scale = np.nan_to_num(_col(df, by), nan=1.0)
    return v / np.maximum(scale, 0.5)


# --------------------------------------------------------------- the grid
def build_candidates(
    families: Sequence[str] | None = None,
) -> list[Candidate]:
    """Enumerate the search space.

    Deliberately small and interpretable. A grid of ten thousand variants would
    not find more edge, it would only make the multiple-testing correction
    swallow whatever edge there is.
    """
    out: list[Candidate] = []

    def add(family: str, params: dict, fn, threshold: float) -> None:
        if families and family not in families:
            return
        out.append(Candidate(family, params, fn, threshold))

    # --- order flow: the highest-information family at this horizon ---------
    for window in ("1s", "5s"):
        for th in (0.2, 0.35, 0.5, 0.65):
            add(
                "order_flow",
                {"window": window, "th": th},
                lambda df, w=window: _col(df, f"volume_imbalance_{w}"),
                th,
            )
    for th in (0.25, 0.4, 0.55):
        add(
            "order_flow_confirmed",
            {"th": th},
            # Both windows must agree, otherwise the score collapses to zero.
            lambda df: np.where(
                np.sign(_col(df, "volume_imbalance_1s"))
                == np.sign(_col(df, "volume_imbalance_5s")),
                (_col(df, "volume_imbalance_1s") + _col(df, "volume_imbalance_5s")) / 2,
                0.0,
            ),
            th,
        )

    # --- top of book -------------------------------------------------------
    for th in (0.15, 0.3, 0.45, 0.6):
        add("book_imbalance_l1", {"th": th},
            lambda df: _col(df, "book_imbalance_l1"), th)
    for th in (0.2, 0.35, 0.5):
        add("depth_imbalance_5", {"th": th},
            lambda df: _col(df, "depth_imbalance_5"), th)
    for th in (0.2, 0.4, 0.8, 1.2):
        add("micro_price_dev", {"th_bps": th},
            lambda df: _col(df, "micro_price_dev_bps"), th)

    # --- price action ------------------------------------------------------
    for horizon in ("500ms", "1000ms", "2000ms"):
        for th in (0.5, 1.0, 1.5, 2.0):
            add(
                "momentum",
                {"lookback": horizon, "z": th},
                lambda df, h=horizon: _norm(df, f"return_{h}", "realized_vol_30s_bps"),
                th,
            )
    for th in (1.0, 1.5, 2.0, 2.5):
        add("mean_reversion_bb", {"z": th}, lambda df: -_col(df, "bb_z"), th)
    for th in (1.0, 1.5, 2.0):
        add(
            "mean_reversion_vwap",
            {"z": th},
            lambda df: -_norm(df, "vwap_deviation_bps", "realized_vol_30s_bps"),
            th,
        )

    # --- combinations ------------------------------------------------------
    for th in (0.3, 0.5, 0.7):
        add(
            "flow_plus_book",
            {"th": th},
            lambda df: 0.5 * _col(df, "volume_imbalance_1s")
            + 0.5 * _col(df, "book_imbalance_l1"),
            th,
        )
    for th in (0.3, 0.5):
        add(
            "flow_against_stretch",
            {"th": th},
            # Fade a stretched band only when flow agrees with the fade.
            lambda df: np.where(
                np.sign(-_col(df, "bb_z")) == np.sign(_col(df, "volume_imbalance_1s")),
                _col(df, "volume_imbalance_1s"),
                0.0,
            ),
            th,
        )
    for th in (0.4, 0.7, 1.0):
        add(
            "breakout_vol",
            {"th": th},
            lambda df: np.where(
                np.nan_to_num(_col(df, "vol_ratio_5s_30s"), nan=0.0) > 1.6,
                _norm(df, "return_1000ms", "realized_vol_30s_bps"),
                0.0,
            ),
            th,
        )
    return out


# --------------------------------------------------------------- evaluation
@dataclass
class CandidateResult:
    name: str
    family: str
    params: dict[str, Any]
    n: int
    coverage: float
    win_rate: float
    ci95: tuple[float, float]
    expected_value: float | None
    eligible: bool
    reason: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "strategy": self.name,
            "family": self.family,
            "params": self.params,
            "trades": self.n,
            "coverage": round(self.coverage, 4),
            "win_rate": round(self.win_rate, 5),
            "ci95": [round(self.ci95[0], 5), round(self.ci95[1], 5)],
            "expected_value_per_trade": (
                round(self.expected_value, 5) if self.expected_value is not None else None
            ),
            "eligible": self.eligible,
            "reason": self.reason,
        }


def _oos_frame(ds: Dataset, n_splits: int, embargo_s: float):
    """Concatenate the out-of-sample fold indices, in time order."""
    splits = list(
        walk_forward_splits(
            ds.ts.to_numpy(), n_splits=n_splits, horizon_s=ds.horizon_s,
            embargo_s=embargo_s,
        )
    )
    if not splits:
        return None, None, None
    idx = np.concatenate([s.test_idx for s in splits])
    return ds.X.iloc[idx], ds.y.to_numpy()[idx], splits


def search(
    ds: Dataset,
    payout: float | None = None,
    n_splits: int = 5,
    embargo_s: float = 30.0,
    families: Sequence[str] | None = None,
    min_trades: int = MIN_TRADES,
    max_rotations: int = 2000,
) -> dict[str, Any]:
    """Run the full search and return a report with a corrected verdict."""
    X, y, splits = _oos_frame(ds, n_splits, embargo_s)
    if X is None:
        return {
            "status": "INSUFFICIENT_DATA",
            "conclusion": (
                f"{len(ds)} labelled rows is not enough to build purged "
                "out-of-sample folds. Import or record more data."
            ),
            "rows": len(ds),
        }

    candidates = build_candidates(families)
    results: list[CandidateResult] = []
    # Signed prediction vectors, kept for the rotation test.
    pred_vectors: list[np.ndarray] = []
    eligible_idx: list[int] = []

    y_signed = np.where(y == 1, 1.0, -1.0)
    be = break_even_win_rate(payout)

    for cand in candidates:
        take, up = cand.signals(X)
        n = int(take.sum())
        if n == 0:
            results.append(
                CandidateResult(cand.name, cand.family, cand.params, 0, 0.0, 0.0,
                                (0.0, 0.0), None, False,
                                "never reached its threshold")
            )
            continue
        pred = np.zeros(len(y), dtype=float)
        pred[take] = np.where(up[take], 1.0, -1.0)
        correct = int(((pred > 0) == (y == 1))[take].sum())
        wr = correct / n
        lo, hi = wilson_interval(correct, n)
        ev = expected_value_per_trade(wr, payout) if payout is not None else None
        eligible = n >= min_trades
        res = CandidateResult(
            cand.name, cand.family, cand.params, n, n / len(y), wr, (lo, hi), ev,
            eligible,
            "" if eligible else f"only {n} trades (need {min_trades})",
        )
        results.append(res)
        if eligible:
            pred_vectors.append(pred)
            eligible_idx.append(len(results) - 1)

    if not eligible_idx:
        return {
            "status": "NO_ELIGIBLE_CANDIDATE",
            "candidates_tested": len(candidates),
            "min_trades": min_trades,
            "results": [r.to_dict() for r in results],
            "conclusion": (
                "No candidate took enough out-of-sample trades to be judged. "
                "NESSUN EDGE ROBUSTO IDENTIFICATO."
            ),
        }

    # Rank by the lower confidence bound, not the point estimate: a strategy
    # whose edge might be zero must not outrank one whose worst case is positive.
    ranked = sorted(
        (results[i] for i in eligible_idx),
        key=lambda r: (r.ci95[0], r.win_rate),
        reverse=True,
    )
    best = ranked[0]

    reality = rotation_reality_check(
        pred_vectors, y_signed, horizon_s=ds.horizon_s,
        rows_per_second=_rows_per_second(ds), max_rotations=max_rotations,
        names=[results[i].name for i in eligible_idx],
    )

    verdict = _verdict(best, reality, payout, be, len(candidates))
    return {
        "status": "COMPLETE",
        "rows_out_of_sample": int(len(y)),
        "folds": len(splits),
        "candidates_tested": len(candidates),
        "candidates_eligible": len(eligible_idx),
        "payout": payout,
        "break_even_win_rate": round(be, 5) if be is not None else None,
        "best": best.to_dict(),
        "leaderboard": [r.to_dict() for r in ranked[:15]],
        "all_results": [r.to_dict() for r in results],
        "reality_check": reality,
        **verdict,
    }


def _rows_per_second(ds: Dataset) -> float:
    ts = ds.ts.to_numpy()
    if len(ts) < 2:
        return 10.0
    span_s = (ts[-1] - ts[0]) / 1000.0
    return len(ts) / span_s if span_s > 0 else 10.0


def rotation_reality_check(
    pred_vectors: list[np.ndarray],
    y_signed: np.ndarray,
    horizon_s: float,
    rows_per_second: float,
    max_rotations: int = 2000,
    names: Sequence[str] | None = None,
) -> dict[str, Any]:
    """White's Reality Check via circular rotation of the label series.

    For every candidate and every rotation `k`, the number of correct calls is

        correct(k) = (dot(pred, rotate(y, k)) + n_taken) / 2

    and the whole set of dot products across rotations is one FFT-based circular
    cross-correlation. The null distribution is the maximum win rate across
    candidates at each rotation; the p-value is how often that null maximum
    reaches the observed best.

    Rotations smaller than the label's own memory are excluded: at those offsets
    the rotated labels still overlap the originals, so they are not a valid null.
    """
    n_rows = len(y_signed)
    horizon_rows = max(1, int(horizon_s * rows_per_second))
    # Stay clear of the overlapping-label region on both ends of the circle.
    guard = max(10 * horizon_rows, 200)
    if n_rows <= 4 * guard:
        return {
            "ran": False,
            "reason": (
                f"only {n_rows} out-of-sample rows for a guard band of {guard}; "
                "not enough independent rotations to build a null"
            ),
        }

    fy = np.fft.rfft(y_signed, n=n_rows)
    observed: list[float] = []
    curves: list[np.ndarray] = []
    kept_names: list[str] = []

    for i, pred in enumerate(pred_vectors):
        n_taken = float(np.abs(pred).sum())
        if n_taken == 0:
            continue
        fp = np.fft.rfft(pred, n=n_rows)
        # c[k] = sum_i pred[i] * y[(i + k) % n]
        c = np.fft.irfft(fp * np.conj(fy), n=n_rows)
        wr = (c + n_taken) / (2.0 * n_taken)
        observed.append(float(wr[0]))
        curves.append(wr)
        kept_names.append(names[i] if names and i < len(names) else f"candidate_{i}")

    if not curves:
        return {"ran": False, "reason": "no candidate took any trade"}

    stacked = np.vstack(curves)
    valid = np.arange(guard, n_rows - guard)
    if len(valid) > max_rotations:
        step = len(valid) // max_rotations
        valid = valid[::step][:max_rotations]

    null_max = stacked[:, valid].max(axis=0)
    best_i = int(np.argmax(observed))
    observed_best = float(observed[best_i])
    exceed = int((null_max >= observed_best).sum())
    p_value = (1.0 + exceed) / (1.0 + len(valid))

    return {
        "ran": True,
        # The correction is applied to the single most flattering candidate,
        # which is usually NOT the one the leaderboard ranks first (ranking uses
        # the confidence-interval lower bound). Testing the most flattering one
        # is the conservative choice: it makes passing harder, not easier.
        "observed_best_win_rate": round(observed_best, 5),
        "observed_best_candidate": kept_names[best_i],
        "rotations": int(len(valid)),
        "guard_rows": int(guard),
        "null_max_mean": round(float(null_max.mean()), 5),
        "null_max_p95": round(float(np.percentile(null_max, 95)), 5),
        "null_max_max": round(float(null_max.max()), 5),
        "p_value_family_wise": round(p_value, 5),
        "significant_after_correction": bool(p_value < 0.05),
        "note": (
            "p is the probability that a search this wide produces a best "
            "candidate this good when nothing predicts anything. Compare the "
            "observed best against null_max_p95, not against 0.5."
        ),
    }


def _verdict(
    best: CandidateResult,
    reality: dict[str, Any],
    payout: float | None,
    break_even: float | None,
    n_candidates: int,
) -> dict[str, Any]:
    notes: list[str] = [
        f"{n_candidates} candidates were tested; the winner must therefore beat "
        f"the distribution of the best-of-{n_candidates}, not a coin flip."
    ]

    if not reality.get("ran"):
        return {
            "verdict": "INCONCLUSIVE",
            "notes": notes + [reality.get("reason", "reality check did not run")],
            "conclusion": "NESSUN EDGE ROBUSTO IDENTIFICATO / NO ROBUST EDGE IDENTIFIED",
        }

    p = reality["p_value_family_wise"]
    null_p95 = reality["null_max_p95"]
    notes.append(
        f"ranked first by worst case: {best.name} at {best.win_rate:.4f} over "
        f"{best.n} trades. The correction is applied to the most flattering "
        f"candidate instead ({reality['observed_best_candidate']} at "
        f"{reality['observed_best_win_rate']:.4f}), which is the harder test; "
        f"a random-alignment search of the same breadth reaches {null_p95:.4f} "
        f"5% of the time (family-wise p = {p:.4f})."
    )

    if p >= 0.05:
        return {
            "verdict": "NO EDGE",
            "notes": notes + [
                "The best candidate is inside what the search itself produces "
                "from noise. This is the expected outcome at a 5 second horizon."
            ],
            "conclusion": "NESSUN EDGE ROBUSTO IDENTIFICATO / NO ROBUST EDGE IDENTIFIED",
        }

    if break_even is not None and best.win_rate <= break_even:
        notes.append(
            f"statistically real, but {best.win_rate:.4f} is below the "
            f"{break_even:.4f} break-even at payout {payout}: it predicts, and "
            "it still loses money."
        )
        return {
            "verdict": "STATISTICAL EDGE, NOT PROFITABLE",
            "notes": notes,
            "conclusion": (
                "An edge exists but does not clear the payout. Not tradable as is."
            ),
        }

    if best.ci95[0] <= 0.5:
        notes.append(
            f"the 95% interval [{best.ci95[0]:.4f}, {best.ci95[1]:.4f}] still "
            "touches 0.5, so the effect is not tightly bounded away from chance."
        )
        return {
            "verdict": "PROMISING",
            "notes": notes,
            "conclusion": (
                "Survives the multiple-testing correction but needs more data "
                "before it can be called proven."
            ),
        }

    return {
        "verdict": "CANDIDATE EDGE",
        "notes": notes + [
            "Survives correction for search breadth on out-of-sample folds. "
            "This is a hypothesis worth forward-testing on data collected "
            "AFTER today, not a proven result: the same folds were used to "
            "rank candidates, so a fresh out-of-time sample is the real test."
        ],
        "conclusion": (
            f"{best.name}: {best.win_rate:.2%} over {best.n} out-of-sample "
            f"trades, family-wise p = {reality['p_value_family_wise']:.4f}. "
            "Forward-test before believing it."
        ),
    }
