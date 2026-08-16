"""Model calibration on a purged hold-out tail.

`_fit_and_save` used to hard-code `calibrated=False`. The live decision engine
blends an uncalibrated model at 0.3 weight instead of 0.5, so no amount of
retraining could ever promote a model - the "learns on its own" path had a
ceiling built into it. These tests pin the fix, including the part that
matters most: it must refuse to calibrate rather than fake it.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from app.config import Settings
from app.ml.dataset import Dataset
from app.ml.runner import _fit_with_calibration


def make_dataset(n: int, horizon_s: float = 5.0, seed: int = 7) -> Dataset:
    """A learnable series: the label follows one feature, with noise."""
    rng = np.random.default_rng(seed)
    signal = rng.normal(size=n)
    noise = rng.normal(size=n)
    X = pd.DataFrame({"signal": signal, "noise": noise})
    y = pd.Series((signal + 0.5 * noise > 0).astype(int))
    ts = pd.Series(1_700_000_000_000 + np.arange(n) * 100)
    return Dataset(
        X=X, y=y, ts=ts, entry=pd.Series(np.full(n, 100.0)),
        exit=pd.Series(np.full(n, 100.0)), horizon_s=horizon_s,
    )


def settings(**over) -> Settings:
    base = dict(env="test", exchanges="synthetic", allow_synthetic_source=True)
    base.update(over)
    return Settings(**base)


def test_a_large_dataset_produces_a_calibrated_model():
    # 8000 rows at 100ms is 800 seconds: enough that the hold-out tail still
    # has 500+ usable rows after the horizon + embargo purge gap is removed.
    ds = make_dataset(8000)
    model, calibrated, note = _fit_with_calibration(
        settings(), ds, "logistic_regression", ds.X
    )
    assert calibrated is True
    assert "isotonic" in note["calibration"]
    # Training and calibration rows are separated by horizon + embargo.
    assert note["purge_gap_ms"] > 0
    assert note["train_rows"] + note["calibration_rows"] <= len(ds)
    probs = model.predict_proba(ds.X.iloc[:10])
    assert probs.shape == (10, 2)
    assert np.all((probs >= 0) & (probs <= 1))


def test_a_small_dataset_stays_uncalibrated_and_says_so():
    ds = make_dataset(400)  # 40 seconds of data: nothing to hold out
    model, calibrated, note = _fit_with_calibration(
        settings(), ds, "logistic_regression", ds.X
    )
    assert calibrated is False
    assert "skipped" in note["calibration"]
    assert model.predict_proba(ds.X.iloc[:5]).shape == (5, 2)


def test_calibration_can_be_switched_off():
    ds = make_dataset(8000)
    _, calibrated, _ = _fit_with_calibration(
        settings(calibrate_models=False), ds, "logistic_regression", ds.X
    )
    assert calibrated is False


def test_a_single_class_hold_out_is_refused():
    """Isotonic on one class is meaningless; better uncalibrated than wrong."""
    ds = make_dataset(8000)
    y = ds.y.to_numpy().copy()
    y[int(len(y) * 0.8):] = 1  # the tail becomes all-UP
    ds.y = pd.Series(y)
    _, calibrated, note = _fit_with_calibration(
        settings(), ds, "logistic_regression", ds.X
    )
    assert calibrated is False
    assert "skipped" in note["calibration"]
