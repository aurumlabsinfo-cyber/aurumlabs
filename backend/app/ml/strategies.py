"""Rule-based strategy baselines.

These exist so the machine-learning result has something honest to be compared
against. A gradient booster that cannot beat "buy when the book is bid-heavy"
has not learned anything worth deploying.

Each strategy is parameter-light and evaluated on exactly the same
out-of-sample folds as the models, so the comparison is like-for-like. They are
stateless functions of the feature row - no fitting, therefore no overfitting,
therefore their out-of-sample number *is* their number.
"""

from __future__ import annotations

from typing import Any, Callable

import numpy as np
import pandas as pd

from app.ml.dataset import Dataset
from app.ml.validation import selective_metrics, walk_forward_splits
from app.signals.statistics import binomial_p_value, wilson_interval

#: (probability of up in [0,1], confidence in [0,1]); NaN prob => no trade.
StrategyFn = Callable[[pd.DataFrame], tuple[np.ndarray, np.ndarray]]


def _col(df: pd.DataFrame, name: str) -> np.ndarray:
    if name not in df.columns:
        return np.full(len(df), np.nan)
    return df[name].to_numpy(dtype=float)


def _sigmoid(x: np.ndarray, k: float = 1.0) -> np.ndarray:
    return 1.0 / (1.0 + np.exp(-k * np.nan_to_num(x, nan=0.0)))


def momentum(df: pd.DataFrame) -> tuple[np.ndarray, np.ndarray]:
    r1 = _col(df, "return_1000ms")
    vol = np.where(np.isnan(_col(df, "realized_vol_30s_bps")), 1.0,
                   _col(df, "realized_vol_30s_bps"))
    z = r1 / np.maximum(vol, 0.5)
    return _sigmoid(z, 1.5), np.tanh(np.abs(z))


def mean_reversion(df: pd.DataFrame) -> tuple[np.ndarray, np.ndarray]:
    z = _col(df, "bb_z")
    return _sigmoid(-z, 1.0), np.tanh(np.abs(z) / 2.0)


def order_book_imbalance(df: pd.DataFrame) -> tuple[np.ndarray, np.ndarray]:
    d5 = _col(df, "depth_imbalance_5")
    l1 = _col(df, "book_imbalance_l1")
    blend = np.nan_to_num(0.6 * d5 + 0.4 * l1)
    return _sigmoid(blend, 3.0), np.abs(blend)


def order_flow(df: pd.DataFrame) -> tuple[np.ndarray, np.ndarray]:
    vi1 = _col(df, "volume_imbalance_1s")
    vi5 = _col(df, "volume_imbalance_5s")
    blend = np.nan_to_num(0.6 * vi1 + 0.4 * vi5)
    return _sigmoid(blend, 3.0), np.abs(blend)


def breakout(df: pd.DataFrame) -> tuple[np.ndarray, np.ndarray]:
    r5 = _col(df, "return_5000ms")
    ratio = _col(df, "vol_ratio_5s_30s")
    active = np.nan_to_num(ratio, nan=0.0) > 1.8
    prob = _sigmoid(np.nan_to_num(r5), 0.5)
    conf = np.where(active, np.tanh(np.abs(np.nan_to_num(r5)) / 3.0), 0.0)
    return prob, conf


def volatility_expansion(df: pd.DataFrame) -> tuple[np.ndarray, np.ndarray]:
    ratio = np.nan_to_num(_col(df, "vol_ratio_5s_30s"), nan=1.0)
    micro = np.nan_to_num(_col(df, "micro_price_dev_bps"))
    active = ratio > 1.5
    return _sigmoid(micro, 2.0), np.where(active, np.tanh(np.abs(micro)), 0.0)


def liquidation_pressure(df: pd.DataFrame) -> tuple[np.ndarray, np.ndarray]:
    """Forced sellers push price down, forced buyers push it up - briefly.

    Requires the futures feed; without it the confidence is zero everywhere and
    the strategy correctly declines to trade rather than inventing a view.
    """
    liq = np.nan_to_num(_col(df, "liq_imbalance_5s"))
    removal_b = np.nan_to_num(_col(df, "liquidity_removal_bid"))
    removal_a = np.nan_to_num(_col(df, "liquidity_removal_ask"))
    pressure = liq + 0.5 * (removal_a - removal_b)
    has_data = ~np.isnan(_col(df, "liq_imbalance_5s"))
    return _sigmoid(pressure, 2.0), np.where(has_data, np.tanh(np.abs(pressure)), 0.0)


def burst15(df: pd.DataFrame) -> tuple[np.ndarray, np.ndarray]:
    """AURUM BURST-15's entry rule, scored like every other candidate.

    The session layer (cooldown, trade cap, stop-loss) is deliberately absent:
    those manage risk, they do not predict direction, and folding them in here
    would mix the two questions. Use `app.ml.burst_backtest` for the full
    session simulation.
    """
    from app.config import get_settings

    try:
        s = get_settings()
        n5_min, r10_min, need_agree = (
            s.burst_n5_min, s.burst_r10_min_bps, s.burst_require_ofi_agree,
        )
    except Exception:  # noqa: BLE001 - research must run without an environment
        n5_min, r10_min, need_agree = 40, 0.5, True

    n5 = np.nan_to_num(_col(df, "trade_count_5s"), nan=0.0)
    r10 = _col(df, "return_10000ms")
    ofi = np.nan_to_num(_col(df, "ofi_notional_5s"), nan=0.0)
    has_r10 = ~np.isnan(r10)
    r10 = np.nan_to_num(r10, nan=0.0)

    active = has_r10 & (n5 >= n5_min) & (np.abs(r10) >= r10_min) & (r10 != 0)
    if need_agree:
        active &= np.sign(ofi) == np.sign(r10)
    # Direction is the sign of the 10s move; the magnitude only scales how
    # loudly the rule says it.
    prob = np.where(r10 > 0, 1.0, 0.0)
    conf = np.where(active, np.tanh(np.abs(r10) / max(r10_min * 2.0, 1e-9)), 0.0)
    return prob, conf


def ensemble(df: pd.DataFrame) -> tuple[np.ndarray, np.ndarray]:
    parts = [
        (order_flow(df), 1.8), (order_book_imbalance(df), 1.6), (momentum(df), 1.1),
        (mean_reversion(df), 1.1), (breakout(df), 0.8),
    ]
    num = np.zeros(len(df))
    den = np.zeros(len(df))
    for (prob, conf), w in parts:
        num += (prob - 0.5) * conf * w
        den += conf * w
    score = np.divide(num, np.maximum(den, 1e-9))
    return 0.5 + score, np.tanh(den / 3.0)


STRATEGIES: dict[str, StrategyFn] = {
    "momentum": momentum,
    "mean_reversion": mean_reversion,
    "order_book_imbalance": order_book_imbalance,
    "order_flow": order_flow,
    "breakout": breakout,
    "volatility_expansion": volatility_expansion,
    "liquidation_pressure": liquidation_pressure,
    "burst15": burst15,
    "ensemble": ensemble,
}


def evaluate_strategy(
    ds: Dataset,
    name: str,
    min_confidence: float = 0.15,
    n_splits: int = 5,
    embargo_s: float = 30.0,
    payout: float | None = None,
) -> dict[str, Any]:
    """Score a rule strategy on the same out-of-sample folds as the models."""
    fn = STRATEGIES[name]
    splits = list(
        walk_forward_splits(
            ds.ts.to_numpy(), n_splits=n_splits, horizon_s=ds.horizon_s,
            embargo_s=embargo_s,
        )
    )
    if not splits:
        return {"strategy": name, "error": "not enough data for out-of-sample folds"}

    test_idx = np.concatenate([s.test_idx for s in splits])
    df = ds.X.iloc[test_idx]
    y = ds.y.to_numpy()[test_idx]
    prob, conf = fn(df)
    prob = np.clip(np.nan_to_num(prob, nan=0.5), 0.0, 1.0)
    conf = np.nan_to_num(conf, nan=0.0)

    tradable = conf >= min_confidence
    n = int(tradable.sum())
    if n == 0:
        return {
            "strategy": name, "n": 0, "coverage": 0.0,
            "note": "strategy never reached its confidence threshold - NO TRADE",
        }
    pred = (prob[tradable] >= 0.5).astype(int)
    truth = y[tradable]
    correct = int((pred == truth).sum())
    wr = correct / n
    lo, hi = wilson_interval(correct, n)
    return {
        "strategy": name,
        "n": n,
        "coverage": round(n / len(y), 4),
        "win_rate": round(wr, 5),
        "ci95": [round(lo, 5), round(hi, 5)],
        "p_value": round(binomial_p_value(correct, n) or 1.0, 6),
        "beats_chance": bool(lo > 0.5),
        "selective": selective_metrics(truth, prob[tradable], payout=payout),
        "folds": len(splits),
    }


def evaluate_all(
    ds: Dataset, payout: float | None = None, n_splits: int = 5
) -> dict[str, Any]:
    return {
        name: evaluate_strategy(ds, name, payout=payout, n_splits=n_splits)
        for name in STRATEGIES
    }
