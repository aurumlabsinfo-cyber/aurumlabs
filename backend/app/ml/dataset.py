"""Supervised dataset construction from recorded data.

The single most important property here is that **labels come strictly from the
future and features strictly from the past**, with the boundary at the feature
row's own timestamp.

    features at t  ---->  label = sign( mid(t + h) - mid(t) )

`mid(t)` is the value the feature engine itself recorded at t (already causal),
and `mid(t + h)` is resolved from the tick table using the first tick at or
after t + h. If no tick exists inside the tolerance window, the row is dropped
rather than forward-filled - a forward-filled label is a fabricated one.

Synthetic rows are excluded by default and can only be included by an explicit
flag, which taints every downstream report.
"""

from __future__ import annotations

import bisect
from dataclasses import dataclass, field
from typing import Any, Sequence

import numpy as np
import pandas as pd
from sqlalchemy import select

from app.core.logging_conf import get_logger
from app.db.engine import session_scope
from app.db.models import FeatureRow, MarketTickRow

log = get_logger(__name__)

#: Columns that are metadata, not predictors.
NON_FEATURE_KEYS = {
    "mid", "micro_price", "book_synced", "data_quality", "history_span_ms",
    "tick_count", "bb_mid", "bb_upper", "bb_lower", "vwap_60s", "ema_9", "ema_21",
    "large_trade_threshold_notional",
}


@dataclass
class Dataset:
    X: pd.DataFrame
    y: pd.Series  # 1 = up, 0 = down
    ts: pd.Series  # feature timestamp (ms)
    entry: pd.Series
    exit: pd.Series
    horizon_s: float
    meta: dict[str, Any] = field(default_factory=dict)

    def __len__(self) -> int:
        return len(self.y)

    @property
    def feature_names(self) -> list[str]:
        return list(self.X.columns)

    def describe(self) -> dict[str, Any]:
        return {
            "rows": len(self),
            "features": len(self.X.columns),
            "horizon_s": self.horizon_s,
            "class_balance_up": round(float(self.y.mean()), 4) if len(self) else None,
            "start_ts": int(self.ts.iloc[0]) if len(self) else None,
            "end_ts": int(self.ts.iloc[-1]) if len(self) else None,
            "duration_minutes": (
                round((int(self.ts.iloc[-1]) - int(self.ts.iloc[0])) / 60000, 2)
                if len(self) > 1 else 0
            ),
            **self.meta,
        }


async def load_raw(
    symbol: str,
    include_synthetic: bool = False,
    start_ts: int | None = None,
    end_ts: int | None = None,
    limit: int | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Return (features, ticks) as DataFrames ordered by timestamp."""
    fstmt = select(
        FeatureRow.ts, FeatureRow.mid, FeatureRow.payload, FeatureRow.is_synthetic,
        FeatureRow.book_synced, FeatureRow.data_quality,
    ).where(FeatureRow.symbol == symbol).order_by(FeatureRow.ts.asc())
    tstmt = select(MarketTickRow.ts, MarketTickRow.mid).where(
        MarketTickRow.symbol == symbol
    ).order_by(MarketTickRow.ts.asc())

    if not include_synthetic:
        fstmt = fstmt.where(FeatureRow.is_synthetic.is_(False))
        tstmt = tstmt.where(MarketTickRow.is_synthetic.is_(False))
    if start_ts:
        fstmt = fstmt.where(FeatureRow.ts >= start_ts)
        tstmt = tstmt.where(MarketTickRow.ts >= start_ts)
    if end_ts:
        fstmt = fstmt.where(FeatureRow.ts <= end_ts)
        tstmt = tstmt.where(MarketTickRow.ts <= end_ts)
    if limit:
        fstmt = fstmt.limit(limit)

    async with session_scope() as s:
        frows = (await s.execute(fstmt)).all()
        trows = (await s.execute(tstmt)).all()

    features = pd.DataFrame(
        [
            {
                "ts": r.ts, "mid": r.mid, "is_synthetic": r.is_synthetic,
                "book_synced": r.book_synced, "data_quality": r.data_quality,
                **(r.payload or {}),
            }
            for r in frows
        ]
    )
    ticks = pd.DataFrame([{"ts": r.ts, "mid": r.mid} for r in trows])
    return features, ticks


def build_dataset(
    features: pd.DataFrame,
    ticks: pd.DataFrame,
    horizon_s: float,
    tolerance_ms: int = 750,
    min_data_quality: float = 0.0,
    require_book_synced: bool = True,
    drop_ties: bool = True,
) -> Dataset:
    """Attach forward-looking labels to causal feature rows."""
    if features.empty or ticks.empty:
        return Dataset(
            pd.DataFrame(), pd.Series(dtype=float), pd.Series(dtype="int64"),
            pd.Series(dtype=float), pd.Series(dtype=float), horizon_s,
            meta={"error": "no data"},
        )

    features = features.sort_values("ts").reset_index(drop=True)
    ticks = ticks.sort_values("ts").reset_index(drop=True)
    tick_ts: list[int] = ticks["ts"].tolist()
    tick_mid: list[float] = ticks["mid"].tolist()

    horizon_ms = int(horizon_s * 1000)
    rows: list[int] = []
    exits: list[float] = []
    dropped_no_future = 0

    for i, (ts, _mid) in enumerate(zip(features["ts"], features["mid"])):
        target = int(ts) + horizon_ms
        idx = bisect.bisect_left(tick_ts, target)
        if idx >= len(tick_ts) or tick_ts[idx] - target > tolerance_ms:
            dropped_no_future += 1
            continue
        rows.append(i)
        exits.append(tick_mid[idx])

    sub = features.iloc[rows].reset_index(drop=True)
    exit_series = pd.Series(exits, name="exit")
    entry_series = sub["mid"].reset_index(drop=True)

    mask = pd.Series(True, index=sub.index)
    if require_book_synced and "book_synced" in sub:
        mask &= sub["book_synced"].fillna(False).astype(bool)
    if min_data_quality > 0 and "data_quality" in sub:
        mask &= sub["data_quality"].fillna(0) >= min_data_quality

    diff = exit_series - entry_series
    ties = int((diff == 0).sum())
    if drop_ties:
        mask &= diff != 0

    sub = sub[mask].reset_index(drop=True)
    exit_series = exit_series[mask].reset_index(drop=True)
    entry_series = entry_series[mask].reset_index(drop=True)
    y = (exit_series > entry_series).astype(int)

    drop_cols = {"ts", "is_synthetic", "book_synced", "data_quality"} | NON_FEATURE_KEYS
    x_cols = [
        c for c in sub.columns
        if c not in drop_cols and pd.api.types.is_numeric_dtype(sub[c])
    ]
    X = sub[x_cols].astype(float)
    # Constant columns carry no information and destabilise linear models.
    keep = [c for c in X.columns if X[c].nunique(dropna=True) > 1]
    X = X[keep]

    return Dataset(
        X=X,
        y=y,
        ts=sub["ts"].astype("int64"),
        entry=entry_series,
        exit=exit_series,
        horizon_s=horizon_s,
        meta={
            "dropped_no_future_tick": dropped_no_future,
            "ties_dropped": ties if drop_ties else 0,
            "tie_fraction": round(ties / max(len(features), 1), 5),
            "rows_before_filters": len(features),
            "contains_synthetic": bool(sub.get("is_synthetic", pd.Series([False])).any())
            if "is_synthetic" in sub else False,
            "label_tolerance_ms": tolerance_ms,
        },
    )


def impute(X: pd.DataFrame) -> pd.DataFrame:
    """Replace missing values causally-safely.

    Missing values here mean "not computable yet" (e.g. no 5s of history). They
    are filled with 0 after the model's scaler, never with a future or global
    statistic that would leak information across the split boundary.
    """
    return X.replace([np.inf, -np.inf], np.nan).fillna(0.0)


def horizons_from_string(value: str) -> list[float]:
    return [float(v.strip()) for v in value.split(",") if v.strip()]


def label_balance(y: Sequence[int]) -> dict[str, Any]:
    arr = np.asarray(y)
    n = len(arr)
    ups = int(arr.sum())
    return {
        "n": n,
        "up": ups,
        "down": n - ups,
        "up_fraction": round(ups / n, 5) if n else None,
        # A market that is 50/50 at this horizon is the null hypothesis.
        "baseline_accuracy": round(max(ups, n - ups) / n, 5) if n else None,
    }
