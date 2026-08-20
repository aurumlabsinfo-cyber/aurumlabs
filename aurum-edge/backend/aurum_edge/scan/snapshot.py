"""The MarketSnapshot: one immutable, synchronised view of one symbol.

Every number a decision uses comes from a single instance of this object.  There
is no "RSI agent" or "order-flow agent" reaching back into the feed at a
slightly different moment - those are fields, not services.  The snapshot is
frozen, so nothing can mutate under a decision while it is being taken, and it
is the exact payload stored next to the trade it produced.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, asdict
from enum import Enum
from typing import Any


class DataQuality(str, Enum):
    OK = "OK"
    DEGRADED = "DEGRADED"      # usable to rank, not to trade
    BAD = "BAD"                # not usable at all


class FeedSource(str, Enum):
    BYBIT = "bybit"            # the real exchange
    REPLAY = "replay"          # recorded Bybit frames, replayed
    FAKE = "fake"              # deterministic test double

    @property
    def is_real(self) -> bool:
        return self is FeedSource.BYBIT


# Feature order is part of the model contract: live scoring and training read the
# same names in the same order, so a stored model can never be fed a shuffled
# vector.  Directional features are already signed by the candidate side.
FEATURE_NAMES: tuple[str, ...] = (
    "mom_250ms",
    "mom_1s",
    "mom_3s",
    "mom_5s",
    "mom_15s",
    "mom_60s",
    "ofi_1s",
    "ofi_5s",
    "imbalance_top",
    "imbalance_depth",
    "aggression_5s",
    "aggression_60s",
    "microprice_edge",
    "volume_accel",
    "vol_ratio",
    "spread_over_vol",
    "depth_ratio",
    "trade_rate",
    "oi_change",
)

# Features are ratios; a thin window can make one explode.  Bounding them keeps
# any single number from deciding a trade on its own.
FEATURE_CLIP = 5.0


@dataclass(frozen=True)
class MarketSnapshot:
    # identity ------------------------------------------------------------
    symbol: str
    source: str
    seq: int

    # time ----------------------------------------------------------------
    ts_exchange_ms: float
    ts_local_ms: float
    latency_ms: float
    book_age_ms: float
    trade_age_ms: float

    # price ---------------------------------------------------------------
    bid: float
    ask: float
    bid_size: float
    ask_size: float
    spread: float
    spread_bps: float
    mid: float
    last_price: float
    microprice: float
    microprice_edge_bps: float

    # returns, in basis points -------------------------------------------
    ret_250ms_bps: float
    ret_1s_bps: float
    ret_3s_bps: float
    ret_5s_bps: float
    ret_15s_bps: float
    ret_60s_bps: float

    # volatility ----------------------------------------------------------
    volatility_bps: float          # stdev of 1 s log returns over 60 s, in bps
    range_60s_bps: float

    # flow ----------------------------------------------------------------
    trades_5s: int
    trades_60s: int
    trade_rate_hz: float
    volume_5s_usd: float
    volume_60s_usd: float
    volume_acceleration: float     # 5 s rate vs 60 s rate, 1.0 = unchanged
    buy_volume_5s_usd: float
    sell_volume_5s_usd: float
    aggression_5s: float           # (buy - sell) / (buy + sell)
    aggression_60s: float
    ofi_1s: float                  # normalised order flow imbalance
    ofi_5s: float
    imbalance_top: float
    imbalance_depth: float

    # depth / liquidity ---------------------------------------------------
    depth_bid_usd: float
    depth_ask_usd: float
    book_state: str
    book_levels_bid: int
    book_levels_ask: int
    depth_topic: str
    book_top: tuple[tuple[float, float], ...]
    # cumulative notional available within N bps of the mid, per side.  This is
    # what lets slippage be estimated from the snapshot itself instead of
    # re-reading a book that has moved on since the decision was taken.
    depth_curve_bps: tuple[float, ...]
    depth_curve_bid_usd: tuple[float, ...]
    depth_curve_ask_usd: tuple[float, ...]

    # market context ------------------------------------------------------
    open_interest: float | None
    open_interest_change_bps: float
    funding_rate: float
    turnover_24h_usd: float

    # instrument ----------------------------------------------------------
    tick_size: float
    qty_step: float
    min_qty: float
    min_notional_usd: float
    max_leverage: float

    # quality -------------------------------------------------------------
    quality: str
    quality_score: float
    quality_reasons: tuple[str, ...]
    tradable: bool

    # ---------------------------------------------------------------- helpers
    @property
    def is_real(self) -> bool:
        return self.source == FeedSource.BYBIT.value

    def features(self, side: str) -> dict[str, float]:
        """Side-signed feature dict.  ``side`` is "LONG" or "SHORT".

        Every value is bounded to +/-``FEATURE_CLIP``.  Without that, a thin
        warm-up window makes a ratio explode, the logistic saturates at p=1.000
        and one outlier decides the trade - which is exactly how a model starts
        believing its own noise.
        """
        s = 1.0 if side == "LONG" else -1.0
        vol = max(self.volatility_bps, 0.5)
        raw = {
            "mom_250ms": s * self.ret_250ms_bps / vol,
            "mom_1s": s * self.ret_1s_bps / vol,
            "mom_3s": s * self.ret_3s_bps / vol,
            "mom_5s": s * self.ret_5s_bps / vol,
            "mom_15s": s * self.ret_15s_bps / vol,
            "mom_60s": s * self.ret_60s_bps / vol,
            "ofi_1s": s * self.ofi_1s,
            "ofi_5s": s * self.ofi_5s,
            "imbalance_top": s * self.imbalance_top,
            "imbalance_depth": s * self.imbalance_depth,
            "aggression_5s": s * self.aggression_5s,
            "aggression_60s": s * self.aggression_60s,
            "microprice_edge": s * self.microprice_edge_bps / max(self.spread_bps, 0.1),
            "volume_accel": _log_ratio(self.volume_acceleration),
            "vol_ratio": math.log1p(vol / 10.0),
            "spread_over_vol": self.spread_bps / vol,
            "depth_ratio": _log_ratio(
                (self.depth_bid_usd + 1.0) / (self.depth_ask_usd + 1.0) if s > 0
                else (self.depth_ask_usd + 1.0) / (self.depth_bid_usd + 1.0)
            ),
            "trade_rate": math.log1p(self.trade_rate_hz),
            "oi_change": s * self.open_interest_change_bps / 10.0,
        }
        return {
            name: max(-FEATURE_CLIP, min(FEATURE_CLIP, value))
            for name, value in raw.items()
        }

    def feature_vector(self, side: str) -> list[float]:
        feats = self.features(side)
        return [feats[name] for name in FEATURE_NAMES]

    def slippage_bps_for(self, side: str, notional_usd: float) -> float:
        """Expected slippage against the mid for a taker order of that size.

        Walks the snapshot's own depth curve: liquidity inside each bps band is
        assumed uniform, so the answer is the notional-weighted average distance
        from the mid.  When the order is bigger than the visible book the answer
        is the last band, penalised - never a silently optimistic number.
        """
        if notional_usd <= 0:
            return 0.0
        # A buy consumes asks, a sell consumes bids.
        curve = self.depth_curve_ask_usd if side in ("LONG", "Buy") else self.depth_curve_bid_usd
        bands = self.depth_curve_bps
        if not curve or not bands:
            return max(self.spread_bps, 0.0)

        remaining = notional_usd
        weighted = 0.0
        prev_cum = 0.0
        prev_bps = 0.0
        for bps, cum in zip(bands, curve):
            available = max(cum - prev_cum, 0.0)
            if available > 0:
                take = min(available, remaining)
                # uniform liquidity between prev_bps and bps
                fraction = take / available
                avg_bps = prev_bps + (bps - prev_bps) * fraction / 2.0
                weighted += take * avg_bps
                remaining -= take
            prev_cum, prev_bps = cum, bps
            if remaining <= 1e-9:
                break
        if remaining > 1e-9:
            # deeper than anything quoted: charge the far band plus a penalty
            weighted += remaining * (prev_bps * 1.5 + max(self.spread_bps, 1.0))
        return weighted / notional_usd

    def liquidity_usd(self, side: str) -> float:
        curve = self.depth_curve_ask_usd if side in ("LONG", "Buy") else self.depth_curve_bid_usd
        return curve[-1] if curve else 0.0

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["quality_reasons"] = list(self.quality_reasons)
        data["book_top"] = [list(level) for level in self.book_top]
        for key in ("depth_curve_bps", "depth_curve_bid_usd", "depth_curve_ask_usd"):
            data[key] = list(getattr(self, key))
        return data

    def compact(self) -> dict[str, Any]:
        """Small payload for the dashboard's opportunity table."""
        return {
            "symbol": self.symbol,
            "mid": self.mid,
            "bid": self.bid,
            "ask": self.ask,
            "spread_bps": round(self.spread_bps, 3),
            "ret_1s_bps": round(self.ret_1s_bps, 2),
            "ret_5s_bps": round(self.ret_5s_bps, 2),
            "ret_15s_bps": round(self.ret_15s_bps, 2),
            "volatility_bps": round(self.volatility_bps, 2),
            "ofi_5s": round(self.ofi_5s, 3),
            "imbalance_top": round(self.imbalance_top, 3),
            "aggression_5s": round(self.aggression_5s, 3),
            "volume_accel": round(self.volume_acceleration, 2),
            "volume_5s_usd": round(self.volume_5s_usd, 1),
            "open_interest": self.open_interest,
            "latency_ms": round(self.latency_ms, 1),
            "book_state": self.book_state,
            "quality": self.quality,
            "tradable": self.tradable,
            "source": self.source,
            "ts_exchange_ms": self.ts_exchange_ms,
        }


def _log_ratio(value: float) -> float:
    return math.log(max(value, 1e-6))
