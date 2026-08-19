"""Regime classification.

A hypothesis that works in a quiet range and loses in a volatility burst is not
a broken hypothesis — it is a conditional one, and the difference only shows up
if the regime it was measured in is recorded alongside it.  Every feature
snapshot, signal and trade therefore carries the regime that was in force when
it was produced, and the validation lab reports stability *across* regimes
rather than averaging them into one number.

Classification uses the symbol's own recent history rather than fixed
thresholds: 30 bps of 60-second volatility is quiet for DOGE and a storm for
BTC, so the percentile is taken against that symbol's own distribution.
"""

from __future__ import annotations

from collections import deque
from typing import TYPE_CHECKING, Any

from ..config import Config
from ..domain import Regime

if TYPE_CHECKING:  # pragma: no cover - import cycle only matters for typing
    from .engine import SymbolHistory


class RegimeClassifier:
    def __init__(self, config: Config) -> None:
        self.config = config
        self.cadence_ms = config.features.cadence_ms
        #: Enough readings to make a percentile meaningful without letting a
        #: whole day of a different market dominate today's classification.
        capacity = 2000
        self._vol_history: dict[str, deque[float]] = {}
        self._capacity = capacity
        self.current: dict[str, Regime] = {}

    def classify(self, symbol: str, history: SymbolHistory, values: dict[str, float]) -> Regime:
        lookback_ms = self.config.regime.vol_lookback_s * 1000
        trend_ms = self.config.regime.trend_lookback_s * 1000

        vol = history.volatility_bps(lookback_ms)
        trend = history.return_bps(trend_ms)
        if vol is None or trend is None:
            self.current[symbol] = Regime.UNKNOWN
            return Regime.UNKNOWN

        readings = self._vol_history.setdefault(symbol, deque(maxlen=self._capacity))
        readings.append(vol)
        if len(readings) < 40:
            # Too little history to say anything about where this sits in the
            # distribution. UNKNOWN is an honest answer; a guess is not.
            self.current[symbol] = Regime.UNKNOWN
            return Regime.UNKNOWN

        percentile = self._percentile_of(readings, vol)
        values["vol_percentile"] = percentile
        # Trend measured in units of the volatility that produced it: a 20 bps
        # move is a trend in a quiet market and noise in a fast one.
        trend_z = trend / vol if vol > 0 else 0.0
        values["trend_z"] = trend_z

        if percentile >= self.config.regime.high_vol_percentile:
            regime = Regime.HIGH_VOL
        elif abs(trend_z) >= 1.0:
            regime = Regime.TRENDING_UP if trend_z > 0 else Regime.TRENDING_DOWN
        elif percentile <= self.config.regime.low_vol_percentile:
            regime = Regime.QUIET_RANGE
        else:
            regime = Regime.NORMAL_RANGE

        self.current[symbol] = regime
        return regime

    @staticmethod
    def _percentile_of(readings: deque[float], value: float) -> float:
        count = len(readings)
        if count == 0:
            return 50.0
        below = sum(1 for reading in readings if reading < value)
        return below / count * 100.0

    def snapshot(self) -> dict[str, Any]:
        return {
            "regimes": {symbol: regime.value for symbol, regime in self.current.items()},
            "counts": self._counts(),
        }

    def _counts(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        for regime in self.current.values():
            counts[regime.value] = counts.get(regime.value, 0) + 1
        return counts
