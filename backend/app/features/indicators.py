"""Classical technical indicators.

Deliberately secondary. At a five second horizon, order flow and book pressure
dominate; EMA/RSI/Bollinger are included for context and as control features in
the model comparison, not as the primary signal.

Every function is causal: it reads only closed bars up to `now`.
"""

from __future__ import annotations

import math
from typing import Sequence

from app.features.rolling import Bar


def ema(values: Sequence[float], period: int) -> float | None:
    if len(values) < period or period <= 0:
        return None
    k = 2.0 / (period + 1.0)
    out = values[0]
    for v in values[1:]:
        out = v * k + out * (1 - k)
    return out


def rsi(values: Sequence[float], period: int = 14) -> float | None:
    if len(values) < period + 1:
        return None
    gains = losses = 0.0
    for i in range(1, period + 1):
        d = values[i] - values[i - 1]
        gains += max(d, 0.0)
        losses += max(-d, 0.0)
    avg_gain = gains / period
    avg_loss = losses / period
    for i in range(period + 1, len(values)):
        d = values[i] - values[i - 1]
        avg_gain = (avg_gain * (period - 1) + max(d, 0.0)) / period
        avg_loss = (avg_loss * (period - 1) + max(-d, 0.0)) / period
    if avg_loss == 0:
        return 100.0 if avg_gain > 0 else 50.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))


def vwap(bars: Sequence[Bar]) -> float | None:
    notional = sum(b.notional for b in bars)
    volume = sum(b.volume for b in bars)
    if volume <= 0:
        return None
    return notional / volume


def bollinger(
    values: Sequence[float], period: int = 20, k: float = 2.0
) -> tuple[float, float, float, float] | None:
    """(mid, upper, lower, z-score of the last value)."""
    if len(values) < period:
        return None
    window = values[-period:]
    mean = sum(window) / period
    var = sum((v - mean) ** 2 for v in window) / period
    sd = math.sqrt(var)
    if sd == 0:
        return (mean, mean, mean, 0.0)
    return (mean, mean + k * sd, mean - k * sd, (values[-1] - mean) / sd)


def atr(bars: Sequence[Bar], period: int = 14) -> float | None:
    if len(bars) < period + 1:
        return None
    trs: list[float] = []
    for i in range(1, len(bars)):
        prev_close = bars[i - 1].close
        b = bars[i]
        trs.append(
            max(b.high - b.low, abs(b.high - prev_close), abs(b.low - prev_close))
        )
    if len(trs) < period:
        return None
    out = sum(trs[:period]) / period
    for tr in trs[period:]:
        out = (out * (period - 1) + tr) / period
    return out
