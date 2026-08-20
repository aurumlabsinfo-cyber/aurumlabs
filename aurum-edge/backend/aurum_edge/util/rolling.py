"""Small fixed-cost rolling structures used by the market core.

Everything here is pure Python and allocation free on the hot path: the scanner
touches these thousands of times per second across the whole Bybit universe.
"""

from __future__ import annotations

import math
from collections import deque
from dataclasses import dataclass, field
from typing import Deque, Iterable


@dataclass
class TimeSeries:
    """(timestamp_ms, value) samples kept for a bounded window."""

    window_ms: float
    max_points: int = 4096
    points: Deque[tuple[float, float]] = field(default_factory=deque)

    def push(self, ts_ms: float, value: float) -> None:
        pts = self.points
        # out-of-order samples are dropped: a decision must never be built on a
        # series that went backwards in time.
        if pts and ts_ms < pts[-1][0]:
            return
        pts.append((ts_ms, value))
        cutoff = ts_ms - self.window_ms
        while pts and pts[0][0] < cutoff:
            pts.popleft()
        while len(pts) > self.max_points:
            pts.popleft()

    def last(self) -> float | None:
        return self.points[-1][1] if self.points else None

    def last_ts(self) -> float | None:
        return self.points[-1][0] if self.points else None

    def value_at_or_before(self, ts_ms: float) -> float | None:
        """Most recent value not newer than ``ts_ms`` (linear scan from the back).

        Windows are short (<= 120 s) so a backwards scan is cheaper than keeping
        an index, and it is exact rather than interpolated.
        """
        for ts, value in reversed(self.points):
            if ts <= ts_ms:
                return value
        return None

    def span_ms(self) -> float:
        if len(self.points) < 2:
            return 0.0
        return self.points[-1][0] - self.points[0][0]

    def __len__(self) -> int:  # pragma: no cover - trivial
        return len(self.points)


@dataclass
class RollingSum:
    """Sum of (timestamp_ms, value) over a trailing window."""

    window_ms: float
    total: float = 0.0
    count: int = 0
    points: Deque[tuple[float, float]] = field(default_factory=deque)

    def push(self, ts_ms: float, value: float) -> None:
        self.points.append((ts_ms, value))
        self.total += value
        self.count += 1
        self.expire(ts_ms)

    def expire(self, now_ms: float) -> None:
        cutoff = now_ms - self.window_ms
        pts = self.points
        while pts and pts[0][0] < cutoff:
            _, value = pts.popleft()
            self.total -= value
            self.count -= 1
        if not pts:
            self.total = 0.0
            self.count = 0

    def value(self, now_ms: float) -> float:
        self.expire(now_ms)
        return self.total

    def span_ms(self, now_ms: float) -> float:
        """How much history this window actually holds right now.

        Rates must be divided by this, not by the nominal window: during the
        first minute a 60 s window holds far less than 60 s, and dividing by 60
        would report an acceleration that is pure warm-up artefact.
        """
        self.expire(now_ms)
        if not self.points:
            return 0.0
        return max(now_ms - self.points[0][0], 0.0)

    def rate(self, now_ms: float, min_span_ms: float = 1_000.0) -> float:
        span = self.span_ms(now_ms)
        if span < min_span_ms:
            return 0.0
        return self.total / (span / 1000.0)


@dataclass
class WelfordVol:
    """Realised volatility of log returns sampled on a fixed grid."""

    window_ms: float
    samples: Deque[tuple[float, float]] = field(default_factory=deque)

    def push(self, ts_ms: float, ret: float) -> None:
        self.samples.append((ts_ms, ret))
        cutoff = ts_ms - self.window_ms
        while self.samples and self.samples[0][0] < cutoff:
            self.samples.popleft()

    def stdev(self) -> float:
        n = len(self.samples)
        if n < 3:
            return 0.0
        mean = sum(v for _, v in self.samples) / n
        var = sum((v - mean) ** 2 for _, v in self.samples) / (n - 1)
        return math.sqrt(max(var, 0.0))


def clamp(value: float, low: float, high: float) -> float:
    return low if value < low else high if value > high else value


def safe_div(num: float, den: float, default: float = 0.0) -> float:
    return num / den if den else default


def mean(values: Iterable[float]) -> float:
    values = list(values)
    return sum(values) / len(values) if values else 0.0
