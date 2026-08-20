"""Time handling.

Two clocks are kept apart on purpose:

* ``now_ms()``      wall clock in milliseconds, comparable with Bybit timestamps;
* ``mono()``        monotonic seconds, used for every timeout and age measurement
                    so that an NTP step can never make a stale feed look fresh.

``Clock`` is injectable so the test-suite can run deterministically without
sleeping.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field


def now_ms() -> float:
    return time.time() * 1000.0


def mono() -> float:
    return time.monotonic()


class Clock:
    """Real clock."""

    def now_ms(self) -> float:
        return now_ms()

    def mono(self) -> float:
        return mono()


@dataclass
class ManualClock(Clock):
    """Deterministic clock for tests."""

    wall_ms: float = 1_700_000_000_000.0
    mono_s: float = 1_000.0

    def now_ms(self) -> float:
        return self.wall_ms

    def mono(self) -> float:
        return self.mono_s

    def advance(self, seconds: float) -> None:
        self.mono_s += seconds
        self.wall_ms += seconds * 1000.0


@dataclass
class SkewTracker:
    """Exchange-vs-local clock offset and one-way latency, both smoothed."""

    alpha: float = 0.15
    latency_ms: float = 0.0
    latency_peak_ms: float = 0.0
    skew_ms: float = 0.0
    samples: int = 0
    _last_reset: float = field(default=0.0)

    def observe(self, exchange_ms: float, local_ms: float) -> float:
        """Record one message and return its observed latency in milliseconds."""
        raw = local_ms - exchange_ms
        if self.samples == 0:
            self.latency_ms = max(raw, 0.0)
            self.skew_ms = raw
        else:
            self.latency_ms += self.alpha * (max(raw, 0.0) - self.latency_ms)
            self.skew_ms += self.alpha * (raw - self.skew_ms)
        self.samples += 1
        self.latency_peak_ms = max(self.latency_peak_ms * 0.999, raw)
        return raw

    def reset_peak(self) -> None:
        self.latency_peak_ms = self.latency_ms
