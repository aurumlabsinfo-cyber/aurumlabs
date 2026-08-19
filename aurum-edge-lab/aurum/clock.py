"""Clock and latency tracking.

The venue stamps events with *its* clock.  If the local clock drifts, every
latency number and every "how old is this book?" check drifts with it, and the
data-quality gate starts lying in whichever direction the drift points.

:class:`Clock` keeps a measured offset (venue minus local) obtained from the
venue's time endpoint, applies it when reading venue timestamps, and exposes the
drift so ``/health`` can show it.
"""

from __future__ import annotations

import statistics
import time
from collections import deque
from dataclasses import dataclass, field


def now_ms() -> int:
    return int(time.time() * 1000)


def monotonic_ms() -> float:
    return time.monotonic() * 1000.0


@dataclass
class Clock:
    """Local clock corrected by a measured venue offset."""

    offset_ms: float = 0.0
    last_sync_ms: int = 0
    sync_count: int = 0
    round_trip_ms: float = 0.0
    _samples: deque[float] = field(default_factory=lambda: deque(maxlen=16))

    def now_ms(self) -> int:
        return int(time.time() * 1000)

    def venue_now_ms(self) -> int:
        """Best estimate of the venue's current clock."""
        return int(time.time() * 1000 + self.offset_ms)

    def observe(self, venue_ms: int, sent_ms: float, received_ms: float) -> float:
        """Record one time-sync round trip and update the offset.

        ``venue_ms`` is the venue's reported time; ``sent_ms``/``received_ms``
        bracket the request on the local clock.  The midpoint of the round trip
        is the local instant the venue's stamp corresponds to, which removes
        half the network delay from the estimate (NTP's trick, minus the
        statistical filtering).
        """
        self.round_trip_ms = received_ms - sent_ms
        local_mid = (sent_ms + received_ms) / 2.0
        sample = venue_ms - local_mid
        self._samples.append(sample)
        # Median over the recent samples: one delayed response should not move
        # the correction the whole feed is judged against.
        self.offset_ms = statistics.median(self._samples)
        self.last_sync_ms = self.now_ms()
        self.sync_count += 1
        return self.offset_ms

    def age_ms(self, venue_ts_ms: int) -> float:
        """How old a venue-stamped event is, in local terms."""
        return self.venue_now_ms() - venue_ts_ms

    def to_dict(self) -> dict[str, float | int]:
        return {
            "offset_ms": round(self.offset_ms, 2),
            "round_trip_ms": round(self.round_trip_ms, 2),
            "last_sync_ms": self.last_sync_ms,
            "sync_count": self.sync_count,
            "samples": len(self._samples),
        }


@dataclass
class LatencyTracker:
    """Rolling latency distribution for one feed or symbol."""

    window: int = 512
    _values: deque[float] = field(default_factory=lambda: deque(maxlen=512))

    def __post_init__(self) -> None:
        self._values = deque(maxlen=self.window)

    def record(self, latency_ms: float) -> None:
        self._values.append(float(latency_ms))

    @property
    def count(self) -> int:
        return len(self._values)

    @property
    def last(self) -> float:
        return self._values[-1] if self._values else 0.0

    @property
    def mean(self) -> float:
        return statistics.fmean(self._values) if self._values else 0.0

    def percentile(self, pct: float) -> float:
        if not self._values:
            return 0.0
        ordered = sorted(self._values)
        idx = min(len(ordered) - 1, max(0, int(round(pct / 100.0 * (len(ordered) - 1)))))
        return ordered[idx]

    def to_dict(self) -> dict[str, float]:
        return {
            "count": self.count,
            "last_ms": round(self.last, 2),
            "mean_ms": round(self.mean, 2),
            "p50_ms": round(self.percentile(50), 2),
            "p95_ms": round(self.percentile(95), 2),
            "p99_ms": round(self.percentile(99), 2),
        }
