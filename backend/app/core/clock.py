"""Time helpers.

The whole system works in milliseconds since the Unix epoch (UTC). Two clocks
are recorded for every inbound message:

* ``exchange_timestamp`` - the venue's own timestamp (as reported by the venue)
* ``server_timestamp``   - when this process received the message

``latency_ms = server_timestamp - exchange_timestamp``. Note that this includes
any clock skew between the venue and this host; the market-data engine keeps a
running estimate of that skew from REST time endpoints so latency can be
interpreted honestly rather than silently trusted.
"""

from __future__ import annotations

import time
from datetime import datetime, timezone


def now_ms() -> int:
    return time.time_ns() // 1_000_000


def now_ns() -> int:
    return time.time_ns()


def monotonic_ms() -> float:
    return time.monotonic() * 1000.0


def to_dt(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)


def from_dt(dt: datetime) -> int:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)
