"""In-process async publish/subscribe bus.

The hot path (WebSocket reader -> order book -> features -> signals) must never
block on a slow consumer such as a browser WebSocket or the database writer.
Each subscriber therefore owns a bounded queue; when a subscriber falls behind,
its *oldest* messages are dropped and the drop is counted, rather than applying
back-pressure to the market-data reader.
"""

from __future__ import annotations

import asyncio
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any

from app.core.logging_conf import get_logger

log = get_logger(__name__)


class Topic:
    TICK = "tick"  # consolidated best bid/ask + mid
    TRADE = "trade"
    BOOK = "book"  # order book state summary
    DEPTH = "depth"  # raw sequenced diff, for book research/persistence
    FEATURES = "features"
    AGENTS = "agents"
    SIGNAL = "signal"  # signal created / state changed
    DECISION = "decision"  # every evaluated window, emitted or gated out
    HEALTH = "health"
    EVENT = "event"  # system events / errors


@dataclass
class Subscription:
    topic: str
    queue: asyncio.Queue
    dropped: int = 0
    _bus: "EventBus | None" = field(default=None, repr=False)

    async def __aenter__(self) -> "Subscription":
        return self

    async def __aexit__(self, *exc: Any) -> None:
        self.close()

    def close(self) -> None:
        if self._bus is not None:
            self._bus.unsubscribe(self)
            self._bus = None

    def __aiter__(self):
        return self

    async def __anext__(self):
        return await self.queue.get()


class EventBus:
    def __init__(self, maxsize: int = 256) -> None:
        self._subs: dict[str, list[Subscription]] = defaultdict(list)
        self._maxsize = maxsize
        self.published: dict[str, int] = defaultdict(int)

    def subscribe(self, topic: str, maxsize: int | None = None) -> Subscription:
        sub = Subscription(topic=topic, queue=asyncio.Queue(maxsize or self._maxsize))
        sub._bus = self
        self._subs[topic].append(sub)
        return sub

    def unsubscribe(self, sub: Subscription) -> None:
        subs = self._subs.get(sub.topic)
        if subs and sub in subs:
            subs.remove(sub)

    def publish(self, topic: str, message: Any) -> None:
        """Non-blocking fan-out. Safe to call from any coroutine."""
        self.published[topic] += 1
        for sub in self._subs.get(topic, ()):
            queue = sub.queue
            if queue.full():
                try:
                    queue.get_nowait()  # drop oldest
                    sub.dropped += 1
                except asyncio.QueueEmpty:  # pragma: no cover - race
                    pass
            try:
                queue.put_nowait(message)
            except asyncio.QueueFull:  # pragma: no cover - race
                sub.dropped += 1

    def subscriber_count(self, topic: str | None = None) -> int:
        if topic is not None:
            return len(self._subs.get(topic, ()))
        return sum(len(v) for v in self._subs.values())

    def stats(self) -> dict[str, Any]:
        return {
            "published": dict(self.published),
            "subscribers": {k: len(v) for k, v in self._subs.items() if v},
            "dropped": {
                k: sum(s.dropped for s in v) for k, v in self._subs.items() if v
            },
        }
