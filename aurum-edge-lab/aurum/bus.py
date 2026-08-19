"""Bounded fan-out event bus.

The rule from the blueprint: high-frequency ingestion must never be blocked by
database writes, research or the UI.  So publishing is non-blocking and every
subscriber owns its own bounded queue.  A slow subscriber drops *its own*
backlog and says so in its counters; it cannot apply backpressure to the socket
reader, and it cannot make another subscriber lose data.

Dropping is deliberate and visible, never silent: ``dropped`` per channel is
surfaced by ``/health`` and ``/diagnostics``.  Oldest events go first, because
for live state the newest event is the one that matters.
"""

from __future__ import annotations

import asyncio
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Generic, TypeVar

T = TypeVar("T")


@dataclass
class ChannelStats:
    name: str
    topic: str
    capacity: int
    published: int = 0
    delivered: int = 0
    dropped: int = 0
    high_water: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "topic": self.topic,
            "capacity": self.capacity,
            "published": self.published,
            "delivered": self.delivered,
            "dropped": self.dropped,
            "high_water": self.high_water,
            "drop_rate": round(self.dropped / self.published, 6) if self.published else 0.0,
        }


class Channel(Generic[T]):
    """One subscriber's bounded queue."""

    def __init__(self, name: str, topic: str, capacity: int) -> None:
        self._queue: asyncio.Queue[T] = asyncio.Queue(maxsize=capacity)
        self.stats = ChannelStats(name=name, topic=topic, capacity=capacity)

    def offer(self, item: T) -> bool:
        """Non-blocking publish.  Returns False when an old item was dropped."""
        self.stats.published += 1
        try:
            self._queue.put_nowait(item)
        except asyncio.QueueFull:
            try:
                self._queue.get_nowait()
                self._queue.task_done()
                self.stats.dropped += 1
            except asyncio.QueueEmpty:  # pragma: no cover - racing consumer drained it
                pass
            try:
                self._queue.put_nowait(item)
            except asyncio.QueueFull:  # pragma: no cover - consumer refilled it
                self.stats.dropped += 1
                return False
            self.stats.high_water = self.stats.capacity
            return False
        size = self._queue.qsize()
        if size > self.stats.high_water:
            self.stats.high_water = size
        return True

    async def get(self) -> T:
        item = await self._queue.get()
        self._queue.task_done()
        self.stats.delivered += 1
        return item

    async def get_batch(self, max_items: int, timeout: float | None = None) -> list[T]:
        """Wait for at least one item, then drain up to ``max_items``."""
        try:
            if timeout is None:
                first = await self._queue.get()
            else:
                first = await asyncio.wait_for(self._queue.get(), timeout)
        except (TimeoutError, asyncio.TimeoutError):
            return []
        self._queue.task_done()
        self.stats.delivered += 1
        batch = [first]
        while len(batch) < max_items:
            try:
                item = self._queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            self._queue.task_done()
            self.stats.delivered += 1
            batch.append(item)
        return batch

    def qsize(self) -> int:
        return self._queue.qsize()

    def drain(self) -> list[T]:
        items: list[T] = []
        while True:
            try:
                items.append(self._queue.get_nowait())
            except asyncio.QueueEmpty:
                break
            self._queue.task_done()
            self.stats.delivered += 1
        return items


class EventBus:
    """Topic fan-out.  Publishing is synchronous, non-blocking and cheap."""

    def __init__(self, default_capacity: int = 10_000) -> None:
        self.default_capacity = default_capacity
        self._channels: dict[str, list[Channel[Any]]] = defaultdict(list)

    def subscribe(self, topic: str, name: str, capacity: int | None = None) -> Channel[Any]:
        channel: Channel[Any] = Channel(name, topic, capacity or self.default_capacity)
        self._channels[topic].append(channel)
        return channel

    def unsubscribe(self, channel: Channel[Any]) -> None:
        subscribers = self._channels.get(channel.stats.topic)
        if subscribers and channel in subscribers:
            subscribers.remove(channel)

    def publish(self, topic: str, item: Any) -> int:
        """Deliver to every subscriber.  Returns how many took it without a drop."""
        accepted = 0
        for channel in self._channels.get(topic, ()):
            if channel.offer(item):
                accepted += 1
        return accepted

    def subscriber_count(self, topic: str) -> int:
        return len(self._channels.get(topic, ()))

    def stats(self) -> list[dict[str, Any]]:
        return [
            {**channel.stats.to_dict(), "queued": channel.qsize()}
            for channels in self._channels.values()
            for channel in channels
        ]

    def total_dropped(self) -> int:
        return sum(c.stats.dropped for channels in self._channels.values() for c in channels)


# Topic names, in one place so a typo is an import error rather than silence.
TOPIC_MARKET_EVENT = "market.event"
TOPIC_BOOK = "market.book"
TOPIC_TRADE = "market.trade"
TOPIC_FEATURES = "features.snapshot"
TOPIC_SIGNAL = "execution.signal"
TOPIC_TRADE_CLOSED = "execution.trade"
TOPIC_WALLET = "wallet.update"
TOPIC_RESEARCH = "research.update"
TOPIC_SYSTEM = "system.event"
