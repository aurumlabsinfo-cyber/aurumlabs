"""Feed adapter interface.

An adapter's whole job is to turn one venue's wire format into the normalised
objects below and to report honestly what state it is in.  It does not maintain
order books, judge data quality or decide anything — those live above it, so
that adding a second venue never means re-implementing them.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable

from ..domain import FeedState, MarketEvent

EventCallback = Callable[[MarketEvent], None]
StateCallback = Callable[[str, FeedState, str], None]


@dataclass(slots=True)
class DepthSnapshot:
    """REST order-book snapshot used to seed or resynchronise a local book."""

    symbol: str
    last_update_id: int
    ts_ms: int
    recv_ms: int
    bids: list[tuple[float, float]]
    asks: list[tuple[float, float]]


@dataclass(slots=True)
class DepthUpdate:
    """One incremental depth event.

    ``first_id``/``final_id``/``prev_final_id`` are the venue's sequence fields.
    USD-M futures carry all three (``U``, ``u``, ``pu``); ``pu`` is what lets a
    consumer detect a dropped event without waiting for a mismatch to show up as
    a crossed book, and it is the field tutorials written for spot leave out.
    """

    symbol: str
    ts_ms: int
    recv_ms: int
    first_id: int
    final_id: int
    prev_final_id: int
    bids: list[tuple[float, float]]
    asks: list[tuple[float, float]]


@dataclass
class FeedStats:
    connected_since_ms: int = 0
    messages: int = 0
    reconnects: int = 0
    errors: int = 0
    last_message_ms: int = 0
    last_error: str = ""
    subscriptions: int = 0
    connections: int = 0
    endpoint: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "connected_since_ms": self.connected_since_ms,
            "messages": self.messages,
            "reconnects": self.reconnects,
            "errors": self.errors,
            "last_message_ms": self.last_message_ms,
            "last_error": self.last_error,
            "subscriptions": self.subscriptions,
            "connections": self.connections,
            "endpoint": self.endpoint,
        }


class MarketFeed(ABC):
    """Live or replayed market data for a fixed set of symbols."""

    #: ``live`` feeds are permitted in production; anything else is not.
    kind: str = "live"

    def __init__(self, symbols: list[str]) -> None:
        self.symbols = list(symbols)
        self.stats = FeedStats()
        self._on_event: EventCallback | None = None
        self._on_state: StateCallback | None = None
        self._state = FeedState.DISCONNECTED

    @property
    def state(self) -> FeedState:
        return self._state

    def on_event(self, callback: EventCallback) -> None:
        self._on_event = callback

    def on_state_change(self, callback: StateCallback) -> None:
        self._on_state = callback

    def _emit(self, event: MarketEvent) -> None:
        self.stats.messages += 1
        self.stats.last_message_ms = event.recv_ms
        if self._on_event is not None:
            self._on_event(event)

    def _set_state(self, state: FeedState, detail: str = "", symbol: str = "*") -> None:
        self._state = state
        if self._on_state is not None:
            self._on_state(symbol, state, detail)

    @abstractmethod
    async def start(self) -> None: ...

    @abstractmethod
    async def stop(self) -> None: ...

    @abstractmethod
    async def fetch_depth_snapshot(self, symbol: str, limit: int) -> DepthSnapshot: ...

    async def sync_time(self) -> int | None:
        """Venue server time in ms, or None when the venue exposes none."""
        return None

    async def verify_endpoints(self) -> dict[str, Any]:
        """Check that the configured endpoints answer, before anything depends
        on them.  Reported by ``/health`` so a routing change is visible at once
        rather than as an unexplained silence."""
        return {"checked": False, "reason": "adapter does not implement endpoint verification"}
