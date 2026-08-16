"""Exchange adapter interface.

Adding a venue means implementing `ExchangeAdapter` and registering it in
`app.marketdata.registry`. Nothing else in the system changes: the order book,
feature engine, agents and signal engine only speak the canonical types in
`app.marketdata.types`.
"""

from __future__ import annotations

import abc
import asyncio
import random
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable

from app.core.clock import now_ms, monotonic_ms
from app.core.logging_conf import get_logger
from app.marketdata.types import DepthSnapshot

log = get_logger(__name__)

Emit = Callable[[Any], None]


class Capability(str, Enum):
    TRADES = "trades"
    BOOK_TICKER = "book_ticker"
    DEPTH_DIFF = "depth_diff"
    KLINES = "klines"
    FUNDING = "funding"
    OPEN_INTEREST = "open_interest"
    LIQUIDATIONS = "liquidations"


@dataclass
class ConnectionState:
    connected: bool = False
    connected_since: int | None = None
    last_message_ts: int | None = None
    messages: int = 0
    reconnects: int = 0
    last_error: str | None = None
    last_error_ts: int | None = None
    clock_skew_ms: float = 0.0
    streams: list[str] = field(default_factory=list)


class ExchangeAdapter(abc.ABC):
    """One venue, one symbol."""

    name: str = "abstract"
    capabilities: set[Capability] = set()
    #: True only for adapters that emit model-generated (non-market) data.
    synthetic: bool = False

    def __init__(self, symbol: str) -> None:
        self.symbol = symbol
        self.state = ConnectionState()
        self._stop = asyncio.Event()

    # ------------------------------------------------------------- streaming
    @abc.abstractmethod
    async def _stream_once(self, emit: Emit) -> None:
        """Open one connection and pump messages until it drops."""

    async def run(self, emit: Emit) -> None:
        """Run forever, reconnecting with exponential backoff + jitter."""
        delay = 1.0
        while not self._stop.is_set():
            try:
                self.state.streams = self.stream_names()
                await self._stream_once(emit)
                delay = 1.0  # clean close -> reconnect promptly
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - adapters must never die
                self.state.connected = False
                self.state.last_error = f"{type(exc).__name__}: {exc}"
                self.state.last_error_ts = now_ms()
                log.warning(
                    "adapter.stream_error", adapter=self.name, error=str(exc),
                )
            if self._stop.is_set():
                break
            self.state.connected = False
            self.state.reconnects += 1
            sleep_for = min(delay, 30.0) * (0.5 + random.random())
            await asyncio.sleep(sleep_for)
            delay = min(delay * 2, 30.0)

    def stop(self) -> None:
        self._stop.set()

    def stream_names(self) -> list[str]:
        return []

    def mark_message(self) -> None:
        self.state.messages += 1
        self.state.last_message_ts = now_ms()

    # ------------------------------------------------------------------ REST
    async def fetch_depth_snapshot(self, limit: int = 1000) -> DepthSnapshot:
        raise NotImplementedError(f"{self.name} has no depth snapshot endpoint")

    async def fetch_klines(
        self, interval: str = "1s", limit: int = 1000, end_ms: int | None = None
    ) -> list[dict]:
        raise NotImplementedError(f"{self.name} has no kline endpoint")

    async def fetch_server_time(self) -> int | None:
        return None

    async def measure_clock_skew(self) -> float | None:
        """Round-trip-corrected estimate of (venue clock - local clock) in ms."""
        t0 = monotonic_ms()
        local_before = now_ms()
        server = await self.fetch_server_time()
        if server is None:
            return None
        rtt = monotonic_ms() - t0
        skew = server - (local_before + rtt / 2.0)
        self.state.clock_skew_ms = skew
        return skew

    async def close(self) -> None:
        return None
