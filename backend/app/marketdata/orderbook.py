"""A real local order book with sequence validation and automatic resync.

Procedure (matches Binance's documented diff-depth algorithm, and generalises
to any venue that publishes `[first_update_id, final_update_id]` ranges):

1. buffer incoming diff events;
2. fetch a REST depth snapshot (`last_update_id`);
3. discard buffered events whose `final_update_id <= last_update_id`;
4. the first applied event must satisfy
   `first_update_id <= last_update_id + 1 <= final_update_id`;
5. every subsequent event must be contiguous with the previous one
   (`first_update_id == prev_final_update_id + 1`, or, when the venue provides
   an explicit `prev_final_update_id`, that field must match);
6. any violation marks the book DESYNCED and triggers a resync.

While the book is not synced, `synced` is False and the signal engine refuses to
trade. Nothing is interpolated or guessed.
"""

from __future__ import annotations

import heapq
from dataclasses import dataclass
from typing import Iterable

from app.core.clock import now_ms
from app.core.logging_conf import get_logger
from app.marketdata.types import DepthSnapshot, DepthUpdate

log = get_logger(__name__)


@dataclass(slots=True)
class BookLevel:
    price: float
    quantity: float


@dataclass
class BookStats:
    applied_updates: int = 0
    buffered_updates: int = 0
    dropped_stale: int = 0
    gaps_detected: int = 0
    resyncs: int = 0
    last_gap_ts: int | None = None
    last_resync_ts: int | None = None
    last_update_ts: int | None = None


class LocalOrderBook:
    """Maintains full depth for one symbol on one venue."""

    def __init__(self, exchange: str, symbol: str, max_buffer: int = 5000) -> None:
        self.exchange = exchange
        self.symbol = symbol
        self.bids: dict[float, float] = {}
        self.asks: dict[float, float] = {}
        self.last_update_id: int = 0
        self.synced: bool = False
        self.desync_reason: str | None = "not initialised"
        self.stats = BookStats()
        self._buffer: list[DepthUpdate] = []
        self._max_buffer = max_buffer
        self._awaiting_snapshot: bool = True
        self._first_applied: bool = False

    # ------------------------------------------------------------- lifecycle
    def begin_resync(self, reason: str) -> None:
        """Mark the book unusable and start buffering diffs again."""
        if self.synced:
            log.warning(
                "orderbook.desync", exchange=self.exchange, symbol=self.symbol,
                reason=reason,
            )
        self.synced = False
        self.desync_reason = reason
        self._awaiting_snapshot = True
        self._first_applied = False
        self._buffer.clear()
        self.stats.resyncs += 1
        self.stats.last_resync_ts = now_ms()

    def buffer(self, update: DepthUpdate) -> None:
        if len(self._buffer) >= self._max_buffer:
            # Snapshot is taking far too long; restart the cycle rather than
            # growing without bound.
            self._buffer = self._buffer[-(self._max_buffer // 2) :]
        self._buffer.append(update)
        self.stats.buffered_updates += 1

    def apply_snapshot(self, snap: DepthSnapshot) -> bool:
        """Install a REST snapshot, then replay the compatible buffer.

        Returns True when the book ends up synced.
        """
        self.bids = {p: q for p, q in snap.bids if q > 0}
        self.asks = {p: q for p, q in snap.asks if q > 0}
        self.last_update_id = snap.last_update_id
        self._awaiting_snapshot = False
        self._first_applied = False

        pending = [u for u in self._buffer if u.final_update_id > snap.last_update_id]
        self.stats.dropped_stale += len(self._buffer) - len(pending)
        self._buffer.clear()

        for upd in pending:
            if not self._apply_checked(upd):
                # Snapshot is older than the stream gap: caller must refetch.
                return False
        if not pending:
            # No diffs yet: the snapshot itself is a valid book, but we are not
            # synced until a contiguous diff confirms the stream position.
            self.synced = bool(self.bids and self.asks)
            self.desync_reason = None if self.synced else "empty snapshot"
        return self.synced

    def apply(self, upd: DepthUpdate) -> bool:
        """Apply a live diff. Returns False if a resync is required."""
        if self._awaiting_snapshot:
            self.buffer(upd)
            return True
        return self._apply_checked(upd)

    # -------------------------------------------------------------- internal
    def _apply_checked(self, upd: DepthUpdate) -> bool:
        if upd.final_update_id <= self.last_update_id:
            self.stats.dropped_stale += 1
            return True  # already contained in the snapshot

        if not self._first_applied:
            ok = upd.first_update_id <= self.last_update_id + 1 <= upd.final_update_id
            if not ok:
                self.stats.gaps_detected += 1
                self.stats.last_gap_ts = now_ms()
                self.begin_resync(
                    f"snapshot/stream mismatch U={upd.first_update_id} "
                    f"u={upd.final_update_id} lastUpdateId={self.last_update_id}"
                )
                return False
            self._first_applied = True
        else:
            expected = self.last_update_id + 1
            prev = upd.prev_final_update_id
            contiguous = (
                prev == self.last_update_id if prev is not None
                else upd.first_update_id <= expected <= upd.final_update_id
            )
            if not contiguous:
                self.stats.gaps_detected += 1
                self.stats.last_gap_ts = now_ms()
                self.begin_resync(
                    f"sequence gap: expected {expected}, got U={upd.first_update_id} "
                    f"u={upd.final_update_id} pu={prev}"
                )
                return False

        self._apply_levels(self.bids, upd.bids)
        self._apply_levels(self.asks, upd.asks)
        self.last_update_id = upd.final_update_id
        self.stats.applied_updates += 1
        self.stats.last_update_ts = upd.server_ts

        if self.bids and self.asks:
            # A crossed book means our state is wrong; never publish it.
            if self.best_bid()[0] >= self.best_ask()[0]:
                self.stats.gaps_detected += 1
                self.begin_resync("crossed book after update")
                return False
            self.synced = True
            self.desync_reason = None
        else:
            self.synced = False
            self.desync_reason = "one side empty"
        return True

    @staticmethod
    def _apply_levels(
        side: dict[float, float], levels: Iterable[tuple[float, float]]
    ) -> None:
        for price, qty in levels:
            if qty <= 0:
                side.pop(price, None)
            else:
                side[price] = qty

    # ------------------------------------------------------------ accessors
    def best_bid(self) -> tuple[float, float]:
        if not self.bids:
            return (0.0, 0.0)
        p = max(self.bids)
        return (p, self.bids[p])

    def best_ask(self) -> tuple[float, float]:
        if not self.asks:
            return (0.0, 0.0)
        p = min(self.asks)
        return (p, self.asks[p])

    def top(self, n: int = 20) -> tuple[list[BookLevel], list[BookLevel]]:
        bids = [
            BookLevel(p, self.bids[p]) for p in heapq.nlargest(n, self.bids)
        ]
        asks = [
            BookLevel(p, self.asks[p]) for p in heapq.nsmallest(n, self.asks)
        ]
        return bids, asks

    def mid(self) -> float:
        bp, _ = self.best_bid()
        ap, _ = self.best_ask()
        if bp <= 0 or ap <= 0:
            return 0.0
        return (bp + ap) / 2.0

    def depth_notional(self, levels: int = 10) -> tuple[float, float]:
        bids, asks = self.top(levels)
        return (
            sum(lvl.price * lvl.quantity for lvl in bids),
            sum(lvl.price * lvl.quantity for lvl in asks),
        )

    def depth_qty(self, levels: int = 10) -> tuple[float, float]:
        bids, asks = self.top(levels)
        return (sum(lvl.quantity for lvl in bids), sum(lvl.quantity for lvl in asks))

    def depth_within_bps(self, bps: float) -> tuple[float, float]:
        """Resting quantity within `bps` of the mid on each side."""
        m = self.mid()
        if m <= 0:
            return (0.0, 0.0)
        band = m * bps / 10_000.0
        bid_qty = sum(q for p, q in self.bids.items() if p >= m - band)
        ask_qty = sum(q for p, q in self.asks.items() if p <= m + band)
        return (bid_qty, ask_qty)

    def snapshot_dict(self, levels: int = 20) -> dict:
        bids, asks = self.top(levels)
        return {
            "exchange": self.exchange,
            "symbol": self.symbol,
            "last_update_id": self.last_update_id,
            "synced": self.synced,
            "desync_reason": self.desync_reason,
            "bids": [[lvl.price, lvl.quantity] for lvl in bids],
            "asks": [[lvl.price, lvl.quantity] for lvl in asks],
            "bid_levels": len(self.bids),
            "ask_levels": len(self.asks),
            "ts": now_ms(),
        }
