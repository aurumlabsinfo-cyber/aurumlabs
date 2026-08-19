"""Local L2 order book with sequence validation and deterministic resync.

The venue sends a snapshot and a stream of diffs.  Getting the join between them
wrong produces a book that looks fine and is quietly wrong, which is worse than
one that is obviously broken — so every rule below fails *loudly* into
``DESYNCED`` and refuses to serve data until a fresh snapshot arrives.

The USD-M futures sequencing rules, which differ from spot:

* Drop any diff whose ``u`` is older than the snapshot's ``lastUpdateId``.
* The first diff applied must satisfy ``U <= lastUpdateId`` and ``u >= lastUpdateId``.
* Every later diff must satisfy ``pu == previous u``.  ``pu`` is the futures-only
  field that catches a dropped event immediately; a spot-style implementation
  that only checks ``U == previous u + 1`` misses gaps this stream can produce.

A book that never receives its qualifying first diff, or whose ``pu`` chain
breaks, is not "probably fine" — it is desynced, and the data-quality gate blocks
trading on that symbol until a resync completes.
"""

from __future__ import annotations

import heapq
from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum
from typing import Any

from ..adapters.base import DepthSnapshot, DepthUpdate
from ..domain import BookLevel, BookSnapshot, Side

#: ``(ts_ms, added_bid, removed_bid, added_ask, removed_ask)``
LiquidityCallback = Callable[[int, float, float, float, float], None]


class BookState(str, Enum):
    EMPTY = "EMPTY"
    BUFFERING = "BUFFERING"     # diffs arriving, waiting for the REST snapshot
    READY = "READY"
    DESYNCED = "DESYNCED"


@dataclass
class BookStats:
    updates_applied: int = 0
    updates_buffered: int = 0
    updates_dropped_old: int = 0
    sequence_gaps: int = 0
    resyncs: int = 0
    refreshes: int = 0
    refresh_rollbacks: int = 0
    snapshots_applied: int = 0
    crossed_events: int = 0
    last_gap_detail: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "updates_applied": self.updates_applied,
            "updates_buffered": self.updates_buffered,
            "updates_dropped_old": self.updates_dropped_old,
            "sequence_gaps": self.sequence_gaps,
            "resyncs": self.resyncs,
            "refreshes": self.refreshes,
            "refresh_rollbacks": self.refresh_rollbacks,
            "snapshots_applied": self.snapshots_applied,
            "crossed_events": self.crossed_events,
            "last_gap_detail": self.last_gap_detail,
        }


class OrderBook:
    """One symbol's book.  Not thread-safe: it is owned by the ingest task."""

    #: Buffered diffs while waiting for a snapshot.  Beyond this the snapshot is
    #: hopeless anyway and we start again rather than replaying stale history.
    MAX_BUFFER = 2000

    def __init__(
        self,
        symbol: str,
        max_levels: int = 1000,
        on_liquidity: LiquidityCallback | None = None,
    ) -> None:
        self.symbol = symbol.upper()
        self.max_levels = max_levels
        self.state = BookState.EMPTY
        self.stats = BookStats()
        #: Called with every applied diff's liquidity accounting.  It lives here
        #: rather than in the caller because the sign of a size change is only
        #: knowable against the previous resting size, which the book owns — and
        #: because diffs replayed after a resync must be accounted for too.
        self.on_liquidity = on_liquidity

        self._bids: dict[float, float] = {}
        self._asks: dict[float, float] = {}
        self._buffer: list[DepthUpdate] = []
        #: True when the book holds a snapshot that no diff has joined yet.
        self._awaiting_join = False

        self.last_update_id = 0
        self.snapshot_ts_ms = 0
        self.ts_ms = 0
        self.recv_ms = 0
        self._version = 0
        self._cached_view: tuple[int, int, BookSnapshot] | None = None

    # ------------------------------------------------------------- accessors

    @property
    def ready(self) -> bool:
        return self.state is BookState.READY

    @property
    def levels(self) -> int:
        return min(len(self._bids), len(self._asks))

    def top(self, depth: int = 10) -> BookSnapshot:
        """Top ``depth`` levels per side.

        Cached on ``(version, depth)``: the feature engine asks four times a
        second per symbol while diffs land ten times a second, so recomputing
        the sorted view on every read would be pure waste.
        """
        if self._cached_view is not None:
            version, cached_depth, snapshot = self._cached_view
            if version == self._version and cached_depth == depth:
                return snapshot

        bid_prices = heapq.nlargest(depth, self._bids) if self._bids else []
        ask_prices = heapq.nsmallest(depth, self._asks) if self._asks else []
        bids = [BookLevel(p, self._bids[p]) for p in bid_prices]
        asks = [BookLevel(p, self._asks[p]) for p in ask_prices]
        crossed = bool(bids and asks and bids[0].price >= asks[0].price)
        snapshot = BookSnapshot(
            symbol=self.symbol,
            ts_ms=self.ts_ms,
            recv_ms=self.recv_ms,
            bids=bids,
            asks=asks,
            last_update_id=self.last_update_id,
            is_crossed=crossed,
        )
        self._cached_view = (self._version, depth, snapshot)
        return snapshot

    def resting_qty(self, side: Side, price: float) -> float:
        """Size currently resting at ``price``.  Used to sign a depth diff into
        liquidity added versus liquidity removed, which is only knowable before
        the diff is applied."""
        book = self._bids if side is Side.BUY else self._asks
        return book.get(price, 0.0)

    def depth_notional(self, side: Side, levels: int) -> float:
        book = self._bids if side is Side.BUY else self._asks
        prices = heapq.nlargest(levels, book) if side is Side.BUY else heapq.nsmallest(levels, book)
        return sum(p * book[p] for p in prices)

    # -------------------------------------------------------------- mutation

    def _joins(self, update: DepthUpdate) -> bool:
        """May ``update`` be the first diff applied after a snapshot?

        Two ways it legally can: it straddles ``lastUpdateId`` (the documented
        rule), or its ``pu`` says it continues exactly from it — which happens
        whenever the snapshot lands on an event boundary.  Rejecting the second
        case sends the book into a resync loop against a perfectly healthy feed.
        """
        straddles = update.first_id <= self.last_update_id <= update.final_id
        continues = update.prev_final_id == self.last_update_id
        return straddles or continues

    def apply_snapshot(self, snapshot: DepthSnapshot) -> bool:
        """Seed the book from a snapshot and rejoin the diff stream.

        Returns True when the book is usable.  A snapshot with no buffered diffs
        is the normal case — it means the snapshot arrived before the first diff
        did — and yields a valid book whose *next* diff has to pass the join
        test.  False means the buffered diffs prove a hole the snapshot cannot
        bridge; the caller should fetch a fresher snapshot, and the pending
        diffs are kept so it has something to join onto.
        """
        self._bids = {p: q for p, q in snapshot.bids if q > 0}
        self._asks = {p: q for p, q in snapshot.asks if q > 0}
        self.last_update_id = snapshot.last_update_id
        self.snapshot_ts_ms = snapshot.ts_ms
        self.ts_ms = snapshot.ts_ms
        self.recv_ms = snapshot.recv_ms
        self.stats.snapshots_applied += 1
        self._bump_version()

        pending = [u for u in self._buffer if u.final_id >= snapshot.last_update_id]
        self.stats.updates_dropped_old += len(self._buffer) - len(pending)

        self.state = BookState.READY
        self._awaiting_join = True
        for update in pending:
            if self._awaiting_join:
                if not self._joins(update):
                    # The snapshot predates the buffered diffs with a gap in
                    # between. Hold the diffs; a fresher snapshot can bridge.
                    self._buffer = pending
                    self.state = BookState.BUFFERING
                    self.stats.last_gap_detail = (
                        f"snapshot lastUpdateId={snapshot.last_update_id} does not bridge to "
                        f"U={update.first_id}/pu={update.prev_final_id}"
                    )
                    return False
                self._awaiting_join = False
                self._apply_levels(update)
                continue
            if not self._apply_sequenced(update):
                self._buffer = []
                return False
        self._buffer = []
        return True

    def apply_update(self, update: DepthUpdate) -> bool:
        """Apply one diff.  Returns False when the book went out of sync."""
        if self.state in (BookState.EMPTY, BookState.BUFFERING):
            # Diffs are arriving but no snapshot has bridged them yet. Keeping
            # them is the whole point: they are what the snapshot joins onto.
            self.state = BookState.BUFFERING
            self._buffer.append(update)
            self.stats.updates_buffered += 1
            if len(self._buffer) > self.MAX_BUFFER:
                # Snapshot never arrived or never bridged: keep the newest half
                # so a late snapshot still has recent diffs to join onto.
                self._buffer = self._buffer[-(self.MAX_BUFFER // 2) :]
            return True
        if self.state is BookState.DESYNCED:
            return False
        return self._apply_sequenced(update)

    def _apply_sequenced(self, update: DepthUpdate) -> bool:
        if update.final_id <= self.last_update_id:
            self.stats.updates_dropped_old += 1
            return True
        if self._awaiting_join:
            # Snapshot is in place but no diff has joined it yet: this one has to
            # pass the join test, not the running pu chain.
            if not self._joins(update):
                self.stats.sequence_gaps += 1
                self.stats.last_gap_detail = (
                    f"first diff after snapshot does not join: lastUpdateId={self.last_update_id}, "
                    f"U={update.first_id}, u={update.final_id}, pu={update.prev_final_id}"
                )
                self.state = BookState.DESYNCED
                return False
            self._awaiting_join = False
            self._apply_levels(update)
            return True
        # ``pu`` is authoritative when the venue provides it; some frames carry 0
        # (the very first event after a listen-key rotation), so fall back to the
        # contiguity rule rather than declaring a false gap.
        if update.prev_final_id:
            in_sequence = update.prev_final_id == self.last_update_id
        else:
            in_sequence = update.first_id <= self.last_update_id + 1 <= update.final_id
        if not in_sequence:
            self.stats.sequence_gaps += 1
            self.stats.last_gap_detail = (
                f"expected pu={self.last_update_id}, got pu={update.prev_final_id} "
                f"(U={update.first_id}, u={update.final_id})"
            )
            self.state = BookState.DESYNCED
            return False
        self._apply_levels(update)
        return True

    def _apply_levels(self, update: DepthUpdate) -> None:
        added_bid = removed_bid = added_ask = removed_ask = 0.0
        for price, qty in update.bids:
            delta = qty - self._bids.get(price, 0.0)
            if delta >= 0:
                added_bid += delta
            else:
                removed_bid -= delta
            if qty <= 0:
                self._bids.pop(price, None)
            else:
                self._bids[price] = qty
        for price, qty in update.asks:
            delta = qty - self._asks.get(price, 0.0)
            if delta >= 0:
                added_ask += delta
            else:
                removed_ask -= delta
            if qty <= 0:
                self._asks.pop(price, None)
            else:
                self._asks[price] = qty
        if self.on_liquidity is not None:
            self.on_liquidity(update.ts_ms, added_bid, removed_bid, added_ask, removed_ask)
        self.last_update_id = update.final_id
        self.ts_ms = update.ts_ms
        self.recv_ms = update.recv_ms
        self.stats.updates_applied += 1
        self._trim()
        self._bump_version()
        if self._is_crossed():
            self.stats.crossed_events += 1

    def _is_crossed(self) -> bool:
        if not self._bids or not self._asks:
            return False
        return max(self._bids) >= min(self._asks)

    def _trim(self) -> None:
        """Bound memory: a diff stream slowly accumulates far-away price levels
        that no snapshot ever clears."""
        if len(self._bids) > self.max_levels * 2:
            keep = set(heapq.nlargest(self.max_levels, self._bids))
            self._bids = {p: q for p, q in self._bids.items() if p in keep}
        if len(self._asks) > self.max_levels * 2:
            keep = set(heapq.nsmallest(self.max_levels, self._asks))
            self._asks = {p: q for p, q in self._asks.items() if p in keep}

    def _bump_version(self) -> None:
        self._version += 1
        self._cached_view = None

    def refresh_from(self, snapshot: DepthSnapshot) -> bool:
        """Re-verify a healthy book against a fresh snapshot, safely.

        A periodic refresh exists to catch drift, and it must never be able to
        *cause* any. The snapshot can easily arrive too old to join: the diff
        stream keeps moving during the REST round trip, and a venue under load
        can answer with a book from before the diffs already buffered here.
        Applying it blindly would tear down a book that was correct and leave it
        desynced — the refresh doing exactly the damage it was added to prevent.

        So the whole state is saved first and restored on failure. A refresh is
        either an improvement or a no-op, and the rollback is counted.

        The subtle case is a snapshot that is *behind* the book rather than
        ahead of it — a REST node lagging the stream.  ``apply_snapshot`` calls
        that a success, and correctly so: with nothing buffered there is no
        evidence of a hole, so the book it builds is internally consistent.  It
        is just consistent with the past.  The next diff then has to join a
        ``lastUpdateId`` the stream left behind long ago, fails, and desyncs a
        book that was correct.  A refresh may only ever move the book forward.
        """
        if snapshot.last_update_id < self.last_update_id:
            self.stats.refreshes += 1
            self.stats.refresh_rollbacks += 1
            self.stats.last_gap_detail = (
                f"refresh snapshot lastUpdateId={snapshot.last_update_id} is behind the book "
                f"at {self.last_update_id}; kept the book"
            )
            return False

        saved = (
            dict(self._bids),
            dict(self._asks),
            self.last_update_id,
            self.state,
            self._awaiting_join,
            list(self._buffer),
            self.snapshot_ts_ms,
            self.ts_ms,
            self.recv_ms,
        )
        self.stats.refreshes += 1
        if self.apply_snapshot(snapshot) and self.ready:
            return True

        (
            self._bids,
            self._asks,
            self.last_update_id,
            self.state,
            self._awaiting_join,
            self._buffer,
            self.snapshot_ts_ms,
            self.ts_ms,
            self.recv_ms,
        ) = saved
        self.stats.refresh_rollbacks += 1
        self._bump_version()
        return False

    def mark_desynced(self, reason: str = "") -> None:
        self.state = BookState.DESYNCED
        if reason:
            self.stats.last_gap_detail = reason

    def begin_resync(self) -> None:
        """Discard the book and wait for a fresh snapshot.

        Diffs arriving during the REST round trip are buffered, not dropped —
        that buffer is exactly what lets the new snapshot be joined without a
        hole.
        """
        self.stats.resyncs += 1
        self._bids.clear()
        self._asks.clear()
        self.last_update_id = 0
        self._awaiting_join = False
        self.state = BookState.BUFFERING
        self._bump_version()

    def to_dict(self) -> dict[str, Any]:
        view = self.top(10)
        return {
            "symbol": self.symbol,
            "state": self.state.value,
            "ready": self.ready,
            "last_update_id": self.last_update_id,
            "ts_ms": self.ts_ms,
            "levels": {"bid": len(self._bids), "ask": len(self._asks)},
            "best_bid": view.best_bid,
            "best_ask": view.best_ask,
            "mid": view.mid,
            "spread_bps": view.spread_bps(),
            "is_crossed": view.is_crossed,
            "buffered": len(self._buffer),
            "stats": self.stats.to_dict(),
        }


def update_from_event(symbol: str, ts_ms: int, recv_ms: int, payload: dict[str, Any]) -> DepthUpdate:
    """Build a :class:`DepthUpdate` from a normalised depth event payload."""
    return DepthUpdate(
        symbol=symbol,
        ts_ms=ts_ms,
        recv_ms=recv_ms,
        first_id=int(payload.get("U", 0)),
        final_id=int(payload.get("u", 0)),
        prev_final_id=int(payload.get("pu", 0)),
        bids=[(float(p), float(q)) for p, q in payload.get("b", ())],
        asks=[(float(p), float(q)) for p, q in payload.get("a", ())],
    )
