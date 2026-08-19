from __future__ import annotations

from aurum.adapters.base import DepthSnapshot, DepthUpdate
from aurum.domain import Side
from aurum.market.orderbook import BookState, OrderBook, update_from_event


def snapshot(last_id: int = 100) -> DepthSnapshot:
    return DepthSnapshot(
        symbol="BTCUSDT",
        last_update_id=last_id,
        ts_ms=1_000,
        recv_ms=1_005,
        bids=[(60000.0, 2.0), (59999.0, 3.0), (59998.0, 4.0)],
        asks=[(60001.0, 1.5), (60002.0, 2.5), (60003.0, 3.5)],
    )


def diff(first: int, final: int, prev: int, *, bids=(), asks=(), ts: int = 2_000) -> DepthUpdate:
    return DepthUpdate("BTCUSDT", ts, ts + 5, first, final, prev, list(bids), list(asks))


def test_book_is_not_ready_before_a_snapshot() -> None:
    book = OrderBook("BTCUSDT")
    assert book.state is BookState.EMPTY
    book.apply_update(diff(101, 105, 100))
    assert book.state is BookState.BUFFERING
    assert not book.ready
    assert book.top(5).mid is None


def test_snapshot_joins_the_buffered_diff_stream() -> None:
    book = OrderBook("BTCUSDT")
    book.apply_update(diff(95, 99, 94))     # entirely before the snapshot
    book.apply_update(diff(99, 102, 98))    # brackets lastUpdateId=100 -> first applied
    book.apply_update(diff(103, 104, 102, bids=[(60000.0, 5.0)]))
    assert book.apply_snapshot(snapshot(100)) is True
    assert book.ready
    assert book.last_update_id == 104
    assert book.top(1).bids[0].qty == 5.0
    assert book.stats.updates_dropped_old == 1


def test_snapshot_without_a_bridging_diff_stays_buffering() -> None:
    book = OrderBook("BTCUSDT")
    book.apply_update(diff(200, 205, 199))  # far ahead of the snapshot: a hole
    assert book.apply_snapshot(snapshot(100)) is False
    assert book.state is BookState.BUFFERING
    assert not book.ready


def test_pu_mismatch_desyncs_the_book() -> None:
    book = OrderBook("BTCUSDT")
    book.apply_update(diff(99, 102, 98))
    assert book.apply_snapshot(snapshot(100)) is True

    # pu must equal the previous u (102). 150 means events were dropped.
    assert book.apply_update(diff(151, 155, 150)) is False
    assert book.state is BookState.DESYNCED
    assert book.stats.sequence_gaps == 1
    assert "expected pu=102" in book.stats.last_gap_detail

    # A desynced book refuses further updates rather than drifting quietly.
    assert book.apply_update(diff(156, 157, 155)) is False


def test_resync_is_deterministic() -> None:
    book = OrderBook("BTCUSDT")
    book.apply_update(diff(99, 102, 98))
    book.apply_snapshot(snapshot(100))
    book.apply_update(diff(151, 155, 150))
    assert book.state is BookState.DESYNCED

    book.begin_resync()
    assert book.state is BookState.BUFFERING
    assert book.stats.resyncs == 1
    assert book.last_update_id == 0

    # Diffs arriving during the REST round trip are buffered, not lost.
    book.apply_update(diff(299, 305, 298, bids=[(60000.5, 7.0)]))
    assert book.apply_snapshot(snapshot(300)) is True
    assert book.ready
    assert book.last_update_id == 305
    assert book.top(1).bids[0].price == 60000.5


def test_snapshot_with_no_pending_diffs_is_usable() -> None:
    """The normal startup case: the snapshot lands before the first diff."""
    book = OrderBook("BTCUSDT")
    assert book.apply_snapshot(snapshot(100)) is True
    assert book.ready
    assert book.top(1).mid == 60000.5
    # The next diff still has to join, and one that continues from the snapshot does.
    assert book.apply_update(diff(101, 105, 100, bids=[(60000.0, 9.0)])) is True
    assert book.top(1).bids[0].qty == 9.0


def test_first_live_diff_that_cannot_join_desyncs() -> None:
    book = OrderBook("BTCUSDT")
    book.apply_snapshot(snapshot(100))
    assert book.apply_update(diff(500, 505, 499)) is False
    assert book.state is BookState.DESYNCED
    assert "does not join" in book.stats.last_gap_detail


def test_liquidity_callback_fires_for_live_and_replayed_diffs() -> None:
    seen: list[tuple[int, float, float, float, float]] = []
    book = OrderBook("BTCUSDT", on_liquidity=lambda *args: seen.append(args))

    # Buffered before the snapshot: these are replayed by apply_snapshot and
    # must be accounted for too, otherwise every resync silently loses flow.
    book.apply_update(diff(99, 102, 98, bids=[(60000.0, 5.0)]))
    book.apply_snapshot(snapshot(100))
    assert len(seen) == 1
    assert seen[0][1] == 3.0  # 5.0 replacing the snapshot's 2.0 is 3.0 added

    book.apply_update(diff(103, 104, 102, asks=[(60001.0, 0.5)]))
    assert len(seen) == 2
    assert seen[1][4] == 1.0  # 1.5 -> 0.5 on the ask is 1.0 removed


def test_zero_quantity_removes_a_level() -> None:
    book = OrderBook("BTCUSDT")
    book.apply_update(diff(99, 102, 98))
    book.apply_snapshot(snapshot(100))
    assert book.resting_qty(Side.BUY, 59999.0) == 3.0
    book.apply_update(diff(103, 103, 102, bids=[(59999.0, 0.0)]))
    assert book.resting_qty(Side.BUY, 59999.0) == 0.0
    assert [level.price for level in book.top(5).bids] == [60000.0, 59998.0]


def test_stale_diffs_are_dropped_not_applied() -> None:
    book = OrderBook("BTCUSDT")
    book.apply_update(diff(99, 102, 98))
    book.apply_snapshot(snapshot(100))
    before = book.last_update_id
    assert book.apply_update(diff(50, 60, 49, bids=[(1.0, 1.0)])) is True
    assert book.last_update_id == before
    assert book.resting_qty(Side.BUY, 1.0) == 0.0


def test_crossed_book_is_detected() -> None:
    book = OrderBook("BTCUSDT")
    book.apply_update(diff(99, 102, 98))
    book.apply_snapshot(snapshot(100))
    assert book.top(5).is_crossed is False
    book.apply_update(diff(103, 103, 102, bids=[(60005.0, 1.0)]))
    assert book.top(5).is_crossed is True
    assert book.stats.crossed_events == 1


def test_microprice_leans_toward_the_thinner_side() -> None:
    book = OrderBook("BTCUSDT")
    book.apply_update(diff(99, 102, 98))
    book.apply_snapshot(snapshot(100))
    view = book.top(5)
    mid = view.mid
    micro = view.microprice()
    # bid size 2.0 vs ask size 1.5: less resting size on the ask, price pressure up.
    assert micro is not None and mid is not None
    assert micro > mid


def test_top_view_is_cached_until_the_book_changes() -> None:
    book = OrderBook("BTCUSDT")
    book.apply_update(diff(99, 102, 98))
    book.apply_snapshot(snapshot(100))
    first = book.top(5)
    assert book.top(5) is first
    book.apply_update(diff(103, 104, 102, bids=[(59997.0, 1.0)]))
    assert book.top(5) is not first


def test_update_from_event_reads_the_futures_sequence_fields() -> None:
    update = update_from_event(
        "BTCUSDT", 1, 2, {"U": 10, "u": 20, "pu": 9, "b": [(1.0, 2.0)], "a": [(3.0, 4.0)]}
    )
    assert (update.first_id, update.final_id, update.prev_final_id) == (10, 20, 9)
    assert update.bids == [(1.0, 2.0)]


def test_buffer_is_bounded() -> None:
    book = OrderBook("BTCUSDT")
    for i in range(OrderBook.MAX_BUFFER + 500):
        book.apply_update(diff(i, i + 1, i - 1))
    assert len(book._buffer) <= OrderBook.MAX_BUFFER
