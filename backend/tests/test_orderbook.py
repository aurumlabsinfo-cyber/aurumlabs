"""Local order book: sequencing, gap detection, resync, crossed-book guard."""

from __future__ import annotations

from app.marketdata.orderbook import LocalOrderBook
from app.marketdata.types import DepthSnapshot, DepthUpdate


def snap(last_id: int = 100) -> DepthSnapshot:
    return DepthSnapshot(
        exchange="test", symbol="BTCUSDT", last_update_id=last_id,
        bids=[(100.0, 1.0), (99.0, 2.0)], asks=[(101.0, 1.5), (102.0, 3.0)],
        server_ts=0,
    )


def upd(first: int, final: int, bids=None, asks=None, prev=None) -> DepthUpdate:
    return DepthUpdate(
        exchange="test", symbol="BTCUSDT", first_update_id=first,
        final_update_id=final, prev_final_update_id=prev,
        bids=bids or [], asks=asks or [], exchange_ts=0, server_ts=0,
    )


def test_snapshot_then_contiguous_updates_sync_the_book():
    book = LocalOrderBook("test", "BTCUSDT")
    book.buffer(upd(101, 105, bids=[(100.5, 0.5)]))
    assert book.apply_snapshot(snap(100)) is True
    assert book.synced is True
    assert book.last_update_id == 105
    assert book.best_bid() == (100.5, 0.5)


def test_updates_older_than_snapshot_are_discarded():
    book = LocalOrderBook("test", "BTCUSDT")
    book.buffer(upd(50, 80, bids=[(1.0, 9.9)]))  # entirely before the snapshot
    book.buffer(upd(99, 101, bids=[(100.25, 0.4)]))
    book.apply_snapshot(snap(100))
    assert 1.0 not in book.bids
    assert book.bids[100.25] == 0.4
    assert book.stats.dropped_stale >= 1


def test_first_update_must_straddle_the_snapshot_id():
    book = LocalOrderBook("test", "BTCUSDT")
    # Snapshot at 100, but the stream restarts at 200: unrecoverable gap.
    book.buffer(upd(200, 205))
    assert book.apply_snapshot(snap(100)) is False
    assert book.synced is False
    assert book.stats.gaps_detected == 1


def test_sequence_gap_triggers_resync():
    book = LocalOrderBook("test", "BTCUSDT")
    book.buffer(upd(101, 105))
    book.apply_snapshot(snap(100))
    assert book.synced is True

    ok = book.apply(upd(120, 130))  # 106..119 missing
    assert ok is False
    assert book.synced is False
    assert book.stats.gaps_detected == 1
    assert book.stats.resyncs >= 1
    assert "gap" in (book.desync_reason or "")


def test_futures_style_prev_update_id_is_enforced():
    book = LocalOrderBook("test", "BTCUSDT")
    book.buffer(upd(101, 105, prev=100))
    book.apply_snapshot(snap(100))
    assert book.apply(upd(106, 110, prev=105)) is True
    assert book.apply(upd(111, 115, prev=999)) is False  # pu mismatch


def test_zero_quantity_removes_a_level():
    book = LocalOrderBook("test", "BTCUSDT")
    book.buffer(upd(101, 101))
    book.apply_snapshot(snap(100))
    book.apply(upd(102, 102, bids=[(99.0, 0.0)]))
    assert 99.0 not in book.bids


def test_crossed_book_forces_resync():
    book = LocalOrderBook("test", "BTCUSDT")
    book.buffer(upd(101, 101))
    book.apply_snapshot(snap(100))
    # Bid above the best ask: our state is wrong, never publish it.
    assert book.apply(upd(102, 102, bids=[(105.0, 1.0)])) is False
    assert book.synced is False
    assert "crossed" in (book.desync_reason or "")


def test_derived_metrics():
    book = LocalOrderBook("test", "BTCUSDT")
    book.buffer(upd(101, 101))
    book.apply_snapshot(snap(100))
    assert book.mid() == 100.5
    bid_qty, ask_qty = book.depth_qty(2)
    assert bid_qty == 3.0 and ask_qty == 4.5
    within = book.depth_within_bps(200.0)
    assert within[0] > 0 and within[1] > 0


def test_book_is_never_synced_before_a_snapshot():
    book = LocalOrderBook("test", "BTCUSDT")
    assert book.synced is False
    book.apply(upd(1, 2, bids=[(1.0, 1.0)]))  # buffered, not applied
    assert book.synced is False
    assert book.last_update_id == 0
