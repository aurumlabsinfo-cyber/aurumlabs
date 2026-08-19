from __future__ import annotations

from aurum.config import QualityConfig
from aurum.domain import BookLevel, BookSnapshot, FeedState, QualityFlag
from aurum.market.quality import QualityGate, SymbolQualityTracker


def book(bid: float = 60000.0, ask: float = 60001.0, levels: int = 10) -> BookSnapshot:
    return BookSnapshot(
        symbol="BTCUSDT",
        ts_ms=1_000_000,
        recv_ms=1_000_005,
        bids=[BookLevel(bid - i, 1.0 + i) for i in range(levels)],
        asks=[BookLevel(ask + i, 1.0 + i) for i in range(levels)],
        last_update_id=1,
        is_crossed=bid >= ask,
    )


def healthy_tracker(now: int = 1_000_000) -> SymbolQualityTracker:
    tracker = SymbolQualityTracker("BTCUSDT", QualityConfig())
    tracker.feed_state = FeedState.LIVE
    for i in range(120):
        tracker.record_event(now - 60_000 + i * 500, now - 60_000 + i * 500 + 20)
    tracker.record_event(now - 100, now - 80)
    return tracker


def evaluate(tracker: SymbolQualityTracker, *, now: int = 1_000_000, snapshot: BookSnapshot | None = None,
             ready: bool = True, state: str = "READY", snapshot_age_s: float = 5.0):
    return tracker.evaluate(
        now_ms=now,
        book=snapshot if snapshot is not None else book(),
        book_ready=ready,
        book_state=state,
        stale_after_ms=3000,
        snapshot_age_s=snapshot_age_s,
    )


def test_healthy_symbol_scores_high_and_is_tradable() -> None:
    quality = evaluate(healthy_tracker())
    assert quality.state is FeedState.LIVE
    assert quality.flags == []
    assert quality.score > 0.8
    assert quality.tradable
    allowed, reason = QualityGate(QualityConfig()).check(quality)
    assert allowed and reason == "ok"


def test_stale_feed_blocks_trading() -> None:
    tracker = healthy_tracker()
    quality = evaluate(tracker, now=1_000_000 + 30_000)
    assert quality.state is FeedState.STALE
    assert QualityFlag.STALE_FEED in quality.flags
    assert quality.score == 0.0
    allowed, reason = QualityGate(QualityConfig()).check(quality)
    assert not allowed and "STALE" in reason


def test_crossed_book_blocks_trading() -> None:
    quality = evaluate(healthy_tracker(), snapshot=book(bid=60002.0, ask=60001.0))
    assert QualityFlag.CROSSED_BOOK in quality.flags
    assert not quality.tradable


def test_desynced_book_blocks_trading_independently_of_the_score() -> None:
    quality = evaluate(healthy_tracker(), ready=False, state="DESYNCED")
    assert QualityFlag.SEQUENCE_GAP in quality.flags
    assert quality.score == 0.0
    allowed, _ = QualityGate(QualityConfig()).check(quality)
    assert not allowed


def test_wide_spread_flags_and_degrades_the_score() -> None:
    wide = book(bid=60000.0, ask=60400.0)  # ~66 bps, over the 20 bps limit
    quality = evaluate(healthy_tracker(), snapshot=wide)
    assert QualityFlag.WIDE_SPREAD in quality.flags
    assert not quality.tradable


def test_thin_book_is_blocked() -> None:
    quality = evaluate(healthy_tracker(), snapshot=book(levels=2))
    assert QualityFlag.THIN_BOOK in quality.flags
    assert quality.score == 0.0


def test_one_broken_dimension_cannot_be_averaged_away() -> None:
    """The score multiplies factors precisely so a single failure dominates."""
    tracker = healthy_tracker()
    good = evaluate(tracker)
    crossed = evaluate(tracker, snapshot=book(bid=60002.0, ask=60001.0))
    assert good.score > 0.8
    assert crossed.score == 0.0


def test_high_latency_is_flagged() -> None:
    tracker = SymbolQualityTracker("BTCUSDT", QualityConfig(max_latency_ms=100.0))
    tracker.feed_state = FeedState.LIVE
    for i in range(200):
        tracker.record_event(1_000_000 - 60_000 + i * 300, 1_000_000 - 60_000 + i * 300 + 900)
    tracker.record_event(999_900, 1_000_800)
    quality = evaluate(tracker)
    assert QualityFlag.HIGH_LATENCY in quality.flags


def test_gate_summary_names_the_blocked_symbols() -> None:
    gate = QualityGate(QualityConfig())
    healthy = evaluate(healthy_tracker())
    blocked = evaluate(healthy_tracker(), ready=False, state="DESYNCED")
    blocked.symbol = "ETHUSDT"
    summary = gate.summary({"BTCUSDT": healthy, "ETHUSDT": blocked})
    assert summary["symbols"] == 2
    assert summary["tradable"] == 1
    assert "ETHUSDT" in summary["blocked"]
