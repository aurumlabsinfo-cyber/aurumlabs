"""Per-symbol data-quality scoring and the trading block.

The blueprint's rule is that data quality can block execution *independently
from strategy confidence*.  So this module answers one question — "is what we
know about this symbol good enough to risk money on?" — and its answer is not
negotiable by anything downstream.  A perfectly validated champion with a 5 bps
edge does not trade a symbol whose book is desynced.

The score is a product of per-dimension factors rather than a weighted sum: a
weighted sum lets nine healthy dimensions hide one broken one, which is the
exact failure this gate exists to prevent.  Any hard flag forces ``tradable`` to
False regardless of the score.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from ..config import QualityConfig
from ..domain import BookSnapshot, FeedState, QualityFlag, SymbolQuality


def _decay(value: float, limit: float, floor: float = 0.05) -> float:
    """1.0 while well inside ``limit``, falling to ``floor`` at and beyond it."""
    if limit <= 0:
        return 1.0
    ratio = max(0.0, value) / limit
    if ratio <= 0.5:
        return 1.0
    if ratio >= 1.0:
        return floor
    # Linear between half the limit and the limit itself.
    return 1.0 - (ratio - 0.5) / 0.5 * (1.0 - floor)


@dataclass
class SymbolQualityTracker:
    """Rolling health for one symbol."""

    symbol: str
    config: QualityConfig
    events: int = 0
    window_started_ms: int = 0
    events_in_window: int = 0
    events_per_min: float = 0.0
    last_event_ms: int = 0
    last_recv_ms: int = 0
    latency_ms: float = 0.0
    sequence_gaps: int = 0
    gaps_in_window: int = 0
    resyncs: int = 0
    feed_state: FeedState = FeedState.DISCONNECTED
    detail: str = ""
    last_quality: SymbolQuality | None = field(default=None, repr=False)

    def record_event(self, ts_ms: int, recv_ms: int) -> None:
        self.events += 1
        self.events_in_window += 1
        self.last_event_ms = ts_ms
        self.last_recv_ms = recv_ms
        # Exponential smoothing: one delayed frame should not condemn a feed,
        # a sustained delay should.
        instant = max(0.0, float(recv_ms - ts_ms))
        self.latency_ms = instant if self.events == 1 else 0.85 * self.latency_ms + 0.15 * instant
        if self.window_started_ms == 0:
            self.window_started_ms = recv_ms

    def record_gap(self) -> None:
        self.sequence_gaps += 1
        self.gaps_in_window += 1

    def record_resync(self) -> None:
        self.resyncs += 1

    def roll_window(self, now_ms: int) -> None:
        elapsed = now_ms - self.window_started_ms
        if self.window_started_ms and elapsed >= 60_000:
            self.events_per_min = self.events_in_window * 60_000.0 / elapsed
            self.events_in_window = 0
            self.gaps_in_window = 0
            self.window_started_ms = now_ms
        elif self.window_started_ms and elapsed > 0:
            # Partial window: extrapolate so a symbol that has been silent for
            # 40 s is not credited with the rate it had a minute ago.
            self.events_per_min = self.events_in_window * 60_000.0 / elapsed

    def evaluate(
        self,
        *,
        now_ms: int,
        book: BookSnapshot | None,
        book_ready: bool,
        book_state: str,
        stale_after_ms: int,
        snapshot_age_s: float,
    ) -> SymbolQuality:
        self.roll_window(now_ms)
        flags: list[QualityFlag] = []
        factors: list[float] = []

        age_ms = now_ms - self.last_event_ms if self.last_event_ms else float("inf")
        if self.feed_state in (FeedState.DISCONNECTED, FeedState.ERROR) or self.last_event_ms == 0:
            state = FeedState.DISCONNECTED if self.last_event_ms == 0 else self.feed_state
            flags.append(QualityFlag.STALE_FEED)
            factors.append(0.0)
        elif age_ms > stale_after_ms:
            state = FeedState.STALE
            flags.append(QualityFlag.STALE_FEED)
            factors.append(0.0)
        elif not book_ready:
            state = FeedState.DESYNCED if book_state == "DESYNCED" else FeedState.SYNCING
            flags.append(QualityFlag.RESYNCING if book_state != "DESYNCED" else QualityFlag.SEQUENCE_GAP)
            factors.append(0.0)
        else:
            state = FeedState.LIVE
            factors.append(_decay(age_ms, stale_after_ms))

        # Latency
        if self.latency_ms > self.config.max_latency_ms:
            flags.append(QualityFlag.HIGH_LATENCY)
        factors.append(_decay(self.latency_ms, self.config.max_latency_ms))

        # Book shape
        spread_bps: float | None = None
        book_levels = 0
        if book is not None:
            book_levels = min(len(book.bids), len(book.asks))
            spread_bps = book.spread_bps()
            if book.is_crossed:
                flags.append(QualityFlag.CROSSED_BOOK)
                factors.append(0.0)
            if book_levels < self.config.min_book_levels:
                flags.append(QualityFlag.THIN_BOOK)
                factors.append(0.0)
            if spread_bps is None:
                flags.append(QualityFlag.THIN_BOOK)
                factors.append(0.0)
            else:
                if spread_bps > self.config.max_spread_bps:
                    flags.append(QualityFlag.WIDE_SPREAD)
                factors.append(_decay(spread_bps, self.config.max_spread_bps))
        else:
            flags.append(QualityFlag.NO_SNAPSHOT)
            factors.append(0.0)

        # Snapshot freshness
        if snapshot_age_s > self.config.snapshot_max_age_s:
            flags.append(QualityFlag.NO_SNAPSHOT)
            factors.append(0.3)

        # Sequence integrity and event rate
        if self.gaps_in_window > self.config.max_sequence_gaps_per_min:
            flags.append(QualityFlag.SEQUENCE_GAP)
        factors.append(_decay(self.gaps_in_window, max(1, self.config.max_sequence_gaps_per_min)))

        if self.config.min_events_per_min > 0 and state is FeedState.LIVE:
            if self.events_per_min < self.config.min_events_per_min:
                flags.append(QualityFlag.LOW_EVENT_RATE)
            factors.append(
                min(1.0, self.events_per_min / self.config.min_events_per_min)
                if self.config.min_events_per_min
                else 1.0
            )

        score = 1.0
        for factor in factors:
            score *= max(0.0, min(1.0, factor))

        quality = SymbolQuality(
            symbol=self.symbol,
            ts_ms=now_ms,
            score=round(score, 6),
            state=state,
            flags=sorted(set(flags), key=lambda f: f.value),
            latency_ms=self.latency_ms,
            spread_bps=spread_bps,
            events_per_min=self.events_per_min,
            sequence_gaps=self.sequence_gaps,
            resyncs=self.resyncs,
            last_event_ms=self.last_event_ms,
            snapshot_age_s=snapshot_age_s,
            book_levels=book_levels,
        )
        self.last_quality = quality
        return quality


class QualityGate:
    """The single place that answers "may this symbol be traded right now?"."""

    def __init__(self, config: QualityConfig) -> None:
        self.config = config

    def check(self, quality: SymbolQuality | None) -> tuple[bool, str]:
        """Returns ``(allowed, reason)``.  The reason is shown verbatim in
        ``/diagnostics``, so it has to say something a human can act on."""
        if quality is None:
            return False, "no data-quality assessment yet for this symbol"
        if quality.state is not FeedState.LIVE:
            return False, f"feed state is {quality.state.value}"
        if quality.flags:
            return False, "quality flags: " + ", ".join(f.value for f in quality.flags)
        if quality.score < self.config.min_score_to_trade:
            return (
                False,
                f"quality score {quality.score:.3f} below minimum {self.config.min_score_to_trade:.3f}",
            )
        return True, "ok"

    def summary(self, qualities: dict[str, SymbolQuality]) -> dict[str, Any]:
        if not qualities:
            return {"symbols": 0, "tradable": 0, "mean_score": 0.0, "blocked": {}, "worst": None}
        blocked: dict[str, list[str]] = {}
        for symbol, quality in qualities.items():
            allowed, reason = self.check(quality)
            if not allowed:
                blocked[symbol] = [reason]
        scores = [q.score for q in qualities.values()]
        worst = min(qualities.values(), key=lambda q: q.score)
        return {
            "symbols": len(qualities),
            "tradable": len(qualities) - len(blocked),
            "mean_score": round(sum(scores) / len(scores), 4),
            "min_score_to_trade": self.config.min_score_to_trade,
            "blocked": blocked,
            "worst": {"symbol": worst.symbol, "score": round(worst.score, 4), "state": worst.state.value},
        }
