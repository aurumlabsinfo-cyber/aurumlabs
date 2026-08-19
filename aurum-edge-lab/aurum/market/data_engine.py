"""Market data engine: ingestion, books, flow history, quality, resync.

Everything the feed produces lands in a bounded queue and is processed by one
task per stage.  The socket reader never waits for a database write, a feature
computation or a websocket client — if a stage falls behind it drops its own
backlog and the drop is counted (see :mod:`aurum.bus`).

This engine owns the per-symbol state that later layers read but never mutate:
the order book, the recent trade tape, the top-of-book history used for order
flow imbalance, the liquidity add/remove tape, mark price and funding.
"""

from __future__ import annotations

import asyncio
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Callable, Deque

from ..adapters.base import MarketFeed
from ..bus import TOPIC_BOOK, TOPIC_MARKET_EVENT, TOPIC_TRADE, EventBus
from ..clock import Clock, LatencyTracker
from ..config import Config
from ..domain import (
    BookSnapshot,
    EventKind,
    FeedState,
    MarketEvent,
    Side,
    SymbolQuality,
    TradeTick,
    now_ms,
)
from ..logging_setup import get_logger
from ..storage.repositories import Repositories
from .orderbook import BookState, OrderBook, update_from_event
from .quality import QualityGate, SymbolQualityTracker

log = get_logger("market.engine")


@dataclass(slots=True)
class TopOfBook:
    ts_ms: int
    bid: float
    bid_qty: float
    ask: float
    ask_qty: float


@dataclass(slots=True)
class LiquidityDelta:
    ts_ms: int
    added_bid: float
    removed_bid: float
    added_ask: float
    removed_ask: float


@dataclass
class SymbolState:
    """Everything known about one symbol, owned by the ingest task."""

    symbol: str
    book: OrderBook
    quality: SymbolQualityTracker
    trades: Deque[TradeTick] = field(default_factory=lambda: deque(maxlen=4000))
    tops: Deque[TopOfBook] = field(default_factory=lambda: deque(maxlen=6000))
    liquidity: Deque[LiquidityDelta] = field(default_factory=lambda: deque(maxlen=6000))
    mark_price: float = 0.0
    index_price: float = 0.0
    funding_rate: float = 0.0
    next_funding_ms: int = 0
    last_mark_ms: int = 0
    snapshot_ts_ms: int = 0
    resync_pending: bool = False
    last_resync_ms: int = 0
    resyncs_this_hour: int = 0
    resync_hour_started_ms: int = 0
    latency: LatencyTracker = field(default_factory=LatencyTracker)

    def prune(self, cutoff_ms: int) -> None:
        while self.trades and self.trades[0].ts_ms < cutoff_ms:
            self.trades.popleft()
        while self.tops and self.tops[0].ts_ms < cutoff_ms:
            self.tops.popleft()
        while self.liquidity and self.liquidity[0].ts_ms < cutoff_ms:
            self.liquidity.popleft()


class DataEngine:
    def __init__(
        self,
        config: Config,
        feed: MarketFeed,
        bus: EventBus,
        repos: Repositories,
        clock: Clock | None = None,
    ) -> None:
        self.config = config
        self.feed = feed
        self.bus = bus
        self.repos = repos
        self.clock = clock or Clock()
        self.gate = QualityGate(config.quality)

        self.symbols = config.market.symbol_names
        self.states: dict[str, SymbolState] = {}
        for symbol in self.symbols:
            state = SymbolState(
                symbol=symbol,
                book=OrderBook(symbol, max_levels=config.market.snapshot_limit),
                quality=SymbolQualityTracker(symbol=symbol, config=config.quality),
            )
            state.book.on_liquidity = self._make_liquidity_sink(state)
            self.states[symbol] = state
        self.qualities: dict[str, SymbolQuality] = {}

        self._ingest = bus.subscribe(TOPIC_MARKET_EVENT, "data-engine", capacity=config.market.queue_size)
        self._tasks: list[asyncio.Task[None]] = []
        self._running = False
        self.started_ms = 0
        self.endpoint_check: dict[str, Any] = {"checked": False}
        self.processed = 0
        self.persist_sample = 0
        #: Newest venue timestamp seen on any symbol. Under a live feed this
        #: tracks wall time; under replay it is the only clock that means
        #: anything, which is why downstream sampling reads it and not ``now``.
        self.data_time_ms = 0
        #: Wall-clock instant the last event arrived, on any symbol. This is the
        #: only staleness measure that survives a dead feed: when the venue goes
        #: silent the data clock freezes with it, but this keeps ticking.
        self.last_recv_ms = 0
        #: Optional per-event hook, used by the feature engine in replay mode so
        #: sampling follows the data instead of the wall clock. Left unset in
        #: live mode: nothing that can block belongs on the ingest path.
        self.on_tick: Callable[[int], None] | None = None

        feed.on_event(self._on_feed_event)
        feed.on_state_change(self._on_feed_state)

    # ------------------------------------------------------------- lifecycle

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        self.started_ms = now_ms()

        self.endpoint_check = await self.feed.verify_endpoints()
        if self.endpoint_check.get("ok"):
            log.info("endpoints verified", extra={"rest": self.endpoint_check.get("rest_base", "")})
        else:
            log.warning("endpoint verification failed", extra={"detail": str(self.endpoint_check)[:300]})
            self.repos.system.log(
                "market",
                "endpoint_check_failed",
                "configured market endpoints did not verify",
                level="ERROR",
                detail=self.endpoint_check,
            )

        await self._sync_time()
        await self.feed.start()

        self._tasks = [
            asyncio.create_task(self._process_loop(), name="market-process"),
            asyncio.create_task(self._quality_loop(), name="market-quality"),
            asyncio.create_task(self._snapshot_loop(), name="market-snapshots"),
            asyncio.create_task(self._time_sync_loop(), name="market-timesync"),
        ]
        # Seed every book before the first diff would have to be buffered.
        await asyncio.gather(*(self._resync(symbol, reason="initial") for symbol in self.symbols))

    async def stop(self) -> None:
        self._running = False
        await self.feed.stop()
        for task in self._tasks:
            task.cancel()
        for task in self._tasks:
            try:
                await task
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
                pass
        self._tasks.clear()

    # --------------------------------------------------------------- ingress

    def _on_feed_event(self, event: MarketEvent) -> None:
        """Called from the socket task. Must not block and must not raise."""
        self.bus.publish(TOPIC_MARKET_EVENT, event)

    def _on_feed_state(self, symbol: str, state: FeedState, detail: str) -> None:
        targets = self.symbols if symbol == "*" else [symbol]
        for name in targets:
            state_obj = self.states.get(name)
            if state_obj is not None:
                state_obj.quality.feed_state = state
                state_obj.quality.detail = detail
        if state in (FeedState.ERROR, FeedState.DISCONNECTED) and detail:
            self.repos.system.log("market", "feed_state", detail, level="WARNING", detail={"state": state.value})

    async def _process_loop(self) -> None:
        persist_events = self.config.storage.persist_market_events
        while self._running:
            batch = await self._ingest.get_batch(512, timeout=0.5)
            for event in batch:
                try:
                    self._process(event, persist_events)
                    if self.on_tick is not None:
                        self.on_tick(self.data_time_ms)
                except Exception as exc:  # noqa: BLE001 - one bad frame must not kill ingestion
                    log.exception("event processing failed", extra={"symbol": event.symbol})
                    self.repos.system.log(
                        "market", "process_error", str(exc), level="ERROR", detail={"symbol": event.symbol}
                    )

    def _process(self, event: MarketEvent, persist: bool) -> None:
        state = self.states.get(event.symbol)
        if state is None:
            return
        self.processed += 1
        if event.ts_ms > self.data_time_ms:
            self.data_time_ms = event.ts_ms
        self.last_recv_ms = now_ms()
        state.quality.record_event(event.ts_ms, event.recv_ms)
        if self.feed.kind == "live":
            state.latency.record(event.latency_ms)

        if persist:
            self.repos.market.record_event(event)

        if event.kind is EventKind.DEPTH:
            self._apply_depth(state, event)
        elif event.kind is EventKind.TRADE:
            tick = TradeTick(
                symbol=event.symbol,
                ts_ms=event.ts_ms,
                recv_ms=event.recv_ms,
                price=float(event.payload.get("price", 0.0)),
                qty=float(event.payload.get("qty", 0.0)),
                aggressor=Side(event.payload.get("aggressor", "BUY")),
                trade_id=int(event.payload.get("trade_id", 0)),
            )
            state.trades.append(tick)
            self.bus.publish(TOPIC_TRADE, tick)
        elif event.kind is EventKind.BOOK_TICKER:
            state.tops.append(
                TopOfBook(
                    ts_ms=event.ts_ms,
                    bid=float(event.payload.get("bid", 0.0)),
                    bid_qty=float(event.payload.get("bid_qty", 0.0)),
                    ask=float(event.payload.get("ask", 0.0)),
                    ask_qty=float(event.payload.get("ask_qty", 0.0)),
                )
            )
        elif event.kind is EventKind.MARK_PRICE:
            state.mark_price = float(event.payload.get("mark_price", 0.0))
            state.index_price = float(event.payload.get("index_price", 0.0))
            state.funding_rate = float(event.payload.get("funding_rate", 0.0))
            state.next_funding_ms = int(event.payload.get("next_funding_ms", 0))
            state.last_mark_ms = event.ts_ms

    @staticmethod
    def _make_liquidity_sink(state: SymbolState):
        def sink(ts_ms: int, added_bid: float, removed_bid: float, added_ask: float, removed_ask: float) -> None:
            state.liquidity.append(LiquidityDelta(ts_ms, added_bid, removed_bid, added_ask, removed_ask))

        return sink

    def _apply_depth(self, state: SymbolState, event: MarketEvent) -> None:
        update = update_from_event(event.symbol, event.ts_ms, event.recv_ms, event.payload)
        ok = state.book.apply_update(update)
        if not ok:
            state.quality.record_gap()
            log.warning(
                "sequence gap", extra={"symbol": state.symbol, "detail": state.book.stats.last_gap_detail}
            )
            self.repos.system.log(
                "market",
                "sequence_gap",
                f"{state.symbol}: {state.book.stats.last_gap_detail}",
                level="WARNING",
                detail={"symbol": state.symbol},
            )
            self._schedule_resync(state.symbol, reason="sequence gap")
            return

        if state.book.ready:
            view = state.book.top(self.config.market.depth_levels)
            if view.bids and view.asks:
                # Depth diffs also refresh top-of-book, which matters when the
                # bookTicker stream is not subscribed.
                if not state.tops or state.tops[-1].ts_ms < event.ts_ms:
                    state.tops.append(
                        TopOfBook(event.ts_ms, view.bids[0].price, view.bids[0].qty,
                                  view.asks[0].price, view.asks[0].qty)
                    )
            self.bus.publish(TOPIC_BOOK, view)

    # ---------------------------------------------------------------- resync

    def _schedule_resync(self, symbol: str, reason: str) -> None:
        state = self.states[symbol]
        if state.resync_pending:
            return
        state.resync_pending = True
        asyncio.create_task(self._resync(symbol, reason=reason), name=f"resync-{symbol}")

    async def _resync(self, symbol: str, *, reason: str) -> None:
        """Fetch a fresh snapshot and rejoin the diff stream deterministically."""
        state = self.states[symbol]
        try:
            stamp = now_ms()
            if state.resync_hour_started_ms == 0 or stamp - state.resync_hour_started_ms > 3_600_000:
                state.resync_hour_started_ms = stamp
                state.resyncs_this_hour = 0
            if state.resyncs_this_hour >= self.config.market.max_resyncs_per_hour:
                log.error("resync budget exhausted", extra={"symbol": symbol})
                self.repos.system.log(
                    "market",
                    "resync_budget",
                    f"{symbol} exceeded {self.config.market.max_resyncs_per_hour} resyncs in an hour",
                    level="ERROR",
                )
                state.book.mark_desynced("resync budget exhausted")
                return

            cooldown = self.config.market.resync_cooldown_s
            since_last = (stamp - state.last_resync_ms) / 1000.0
            if state.last_resync_ms and since_last < cooldown:
                await asyncio.sleep(cooldown - since_last)

            state.book.begin_resync()
            state.quality.record_resync()
            state.resyncs_this_hour += 1
            state.last_resync_ms = now_ms()

            for attempt in range(3):
                try:
                    snapshot = await self.feed.fetch_depth_snapshot(symbol, self.config.market.snapshot_limit)
                except Exception as exc:  # noqa: BLE001
                    log.warning(
                        "snapshot fetch failed",
                        extra={"symbol": symbol, "attempt": attempt + 1, "error": str(exc)},
                    )
                    await asyncio.sleep(1.0 * (attempt + 1))
                    continue
                if state.book.apply_snapshot(snapshot):
                    state.snapshot_ts_ms = snapshot.ts_ms
                    log.info(
                        "book synced",
                        extra={"symbol": symbol, "reason": reason, "update_id": snapshot.last_update_id},
                    )
                    return
                # Snapshot too old for the buffered diffs: take a newer one.
                await asyncio.sleep(0.4)
            log.error("resync failed", extra={"symbol": symbol, "reason": reason})
            self.repos.system.log(
                "market", "resync_failed", f"{symbol}: could not rejoin the diff stream", level="ERROR"
            )
        finally:
            state.resync_pending = False

    # ------------------------------------------------------------- periodics

    async def _quality_loop(self) -> None:
        interval = 0.5
        while self._running:
            await asyncio.sleep(interval)
            wall = now_ms()
            # Symbols are aged against the data clock; the feed as a whole is
            # aged against the wall clock. See SymbolQualityTracker.evaluate.
            reference = self.data_time_ms or wall
            feed_silent_ms = (wall - self.last_recv_ms) if self.last_recv_ms else float("inf")
            live_feed = self.feed.kind == "live"
            stamp = wall
            for symbol, state in self.states.items():
                view = state.book.top(self.config.quality.min_book_levels) if state.book.ready else None
                snapshot_age = (
                    (reference - state.book.snapshot_ts_ms) / 1000.0
                    if state.book.snapshot_ts_ms
                    else 1e9
                )
                quality = state.quality.evaluate(
                    now_ms=reference,
                    book=view,
                    book_ready=state.book.ready,
                    book_state=state.book.state.value,
                    stale_after_ms=self.config.market.stale_after_ms,
                    snapshot_age_s=snapshot_age,
                    feed_silent_ms=feed_silent_ms,
                    measure_latency=live_feed,
                )
                self.qualities[symbol] = quality
                if state.book.state is BookState.DESYNCED and not state.resync_pending:
                    self._schedule_resync(symbol, reason="desynced book")
            # One quality row per symbol per 10 s is plenty for forensics.
            if stamp // 10_000 != getattr(self, "_last_quality_persist", 0):
                self._last_quality_persist = stamp // 10_000
                for quality in self.qualities.values():
                    self.repos.market.record_quality(quality)

    async def _snapshot_loop(self) -> None:
        """Persist a periodic book snapshot and prune in-memory history."""
        every_n = max(1, self.config.storage.persist_orderbook_every_n)
        tick = 0
        while self._running:
            await asyncio.sleep(1.0)
            tick += 1
            window_ms = self.config.features.buffer_seconds * 1000
            for state in self.states.values():
                # Age the history against the newest event actually seen for the
                # symbol, not against wall time. Under a live feed the two agree;
                # under replay, or after a feed outage, wall time would discard
                # the entire buffer the moment it ran ahead of the data.
                newest = max(
                    state.book.ts_ms,
                    state.trades[-1].ts_ms if state.trades else 0,
                    state.tops[-1].ts_ms if state.tops else 0,
                )
                if newest:
                    state.prune(newest - window_ms)
                if tick % every_n == 0 and state.book.ready:
                    self.repos.market.record_book(state.book.top(20))

    async def _time_sync_loop(self) -> None:
        while self._running:
            await asyncio.sleep(self.config.market.time_sync_interval_s)
            await self._sync_time()

    async def _sync_time(self) -> None:
        try:
            sent = time.monotonic() * 1000.0
            venue_ms = await self.feed.sync_time()
            received = time.monotonic() * 1000.0
            if venue_ms:
                # monotonic() is not epoch-based; convert the bracket to epoch
                # terms before handing it to the clock.
                epoch_now = time.time() * 1000.0
                drift = received - sent
                self.clock.observe(venue_ms, epoch_now - drift, epoch_now)
        except Exception as exc:  # noqa: BLE001 - a failed sync is reported, not fatal
            log.warning("time sync failed", extra={"error": str(exc)})

    # ---------------------------------------------------------------- reader

    def book_view(self, symbol: str, depth: int | None = None) -> BookSnapshot | None:
        state = self.states.get(symbol)
        if state is None or not state.book.ready:
            return None
        return state.book.top(depth or self.config.market.depth_levels)

    def quality(self, symbol: str) -> SymbolQuality | None:
        return self.qualities.get(symbol)

    def is_tradable(self, symbol: str) -> tuple[bool, str]:
        return self.gate.check(self.qualities.get(symbol))

    def state_of(self, symbol: str) -> SymbolState | None:
        return self.states.get(symbol)

    def connected_symbols(self) -> list[str]:
        return [s for s, q in self.qualities.items() if q.state is FeedState.LIVE]

    def warmed_up(self) -> bool:
        """Every symbol has a ready book and at least some history."""
        return all(
            state.book.ready and len(state.tops) > 10 for state in self.states.values()
        )

    def market_summary(self) -> list[dict[str, Any]]:
        rows = []
        for symbol in self.symbols:
            state = self.states[symbol]
            view = state.book.top(10) if state.book.ready else None
            quality = self.qualities.get(symbol)
            spec = next((s for s in self.config.market.symbols if s.symbol == symbol), None)
            last_trade = state.trades[-1] if state.trades else None
            rows.append(
                {
                    "symbol": symbol,
                    "tier": spec.tier if spec else "",
                    "role": spec.role if spec else "",
                    "mid": view.mid if view else None,
                    "microprice": view.microprice() if view else None,
                    "best_bid": view.best_bid if view else None,
                    "best_ask": view.best_ask if view else None,
                    "spread_bps": view.spread_bps() if view else None,
                    "last_price": last_trade.price if last_trade else None,
                    "last_trade_ms": last_trade.ts_ms if last_trade else None,
                    "mark_price": state.mark_price or None,
                    "index_price": state.index_price or None,
                    "funding_rate": state.funding_rate,
                    "next_funding_ms": state.next_funding_ms,
                    "book_state": state.book.state.value,
                    "trades_buffered": len(state.trades),
                    "quality": quality.to_dict() if quality else None,
                }
            )
        return rows

    def health(self) -> dict[str, Any]:
        live = self.connected_symbols()
        return {
            "venue": self.config.market.venue,
            "feed_kind": self.feed.kind,
            "live": self.feed.kind == "live",
            "state": self.feed.state.value,
            "symbols_configured": len(self.symbols),
            "symbols_live": len(live),
            "connected_symbols": live,
            "events_processed": self.processed,
            "ingest_queue": self._ingest.qsize(),
            "ingest_dropped": self._ingest.stats.dropped,
            "feed": self.feed.stats.to_dict(),
            "clock": self.clock.to_dict(),
            "endpoint_check": self.endpoint_check,
            "books": {s: self.states[s].book.state.value for s in self.symbols},
            "quality": self.gate.summary(self.qualities),
            "uptime_s": round((now_ms() - self.started_ms) / 1000.0, 1) if self.started_ms else 0.0,
        }
