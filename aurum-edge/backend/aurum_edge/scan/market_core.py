"""BLOCK 1 - SCAN.  The single Market Core.

One object owns the connection to Bybit, the per-symbol state and the production
of :class:`MarketSnapshot`.  There is no second place where market data lives.

Feed layout, chosen so the whole USDT-perp universe fits in a handful of
connections without ever trading on a thin book:

* every symbol in the universe gets ``tickers``, ``publicTrade`` and top-of-book;
* the focus set - the most active symbols, refreshed continuously - is upgraded
  to full ``orderbook.50`` depth, and **only symbols with full depth are
  tradable**.  A shallow book can rank an opportunity, never fill one.
"""

from __future__ import annotations

import asyncio
import math
from dataclasses import dataclass, field
from typing import Any, Iterable

import aiohttp

from ..config import Config
from ..util.clock import Clock, SkewTracker
from ..util.logging_setup import get_logger
from ..util.rolling import RollingSum, TimeSeries, WelfordVol, clamp, safe_div
from .book import BookState, OrderBook
from .bybit_rest import BybitRest
from .bybit_ws import WsConnection
from .snapshot import DataQuality, FeedSource, MarketSnapshot

log = get_logger("scan.core")

# Distance bands from the mid used to describe available liquidity inside every
# snapshot.  Roughly Fibonacci-spaced: fine where scalps live, coarse further out.
DEPTH_BANDS_BPS: tuple[float, ...] = (1.0, 2.0, 3.0, 5.0, 8.0, 13.0, 21.0, 34.0)


@dataclass
class Instrument:
    symbol: str
    tick_size: float
    qty_step: float
    min_qty: float
    min_notional_usd: float
    max_leverage: float
    status: str
    contract_type: str

    @classmethod
    def from_bybit(cls, raw: dict[str, Any]) -> "Instrument":
        price_filter = raw.get("priceFilter", {}) or {}
        lot = raw.get("lotSizeFilter", {}) or {}
        lev = raw.get("leverageFilter", {}) or {}
        return cls(
            symbol=raw["symbol"],
            tick_size=float(price_filter.get("tickSize", "0.0001") or 0.0001),
            qty_step=float(lot.get("qtyStep", "0.001") or 0.001),
            min_qty=float(lot.get("minOrderQty", "0") or 0.0),
            min_notional_usd=float(lot.get("minNotionalValue", "5") or 5.0),
            max_leverage=float(lev.get("maxLeverage", "10") or 10.0),
            status=str(raw.get("status", "")),
            contract_type=str(raw.get("contractType", "")),
        )

    def round_qty(self, qty: float) -> float:
        if self.qty_step <= 0:
            return qty
        steps = math.floor(qty / self.qty_step + 1e-9)
        return max(steps * self.qty_step, 0.0)

    def format_qty(self, qty: float) -> str:
        decimals = max(0, -int(math.floor(math.log10(self.qty_step)))) if self.qty_step > 0 else 0
        return f"{qty:.{decimals}f}"


@dataclass
class SymbolState:
    """Everything known about one symbol, updated in place by the feed."""

    symbol: str
    instrument: Instrument
    history_window_s: float
    book: OrderBook = field(init=False)
    focus: bool = False

    def __post_init__(self) -> None:
        window_ms = self.history_window_s * 1000.0
        self.book = OrderBook(self.symbol)
        self.mid_series = TimeSeries(window_ms=window_ms, max_points=3000)
        self.vol = WelfordVol(window_ms=60_000.0)
        self.trades_5s = RollingSum(5_000.0)
        self.trades_60s = RollingSum(60_000.0)
        self.vol_5s = RollingSum(5_000.0)
        self.vol_60s = RollingSum(60_000.0)
        self.buy_5s = RollingSum(5_000.0)
        self.sell_5s = RollingSum(5_000.0)
        self.buy_60s = RollingSum(60_000.0)
        self.sell_60s = RollingSum(60_000.0)
        self.ofi_1s = RollingSum(1_000.0)
        self.ofi_5s = RollingSum(5_000.0)

        self.skew = SkewTracker()
        self.last_price: float = 0.0
        self.mark_price: float = 0.0
        self.funding_rate: float = 0.0
        self.turnover_24h: float = 0.0
        self.open_interest: float | None = None
        self.open_interest_ref: float | None = None
        self.last_trade_ms: float = 0.0
        self.last_ticker_ms: float = 0.0
        self.last_book_local_ms: float = 0.0
        self.last_vol_sample_ms: float = 0.0
        self.last_vol_sample_mid: float = 0.0
        self.updates: int = 0

        self._prev_bid: float | None = None
        self._prev_ask: float | None = None
        self._prev_bid_size: float = 0.0
        self._prev_ask_size: float = 0.0

    # ---------------------------------------------------------------- feed in
    def on_book(self, msg: dict[str, Any], local_ms: float) -> None:
        ok = self.book.apply(msg, local_ms)
        self.last_book_local_ms = local_ms
        self.updates += 1
        exchange_ms = float(msg.get("cts", msg.get("ts", 0)) or 0)
        if exchange_ms:
            self.skew.observe(exchange_ms, local_ms)
        if not ok:
            self._prev_bid = self._prev_ask = None
            return
        self._accumulate_ofi(local_ms)
        mid = self.book.mid
        if mid:
            ts = exchange_ms or local_ms
            self.mid_series.push(ts, mid)
            self._sample_volatility(ts, mid)

    def _accumulate_ofi(self, ts_ms: float) -> None:
        """Classic order-flow imbalance at the best quotes (Cont/Kukanov/Stoikov)."""
        bid, ask = self.book.best_bid_price, self.book.best_ask_price
        bid_size, ask_size = self.book.best_bid_size, self.book.best_ask_size
        if bid is None or ask is None:
            return
        if self._prev_bid is not None and self._prev_ask is not None:
            if bid > self._prev_bid:
                d_bid = bid_size
            elif bid == self._prev_bid:
                d_bid = bid_size - self._prev_bid_size
            else:
                d_bid = -self._prev_bid_size

            if ask < self._prev_ask:
                d_ask = ask_size
            elif ask == self._prev_ask:
                d_ask = ask_size - self._prev_ask_size
            else:
                d_ask = -self._prev_ask_size

            event_usd = (d_bid - d_ask) * ((bid + ask) / 2.0)
            if event_usd:
                self.ofi_1s.push(ts_ms, event_usd)
                self.ofi_5s.push(ts_ms, event_usd)
        self._prev_bid, self._prev_ask = bid, ask
        self._prev_bid_size, self._prev_ask_size = bid_size, ask_size

    def _sample_volatility(self, ts_ms: float, mid: float) -> None:
        if self.last_vol_sample_ms and ts_ms - self.last_vol_sample_ms < 1_000.0:
            return
        if self.last_vol_sample_mid > 0 and mid > 0:
            ret_bps = math.log(mid / self.last_vol_sample_mid) * 10_000.0
            self.vol.push(ts_ms, ret_bps)
        self.last_vol_sample_ms = ts_ms
        self.last_vol_sample_mid = mid

    def on_trades(self, msg: dict[str, Any], local_ms: float) -> None:
        rows = msg.get("data") or []
        for row in rows:
            try:
                ts = float(row.get("T", local_ms))
                price = float(row["p"])
                size = float(row["v"])
            except (KeyError, TypeError, ValueError):
                continue
            usd = price * size
            self.trades_5s.push(ts, 1.0)
            self.trades_60s.push(ts, 1.0)
            self.vol_5s.push(ts, usd)
            self.vol_60s.push(ts, usd)
            if str(row.get("S", "")).lower() == "buy":
                self.buy_5s.push(ts, usd)
                self.buy_60s.push(ts, usd)
            else:
                self.sell_5s.push(ts, usd)
                self.sell_60s.push(ts, usd)
            self.last_price = price
            self.last_trade_ms = max(self.last_trade_ms, ts)
        if rows:
            self.skew.observe(float(msg.get("ts", local_ms) or local_ms), local_ms)
        self.updates += 1

    def on_ticker(self, msg: dict[str, Any], local_ms: float) -> None:
        data = msg.get("data") or {}
        if isinstance(data, list):  # defensive: some feeds wrap in a list
            data = data[0] if data else {}
        for key, setter in (
            ("lastPrice", "last_price"),
            ("markPrice", "mark_price"),
            ("fundingRate", "funding_rate"),
            ("turnover24h", "turnover_24h"),
        ):
            raw = data.get(key)
            if raw not in (None, ""):
                try:
                    setattr(self, setter, float(raw))
                except (TypeError, ValueError):
                    pass
        oi_raw = data.get("openInterest")
        if oi_raw not in (None, ""):
            try:
                oi = float(oi_raw)
            except (TypeError, ValueError):
                oi = None
            if oi is not None:
                if self.open_interest_ref is None:
                    self.open_interest_ref = oi
                self.open_interest = oi
        self.last_ticker_ms = float(msg.get("ts", local_ms) or local_ms)
        self.skew.observe(self.last_ticker_ms, local_ms)
        self.updates += 1

    def reset_book(self, depth_topic: str) -> None:
        self.book = OrderBook(self.symbol, depth_topic=depth_topic)
        self._prev_bid = self._prev_ask = None

    # ---------------------------------------------------------------- snapshot
    def _return_bps(self, now_ms: float, horizon_ms: float) -> float:
        current = self.mid_series.last()
        past = self.mid_series.value_at_or_before(now_ms - horizon_ms)
        if not current or not past or past <= 0:
            return 0.0
        return (current / past - 1.0) * 10_000.0

    def build(self, now_ms: float, seq: int, cfg: Config, source: str) -> MarketSnapshot | None:
        book = self.book
        bid, ask = book.best_bid_price, book.best_ask_price
        mid = book.mid
        if bid is None or ask is None or mid is None or mid <= 0:
            return None

        scan = cfg.scan
        exchange_ms = max(book.last_update_ms, self.last_trade_ms, self.last_ticker_ms)
        book_age_ms = max(now_ms - book.last_update_ms, 0.0) if book.last_update_ms else float("inf")
        trade_age_ms = max(now_ms - self.last_trade_ms, 0.0) if self.last_trade_ms else float("inf")

        vol_5s = self.vol_5s.value(now_ms)
        vol_60s = self.vol_60s.value(now_ms)
        buy_5s = self.buy_5s.value(now_ms)
        sell_5s = self.sell_5s.value(now_ms)
        buy_60s = self.buy_60s.value(now_ms)
        sell_60s = self.sell_60s.value(now_ms)
        trades_5s = int(self.trades_5s.value(now_ms))
        trades_60s = int(self.trades_60s.value(now_ms))

        depth_bid, depth_ask = book.depth_notional(scan.book_depth_levels)
        depth_curve_bid, depth_curve_ask = book.depth_curve(DEPTH_BANDS_BPS)
        top_depth_usd = max((book.best_bid_size + book.best_ask_size) / 2.0 * mid, 1.0)
        ofi_1s = math.tanh(self.ofi_1s.value(now_ms) / (top_depth_usd * 4.0))
        ofi_5s = math.tanh(self.ofi_5s.value(now_ms) / (top_depth_usd * 12.0))

        volatility_bps = max(self.vol.stdev(), 0.0)
        window = [v for _, v in self.mid_series.points]
        range_bps = ((max(window) - min(window)) / mid * 10_000.0) if len(window) > 2 else 0.0

        spread = ask - bid
        spread_bps = spread / mid * 10_000.0
        microprice = book.microprice() or mid
        micro_edge_bps = (microprice - mid) / mid * 10_000.0

        oi_change_bps = 0.0
        if self.open_interest and self.open_interest_ref:
            oi_change_bps = (self.open_interest / self.open_interest_ref - 1.0) * 10_000.0

        # Rates over the history actually held, not over the nominal window.
        rate_5s = self.vol_5s.rate(now_ms, min_span_ms=1_000.0)
        rate_60s = self.vol_60s.rate(now_ms, min_span_ms=10_000.0)
        volume_acceleration = (
            clamp(safe_div(rate_5s, rate_60s, 1.0), 0.0, 10.0) if rate_60s > 0 else 1.0
        )

        latency_ms = self.skew.latency_ms

        # ---- data quality -------------------------------------------------
        reasons: list[str] = []
        score = 1.0
        if book.state is not BookState.OK:
            reasons.append(f"book {book.state.value}: {book.resync_reason or 'not in sync'}")
            score = 0.0
        if book_age_ms > scan.stale_book_ms:
            reasons.append(f"book stale {book_age_ms:.0f}ms > {scan.stale_book_ms:.0f}ms")
            score = 0.0
        if not self.focus or book.depth_topic != scan.depth_topic_focus:
            reasons.append("top-of-book only (not in focus set)")
            score = min(score, 0.5)
        if trade_age_ms > scan.stale_trade_ms:
            reasons.append(f"no trade for {trade_age_ms / 1000:.0f}s")
            score = min(score, 0.4)
        if latency_ms > scan.max_latency_ms:
            reasons.append(f"latency {latency_ms:.0f}ms > {scan.max_latency_ms:.0f}ms")
            score = min(score, 0.3)
        if self.mid_series.span_ms() < 15_000.0:
            reasons.append("warming up: less than 15s of history")
            score = min(score, 0.45)
        if spread_bps > scan.max_spread_bps:
            reasons.append(f"spread {spread_bps:.2f}bps > {scan.max_spread_bps:.2f}bps")
            score = min(score, 0.45)
        if volatility_bps <= 0.0:
            reasons.append("volatility not measurable yet")
            score = min(score, 0.45)

        if score <= 0.0:
            quality = DataQuality.BAD
        elif score < 0.99:
            quality = DataQuality.DEGRADED
        else:
            quality = DataQuality.OK

        return MarketSnapshot(
            symbol=self.symbol,
            source=source,
            seq=seq,
            ts_exchange_ms=exchange_ms,
            ts_local_ms=now_ms,
            latency_ms=latency_ms,
            book_age_ms=min(book_age_ms, 1e9),
            trade_age_ms=min(trade_age_ms, 1e9),
            bid=bid,
            ask=ask,
            bid_size=book.best_bid_size,
            ask_size=book.best_ask_size,
            spread=spread,
            spread_bps=spread_bps,
            mid=mid,
            last_price=self.last_price or mid,
            microprice=microprice,
            microprice_edge_bps=micro_edge_bps,
            ret_250ms_bps=self._return_bps(now_ms, 250.0),
            ret_1s_bps=self._return_bps(now_ms, 1_000.0),
            ret_3s_bps=self._return_bps(now_ms, 3_000.0),
            ret_5s_bps=self._return_bps(now_ms, 5_000.0),
            ret_15s_bps=self._return_bps(now_ms, 15_000.0),
            ret_60s_bps=self._return_bps(now_ms, 60_000.0),
            volatility_bps=volatility_bps,
            range_60s_bps=range_bps,
            trades_5s=trades_5s,
            trades_60s=trades_60s,
            trade_rate_hz=trades_5s / 5.0,
            volume_5s_usd=vol_5s,
            volume_60s_usd=vol_60s,
            volume_acceleration=volume_acceleration,
            buy_volume_5s_usd=buy_5s,
            sell_volume_5s_usd=sell_5s,
            aggression_5s=safe_div(buy_5s - sell_5s, buy_5s + sell_5s, 0.0),
            aggression_60s=safe_div(buy_60s - sell_60s, buy_60s + sell_60s, 0.0),
            ofi_1s=ofi_1s,
            ofi_5s=ofi_5s,
            imbalance_top=safe_div(
                book.best_bid_size - book.best_ask_size,
                book.best_bid_size + book.best_ask_size,
                0.0,
            ),
            imbalance_depth=book.imbalance(scan.book_depth_levels),
            depth_bid_usd=depth_bid,
            depth_ask_usd=depth_ask,
            book_state=book.state.value,
            book_levels_bid=book.levels_count()[0],
            book_levels_ask=book.levels_count()[1],
            depth_topic=book.depth_topic,
            book_top=tuple(
                (p, s) for p, s in (book.sorted_bids()[:3] + book.sorted_asks()[:3])
            ),
            depth_curve_bps=DEPTH_BANDS_BPS,
            depth_curve_bid_usd=tuple(depth_curve_bid),
            depth_curve_ask_usd=tuple(depth_curve_ask),
            open_interest=self.open_interest,
            open_interest_change_bps=oi_change_bps,
            funding_rate=self.funding_rate,
            turnover_24h_usd=self.turnover_24h,
            tick_size=self.instrument.tick_size,
            qty_step=self.instrument.qty_step,
            min_qty=self.instrument.min_qty,
            min_notional_usd=self.instrument.min_notional_usd,
            max_leverage=self.instrument.max_leverage,
            quality=quality.value,
            quality_score=round(score, 3),
            quality_reasons=tuple(reasons),
            tradable=quality is DataQuality.OK,
        )


class MarketCore:
    """Owns the Bybit public connection and every symbol's state."""

    def __init__(
        self,
        cfg: Config,
        clock: Clock,
        rest: BybitRest,
        source: FeedSource = FeedSource.BYBIT,
        ws_url: str | None = None,
        session: aiohttp.ClientSession | None = None,
    ) -> None:
        self.cfg = cfg
        self.clock = clock
        self.rest = rest
        self.source = source
        self.ws_url = ws_url or cfg.bybit.ws_public
        self._session = session

        self.instruments: dict[str, Instrument] = {}
        self.states: dict[str, SymbolState] = {}
        self.universe: list[str] = []
        self.focus: set[str] = set()
        self.connections: list[WsConnection] = []
        self.seq = 0
        self.started_at = 0.0
        self.last_universe_refresh = 0.0
        self.last_focus_refresh = 0.0
        self.universe_error: str = ""
        self.on_reconnect_hook = None  # set by the engine
        self.message_count = 0
        self._stop = asyncio.Event()
        self._tasks: list[asyncio.Task[None]] = []

    # ---------------------------------------------------------------- universe
    async def discover_universe(self) -> list[str]:
        """Every liquid USDT perpetual, ranked by 24 h turnover."""
        instruments = await self.rest.instruments()
        tickers = {t["symbol"]: t for t in await self.rest.tickers()}
        self.instruments = {}
        rows: list[tuple[float, str]] = []
        for raw in instruments:
            inst = Instrument.from_bybit(raw)
            if inst.status != "Trading":
                continue
            if raw.get("quoteCoin") != self.cfg.bybit.quote_coin:
                continue
            if inst.contract_type not in ("LinearPerpetual", "LinearFutures"):
                continue
            if inst.contract_type == "LinearFutures":
                continue  # perpetuals only
            ticker = tickers.get(inst.symbol)
            if not ticker:
                continue
            try:
                turnover = float(ticker.get("turnover24h", 0) or 0)
            except (TypeError, ValueError):
                continue
            if turnover < self.cfg.scan.min_turnover_24h_usd:
                continue
            self.instruments[inst.symbol] = inst
            rows.append((turnover, inst.symbol))

        rows.sort(reverse=True)
        self.universe = [symbol for _, symbol in rows[: self.cfg.scan.max_universe]]
        self.last_universe_refresh = self.clock.mono()
        for symbol in self.universe:
            if symbol not in self.states:
                self.states[symbol] = SymbolState(
                    symbol=symbol,
                    instrument=self.instruments[symbol],
                    history_window_s=self.cfg.scan.history_window_s,
                )
        log.info(
            "universe: %d symbols out of %d instruments (min turnover %.0f USD)",
            len(self.universe), len(instruments), self.cfg.scan.min_turnover_24h_usd,
        )
        return self.universe

    # ---------------------------------------------------------------- feeds
    def _topics_for(self, symbol: str) -> list[str]:
        depth = (
            self.cfg.scan.depth_topic_focus
            if symbol in self.focus
            else self.cfg.scan.depth_topic_universe
        )
        return [f"{depth}.{symbol}", f"publicTrade.{symbol}", f"tickers.{symbol}"]

    async def start(self) -> None:
        if not self.universe:
            await self.discover_universe()
        self.started_at = self.clock.mono()
        self._pick_focus(initial=True)
        # the book must know which topic feeds it: depth quality depends on it
        for symbol, state in self.states.items():
            state.reset_book(
                self.cfg.scan.depth_topic_focus
                if symbol in self.focus
                else self.cfg.scan.depth_topic_universe
            )

        per_conn = max(self.cfg.scan.topics_per_connection // 3, 1)  # 3 topics per symbol
        chunks = [
            self.universe[i : i + per_conn] for i in range(0, len(self.universe), per_conn)
        ] or [[]]
        for index, chunk in enumerate(chunks):
            topics: list[str] = []
            for symbol in chunk:
                topics.extend(self._topics_for(symbol))
            conn = WsConnection(
                name=f"public-{index}",
                url=self.ws_url,
                on_message=self._on_public_message,
                ping_interval_s=self.cfg.scan.ping_interval_s,
                ping_timeout_s=self.cfg.scan.ping_timeout_s,
                stale_feed_ms=self.cfg.scan.stale_feed_ms,
                subscribe_batch=self.cfg.scan.subscribe_batch,
                reconnect_base_delay_s=self.cfg.scan.reconnect_base_delay_s,
                reconnect_max_delay_s=self.cfg.scan.reconnect_max_delay_s,
                session=self._session,
                on_reconnect=self._on_reconnect,
            )
            self.connections.append(conn)
            await conn.start(topics)

        self._tasks.append(asyncio.create_task(self._maintenance_loop(), name="scan-maintenance"))

    async def stop(self) -> None:
        self._stop.set()
        for task in self._tasks:
            task.cancel()
        for task in self._tasks:
            try:
                await task
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
                pass
        await asyncio.gather(*(c.stop() for c in self.connections), return_exceptions=True)

    async def wait_ready(self, timeout: float = 20.0) -> bool:
        results = await asyncio.gather(
            *(c.wait_live(timeout) for c in self.connections), return_exceptions=True
        )
        return all(r is True for r in results)

    async def _on_reconnect(self, name: str) -> None:
        """A connection came back: every book it feeds is a guess until resnapshot."""
        log.warning("[%s] reconnected - invalidating books on that connection", name)
        conn = next((c for c in self.connections if c.name == name), None)
        symbols: Iterable[str] = self.states
        if conn is not None:
            symbols = {t.split(".")[-1] for t in conn.topics}
        for symbol in symbols:
            state = self.states.get(symbol)
            if state is not None:
                state.book.state = BookState.RESYNC
                state.book.resync_reason = f"{name} reconnected"
        if self.on_reconnect_hook is not None:
            await self.on_reconnect_hook(name)

    def _on_public_message(self, msg: dict[str, Any]) -> None:
        topic = msg.get("topic", "")
        if not topic:
            return
        local_ms = self.clock.now_ms()
        self.message_count += 1
        head, _, symbol = topic.rpartition(".")
        state = self.states.get(symbol)
        if state is None:
            return
        if head.startswith("orderbook"):
            state.on_book(msg, local_ms)
        elif head == "publicTrade":
            state.on_trades(msg, local_ms)
        elif head == "tickers":
            state.on_ticker(msg, local_ms)

    # ---------------------------------------------------------------- focus set
    def _activity_score(self, state: SymbolState) -> float:
        now = self.clock.now_ms()
        vol_5s = state.vol_5s.value(now)
        vol_60s = state.vol_60s.value(now)
        accel = safe_div(vol_5s / 5.0, max(vol_60s / 60.0, 1e-9), 1.0)
        volatility = state.vol.stdev()
        return math.log1p(vol_60s) * clamp(accel, 0.2, 6.0) * clamp(volatility, 0.2, 40.0)

    def _pick_focus(self, initial: bool = False) -> set[str]:
        if initial:
            ranked = self.universe[: self.cfg.scan.focus_size]
        else:
            scored = sorted(
                (self._activity_score(s), s.symbol) for s in self.states.values()
            )
            ranked = [symbol for _, symbol in reversed(scored)][: self.cfg.scan.focus_size]
        new_focus = set(ranked)
        for symbol in new_focus:
            state = self.states.get(symbol)
            if state is not None:
                state.focus = True
        for symbol in self.focus - new_focus:
            state = self.states.get(symbol)
            if state is not None:
                state.focus = False
        self.focus = new_focus
        self.last_focus_refresh = self.clock.mono()
        return new_focus

    async def _apply_focus_topics(self, previous: set[str]) -> None:
        """Upgrade new focus symbols to full depth, downgrade the ones that left."""
        entering = self.focus - previous
        leaving = previous - self.focus
        if not entering and not leaving:
            return
        for conn in self.connections:
            add: list[str] = []
            remove: list[str] = []
            conn_symbols = {t.rpartition(".")[2] for t in conn.topics}
            for symbol in entering & conn_symbols:
                remove.append(f"{self.cfg.scan.depth_topic_universe}.{symbol}")
                add.append(f"{self.cfg.scan.depth_topic_focus}.{symbol}")
            for symbol in leaving & conn_symbols:
                remove.append(f"{self.cfg.scan.depth_topic_focus}.{symbol}")
                add.append(f"{self.cfg.scan.depth_topic_universe}.{symbol}")
            if remove:
                await conn.unsubscribe(remove)
            if add:
                await conn.subscribe(add)
        for symbol in entering:
            state = self.states.get(symbol)
            if state is not None:
                state.reset_book(self.cfg.scan.depth_topic_focus)
        for symbol in leaving:
            state = self.states.get(symbol)
            if state is not None:
                state.reset_book(self.cfg.scan.depth_topic_universe)

    async def _maintenance_loop(self) -> None:
        while not self._stop.is_set():
            try:
                await asyncio.sleep(1.0)
                now = self.clock.mono()
                if now - self.last_focus_refresh >= self.cfg.scan.focus_refresh_s:
                    previous = set(self.focus)
                    self._pick_focus()
                    await self._apply_focus_topics(previous)
                if now - self.last_universe_refresh >= self.cfg.scan.universe_refresh_s:
                    try:
                        await self.discover_universe()
                        self.universe_error = ""
                    except Exception as exc:  # noqa: BLE001 - keep trading on old universe
                        self.universe_error = str(exc)
                        log.warning("universe refresh failed: %s", exc)
                self._mark_stale_books()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                log.exception("maintenance loop error: %s", exc)

    def _mark_stale_books(self) -> None:
        now = self.clock.now_ms()
        limit = self.cfg.scan.stale_book_ms
        for state in self.states.values():
            book = state.book
            if book.state is BookState.OK and book.last_local_ms:
                if now - book.last_local_ms > limit:
                    book.mark_stale(f"no update for {now - book.last_local_ms:.0f}ms")

    # ---------------------------------------------------------------- snapshots
    def snapshot(self, symbol: str) -> MarketSnapshot | None:
        state = self.states.get(symbol)
        if state is None:
            return None
        self.seq += 1
        return state.build(self.clock.now_ms(), self.seq, self.cfg, self.source.value)

    def snapshots(self, symbols: Iterable[str] | None = None) -> list[MarketSnapshot]:
        now = self.clock.now_ms()
        out: list[MarketSnapshot] = []
        for symbol in symbols if symbols is not None else self.universe:
            state = self.states.get(symbol)
            if state is None:
                continue
            self.seq += 1
            snap = state.build(now, self.seq, self.cfg, self.source.value)
            if snap is not None:
                out.append(snap)
        return out

    # ---------------------------------------------------------------- health
    def health(self) -> dict[str, Any]:
        conns = [c.health() for c in self.connections]
        live = sum(1 for c in conns if c["live"])
        books_ok = sum(
            1 for s in self.states.values() if s.book.state is BookState.OK
        )
        focus_ok = sum(
            1
            for symbol in self.focus
            if (state := self.states.get(symbol)) and state.book.state is BookState.OK
        )
        latencies = [s.skew.latency_ms for s in self.states.values() if s.skew.samples > 0]
        latencies.sort()
        return {
            "source": self.source.value,
            "connections": conns,
            "connections_live": live,
            "connections_total": len(conns),
            "symbols": len(self.universe),
            "focus": sorted(self.focus),
            "focus_size": len(self.focus),
            "books_ok": books_ok,
            "focus_books_ok": focus_ok,
            "messages": self.message_count,
            "latency_ms_p50": round(latencies[len(latencies) // 2], 1) if latencies else None,
            "latency_ms_p95": round(latencies[int(len(latencies) * 0.95)], 1) if latencies else None,
            "universe_error": self.universe_error,
            "uptime_s": round(self.clock.mono() - self.started_at, 1) if self.started_at else 0.0,
        }
