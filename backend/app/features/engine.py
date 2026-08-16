"""Feature engine.

Consumes the canonical market stream and produces a causal feature vector at a
fixed cadence (default 100 ms). "Causal" means: every value is computed from
information available at or before the vector's timestamp. There is no
forward-filling from the future, no centred window, and no look-ahead of any
kind - this is what makes the recorded `features` table usable for honest
supervised learning later.

Priority order, as specified: order flow > order book > microstructure > price
action > classical indicators.
"""

from __future__ import annotations

import asyncio
import math
from typing import Any

from app.config import Settings
from app.core.bus import EventBus, Topic
from app.core.clock import now_ms
from app.core.logging_conf import get_logger
from app.features.indicators import atr, bollinger, ema, rsi, vwap
from app.features.rolling import BarAggregator, TimeSeries, TradeRecord, TradeWindow
from app.marketdata.engine import MarketDataEngine
from app.marketdata.orderbook import LocalOrderBook
from app.marketdata.types import MarketTick, Trade

log = get_logger(__name__)

RETURN_HORIZONS_MS = (100, 250, 500, 1000, 2000, 3000, 5000)
FLOW_WINDOWS_MS = (1000, 5000, 30000)

#: Minute-scale windows, for horizons measured in minutes rather than seconds.
#: Everything above describes the next few seconds: order-flow imbalance and a
#: 5-second return say nothing about where price is in fifteen minutes. These
#: are computed only when the tick buffer actually covers them, so a 5-second
#: configuration simply reports them as unavailable rather than guessing.
LONG_RETURN_HORIZONS_MS = (15_000, 60_000, 300_000, 900_000)
LONG_VOL_WINDOWS_MS = (60_000, 300_000, 900_000)
LONG_RANGE_WINDOWS_MS = (60_000, 300_000, 900_000)


class FeatureEngine:
    def __init__(
        self, settings: Settings, bus: EventBus, market: MarketDataEngine
    ) -> None:
        self.settings = settings
        self.bus = bus
        self.market = market

        window_ms = settings.tick_buffer_seconds * 1000
        self.mid = TimeSeries(window_ms)
        self.micro = TimeSeries(window_ms)
        self.spread_bps = TimeSeries(window_ms)
        self.bid_depth = TimeSeries(60_000)
        self.ask_depth = TimeSeries(60_000)
        self.trades = TradeWindow(settings.trade_buffer_seconds * 1000)
        self.bars = BarAggregator(interval_ms=1000, maxlen=900)

        self.latest: dict[str, Any] | None = None
        self.computed = 0
        self._tasks: list[asyncio.Task] = []
        self._running = False
        self._last_large_threshold: float | None = None

    # ------------------------------------------------------------- lifecycle
    async def start(self) -> None:
        self._running = True
        self._tasks = [
            asyncio.create_task(self._consume_ticks(), name="feat-ticks"),
            asyncio.create_task(self._consume_trades(), name="feat-trades"),
            asyncio.create_task(self._compute_loop(), name="feat-compute"),
        ]

    async def stop(self) -> None:
        self._running = False
        for t in self._tasks:
            t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()

    async def _consume_ticks(self) -> None:
        sub = self.bus.subscribe(Topic.TICK, maxsize=1024)
        try:
            while self._running:
                tick: MarketTick = await sub.queue.get()
                self.ingest_tick(tick)
        except asyncio.CancelledError:
            raise
        finally:
            sub.close()

    async def _consume_trades(self) -> None:
        sub = self.bus.subscribe(Topic.TRADE, maxsize=4096)
        try:
            while self._running:
                trade: Trade = await sub.queue.get()
                self.ingest_trade(trade)
        except asyncio.CancelledError:
            raise
        finally:
            sub.close()

    async def _compute_loop(self) -> None:
        interval = self.settings.feature_interval_ms / 1000.0
        while self._running:
            await asyncio.sleep(interval)
            try:
                fv = self.compute()
                if fv is not None:
                    self.latest = fv
                    self.computed += 1
                    self.bus.publish(Topic.FEATURES, fv)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                self.market.record_error("features", f"{type(exc).__name__}: {exc}")

    # -------------------------------------------------------------- ingestion
    def ingest_tick(self, tick: MarketTick) -> None:
        self.mid.append(tick.ts, tick.mid)
        self.micro.append(tick.ts, tick.micro_price)
        self.spread_bps.append(tick.ts, tick.spread_bps)
        self.bars.add_price(tick.ts, tick.mid)

    def ingest_trade(self, trade: Trade) -> None:
        self.trades.append(
            TradeRecord(
                ts=trade.server_ts,
                price=trade.price,
                quantity=trade.quantity,
                notional=trade.notional,
                is_buy=(not trade.is_buyer_maker),
            )
        )
        self.bars.add_trade(trade.server_ts, trade.price, trade.quantity)

    # ----------------------------------------------------------- computation
    def compute(self, ts: int | None = None) -> dict[str, Any] | None:
        """Build the feature vector for `ts` (defaults to now)."""
        tick = self.market.last_tick
        if tick is None or self.mid.last is None:
            return None
        ts = ts or now_ms()
        book = self.market.book
        quality = self.market.data_quality()

        f: dict[str, Any] = {}

        # ------------------------------------------------------ price action
        for ms in RETURN_HORIZONS_MS:
            f[f"return_{ms}ms"] = self.mid.returns_since(ms)
        r1 = f.get("return_1000ms")
        r5 = f.get("return_5000ms")
        # velocity: bps per second over the last second
        f["velocity_bps_s"] = r1
        # acceleration: change in 1s velocity between now and 1s ago
        prev_r1 = None
        if len(self.mid) > 3:
            past = self.mid.value_ago(1000)
            past2 = self.mid.value_ago(2000)
            if past and past2 and past2 > 0:
                prev_r1 = (past - past2) / past2 * 10_000.0
        f["acceleration_bps_s2"] = (
            (r1 - prev_r1) if (r1 is not None and prev_r1 is not None) else None
        )
        f["momentum_5s"] = r5
        f["momentum_consistency"] = _consistency(
            [f.get(f"return_{ms}ms") for ms in (500, 1000, 2000, 3000, 5000)]
        )

        # ------------------------------------------------- minute-scale trend
        # Guarded by the buffer: asking for a 15-minute return from a 5-minute
        # buffer must yield "unavailable", never a shorter window silently
        # relabelled as a longer one.
        buffer_ms = self.settings.tick_buffer_seconds * 1000
        for ms in LONG_RETURN_HORIZONS_MS:
            f[f"return_{ms}ms"] = self.mid.returns_since(ms) if ms <= buffer_ms else None
        for ms in LONG_RANGE_WINDOWS_MS:
            label = ms // 60_000
            f[f"range_position_{label}m"] = (
                self.mid.range_position(ms) if ms <= buffer_ms else None
            )
            f[f"drift_bps_min_{label}m"] = (
                self.mid.drift_bps_per_min(ms) if ms <= buffer_ms else None
            )
        f["long_momentum_consistency"] = _consistency(
            [f.get(f"return_{ms}ms") for ms in LONG_RETURN_HORIZONS_MS]
        )

        # -------------------------------------------------------- order book
        f["mid"] = tick.mid
        f["micro_price"] = tick.micro_price
        f["micro_price_dev_bps"] = (
            (tick.micro_price - tick.mid) / tick.mid * 10_000.0 if tick.mid else None
        )
        f["spread"] = tick.spread
        f["spread_bps"] = tick.spread_bps
        f["relative_spread"] = tick.spread / tick.mid if tick.mid else None
        top_total = tick.bid_qty + tick.ask_qty
        # On real data the touch is often a dust order - $96 against $10,583 was
        # measured on BTC - which pins this ratio near +/-1 regardless of any
        # real pressure. Below a minimum notional the number is noise, so it is
        # reported as unavailable rather than as a strong signal.
        bid_notional = tick.bid_qty * tick.bid_price
        ask_notional = tick.ask_qty * tick.ask_price
        min_notional = self.settings.min_l1_notional
        if top_total > 0 and min(bid_notional, ask_notional) >= min_notional:
            f["book_imbalance_l1"] = (tick.bid_qty - tick.ask_qty) / top_total
        else:
            f["book_imbalance_l1"] = None
        f["l1_dust"] = min(bid_notional, ask_notional) < min_notional
        f["bid_qty_l1"] = tick.bid_qty
        f["ask_qty_l1"] = tick.ask_qty
        f.update(self._book_features(book))

        # --------------------------------------------------------- order flow
        self.trades.trim(ts)
        threshold = (
            self.trades.notional_quantile(self.settings.large_trade_quantile)
            or self._last_large_threshold
        )
        self._last_large_threshold = threshold
        for ms in FLOW_WINDOWS_MS:
            flow = self.trades.flow(ms)
            tag = f"{ms // 1000}s"
            f[f"buy_volume_{tag}"] = flow["buy_volume"]
            f[f"sell_volume_{tag}"] = flow["sell_volume"]
            f[f"volume_imbalance_{tag}"] = flow["volume_imbalance"]
            f[f"buy_sell_ratio_{tag}"] = _finite(flow["buy_sell_ratio"])
            f[f"trade_intensity_{tag}"] = flow["trade_intensity"]
            f[f"avg_trade_size_{tag}"] = flow["avg_trade_size"]
            f[f"trade_count_{tag}"] = flow["count"]
            f[f"aggressive_buy_notional_{tag}"] = flow["buy_notional"]
            f[f"aggressive_sell_notional_{tag}"] = flow["sell_notional"]
        buys, sells = self.trades.consecutive()
        f["consecutive_buys"] = float(buys)
        f["consecutive_sells"] = float(sells)
        if threshold:
            f.update(
                {
                    k: v
                    for k, v in self.trades.large_trades(threshold, 5000).items()
                }
            )
            f["large_trade_threshold_notional"] = threshold
        else:
            f["large_trade_count"] = 0.0
            f["large_buy_notional"] = 0.0
            f["large_sell_notional"] = 0.0
            f["large_trade_threshold_notional"] = None

        # --------------------------------------------------------- volatility
        f["realized_vol_1s_bps"] = self.mid.realized_vol_bps(1000)
        f["realized_vol_5s_bps"] = self.mid.realized_vol_bps(5000)
        f["realized_vol_30s_bps"] = self.mid.realized_vol_bps(30000)
        for ms in LONG_VOL_WINDOWS_MS:
            label = ms // 60_000
            f[f"realized_vol_{label}m_bps"] = (
                self.mid.realized_vol_bps(ms) if ms <= buffer_ms else None
            )
        v_short = f["realized_vol_5s_bps"]
        v_long = f["realized_vol_30s_bps"]
        f["vol_ratio_5s_30s"] = (
            (v_short / v_long) if (v_short and v_long and v_long > 0) else None
        )
        f["vol_acceleration"] = (
            (v_short - v_long) if (v_short is not None and v_long is not None) else None
        )
        # Expected move over the signal horizon - drives trigger placement.
        horizon_ms = int(self.settings.signal_horizon_s * 1000)
        lookback_ms = int(self.settings.volatility_window_s * 1000)
        f["sigma_horizon_bps"] = self.mid.sigma_over(horizon_ms, lookback_ms)
        # How often price simply does not move over one horizon. At 5 seconds on
        # real BTC this was 32% - the single largest determinant of whether a
        # binary bet at this horizon can pay at all.
        f["zero_move_fraction"] = self.mid.zero_move_fraction(
            horizon_ms, lookback_ms, tolerance=self.settings.tick_size / 2.0
        )
        f["expected_move_ticks"] = (
            f["sigma_horizon_bps"] / 10_000.0 * tick.mid / self.settings.tick_size
            if f["sigma_horizon_bps"] and self.settings.tick_size > 0
            else None
        )

        # -------------------------------------------------- classical (2nd tier)
        bars = self.bars.closed_bars()
        closes = [b.close for b in bars]
        f["ema_9"] = ema(closes, 9)
        f["ema_21"] = ema(closes, 21)
        f["ema_spread_bps"] = (
            (f["ema_9"] - f["ema_21"]) / f["ema_21"] * 10_000.0
            if f["ema_9"] and f["ema_21"]
            else None
        )
        f["rsi_14"] = rsi(closes, 14)
        f["vwap_60s"] = vwap(bars[-60:]) if bars else None
        f["vwap_deviation_bps"] = (
            (tick.mid - f["vwap_60s"]) / f["vwap_60s"] * 10_000.0
            if f["vwap_60s"]
            else None
        )
        bb = bollinger(closes, 20, 2.0)
        if bb:
            f["bb_mid"], f["bb_upper"], f["bb_lower"], f["bb_z"] = bb
        else:
            f["bb_mid"] = f["bb_upper"] = f["bb_lower"] = f["bb_z"] = None
        f["atr_14"] = atr(bars, 14)

        # ------------------------------------- derivatives (when a futures feed
        # is configured; otherwise these stay None and every consumer treats the
        # information as simply unavailable)
        f.update(self._derivative_features(ts))

        # ------------------------------------------------------------- meta
        f["latency_ms"] = tick.latency_ms
        f["book_synced"] = book.synced
        f["data_quality"] = quality["score"]
        f["history_span_ms"] = self.mid.span_ms()
        f["tick_count"] = len(self.mid)

        return {
            "ts": ts,
            "exchange": tick.exchange,
            "symbol": tick.symbol,
            "source": tick.source.value,
            "is_synthetic": tick.source.value == "SYNTHETIC",
            "features": f,
        }

    def _derivative_features(self, ts: int) -> dict[str, Any]:
        """Funding, open interest and forced-liquidation pressure.

        All None when no futures adapter is configured - never zero-filled, so a
        model cannot mistake "no feed" for "no liquidations".
        """
        out: dict[str, Any] = {
            "funding_rate": None,
            "open_interest": None,
            "mark_index_spread_bps": None,
            "liq_buy_notional_5s": None,
            "liq_sell_notional_5s": None,
            "liq_imbalance_5s": None,
            "liq_count_30s": None,
        }
        d = self.market.derivatives
        if d is not None:
            out["funding_rate"] = d.funding_rate
            out["open_interest"] = d.open_interest
            if d.mark_price and d.index_price:
                out["mark_index_spread_bps"] = (
                    (d.mark_price - d.index_price) / d.index_price * 10_000.0
                )

        liqs = self.market.recent_liquidations
        has_liq_feed = any(
            "LIQUIDATIONS" in {c.value.upper() for c in a.capabilities}
            for a in self.market.adapters.values()
        )
        if has_liq_feed:
            recent = [lvl for lvl in liqs if ts - lvl.server_ts <= 5000]
            # A forced BUY prints as an aggressive buy: shorts being closed.
            buy = sum(lvl.price * lvl.quantity for lvl in recent if lvl.side.value == "BUY")
            sell = sum(lvl.price * lvl.quantity for lvl in recent if lvl.side.value == "SELL")
            total = buy + sell
            out["liq_buy_notional_5s"] = buy
            out["liq_sell_notional_5s"] = sell
            out["liq_imbalance_5s"] = ((buy - sell) / total) if total > 0 else 0.0
            out["liq_count_30s"] = float(
                sum(1 for lvl in liqs if ts - lvl.server_ts <= 30000)
            )
        return out

    def _book_features(self, book: LocalOrderBook) -> dict[str, Any]:
        out: dict[str, Any] = {}
        if not book.synced or not book.bids or not book.asks:
            out.update(
                {
                    "depth_imbalance_5": None,
                    "depth_imbalance_20": None,
                    "depth_notional_bid_20": None,
                    "depth_notional_ask_20": None,
                    "liquidity_concentration_bid": None,
                    "liquidity_concentration_ask": None,
                    "bid_wall_distance_bps": None,
                    "ask_wall_distance_bps": None,
                    "bid_wall_size": None,
                    "ask_wall_size": None,
                    "liquidity_removal_bid": None,
                    "liquidity_removal_ask": None,
                    "depth_within_5bps_bid": None,
                    "depth_within_5bps_ask": None,
                    "book_levels_bid": len(book.bids),
                    "book_levels_ask": len(book.asks),
                }
            )
            return out

        mid = book.mid()
        bid5, ask5 = book.depth_qty(5)
        bid20, ask20 = book.depth_qty(20)
        nb20, na20 = book.depth_notional(20)
        out["depth_imbalance_5"] = (
            (bid5 - ask5) / (bid5 + ask5) if (bid5 + ask5) > 0 else 0.0
        )
        out["depth_imbalance_20"] = (
            (bid20 - ask20) / (bid20 + ask20) if (bid20 + ask20) > 0 else 0.0
        )
        out["depth_notional_bid_20"] = nb20
        out["depth_notional_ask_20"] = na20

        bids, asks = book.top(20)
        out["liquidity_concentration_bid"] = (
            max(lvl.quantity for lvl in bids) / bid20 if bid20 > 0 else None
        )
        out["liquidity_concentration_ask"] = (
            max(lvl.quantity for lvl in asks) / ask20 if ask20 > 0 else None
        )

        # A "wall" is a level materially larger than the local average.
        avg_bid = bid20 / max(len(bids), 1)
        avg_ask = ask20 / max(len(asks), 1)
        k = self.settings.book_wall_multiple
        bid_wall = next((lvl for lvl in bids if lvl.quantity >= k * avg_bid), None)
        ask_wall = next((lvl for lvl in asks if lvl.quantity >= k * avg_ask), None)
        out["bid_wall_size"] = bid_wall.quantity if bid_wall else None
        out["ask_wall_size"] = ask_wall.quantity if ask_wall else None
        out["bid_wall_distance_bps"] = (
            (mid - bid_wall.price) / mid * 10_000.0 if bid_wall and mid else None
        )
        out["ask_wall_distance_bps"] = (
            (ask_wall.price - mid) / mid * 10_000.0 if ask_wall and mid else None
        )

        wb, wa = book.depth_within_bps(5.0)
        out["depth_within_5bps_bid"] = wb
        out["depth_within_5bps_ask"] = wa

        # Liquidity removal: how much of the near-touch depth vanished vs 1s ago.
        ts = now_ms()
        self.bid_depth.append(ts, bid20)
        self.ask_depth.append(ts, ask20)
        prev_bid = self.bid_depth.value_ago(1000)
        prev_ask = self.ask_depth.value_ago(1000)
        out["liquidity_removal_bid"] = (
            (prev_bid - bid20) / prev_bid if prev_bid and prev_bid > 0 else None
        )
        out["liquidity_removal_ask"] = (
            (prev_ask - ask20) / prev_ask if prev_ask and prev_ask > 0 else None
        )
        out["book_levels_bid"] = len(book.bids)
        out["book_levels_ask"] = len(book.asks)
        return out


def _finite(value: float | None) -> float | None:
    if value is None:
        return None
    if isinstance(value, float) and (math.isinf(value) or math.isnan(value)):
        return None
    return value


def _consistency(values: list[float | None]) -> float | None:
    """+1 when every horizon agrees on direction, -1 when they fully disagree."""
    vals = [v for v in values if v is not None]
    if len(vals) < 2:
        return None
    signs = [1 if v > 0 else (-1 if v < 0 else 0) for v in vals]
    return sum(signs) / len(signs)
