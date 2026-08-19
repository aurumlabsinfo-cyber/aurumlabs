"""Feature engine.

Raw events remain the source of truth; this produces a *snapshot* of derived
state on a fixed cadence (250 ms by default) so that research, execution and the
UI all read the same numbers computed the same way at the same instants.

Two properties matter more than the feature list:

**Causality.**  Every value in a snapshot at time *t* is computed only from data
observed at or before *t*.  There is no forward fill from a later event and no
centred window.  A feature that peeked one event into the future would produce a
beautiful backtest and lose money live, so the sampled history is append-only
and indexed by position.

**Stable names.**  A hypothesis records the feature names it depends on and is
replayed months later.  The names below are part of the persisted contract:
adding one is free, renaming one invalidates stored research.

Feature families (blueprint §5), with the exact keys they emit:

===================  ====================================================
Price                mid, microprice, spread, spread_bps, micro_dev_bps
Returns              ret_250ms … ret_60s (basis points, causal)
Volatility           vol_1s … vol_60s (realized, bps), vol_ratio
Order book           depth_bid_N, depth_ask_N, imbalance_N, depth_notional_N
Order flow           buy_vol_W, sell_vol_W, flow_imbalance_W, trades_W, ofi_W
Liquidity            liq_added_bid_W, liq_removed_bid_W, …, liq_pressure_W
Derivatives          mark_dev_bps, index_dev_bps, funding_rate, funding_bps_8h
Meta                 quality_score, latency_ms, book_age_ms, regime_code
===================  ====================================================
"""

from __future__ import annotations

import asyncio
import math
from collections import deque
from dataclasses import dataclass, field
from typing import Any

from ..bus import TOPIC_FEATURES, EventBus
from ..config import Config
from ..domain import BookSnapshot, FeatureSnapshot, Regime, Side, now_ms
from ..logging_setup import get_logger
from ..market.data_engine import DataEngine, SymbolState
from ..storage.repositories import Repositories
from .regime import RegimeClassifier

log = get_logger("features.engine")


@dataclass
class SymbolHistory:
    """Cadence-sampled history for one symbol.

    Sampling on a fixed grid is what makes returns and cross-market lags cheap
    and unambiguous: a horizon of 1 s is exactly ``1000 / cadence_ms`` positions
    back, for every symbol, with no interpolation and no lookahead.
    """

    symbol: str
    cadence_ms: int
    capacity: int
    ts: deque[int] = field(default_factory=deque)
    mid: deque[float] = field(default_factory=deque)
    micro: deque[float] = field(default_factory=deque)
    ret: deque[float] = field(default_factory=deque)  # per-step return, bps

    def __post_init__(self) -> None:
        self.ts = deque(maxlen=self.capacity)
        self.mid = deque(maxlen=self.capacity)
        self.micro = deque(maxlen=self.capacity)
        self.ret = deque(maxlen=self.capacity)

    def append(self, ts_ms: int, mid: float, micro: float) -> None:
        previous = self.mid[-1] if self.mid else 0.0
        step_ret = ((mid - previous) / previous * 10_000.0) if previous > 0 else 0.0
        self.ts.append(ts_ms)
        self.mid.append(mid)
        self.micro.append(micro)
        self.ret.append(step_ret)

    def steps_for(self, horizon_ms: int) -> int:
        return max(1, round(horizon_ms / self.cadence_ms))

    def return_bps(self, horizon_ms: int) -> float | None:
        """Return over ``horizon_ms``, looking only backwards."""
        steps = self.steps_for(horizon_ms)
        if len(self.mid) <= steps:
            return None
        past = self.mid[-steps - 1]
        current = self.mid[-1]
        if past <= 0:
            return None
        return (current - past) / past * 10_000.0

    def volatility_bps(self, window_ms: int) -> float | None:
        """Realized volatility: root sum of squared step returns in the window.

        Not annualised and not scaled — it is the size of the move actually
        observed over that window, which is what a horizon decision needs.
        """
        steps = self.steps_for(window_ms)
        if len(self.ret) < steps + 1:
            return None
        recent = list(self.ret)[-steps:]
        return math.sqrt(sum(r * r for r in recent))

    def forward_return_bps(self, index: int, horizon_ms: int) -> float | None:
        """Return from ``index`` forward.  For research over *stored* history
        only — never called with an index at the live edge."""
        steps = self.steps_for(horizon_ms)
        target = index + steps
        if index < 0 or target >= len(self.mid):
            return None
        start = self.mid[index]
        if start <= 0:
            return None
        return (self.mid[target] - start) / start * 10_000.0

    def __len__(self) -> int:
        return len(self.mid)


def _imbalance(bid: float, ask: float) -> float:
    total = bid + ask
    return (bid - ask) / total if total > 0 else 0.0


class FeatureEngine:
    def __init__(
        self,
        config: Config,
        data_engine: DataEngine,
        bus: EventBus,
        repos: Repositories,
    ) -> None:
        self.config = config
        self.data = data_engine
        self.bus = bus
        self.repos = repos
        self.cadence_ms = config.features.cadence_ms
        capacity = max(64, int(config.features.buffer_seconds * 1000 / self.cadence_ms))
        self.history: dict[str, SymbolHistory] = {
            symbol: SymbolHistory(symbol, self.cadence_ms, capacity) for symbol in data_engine.symbols
        }
        self.regime = RegimeClassifier(config)
        self.latest: dict[str, FeatureSnapshot] = {}
        self.snapshots: dict[str, deque[FeatureSnapshot]] = {
            symbol: deque(maxlen=capacity) for symbol in data_engine.symbols
        }
        self._task: asyncio.Task[None] | None = None
        self._running = False
        self._tick = 0
        self.computed = 0
        self.skipped = 0
        #: Next cadence boundary, in *data* time.
        self._next_sample_ms = 0
        self.gaps_skipped = 0
        #: Under a live feed the data clock and the wall clock agree, so the
        #: engine wakes on wall time and samples the data clock. Under replay
        #: the data clock is the only one that means anything, so sampling is
        #: driven by the ingest path — that is what makes a replay produce the
        #: same snapshots regardless of how fast the file is read.
        self.driven_by_data = config.market.feed == "replay"

    # -------------------------------------------------------------- lifecycle

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        if self.driven_by_data:
            self.data.on_tick = self.pump
        else:
            self._task = asyncio.create_task(self._loop(), name="feature-engine")

    async def stop(self) -> None:
        self._running = False
        if self.data.on_tick is self.pump:
            self.data.on_tick = None
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass
            self._task = None

    async def _loop(self) -> None:
        # Wake at half the cadence so a boundary is never missed by a whole
        # sample just because the loop drifted a few milliseconds.
        interval = self.cadence_ms / 2000.0
        while self._running:
            await asyncio.sleep(interval)
            try:
                self.pump(self.data.data_time_ms or now_ms())
            except Exception:
                log.exception("feature computation failed")

    #: A pump that has fallen further behind than this is recovering from a feed
    #: outage, not catching up on a backlog: the snapshots in between would all
    #: read the same frozen book, so the cursor jumps instead.
    MAX_CATCHUP_SAMPLES = 8

    def pump(self, data_now_ms: int) -> int:
        """Emit a snapshot for every cadence boundary crossed in data time."""
        if not data_now_ms:
            return 0
        if self._next_sample_ms == 0:
            self._next_sample_ms = data_now_ms
        if data_now_ms < self._next_sample_ms:
            return 0
        behind = (data_now_ms - self._next_sample_ms) // self.cadence_ms
        if behind > self.MAX_CATCHUP_SAMPLES:
            self.gaps_skipped += 1
            self._next_sample_ms = data_now_ms
        emitted = 0
        while data_now_ms >= self._next_sample_ms:
            self.compute_all(self._next_sample_ms)
            self._next_sample_ms += self.cadence_ms
            emitted += 1
        return emitted

    def compute_all(self, stamp: int | None = None) -> None:
        self._tick += 1
        stamp = stamp if stamp is not None else now_ms()
        for symbol in self.data.symbols:
            snapshot = self.compute(symbol, stamp)
            if snapshot is None:
                self.skipped += 1
                continue
            self.computed += 1
            self.latest[symbol] = snapshot
            self.snapshots[symbol].append(snapshot)
            self.bus.publish(TOPIC_FEATURES, snapshot)
            if self._tick % self.config.features.persist_every_n == 0:
                self.repos.market.record_features(snapshot)

    # ---------------------------------------------------------------- compute

    def compute(self, symbol: str, stamp: int | None = None) -> FeatureSnapshot | None:
        state = self.data.state_of(symbol)
        if state is None or not state.book.ready:
            return None
        book = state.book.top(max(self.config.features.depth_levels))
        mid = book.mid
        micro = book.microprice()
        if mid is None or micro is None or mid <= 0:
            return None

        ts = stamp if stamp is not None else now_ms()
        history = self.history[symbol]
        history.append(ts, mid, micro)

        values: dict[str, float] = {}
        self._price_features(values, book, mid, micro)
        self._return_features(values, history)
        self._volatility_features(values, history)
        self._book_features(values, book)
        self._flow_features(values, state, book, ts)
        self._liquidity_features(values, state, ts)
        self._derivative_features(values, state, mid)

        regime = self.regime.classify(symbol, history, values)
        quality = self.data.quality(symbol)
        tradable, _ = self.data.is_tradable(symbol)

        values["quality_score"] = quality.score if quality else 0.0
        values["latency_ms"] = quality.latency_ms if quality else 0.0
        values["book_age_ms"] = float(max(0, ts - state.book.ts_ms))
        values["regime_code"] = float(list(Regime).index(regime))

        return FeatureSnapshot(
            symbol=symbol,
            ts_ms=ts,
            values=values,
            regime=regime,
            quality_score=quality.score if quality else 0.0,
            tradable=tradable,
            mid=mid,
            microprice=micro,
            spread_bps=book.spread_bps(),
        )

    # ---- families ---------------------------------------------------------

    def _price_features(self, values: dict[str, float], book: BookSnapshot, mid: float, micro: float) -> None:
        spread = book.spread or 0.0
        values["mid"] = mid
        values["microprice"] = micro
        values["spread"] = spread
        values["spread_bps"] = book.spread_bps() or 0.0
        # How far the size-weighted price sits from the midpoint: the cleanest
        # single read of which side is under pressure right now.
        values["micro_dev_bps"] = (micro - mid) / mid * 10_000.0

    def _return_features(self, values: dict[str, float], history: SymbolHistory) -> None:
        for horizon in self.config.features.return_horizons_ms:
            value = history.return_bps(horizon)
            values[f"ret_{_label(horizon)}"] = value if value is not None else 0.0

    def _volatility_features(self, values: dict[str, float], history: SymbolHistory) -> None:
        windows = self.config.features.volatility_windows_ms
        for window in windows:
            value = history.volatility_bps(window)
            values[f"vol_{_label(window)}"] = value if value is not None else 0.0
        if len(windows) >= 2:
            fast = values.get(f"vol_{_label(windows[0])}", 0.0)
            slow = values.get(f"vol_{_label(windows[-1])}", 0.0)
            # Short vol against long vol: >1 means the market just sped up.
            scale = math.sqrt(windows[-1] / windows[0]) if windows[0] else 1.0
            values["vol_ratio"] = (fast * scale / slow) if slow > 0 else 0.0

    def _book_features(self, values: dict[str, float], book: BookSnapshot) -> None:
        for level in self.config.features.depth_levels:
            bid_qty = book.depth(level, Side.BUY)
            ask_qty = book.depth(level, Side.SELL)
            values[f"depth_bid_{level}"] = bid_qty
            values[f"depth_ask_{level}"] = ask_qty
            values[f"imbalance_{level}"] = _imbalance(bid_qty, ask_qty)
            values[f"depth_notional_{level}"] = book.notional_depth(level, Side.BUY) + book.notional_depth(
                level, Side.SELL
            )

    def _flow_features(
        self, values: dict[str, float], state: SymbolState, book: BookSnapshot, ts: int
    ) -> None:
        for window in self.config.features.ofi_windows_ms:
            cutoff = ts - window
            buy_vol = sell_vol = 0.0
            trades = 0
            for tick in reversed(state.trades):
                if tick.ts_ms < cutoff:
                    break
                trades += 1
                if tick.aggressor is Side.BUY:
                    buy_vol += tick.qty
                else:
                    sell_vol += tick.qty
            label = _label(window)
            values[f"buy_vol_{label}"] = buy_vol
            values[f"sell_vol_{label}"] = sell_vol
            values[f"trades_{label}"] = float(trades)
            values[f"flow_imbalance_{label}"] = _imbalance(buy_vol, sell_vol)
            values[f"ofi_{label}"] = self._ofi(state, cutoff)

        # Normalising OFI by resting depth makes it comparable across symbols
        # whose contract sizes differ by four orders of magnitude.
        reference_depth = max(1e-9, book.depth(5, Side.BUY) + book.depth(5, Side.SELL))
        for window in self.config.features.ofi_windows_ms:
            label = _label(window)
            values[f"ofi_norm_{label}"] = values[f"ofi_{label}"] / reference_depth

    @staticmethod
    def _ofi(state: SymbolState, cutoff_ms: int) -> float:
        """Order flow imbalance over the top-of-book tape since ``cutoff_ms``.

        The standard construction (Cont, Kukanov & Stoikov): a bid that improves
        adds its whole size, a bid that retreats removes the size that was
        there, and symmetrically for asks.  Price *level* changes therefore
        count as flow, which is the point — a bid pulled a tick lower is
        selling pressure even though no trade printed.
        """
        tops = state.tops
        if len(tops) < 2:
            return 0.0
        total = 0.0
        previous = None
        # Walk forward from the first entry inside the window so pairs are
        # consecutive in time.
        start_index = 0
        for index in range(len(tops) - 1, -1, -1):
            if tops[index].ts_ms < cutoff_ms:
                start_index = index
                break
        for index in range(start_index, len(tops)):
            current = tops[index]
            if previous is not None:
                if current.bid > previous.bid:
                    total += current.bid_qty
                elif current.bid == previous.bid:
                    total += current.bid_qty - previous.bid_qty
                else:
                    total -= previous.bid_qty

                if current.ask < previous.ask:
                    total -= current.ask_qty
                elif current.ask == previous.ask:
                    total -= current.ask_qty - previous.ask_qty
                else:
                    total += previous.ask_qty
            previous = current
        return total

    def _liquidity_features(self, values: dict[str, float], state: SymbolState, ts: int) -> None:
        for window in self.config.features.ofi_windows_ms:
            cutoff = ts - window
            added_bid = removed_bid = added_ask = removed_ask = 0.0
            for delta in reversed(state.liquidity):
                if delta.ts_ms < cutoff:
                    break
                added_bid += delta.added_bid
                removed_bid += delta.removed_bid
                added_ask += delta.added_ask
                removed_ask += delta.removed_ask
            label = _label(window)
            values[f"liq_added_bid_{label}"] = added_bid
            values[f"liq_removed_bid_{label}"] = removed_bid
            values[f"liq_added_ask_{label}"] = added_ask
            values[f"liq_removed_ask_{label}"] = removed_ask
            # Net replenishment: positive when bids are being built and asks
            # pulled, which is the shape a sweep leaves behind.
            net_bid = added_bid - removed_bid
            net_ask = added_ask - removed_ask
            values[f"liq_pressure_{label}"] = _imbalance(max(0.0, net_bid), max(0.0, net_ask)) if (
                net_bid > 0 or net_ask > 0
            ) else 0.0
            values[f"liq_net_{label}"] = net_bid - net_ask

    def _derivative_features(self, values: dict[str, float], state: SymbolState, mid: float) -> None:
        mark = state.mark_price
        index = state.index_price
        values["mark_price"] = mark
        values["index_price"] = index
        values["mark_dev_bps"] = ((mark - mid) / mid * 10_000.0) if mark > 0 and mid > 0 else 0.0
        values["index_dev_bps"] = ((mark - index) / index * 10_000.0) if mark > 0 and index > 0 else 0.0
        values["funding_rate"] = state.funding_rate
        # Funding is quoted per settlement interval (8 h on this venue); in bps
        # it is directly comparable with an expected edge.
        values["funding_bps_8h"] = state.funding_rate * 10_000.0
        values["funding_countdown_s"] = (
            max(0.0, (state.next_funding_ms - now_ms()) / 1000.0) if state.next_funding_ms else 0.0
        )

    # ----------------------------------------------------------------- access

    def snapshot(self, symbol: str) -> FeatureSnapshot | None:
        return self.latest.get(symbol)

    def feature_names(self) -> list[str]:
        for snapshot in self.latest.values():
            return sorted(snapshot.values)
        return []

    def warmed_up(self) -> bool:
        needed = max(self.config.features.return_horizons_ms + self.config.features.volatility_windows_ms)
        steps = max(2, round(needed / self.cadence_ms))
        return bool(self.latest) and all(len(h) > steps for h in self.history.values())

    def stats(self) -> dict[str, Any]:
        return {
            "cadence_ms": self.cadence_ms,
            "computed": self.computed,
            "skipped": self.skipped,
            "symbols_with_features": len(self.latest),
            "feature_count": len(self.feature_names()),
            "history_depth": {s: len(h) for s, h in self.history.items()},
            "warmed_up": self.warmed_up(),
            "driven_by": "data-clock" if self.driven_by_data else "wall-clock",
            "gaps_skipped": self.gaps_skipped,
            "next_sample_ms": self._next_sample_ms,
        }


def _label(ms: int) -> str:
    """``250`` -> ``250ms``; ``5000`` -> ``5s``.  Used in feature names, so it
    must stay stable: renaming a label invalidates stored hypotheses."""
    if ms % 1000 == 0:
        return f"{ms // 1000}s"
    return f"{ms}ms"
