"""Small, allocation-light rolling structures used by the feature engine."""

from __future__ import annotations

import bisect
import math
from collections import deque
from dataclasses import dataclass


class TimeSeries:
    """Timestamped scalar series with O(log n) point lookup.

    Kept as two parallel lists (timestamps ascending + values) so `bisect` can
    answer "what was the price 250 ms ago" without scanning.
    """

    def __init__(self, window_ms: int) -> None:
        self.window_ms = window_ms
        self.ts: list[int] = []
        self.values: list[float] = []

    def append(self, ts: int, value: float) -> None:
        # Feeds can deliver out-of-order timestamps; keep the series monotonic.
        if self.ts and ts < self.ts[-1]:
            ts = self.ts[-1]
        self.ts.append(ts)
        self.values.append(value)
        cutoff = ts - self.window_ms
        if self.ts[0] < cutoff:
            idx = bisect.bisect_left(self.ts, cutoff)
            if idx > 0:
                del self.ts[:idx]
                del self.values[:idx]

    def __len__(self) -> int:
        return len(self.ts)

    @property
    def last(self) -> float | None:
        return self.values[-1] if self.values else None

    @property
    def last_ts(self) -> int | None:
        return self.ts[-1] if self.ts else None

    def span_ms(self) -> int:
        return (self.ts[-1] - self.ts[0]) if len(self.ts) > 1 else 0

    def value_at_or_before(self, ts: int) -> float | None:
        """Most recent value at or before `ts`; None if the series starts later."""
        if not self.ts:
            return None
        idx = bisect.bisect_right(self.ts, ts) - 1
        if idx < 0:
            return None
        return self.values[idx]

    def value_ago(self, ms: int) -> float | None:
        if not self.ts:
            return None
        target = self.ts[-1] - ms
        if self.ts[0] > target:
            return None  # not enough history: do NOT extrapolate
        return self.value_at_or_before(target)

    def slice_since(self, ts: int) -> list[float]:
        idx = bisect.bisect_left(self.ts, ts)
        return self.values[idx:]

    def returns_since(self, ms: int) -> float | None:
        """Simple return over the window, in basis points."""
        past = self.value_ago(ms)
        cur = self.last
        if past is None or cur is None or past <= 0:
            return None
        return (cur - past) / past * 10_000.0

    def range_position(self, ms: int) -> float | None:
        """Where the current price sits inside the window's high-low range.

        0.0 = at the low, 1.0 = at the high. `None` when the window is not
        fully covered by history, or when high and low coincide - a flat
        window has no position to report, and returning 0.5 there would
        invent a reading the data does not support.
        """
        if not self.ts:
            return None
        start = self.ts[-1] - ms
        if self.ts[0] > start:
            return None  # window not covered: do NOT report a partial range
        vals = self.slice_since(start)
        if len(vals) < 2:
            return None
        lo, hi = min(vals), max(vals)
        if hi <= lo:
            return None
        cur = self.values[-1]
        return (cur - lo) / (hi - lo)

    def drift_bps_per_min(self, ms: int) -> float | None:
        """Least-squares slope of price over the window, in bps per minute.

        A straight return only compares the two endpoints, so one spike at
        either end dominates it. Over minute-scale windows the fitted slope is
        the more honest description of which way the market has been going.
        """
        if not self.ts:
            return None
        start = self.ts[-1] - ms
        if self.ts[0] > start:
            return None
        idx = bisect.bisect_left(self.ts, start)
        xs, ys = self.ts[idx:], self.values[idx:]
        n = len(xs)
        if n < 3:
            return None
        base = ys[0]
        if base <= 0:
            return None
        t0 = xs[0]
        mx = sum(x - t0 for x in xs) / n
        my = sum(ys) / n
        sxx = sum((x - t0 - mx) ** 2 for x in xs)
        if sxx <= 0:
            return None
        sxy = sum((xs[i] - t0 - mx) * (ys[i] - my) for i in range(n))
        slope_per_ms = sxy / sxx
        return slope_per_ms * 60_000.0 / base * 10_000.0

    def realized_vol_bps(self, ms: int) -> float | None:
        """Std-dev of consecutive log returns over the window, in bps.

        Returned per-observation (not annualised) - at a 5 second horizon an
        annualised number would be meaningless noise amplification.
        """
        if not self.ts:
            return None
        start = self.ts[-1] - ms
        idx = bisect.bisect_left(self.ts, start)
        vals = self.values[idx:]
        if len(vals) < 3:
            return None
        rets = [
            math.log(vals[i] / vals[i - 1])
            for i in range(1, len(vals))
            if vals[i] > 0 and vals[i - 1] > 0
        ]
        if len(rets) < 2:
            return None
        mean = sum(rets) / len(rets)
        var = sum((r - mean) ** 2 for r in rets) / (len(rets) - 1)
        return math.sqrt(var) * 10_000.0

    def zero_move_fraction(
        self, horizon_ms: int, lookback_ms: int, tolerance: float = 0.0
    ) -> float | None:
        """Fraction of horizon-length windows with no net price change.

        This is the tie rate, and on a real BTC venue it is large: measured at
        32% for a 5 second horizon. A binary option that pays nothing on an
        unchanged price makes this the dominant economic fact of the trade, so
        it is measured rather than assumed away.
        """
        if len(self.ts) < 5:
            return None
        end = self.ts[-1]
        start = end - lookback_ms
        if self.ts[0] > start:
            return None
        # Windows deliberately overlap. A 30s lookback stepped by a full 5s
        # horizon yields five samples, which is far too few to read a rate from;
        # overlapping samples are correlated but this is a descriptive
        # frequency, not a significance test, so the trade is worth it.
        step = max(horizon_ms // 10, 100)
        flat = 0
        total = 0
        t = start + horizon_ms
        while t <= end:
            a = self.value_at_or_before(t - horizon_ms)
            b = self.value_at_or_before(t)
            if a is not None and b is not None:
                total += 1
                if abs(b - a) <= tolerance:
                    flat += 1
            t += step
        if total < 5:
            return None
        return flat / total

    def sigma_over(self, horizon_ms: int, lookback_ms: int) -> float | None:
        """Std-dev of returns measured over `horizon_ms`, expressed in bps.

        Used to size the trigger offset: how far does price typically travel in
        one horizon?

        Windows overlap (stepping by a tenth of the horizon rather than a full
        one). Stepping by the full horizon over the default 30s lookback gives
        only five samples, and a standard deviation from five points is far too
        noisy to place a trigger with. Overlapping increments are the standard
        construction for realized volatility at short horizons: the estimator
        stays consistent, the samples are merely correlated.
        """
        if len(self.ts) < 5:
            return None
        end = self.ts[-1]
        start = end - lookback_ms
        if self.ts[0] > start:
            return None
        step = max(horizon_ms // 10, 100)
        samples: list[float] = []
        t = start + horizon_ms
        while t <= end:
            a = self.value_at_or_before(t - horizon_ms)
            b = self.value_at_or_before(t)
            if a and b and a > 0:
                samples.append((b - a) / a * 10_000.0)
            t += step
        if len(samples) < 5:
            return None
        mean = sum(samples) / len(samples)
        var = sum((s - mean) ** 2 for s in samples) / (len(samples) - 1)
        return math.sqrt(var)


@dataclass
class TradeRecord:
    ts: int
    price: float
    quantity: float
    notional: float
    is_buy: bool


class TradeWindow:
    """Rolling window of trades with order-flow aggregates."""

    def __init__(self, window_ms: int) -> None:
        self.window_ms = window_ms
        self.trades: deque[TradeRecord] = deque()

    def append(self, rec: TradeRecord) -> None:
        self.trades.append(rec)
        cutoff = rec.ts - self.window_ms
        while self.trades and self.trades[0].ts < cutoff:
            self.trades.popleft()

    def trim(self, now_ts: int) -> None:
        cutoff = now_ts - self.window_ms
        while self.trades and self.trades[0].ts < cutoff:
            self.trades.popleft()

    def since(self, ms: int) -> list[TradeRecord]:
        if not self.trades:
            return []
        cutoff = self.trades[-1].ts - ms
        return [t for t in self.trades if t.ts >= cutoff]

    def flow(self, ms: int) -> dict[str, float]:
        recs = self.since(ms)
        buy_qty = sum(r.quantity for r in recs if r.is_buy)
        sell_qty = sum(r.quantity for r in recs if not r.is_buy)
        buy_notional = sum(r.notional for r in recs if r.is_buy)
        sell_notional = sum(r.notional for r in recs if not r.is_buy)
        total_qty = buy_qty + sell_qty
        n = len(recs)
        return {
            "count": float(n),
            "buy_volume": buy_qty,
            "sell_volume": sell_qty,
            "buy_notional": buy_notional,
            "sell_notional": sell_notional,
            "total_volume": total_qty,
            "volume_imbalance": (
                (buy_qty - sell_qty) / total_qty if total_qty > 0 else 0.0
            ),
            "buy_sell_ratio": (buy_qty / sell_qty) if sell_qty > 0 else (
                float("inf") if buy_qty > 0 else 1.0
            ),
            "trade_intensity": n / (ms / 1000.0) if ms > 0 else 0.0,
            "avg_trade_size": (total_qty / n) if n else 0.0,
        }

    def consecutive(self) -> tuple[int, int]:
        """(consecutive buys, consecutive sells) at the tail of the window."""
        buys = sells = 0
        for rec in reversed(self.trades):
            if rec.is_buy:
                if sells:
                    break
                buys += 1
            else:
                if buys:
                    break
                sells += 1
        return buys, sells

    def large_trades(self, threshold_notional: float, ms: int) -> dict[str, float]:
        recs = [r for r in self.since(ms) if r.notional >= threshold_notional]
        return {
            "large_trade_count": float(len(recs)),
            "large_buy_notional": sum(r.notional for r in recs if r.is_buy),
            "large_sell_notional": sum(r.notional for r in recs if not r.is_buy),
        }

    def notional_quantile(self, q: float) -> float | None:
        if len(self.trades) < 20:
            return None
        vals = sorted(r.notional for r in self.trades)
        idx = min(len(vals) - 1, max(0, int(q * len(vals))))
        return vals[idx]


@dataclass
class Bar:
    open_ts: int
    open: float
    high: float
    low: float
    close: float
    volume: float = 0.0
    notional: float = 0.0
    trades: int = 0


class BarAggregator:
    """Builds fixed-interval OHLCV bars for the classical indicators."""

    def __init__(self, interval_ms: int = 1000, maxlen: int = 600) -> None:
        self.interval_ms = interval_ms
        self.bars: deque[Bar] = deque(maxlen=maxlen)
        self.current: Bar | None = None

    def add_price(self, ts: int, price: float) -> None:
        bucket = ts - (ts % self.interval_ms)
        if self.current is None or bucket > self.current.open_ts:
            if self.current is not None:
                self.bars.append(self.current)
            self.current = Bar(bucket, price, price, price, price)
        else:
            c = self.current
            c.high = max(c.high, price)
            c.low = min(c.low, price)
            c.close = price

    def add_trade(self, ts: int, price: float, qty: float) -> None:
        self.add_price(ts, price)
        if self.current is not None:
            self.current.volume += qty
            self.current.notional += price * qty
            self.current.trades += 1

    def closed_bars(self) -> list[Bar]:
        return list(self.bars)

    def all_bars(self) -> list[Bar]:
        return list(self.bars) + ([self.current] if self.current else [])
