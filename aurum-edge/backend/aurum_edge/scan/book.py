"""Local order book with strict sequence discipline.

Bybit V5 sends a ``snapshot`` followed by ``delta`` messages, each carrying an
update id ``u`` that must increase by exactly one.  A gap means the local book
is a guess - and a guess is never allowed to back a trade.  When that happens
the book goes to ``RESYNC``, which the health gate treats as "not tradable"
until a fresh snapshot arrives.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class BookState(str, Enum):
    EMPTY = "EMPTY"        # nothing received yet
    OK = "OK"              # in sync
    RESYNC = "RESYNC"      # sequence gap, waiting for a new snapshot
    CROSSED = "CROSSED"    # bid >= ask, the book is not usable
    STALE = "STALE"        # no update for too long


@dataclass
class BookLevel:
    price: float
    size: float


@dataclass
class OrderBook:
    symbol: str
    depth_topic: str = "orderbook.1"
    bids: dict[float, float] = field(default_factory=dict)
    asks: dict[float, float] = field(default_factory=dict)
    update_id: int = 0
    seq: int = 0
    state: BookState = BookState.EMPTY
    last_update_ms: float = 0.0
    last_local_ms: float = 0.0
    snapshots: int = 0
    deltas: int = 0
    gaps: int = 0
    crossed_events: int = 0
    resync_reason: str = ""

    _sorted_bids: list[tuple[float, float]] | None = field(default=None, repr=False)
    _sorted_asks: list[tuple[float, float]] | None = field(default=None, repr=False)

    # ---------------------------------------------------------------- updates
    def apply(self, msg: dict[str, Any], local_ms: float) -> bool:
        """Apply one websocket message.  Returns True if the book is usable after."""
        data = msg.get("data") or {}
        msg_type = msg.get("type", "delta")
        update_id = int(data.get("u", 0) or 0)
        seq = int(data.get("seq", 0) or 0)
        ts = float(msg.get("cts", msg.get("ts", 0)) or 0)

        if msg_type == "snapshot":
            self.bids.clear()
            self.asks.clear()
            self._apply_side(self.bids, data.get("b", []))
            self._apply_side(self.asks, data.get("a", []))
            self.update_id = update_id
            self.seq = seq
            self.snapshots += 1
            self.resync_reason = ""
            self.state = BookState.OK
        else:
            if self.state is BookState.EMPTY:
                # a delta without a snapshot cannot be trusted
                self.resync_reason = "delta before snapshot"
                self.state = BookState.RESYNC
                return False
            if update_id and self.update_id and update_id != self.update_id + 1:
                self.gaps += 1
                self.resync_reason = (
                    f"sequence gap: expected u={self.update_id + 1}, got u={update_id}"
                )
                self.state = BookState.RESYNC
                self.bids.clear()
                self.asks.clear()
                self.update_id = 0
                self._invalidate()
                return False
            if self.state is BookState.RESYNC:
                # still waiting for the snapshot that repairs the gap
                return False
            self._apply_side(self.bids, data.get("b", []))
            self._apply_side(self.asks, data.get("a", []))
            self.update_id = update_id or self.update_id
            self.seq = seq or self.seq
            self.deltas += 1
            self.state = BookState.OK

        self.last_update_ms = ts or self.last_update_ms
        self.last_local_ms = local_ms
        self._invalidate()

        bid, ask = self.best_bid_price, self.best_ask_price
        if bid is not None and ask is not None and bid >= ask:
            self.crossed_events += 1
            self.state = BookState.CROSSED
            self.resync_reason = f"crossed book: bid {bid} >= ask {ask}"
            return False
        return self.state is BookState.OK

    @staticmethod
    def _apply_side(side: dict[float, float], levels: list[list[str]]) -> None:
        for level in levels:
            if len(level) < 2:
                continue
            price = float(level[0])
            size = float(level[1])
            if size <= 0.0:
                side.pop(price, None)
            else:
                side[price] = size

    def _invalidate(self) -> None:
        self._sorted_bids = None
        self._sorted_asks = None

    def mark_stale(self, reason: str = "no update") -> None:
        self.state = BookState.STALE
        self.resync_reason = reason

    # ---------------------------------------------------------------- views
    def sorted_bids(self) -> list[tuple[float, float]]:
        if self._sorted_bids is None:
            self._sorted_bids = sorted(self.bids.items(), key=lambda kv: -kv[0])
        return self._sorted_bids

    def sorted_asks(self) -> list[tuple[float, float]]:
        if self._sorted_asks is None:
            self._sorted_asks = sorted(self.asks.items(), key=lambda kv: kv[0])
        return self._sorted_asks

    @property
    def best_bid_price(self) -> float | None:
        levels = self.sorted_bids()
        return levels[0][0] if levels else None

    @property
    def best_bid_size(self) -> float:
        levels = self.sorted_bids()
        return levels[0][1] if levels else 0.0

    @property
    def best_ask_price(self) -> float | None:
        levels = self.sorted_asks()
        return levels[0][0] if levels else None

    @property
    def best_ask_size(self) -> float:
        levels = self.sorted_asks()
        return levels[0][1] if levels else 0.0

    @property
    def mid(self) -> float | None:
        bid, ask = self.best_bid_price, self.best_ask_price
        if bid is None or ask is None:
            return None
        return (bid + ask) / 2.0

    @property
    def spread(self) -> float | None:
        bid, ask = self.best_bid_price, self.best_ask_price
        if bid is None or ask is None:
            return None
        return ask - bid

    def spread_bps(self) -> float | None:
        spread, mid = self.spread, self.mid
        if spread is None or not mid:
            return None
        return spread / mid * 10_000.0

    def microprice(self) -> float | None:
        bid, ask = self.best_bid_price, self.best_ask_price
        if bid is None or ask is None:
            return None
        bid_size, ask_size = self.best_bid_size, self.best_ask_size
        total = bid_size + ask_size
        if total <= 0:
            return (bid + ask) / 2.0
        # weight each side by the opposite queue: heavy bid -> price near the ask
        return (bid * ask_size + ask * bid_size) / total

    def depth(self, levels: int) -> tuple[float, float]:
        """Notional-free size sums of the top ``levels`` on each side."""
        bid_sum = sum(size for _, size in self.sorted_bids()[:levels])
        ask_sum = sum(size for _, size in self.sorted_asks()[:levels])
        return bid_sum, ask_sum

    def depth_notional(self, levels: int) -> tuple[float, float]:
        bid_sum = sum(p * s for p, s in self.sorted_bids()[:levels])
        ask_sum = sum(p * s for p, s in self.sorted_asks()[:levels])
        return bid_sum, ask_sum

    def imbalance(self, levels: int) -> float:
        bid_sum, ask_sum = self.depth(levels)
        total = bid_sum + ask_sum
        if total <= 0:
            return 0.0
        return (bid_sum - ask_sum) / total

    def levels_count(self) -> tuple[int, int]:
        return len(self.bids), len(self.asks)

    def depth_curve(self, bands_bps: tuple[float, ...]) -> tuple[list[float], list[float]]:
        """Cumulative notional (quote currency) within each bps band of the mid."""
        mid = self.mid
        if mid is None or mid <= 0:
            return [0.0] * len(bands_bps), [0.0] * len(bands_bps)
        bid_cum: list[float] = []
        ask_cum: list[float] = []
        bids = self.sorted_bids()
        asks = self.sorted_asks()
        for band in bands_bps:
            limit = band / 10_000.0 * mid
            bid_cum.append(sum(p * s for p, s in bids if mid - p <= limit))
            ask_cum.append(sum(p * s for p, s in asks if p - mid <= limit))
        return bid_cum, ask_cum

    # ---------------------------------------------------------------- execution
    def walk(self, side: str, qty: float) -> tuple[float, float, bool]:
        """Average price for a market order of ``qty``.

        Returns ``(avg_price, filled_qty, fully_filled)``.  ``side`` is the taker
        side: "Buy" consumes asks, "Sell" consumes bids.
        """
        levels = self.sorted_asks() if side == "Buy" else self.sorted_bids()
        remaining = qty
        notional = 0.0
        for price, size in levels:
            if remaining <= 0:
                break
            take = min(size, remaining)
            notional += take * price
            remaining -= take
        filled = qty - remaining
        if filled <= 0:
            return 0.0, 0.0, False
        return notional / filled, filled, remaining <= 1e-12

    def slippage_bps(self, side: str, qty: float) -> float | None:
        """Expected slippage of a market order against the mid, in bps."""
        mid = self.mid
        if mid is None or qty <= 0:
            return None
        avg, filled, full = self.walk(side, qty)
        if filled <= 0:
            return None
        signed = (avg - mid) if side == "Buy" else (mid - avg)
        bps = signed / mid * 10_000.0
        if not full:
            # not enough displayed liquidity: report the worst level plus a penalty
            bps *= 1.5
        return bps

    def snapshot_dict(self, levels: int = 5) -> dict[str, Any]:
        return {
            "state": self.state.value,
            "update_id": self.update_id,
            "seq": self.seq,
            "bids": [[p, s] for p, s in self.sorted_bids()[:levels]],
            "asks": [[p, s] for p, s in self.sorted_asks()[:levels]],
            "levels": self.levels_count(),
            "gaps": self.gaps,
            "snapshots": self.snapshots,
            "resync_reason": self.resync_reason,
            "depth_topic": self.depth_topic,
        }
