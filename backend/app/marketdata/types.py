"""Canonical, exchange-agnostic market data types.

Adapters translate venue-specific payloads into these structures, so nothing
downstream (order book, features, agents, signals) knows which venue it is
looking at.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class DataSource(str, Enum):
    """Provenance of a datum. Anything not LIVE is unfit for edge research."""

    LIVE = "LIVE"  # real venue feed
    REPLAY = "REPLAY"  # real data replayed from our own database
    SYNTHETIC = "SYNTHETIC"  # model generated - never real market data


class Side(str, Enum):
    BUY = "BUY"
    SELL = "SELL"


@dataclass(slots=True)
class Trade:
    exchange: str
    symbol: str
    trade_id: int
    price: float
    quantity: float
    # True when the buyer was the maker => the aggressor was the SELLER.
    is_buyer_maker: bool
    exchange_ts: int
    server_ts: int
    source: DataSource = DataSource.LIVE

    @property
    def aggressor(self) -> Side:
        return Side.SELL if self.is_buyer_maker else Side.BUY

    @property
    def notional(self) -> float:
        return self.price * self.quantity

    @property
    def latency_ms(self) -> int:
        return self.server_ts - self.exchange_ts


@dataclass(slots=True)
class BookTicker:
    """Best bid/ask (top of book)."""

    exchange: str
    symbol: str
    bid_price: float
    bid_qty: float
    ask_price: float
    ask_qty: float
    exchange_ts: int
    server_ts: int
    update_id: int | None = None
    source: DataSource = DataSource.LIVE

    @property
    def mid(self) -> float:
        return (self.bid_price + self.ask_price) / 2.0

    @property
    def spread(self) -> float:
        return self.ask_price - self.bid_price

    @property
    def spread_bps(self) -> float:
        m = self.mid
        return (self.spread / m) * 10_000.0 if m > 0 else 0.0

    @property
    def micro_price(self) -> float:
        """Size-weighted mid: leans toward the side with less size."""
        total = self.bid_qty + self.ask_qty
        if total <= 0:
            return self.mid
        return (self.bid_price * self.ask_qty + self.ask_price * self.bid_qty) / total

    @property
    def latency_ms(self) -> int:
        return self.server_ts - self.exchange_ts


@dataclass(slots=True)
class DepthUpdate:
    """Incremental order book diff."""

    exchange: str
    symbol: str
    first_update_id: int
    final_update_id: int
    # Optional previous final update id (Binance futures `pu`); None for spot.
    prev_final_update_id: int | None
    bids: list[tuple[float, float]]
    asks: list[tuple[float, float]]
    exchange_ts: int
    server_ts: int
    source: DataSource = DataSource.LIVE


@dataclass(slots=True)
class DepthSnapshot:
    exchange: str
    symbol: str
    last_update_id: int
    bids: list[tuple[float, float]]
    asks: list[tuple[float, float]]
    server_ts: int
    source: DataSource = DataSource.LIVE


@dataclass(slots=True)
class DerivativesTick:
    """Futures-only extras: funding, open interest, liquidations."""

    exchange: str
    symbol: str
    server_ts: int
    exchange_ts: int
    mark_price: float | None = None
    index_price: float | None = None
    funding_rate: float | None = None
    next_funding_ts: int | None = None
    open_interest: float | None = None
    source: DataSource = DataSource.LIVE


@dataclass(slots=True)
class Liquidation:
    exchange: str
    symbol: str
    side: Side
    price: float
    quantity: float
    exchange_ts: int
    server_ts: int
    source: DataSource = DataSource.LIVE


@dataclass(slots=True)
class MarketTick:
    """Consolidated top-of-book state published on every book change."""

    exchange: str
    symbol: str
    ts: int  # server timestamp
    exchange_ts: int
    bid_price: float
    bid_qty: float
    ask_price: float
    ask_qty: float
    mid: float
    micro_price: float
    spread: float
    spread_bps: float
    last_price: float | None
    latency_ms: int
    book_synced: bool
    source: DataSource = DataSource.LIVE
    extra: dict[str, Any] = field(default_factory=dict)

    @property
    def is_synthetic(self) -> bool:
        return self.source is DataSource.SYNTHETIC
