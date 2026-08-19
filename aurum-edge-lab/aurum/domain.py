"""Domain types shared by every layer.

Hot-path objects are plain dataclasses with ``slots=True``: the feature engine
builds one per symbol every 250 ms and the cross-market engine keeps minutes of
them in memory, so allocation cost is not academic here.

Time is always integer milliseconds since the Unix epoch, UTC, in a field whose
name ends in ``_ms``.  Two distinct clocks matter and both are recorded:
``ts_ms`` is the venue's timestamp for the event, ``recv_ms`` is when this
process saw it.  Their difference is the feed latency the data-quality gate
watches, and mixing them up is how a backtest quietly reads the future.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any


def now_ms() -> int:
    """Wall-clock milliseconds, UTC."""
    return int(time.time() * 1000)


# ---------------------------------------------------------------- enumerations


class EventKind(str, Enum):
    TRADE = "trade"
    DEPTH = "depth"
    BOOK_TICKER = "book_ticker"
    MARK_PRICE = "mark_price"
    SNAPSHOT = "snapshot"


class Side(str, Enum):
    BUY = "BUY"
    SELL = "SELL"

    @property
    def opposite(self) -> "Side":
        return Side.SELL if self is Side.BUY else Side.BUY

    @property
    def sign(self) -> int:
        return 1 if self is Side.BUY else -1


class Direction(str, Enum):
    LONG = "LONG"
    SHORT = "SHORT"

    @property
    def sign(self) -> int:
        return 1 if self is Direction.LONG else -1

    @property
    def entry_side(self) -> Side:
        return Side.BUY if self is Direction.LONG else Side.SELL


class FeedState(str, Enum):
    DISCONNECTED = "DISCONNECTED"
    CONNECTING = "CONNECTING"
    SYNCING = "SYNCING"
    LIVE = "LIVE"
    STALE = "STALE"
    DESYNCED = "DESYNCED"
    ERROR = "ERROR"


class QualityFlag(str, Enum):
    OK = "OK"
    STALE_FEED = "STALE_FEED"
    CROSSED_BOOK = "CROSSED_BOOK"
    WIDE_SPREAD = "WIDE_SPREAD"
    SEQUENCE_GAP = "SEQUENCE_GAP"
    THIN_BOOK = "THIN_BOOK"
    HIGH_LATENCY = "HIGH_LATENCY"
    NO_SNAPSHOT = "NO_SNAPSHOT"
    LOW_EVENT_RATE = "LOW_EVENT_RATE"
    RESYNCING = "RESYNCING"


class Regime(str, Enum):
    UNKNOWN = "UNKNOWN"
    QUIET_RANGE = "QUIET_RANGE"
    NORMAL_RANGE = "NORMAL_RANGE"
    TRENDING_UP = "TRENDING_UP"
    TRENDING_DOWN = "TRENDING_DOWN"
    HIGH_VOL = "HIGH_VOL"


class StrategyState(str, Enum):
    RESEARCH = "RESEARCH"
    CANDIDATE = "CANDIDATE"
    CHALLENGER = "CHALLENGER"
    SHADOW = "SHADOW"
    CHAMPION = "CHAMPION"
    DEGRADED = "DEGRADED"
    RETIRED = "RETIRED"
    REJECTED = "REJECTED"

    @property
    def can_trade(self) -> bool:
        """Only a CHAMPION reaches the wallet, and only on paper."""
        return self is StrategyState.CHAMPION


class ValidationStatus(str, Enum):
    UNTESTED = "UNTESTED"
    TRAIN_PASS = "TRAIN_PASS"
    VALIDATION_PASS = "VALIDATION_PASS"
    WALKFORWARD_PASS = "WALKFORWARD_PASS"
    HOLDOUT_PASS = "HOLDOUT_PASS"
    SHADOW_PASS = "SHADOW_PASS"
    REJECTED = "REJECTED"


class CycleState(str, Enum):
    ACTIVE = "ACTIVE"
    FAILED = "FAILED"
    POST_MORTEM = "POST_MORTEM"
    AWAITING_EDGE = "AWAITING_EDGE"
    CLOSED = "CLOSED"


class RejectionReason(str, Enum):
    """Every gate that can turn a would-be trade into no trade.

    ``/diagnostics`` reports counts and percentages per reason, which is what
    makes "0 signals" an explanation rather than a shrug.
    """

    NO_CHAMPION = "NO_CHAMPION"
    NO_VALIDATED_EDGE = "NO_VALIDATED_EDGE"
    WARMUP = "WARMUP"
    DATA_QUALITY = "DATA_QUALITY"
    STALE_FEED = "STALE_FEED"
    CROSSED_BOOK = "CROSSED_BOOK"
    SPREAD_TOO_WIDE = "SPREAD_TOO_WIDE"
    INSUFFICIENT_LIQUIDITY = "INSUFFICIENT_LIQUIDITY"
    CONDITIONS_NOT_MET = "CONDITIONS_NOT_MET"
    EDGE_BELOW_COSTS = "EDGE_BELOW_COSTS"
    LOW_CONFIDENCE = "LOW_CONFIDENCE"
    REGIME_FILTER = "REGIME_FILTER"
    COOLDOWN = "COOLDOWN"
    DUPLICATE_SIGNAL = "DUPLICATE_SIGNAL"
    MAX_POSITIONS = "MAX_POSITIONS"
    MAX_EXPOSURE = "MAX_EXPOSURE"
    SYMBOL_EXPOSURE = "SYMBOL_EXPOSURE"
    POSITION_ALREADY_OPEN = "POSITION_ALREADY_OPEN"
    NOTIONAL_TOO_SMALL = "NOTIONAL_TOO_SMALL"
    INSUFFICIENT_BALANCE = "INSUFFICIENT_BALANCE"
    DAILY_LOSS_LIMIT = "DAILY_LOSS_LIMIT"
    MAX_DRAWDOWN = "MAX_DRAWDOWN"
    CYCLE_NOT_ACTIVE = "CYCLE_NOT_ACTIVE"
    ENTRIES_BLOCKED = "ENTRIES_BLOCKED"
    SHADOW_ONLY = "SHADOW_ONLY"


class ExitReason(str, Enum):
    HORIZON = "HORIZON"
    TAKE_PROFIT = "TAKE_PROFIT"
    STOP_LOSS = "STOP_LOSS"
    MAX_HOLDING = "MAX_HOLDING"
    DATA_QUALITY = "DATA_QUALITY"
    CIRCUIT_BREAKER = "CIRCUIT_BREAKER"
    CYCLE_END = "CYCLE_END"
    SHUTDOWN = "SHUTDOWN"


# ------------------------------------------------------------------- market


@dataclass(slots=True)
class MarketEvent:
    """One raw event from the venue, normalised across stream types."""

    symbol: str
    kind: EventKind
    ts_ms: int
    recv_ms: int
    payload: dict[str, Any] = field(default_factory=dict)
    source: str = "live"

    @property
    def latency_ms(self) -> int:
        return self.recv_ms - self.ts_ms


@dataclass(slots=True)
class TradeTick:
    symbol: str
    ts_ms: int
    recv_ms: int
    price: float
    qty: float
    aggressor: Side
    trade_id: int = 0

    @property
    def notional(self) -> float:
        return self.price * self.qty


@dataclass(slots=True)
class BookLevel:
    price: float
    qty: float


@dataclass(slots=True)
class BookSnapshot:
    """Top-of-book plus depth as of ``ts_ms``, already validated."""

    symbol: str
    ts_ms: int
    recv_ms: int
    bids: list[BookLevel]
    asks: list[BookLevel]
    last_update_id: int
    is_crossed: bool = False

    @property
    def best_bid(self) -> float | None:
        return self.bids[0].price if self.bids else None

    @property
    def best_ask(self) -> float | None:
        return self.asks[0].price if self.asks else None

    @property
    def mid(self) -> float | None:
        if not self.bids or not self.asks:
            return None
        return (self.bids[0].price + self.asks[0].price) / 2.0

    @property
    def spread(self) -> float | None:
        if not self.bids or not self.asks:
            return None
        return self.asks[0].price - self.bids[0].price

    def spread_bps(self) -> float | None:
        mid = self.mid
        spread = self.spread
        if mid is None or spread is None or mid <= 0:
            return None
        return spread / mid * 10_000.0

    def microprice(self) -> float | None:
        """Size-weighted top of book: leans toward the side with less resting size."""
        if not self.bids or not self.asks:
            return None
        bid, ask = self.bids[0], self.asks[0]
        total = bid.qty + ask.qty
        if total <= 0:
            return self.mid
        return (bid.price * ask.qty + ask.price * bid.qty) / total

    def depth(self, levels: int, side: Side) -> float:
        book = self.bids if side is Side.BUY else self.asks
        return sum(level.qty for level in book[:levels])

    def notional_depth(self, levels: int, side: Side) -> float:
        book = self.bids if side is Side.BUY else self.asks
        return sum(level.price * level.qty for level in book[:levels])


@dataclass(slots=True)
class SymbolQuality:
    """Per-symbol data-quality verdict.  ``tradable`` is authoritative: no
    strategy confidence overrides it."""

    symbol: str
    ts_ms: int
    score: float
    state: FeedState
    flags: list[QualityFlag] = field(default_factory=list)
    latency_ms: float = 0.0
    spread_bps: float | None = None
    events_per_min: float = 0.0
    sequence_gaps: int = 0
    resyncs: int = 0
    last_event_ms: int = 0
    snapshot_age_s: float = 0.0
    book_levels: int = 0

    @property
    def tradable(self) -> bool:
        return self.state is FeedState.LIVE and not self.flags

    def to_dict(self) -> dict[str, Any]:
        return {
            "symbol": self.symbol,
            "ts_ms": self.ts_ms,
            "score": round(self.score, 4),
            "state": self.state.value,
            "flags": [f.value for f in self.flags],
            "tradable": self.tradable,
            "latency_ms": round(self.latency_ms, 2),
            "spread_bps": round(self.spread_bps, 4) if self.spread_bps is not None else None,
            "events_per_min": round(self.events_per_min, 1),
            "sequence_gaps": self.sequence_gaps,
            "resyncs": self.resyncs,
            "last_event_ms": self.last_event_ms,
            "snapshot_age_s": round(self.snapshot_age_s, 2),
            "book_levels": self.book_levels,
        }


@dataclass(slots=True)
class FeatureSnapshot:
    """Derived state for one symbol at one instant.

    ``values`` holds the flat feature map (the names are stable and documented
    in ``aurum/features/engine.py``); ``quality`` travels with it so a consumer
    can never use a feature without knowing how much to trust it.
    """

    symbol: str
    ts_ms: int
    values: dict[str, float]
    regime: Regime = Regime.UNKNOWN
    quality_score: float = 0.0
    tradable: bool = False
    mid: float | None = None
    microprice: float | None = None
    spread_bps: float | None = None

    def get(self, name: str, default: float = 0.0) -> float:
        value = self.values.get(name, default)
        return default if value is None else value

    def to_dict(self) -> dict[str, Any]:
        return {
            "symbol": self.symbol,
            "ts_ms": self.ts_ms,
            "regime": self.regime.value,
            "quality_score": round(self.quality_score, 4),
            "tradable": self.tradable,
            "mid": self.mid,
            "microprice": self.microprice,
            "spread_bps": self.spread_bps,
            "values": {k: (round(v, 8) if isinstance(v, float) else v) for k, v in self.values.items()},
        }


# ------------------------------------------------------------------ research


@dataclass(slots=True)
class Condition:
    """One feature predicate, e.g. ``ofi_1000 >= 0.35``."""

    feature: str
    op: str
    threshold: float

    def evaluate(self, snapshot: FeatureSnapshot) -> bool:
        value = snapshot.values.get(self.feature)
        if value is None:
            return False
        if self.op == ">=":
            return value >= self.threshold
        if self.op == "<=":
            return value <= self.threshold
        if self.op == ">":
            return value > self.threshold
        if self.op == "<":
            return value < self.threshold
        raise ValueError(f"unsupported operator {self.op!r}")

    def to_dict(self) -> dict[str, Any]:
        return {"feature": self.feature, "op": self.op, "threshold": self.threshold}

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Condition":
        return cls(feature=data["feature"], op=data["op"], threshold=float(data["threshold"]))

    def describe(self) -> str:
        return f"{self.feature} {self.op} {self.threshold:g}"


@dataclass(slots=True)
class MetricSet:
    """Everything the blueprint's "minimum metrics" list asks for, net of costs."""

    samples: int = 0
    wins: int = 0
    losses: int = 0
    gross_edge_bps: float = 0.0
    net_edge_bps: float = 0.0
    avg_win_bps: float = 0.0
    avg_loss_bps: float = 0.0
    profit_factor: float = 0.0
    max_drawdown_bps: float = 0.0
    tail_loss_bps: float = 0.0
    stdev_bps: float = 0.0
    t_stat: float = 0.0
    p_value: float = 1.0
    ci_low_bps: float = 0.0
    ci_high_bps: float = 0.0
    events_per_hour: float = 0.0
    cost_bps: float = 0.0

    @property
    def win_rate(self) -> float:
        decided = self.wins + self.losses
        return self.wins / decided if decided else 0.0

    def to_dict(self) -> dict[str, Any]:
        return {
            "samples": self.samples,
            "wins": self.wins,
            "losses": self.losses,
            "win_rate": round(self.win_rate, 4),
            "gross_edge_bps": round(self.gross_edge_bps, 4),
            "net_edge_bps": round(self.net_edge_bps, 4),
            "cost_bps": round(self.cost_bps, 4),
            "avg_win_bps": round(self.avg_win_bps, 4),
            "avg_loss_bps": round(self.avg_loss_bps, 4),
            "profit_factor": round(self.profit_factor, 4),
            "max_drawdown_bps": round(self.max_drawdown_bps, 4),
            "tail_loss_bps": round(self.tail_loss_bps, 4),
            "stdev_bps": round(self.stdev_bps, 4),
            "t_stat": round(self.t_stat, 4),
            "p_value": round(self.p_value, 6),
            "ci_low_bps": round(self.ci_low_bps, 4),
            "ci_high_bps": round(self.ci_high_bps, 4),
            "events_per_hour": round(self.events_per_hour, 2),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "MetricSet":
        known = {f for f in cls.__dataclass_fields__}  # type: ignore[attr-defined]
        return cls(**{k: v for k, v in data.items() if k in known})


@dataclass(slots=True)
class Signal:
    """A strategy's decision at one instant — emitted whether or not it trades."""

    signal_id: str
    ts_ms: int
    strategy_id: str
    hypothesis_id: str
    symbol: str
    direction: Direction
    confidence: float
    expected_edge_bps: float
    expected_cost_bps: float
    horizon_ms: int
    regime: Regime
    features: dict[str, float] = field(default_factory=dict)
    accepted: bool = False
    rejection: RejectionReason | None = None
    rejection_detail: str = ""
    shadow: bool = False

    @property
    def net_edge_bps(self) -> float:
        return self.expected_edge_bps - self.expected_cost_bps

    def to_dict(self) -> dict[str, Any]:
        return {
            "signal_id": self.signal_id,
            "ts_ms": self.ts_ms,
            "strategy_id": self.strategy_id,
            "hypothesis_id": self.hypothesis_id,
            "symbol": self.symbol,
            "direction": self.direction.value,
            "confidence": round(self.confidence, 4),
            "expected_edge_bps": round(self.expected_edge_bps, 4),
            "expected_cost_bps": round(self.expected_cost_bps, 4),
            "net_edge_bps": round(self.net_edge_bps, 4),
            "horizon_ms": self.horizon_ms,
            "regime": self.regime.value,
            "accepted": self.accepted,
            "shadow": self.shadow,
            "rejection": self.rejection.value if self.rejection else None,
            "rejection_detail": self.rejection_detail,
        }


# ----------------------------------------------------------------- execution


@dataclass(slots=True)
class Fill:
    """Requested price and achieved price are stored separately, always."""

    ts_ms: int
    side: Side
    requested_price: float
    fill_price: float
    qty: float
    fee_eur: float
    slippage_bps: float
    latency_ms: float
    levels_consumed: int = 1

    @property
    def notional(self) -> float:
        return self.fill_price * self.qty


@dataclass(slots=True)
class Position:
    position_id: str
    symbol: str
    direction: Direction
    qty: float
    entry_ts_ms: int
    entry_price: float
    requested_entry_price: float
    notional_eur: float
    margin_eur: float
    strategy_id: str
    hypothesis_id: str
    signal_id: str
    horizon_ms: int
    entry_fee_eur: float
    entry_slippage_bps: float
    stop_bps: float
    target_bps: float
    cycle_id: int
    features: dict[str, float] = field(default_factory=dict)
    mark_price: float = 0.0
    unrealized_pnl_eur: float = 0.0

    def update_mark(self, price: float, usdt_per_eur: float) -> float:
        self.mark_price = price
        move = (price - self.entry_price) * self.direction.sign
        self.unrealized_pnl_eur = move * self.qty / usdt_per_eur
        return self.unrealized_pnl_eur

    def return_bps(self, price: float) -> float:
        if self.entry_price <= 0:
            return 0.0
        return (price - self.entry_price) / self.entry_price * 10_000.0 * self.direction.sign

    def to_dict(self) -> dict[str, Any]:
        return {
            "position_id": self.position_id,
            "symbol": self.symbol,
            "direction": self.direction.value,
            "qty": self.qty,
            "entry_ts_ms": self.entry_ts_ms,
            "entry_price": self.entry_price,
            "requested_entry_price": self.requested_entry_price,
            "notional_eur": round(self.notional_eur, 4),
            "margin_eur": round(self.margin_eur, 4),
            "mark_price": self.mark_price,
            "unrealized_pnl_eur": round(self.unrealized_pnl_eur, 4),
            "strategy_id": self.strategy_id,
            "hypothesis_id": self.hypothesis_id,
            "signal_id": self.signal_id,
            "horizon_ms": self.horizon_ms,
            "stop_bps": self.stop_bps,
            "target_bps": self.target_bps,
            "cycle_id": self.cycle_id,
        }


@dataclass(slots=True)
class PaperTrade:
    """A closed round trip, with enough context to answer "why this trade?"."""

    trade_id: str
    position_id: str
    symbol: str
    direction: Direction
    qty: float
    entry_ts_ms: int
    exit_ts_ms: int
    requested_entry_price: float
    entry_price: float
    requested_exit_price: float
    exit_price: float
    gross_pnl_eur: float
    fees_eur: float
    net_pnl_eur: float
    return_bps: float
    net_return_bps: float
    entry_slippage_bps: float
    exit_slippage_bps: float
    cost_bps: float
    exit_reason: ExitReason
    strategy_id: str
    strategy_version: int
    hypothesis_id: str
    signal_id: str
    cycle_id: int
    regime: Regime
    features: dict[str, float] = field(default_factory=dict)
    cost_model_version: str = ""
    expected_edge_bps: float = 0.0

    @property
    def holding_ms(self) -> int:
        return self.exit_ts_ms - self.entry_ts_ms

    def to_dict(self, *, with_features: bool = False) -> dict[str, Any]:
        data = {
            "trade_id": self.trade_id,
            "position_id": self.position_id,
            "symbol": self.symbol,
            "direction": self.direction.value,
            "qty": self.qty,
            "entry_ts_ms": self.entry_ts_ms,
            "exit_ts_ms": self.exit_ts_ms,
            "holding_ms": self.holding_ms,
            "requested_entry_price": self.requested_entry_price,
            "entry_price": self.entry_price,
            "requested_exit_price": self.requested_exit_price,
            "exit_price": self.exit_price,
            "gross_pnl_eur": round(self.gross_pnl_eur, 6),
            "fees_eur": round(self.fees_eur, 6),
            "net_pnl_eur": round(self.net_pnl_eur, 6),
            "return_bps": round(self.return_bps, 4),
            "net_return_bps": round(self.net_return_bps, 4),
            "entry_slippage_bps": round(self.entry_slippage_bps, 4),
            "exit_slippage_bps": round(self.exit_slippage_bps, 4),
            "cost_bps": round(self.cost_bps, 4),
            "exit_reason": self.exit_reason.value,
            "strategy_id": self.strategy_id,
            "strategy_version": self.strategy_version,
            "hypothesis_id": self.hypothesis_id,
            "signal_id": self.signal_id,
            "cycle_id": self.cycle_id,
            "regime": self.regime.value,
            "cost_model_version": self.cost_model_version,
            "expected_edge_bps": round(self.expected_edge_bps, 4),
        }
        if with_features:
            data["features"] = self.features
        return data
