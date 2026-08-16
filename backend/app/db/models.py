"""PostgreSQL schema.

Conventions
-----------
* ``ts`` columns are epoch milliseconds (BIGINT) - the engine's native unit -
  and are mirrored by a ``created_at TIMESTAMPTZ`` for human queries.
* every table that can hold non-live data carries ``source`` and
  ``is_synthetic``; the research tooling filters on them so simulator output can
  never leak into an edge evaluation.
"""

from __future__ import annotations

from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    DateTime,
    Float,
    Index,
    Integer,
    MetaData,
    String,
    Text,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase

metadata_obj = MetaData()


class Base(DeclarativeBase):
    metadata = metadata_obj


def _ts_columns():
    return (
        Column("ts", BigInteger, nullable=False, index=True),
        Column(
            "created_at",
            DateTime(timezone=True),
            server_default=func.now(),
            nullable=False,
        ),
    )


class MarketTickRow(Base):
    __tablename__ = "market_ticks"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    ts, created_at = _ts_columns()
    exchange_ts = Column(BigInteger, nullable=False)
    latency_ms = Column(Integer, nullable=False, default=0)
    exchange = Column(String(32), nullable=False)
    symbol = Column(String(32), nullable=False)
    bid_price = Column(Float, nullable=False)
    bid_qty = Column(Float, nullable=False)
    ask_price = Column(Float, nullable=False)
    ask_qty = Column(Float, nullable=False)
    mid = Column(Float, nullable=False)
    micro_price = Column(Float, nullable=False)
    spread = Column(Float, nullable=False)
    spread_bps = Column(Float, nullable=False)
    last_price = Column(Float)
    book_synced = Column(Boolean, nullable=False, default=False)
    source = Column(String(16), nullable=False, default="LIVE")
    is_synthetic = Column(Boolean, nullable=False, default=False)

    __table_args__ = (
        Index("ix_market_ticks_symbol_ts", "symbol", "ts"),
        Index("ix_market_ticks_live_ts", "is_synthetic", "ts"),
    )


class TradeRow(Base):
    __tablename__ = "trades"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    ts, created_at = _ts_columns()
    exchange_ts = Column(BigInteger, nullable=False)
    latency_ms = Column(Integer, nullable=False, default=0)
    exchange = Column(String(32), nullable=False)
    symbol = Column(String(32), nullable=False)
    trade_id = Column(BigInteger, nullable=False)
    price = Column(Float, nullable=False)
    quantity = Column(Float, nullable=False)
    notional = Column(Float, nullable=False)
    is_buyer_maker = Column(Boolean, nullable=False)
    aggressor = Column(String(4), nullable=False)
    source = Column(String(16), nullable=False, default="LIVE")
    is_synthetic = Column(Boolean, nullable=False, default=False)

    __table_args__ = (
        Index("ix_trades_symbol_ts", "symbol", "ts"),
        Index("ix_trades_exchange_tradeid", "exchange", "trade_id"),
    )


class OrderBookSnapshotRow(Base):
    __tablename__ = "order_book_snapshots"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    ts, created_at = _ts_columns()
    exchange = Column(String(32), nullable=False)
    symbol = Column(String(32), nullable=False)
    last_update_id = Column(BigInteger, nullable=False)
    synced = Column(Boolean, nullable=False)
    levels = Column(Integer, nullable=False)
    bids = Column(JSONB, nullable=False)
    asks = Column(JSONB, nullable=False)
    source = Column(String(16), nullable=False, default="LIVE")
    is_synthetic = Column(Boolean, nullable=False, default=False)


class OrderBookUpdateRow(Base):
    __tablename__ = "order_book_updates"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    ts, created_at = _ts_columns()
    exchange_ts = Column(BigInteger, nullable=False)
    exchange = Column(String(32), nullable=False)
    symbol = Column(String(32), nullable=False)
    first_update_id = Column(BigInteger, nullable=False)
    final_update_id = Column(BigInteger, nullable=False)
    prev_final_update_id = Column(BigInteger)
    applied = Column(Boolean, nullable=False, default=True)
    gap_detected = Column(Boolean, nullable=False, default=False)
    bids = Column(JSONB, nullable=False)
    asks = Column(JSONB, nullable=False)
    source = Column(String(16), nullable=False, default="LIVE")
    is_synthetic = Column(Boolean, nullable=False, default=False)


class FeatureRow(Base):
    __tablename__ = "features"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    ts, created_at = _ts_columns()
    exchange = Column(String(32), nullable=False)
    symbol = Column(String(32), nullable=False)
    mid = Column(Float, nullable=False)
    micro_price = Column(Float, nullable=False)
    spread_bps = Column(Float, nullable=False)
    book_synced = Column(Boolean, nullable=False)
    data_quality = Column(Float, nullable=False, default=0.0)
    regime = Column(String(24))
    # Full causal feature vector as computed at `ts` - nothing from the future.
    payload = Column(JSONB, nullable=False)
    source = Column(String(16), nullable=False, default="LIVE")
    is_synthetic = Column(Boolean, nullable=False, default=False)

    __table_args__ = (
        Index("ix_features_symbol_ts", "symbol", "ts"),
        Index("ix_features_live_ts", "is_synthetic", "ts"),
    )


class SignalRow(Base):
    __tablename__ = "signals"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    signal_id = Column(String(40), nullable=False, unique=True, index=True)
    ts, created_at = _ts_columns()
    exchange = Column(String(32), nullable=False)
    symbol = Column(String(32), nullable=False)
    direction = Column(String(10), nullable=False)  # UP | DOWN | NO_TRADE
    status = Column(String(16), nullable=False)  # WAITING|TRIGGERED|ACTIVE|...
    reference_price = Column(Float, nullable=False)
    trigger_price = Column(Float)
    horizon_s = Column(Float, nullable=False)
    confidence = Column(Float, nullable=False)
    prob_up = Column(Float, nullable=False)
    prob_down = Column(Float, nullable=False)
    prob_neutral = Column(Float, nullable=False)
    edge = Column(Float, nullable=False, default=0.0)
    market_regime = Column(String(24))
    no_trade_reasons = Column(JSONB)
    decision = Column(JSONB)
    model_id = Column(String(64))
    data_quality = Column(Float, nullable=False, default=0.0)
    source = Column(String(16), nullable=False, default="LIVE")
    is_synthetic = Column(Boolean, nullable=False, default=False)

    __table_args__ = (Index("ix_signals_symbol_ts", "symbol", "ts"),)


class ShadowDecisionRow(Base):
    """What the engine leaned towards on EVERY evaluated window.

    `signals` only records what passed the gates, which is a biased sample: it
    is the set of moments the engine already liked. Learning from it alone
    teaches the model about the engine's own filter, not about the market.

    This table records the lean on every window - including the ones gated out
    - together with the reasons it was gated. The realised outcome is not
    stored: it is resolved later from the tick table at `ts + horizon`, by the
    same causal path the training labels use, so nothing here can contain a
    value that was not knowable at `ts`.
    """

    __tablename__ = "shadow_decisions"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    ts, created_at = _ts_columns()
    exchange = Column(String(32), nullable=False)
    symbol = Column(String(32), nullable=False)
    #: The direction the aggregate leaned, before any gate was applied.
    lean = Column(String(10), nullable=False)  # UP | DOWN
    prob_up = Column(Float, nullable=False)
    confidence = Column(Float, nullable=False)
    edge = Column(Float, nullable=False, default=0.0)
    horizon_s = Column(Float, nullable=False)
    reference_price = Column(Float, nullable=False)
    market_regime = Column(String(24))
    #: True when this window also became a real signal.
    emitted = Column(Boolean, nullable=False, default=False)
    #: Empty when emitted; otherwise why it was not.
    blocked_by = Column(JSONB)
    data_quality = Column(Float, nullable=False, default=0.0)
    model_id = Column(String(64))
    source = Column(String(16), nullable=False, default="LIVE")
    is_synthetic = Column(Boolean, nullable=False, default=False)

    __table_args__ = (
        Index("ix_shadow_symbol_ts", "symbol", "ts"),
        Index("ix_shadow_live_ts", "is_synthetic", "ts"),
    )


class PaperTradeRow(Base):
    __tablename__ = "paper_trades"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    signal_id = Column(String(40), nullable=False, unique=True, index=True)
    ts, created_at = _ts_columns()
    exchange = Column(String(32), nullable=False)
    symbol = Column(String(32), nullable=False)
    direction = Column(String(10), nullable=False)
    status = Column(String(16), nullable=False)
    trigger_price = Column(Float, nullable=False)
    entry_price = Column(Float)
    expiry_price = Column(Float)
    confidence = Column(Float, nullable=False)
    probability_up = Column(Float, nullable=False)
    probability_down = Column(Float, nullable=False)
    market_regime = Column(String(24))
    horizon_s = Column(Float, nullable=False)
    features = Column(JSONB)
    agents = Column(JSONB)
    triggered_at = Column(BigInteger)
    expires_at = Column(BigInteger)
    settled_at = Column(BigInteger)
    result = Column(String(12))  # WIN | LOSS | TIE | CANCELLED
    pnl_units = Column(Float)  # in stake units; NULL when payout unknown
    payout = Column(Float)
    stake = Column(Float)
    latency_ms = Column(Integer)
    data_quality = Column(Float)
    source = Column(String(16), nullable=False, default="LIVE")
    is_synthetic = Column(Boolean, nullable=False, default=False)

    __table_args__ = (
        Index("ix_paper_trades_result_ts", "result", "ts"),
        Index("ix_paper_trades_live_ts", "is_synthetic", "ts"),
    )


class AgentPredictionRow(Base):
    __tablename__ = "agent_predictions"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    ts, created_at = _ts_columns()
    signal_id = Column(String(40), index=True)
    symbol = Column(String(32), nullable=False)
    agent = Column(String(40), nullable=False)
    direction = Column(String(10), nullable=False)
    confidence = Column(Float, nullable=False)
    score = Column(Float, nullable=False)
    reason = Column(Text)
    features_used = Column(JSONB)
    data_quality = Column(Float, nullable=False, default=0.0)
    source = Column(String(16), nullable=False, default="LIVE")
    is_synthetic = Column(Boolean, nullable=False, default=False)

    __table_args__ = (Index("ix_agent_predictions_agent_ts", "agent", "ts"),)


class ModelVersionRow(Base):
    __tablename__ = "model_versions"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    model_id = Column(String(64), nullable=False, unique=True, index=True)
    created_at = Column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    ts = Column(BigInteger, nullable=False)
    algorithm = Column(String(40), nullable=False)
    horizon_s = Column(Float, nullable=False)
    symbol = Column(String(32), nullable=False)
    feature_names = Column(JSONB, nullable=False)
    train_start_ts = Column(BigInteger, nullable=False)
    train_end_ts = Column(BigInteger, nullable=False)
    test_start_ts = Column(BigInteger)
    test_end_ts = Column(BigInteger)
    n_train = Column(Integer, nullable=False)
    n_test = Column(Integer)
    metrics = Column(JSONB, nullable=False)
    walkforward = Column(JSONB)
    edge_classification = Column(String(20))
    leakage_checks = Column(JSONB)
    params = Column(JSONB)
    artifact_path = Column(Text)
    is_active = Column(Boolean, nullable=False, default=False)
    data_source = Column(String(16), nullable=False, default="LIVE")


class SystemEventRow(Base):
    __tablename__ = "system_events"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    ts, created_at = _ts_columns()
    component = Column(String(40), nullable=False)
    event = Column(String(60), nullable=False)
    severity = Column(String(12), nullable=False, default="INFO")
    detail = Column(JSONB)


class ErrorRow(Base):
    __tablename__ = "errors"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    ts, created_at = _ts_columns()
    component = Column(String(40), nullable=False)
    error_type = Column(String(80), nullable=False)
    message = Column(Text, nullable=False)
    context = Column(JSONB)


class PerformanceMetricRow(Base):
    __tablename__ = "performance_metrics"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    ts, created_at = _ts_columns()
    scope = Column(String(40), nullable=False)  # overall | regime:X | conf:0.7 ...
    symbol = Column(String(32), nullable=False)
    window = Column(String(24), nullable=False)  # all | 1h | 24h ...
    metrics = Column(JSONB, nullable=False)
    is_synthetic = Column(Boolean, nullable=False, default=False)

    __table_args__ = (Index("ix_perf_scope_ts", "scope", "ts"),)


ALL_TABLES = [
    MarketTickRow, TradeRow, OrderBookSnapshotRow, OrderBookUpdateRow, FeatureRow,
    SignalRow, PaperTradeRow, AgentPredictionRow, ModelVersionRow, SystemEventRow,
    ErrorRow, PerformanceMetricRow,
]
