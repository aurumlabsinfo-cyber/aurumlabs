"""Batched persistence.

The market feed produces thousands of rows per minute. Writing them one INSERT
at a time from the hot path would couple feed latency to database latency, so
rows are queued in memory and flushed in batches on a timer. If the database is
unavailable the engine keeps trading on paper and the failure is surfaced in
`/health` - it is never hidden.
"""

from __future__ import annotations

import asyncio
from collections import defaultdict, deque
from typing import Any, Iterable

from sqlalchemy import delete, insert, select, text, update

from app.config import Settings
from app.core.clock import now_ms
from app.core.logging_conf import get_logger
from app.db.engine import session_scope
from app.db.models import (
    AgentPredictionRow,
    ErrorRow,
    FeatureRow,
    MarketTickRow,
    ModelVersionRow,
    OrderBookSnapshotRow,
    OrderBookUpdateRow,
    PaperTradeRow,
    PerformanceMetricRow,
    ShadowDecisionRow,
    SignalRow,
    SystemEventRow,
    TradeRow,
)

log = get_logger(__name__)


class BatchWriter:
    """Queues rows per table and flushes them periodically."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self._queues: dict[Any, deque[dict]] = defaultdict(deque)
        self._task: asyncio.Task | None = None
        self._running = False
        self.stats = {
            "queued": 0, "written": 0, "dropped": 0, "flushes": 0, "failures": 0,
        }
        self.last_error: str | None = None
        self.last_flush_ts: int | None = None
        self.healthy = True

    def add(self, table: Any, row: dict) -> None:
        q = self._queues[table]
        if len(q) >= self.settings.db_batch_max_rows * 10:
            q.popleft()
            self.stats["dropped"] += 1
        q.append(row)
        self.stats["queued"] += 1

    def add_many(self, table: Any, rows: Iterable[dict]) -> None:
        for r in rows:
            self.add(table, r)

    async def start(self) -> None:
        self._running = True
        self._task = asyncio.create_task(self._loop(), name="db-batch-writer")

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        await self.flush()

    async def _loop(self) -> None:
        interval = self.settings.db_flush_interval_ms / 1000.0
        while self._running:
            await asyncio.sleep(interval)
            try:
                await self.flush()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                self.stats["failures"] += 1
                self.last_error = f"{type(exc).__name__}: {exc}"
                self.healthy = False
                log.warning("db.flush_failed", error=str(exc))

    async def flush(self) -> int:
        total = 0
        batches: list[tuple[Any, list[dict]]] = []
        limit = self.settings.db_batch_max_rows
        for table, q in self._queues.items():
            if not q:
                continue
            rows = [q.popleft() for _ in range(min(len(q), limit))]
            batches.append((table, rows))
        if not batches:
            return 0
        try:
            async with session_scope() as s:
                for table, rows in batches:
                    await s.execute(insert(table), rows)
                    total += len(rows)
            self.stats["written"] += total
            self.stats["flushes"] += 1
            self.last_flush_ts = now_ms()
            self.healthy = True
            self.last_error = None
        except Exception:
            # Put the rows back so a transient outage does not lose data.
            for table, rows in batches:
                self._queues[table].extendleft(reversed(rows))
            raise
        return total

    @property
    def pending(self) -> int:
        return sum(len(q) for q in self._queues.values())

    def health(self) -> dict[str, Any]:
        return {
            "healthy": self.healthy,
            "pending_rows": self.pending,
            "last_flush_ts": self.last_flush_ts,
            "last_error": self.last_error,
            **self.stats,
        }


# --------------------------------------------------------------------- reads
async def fetch_signals(
    limit: int = 50, symbol: str | None = None, include_synthetic: bool = True
) -> list[dict]:
    stmt = select(SignalRow).order_by(SignalRow.ts.desc()).limit(limit)
    if symbol:
        stmt = stmt.where(SignalRow.symbol == symbol)
    if not include_synthetic:
        stmt = stmt.where(SignalRow.is_synthetic.is_(False))
    async with session_scope() as s:
        rows = (await s.execute(stmt)).scalars().all()
    return [_row_to_dict(r) for r in rows]


async def fetch_paper_trades(
    limit: int = 100,
    symbol: str | None = None,
    result: str | None = None,
    include_synthetic: bool = True,
) -> list[dict]:
    stmt = select(PaperTradeRow).order_by(PaperTradeRow.ts.desc()).limit(limit)
    if symbol:
        stmt = stmt.where(PaperTradeRow.symbol == symbol)
    if result:
        stmt = stmt.where(PaperTradeRow.result == result)
    if not include_synthetic:
        stmt = stmt.where(PaperTradeRow.is_synthetic.is_(False))
    async with session_scope() as s:
        rows = (await s.execute(stmt)).scalars().all()
    return [_row_to_dict(r) for r in rows]


async def fetch_all_paper_trades(
    include_synthetic: bool = False, since_ts: int | None = None
) -> list[dict]:
    stmt = select(PaperTradeRow).order_by(PaperTradeRow.ts.asc())
    if not include_synthetic:
        stmt = stmt.where(PaperTradeRow.is_synthetic.is_(False))
    if since_ts:
        stmt = stmt.where(PaperTradeRow.ts >= since_ts)
    async with session_scope() as s:
        rows = (await s.execute(stmt)).scalars().all()
    return [_row_to_dict(r) for r in rows]


async def upsert_paper_trade(row: dict) -> None:
    """Insert a paper trade, or update it when its lifecycle advances."""
    async with session_scope() as s:
        existing = (
            await s.execute(
                select(PaperTradeRow.id).where(
                    PaperTradeRow.signal_id == row["signal_id"]
                )
            )
        ).scalar_one_or_none()
        if existing is None:
            await s.execute(insert(PaperTradeRow).values(**row))
        else:
            await s.execute(
                update(PaperTradeRow)
                .where(PaperTradeRow.signal_id == row["signal_id"])
                .values(**{k: v for k, v in row.items() if k != "signal_id"})
            )


async def update_signal_status(signal_id: str, **values: Any) -> None:
    async with session_scope() as s:
        await s.execute(
            update(SignalRow).where(SignalRow.signal_id == signal_id).values(**values)
        )


async def record_model_version(row: dict) -> None:
    async with session_scope() as s:
        await s.execute(insert(ModelVersionRow).values(**row))


async def list_model_versions(limit: int = 50) -> list[dict]:
    async with session_scope() as s:
        rows = (
            await s.execute(
                select(ModelVersionRow).order_by(ModelVersionRow.ts.desc()).limit(limit)
            )
        ).scalars().all()
    return [_row_to_dict(r) for r in rows]


async def set_active_model(model_id: str) -> bool:
    async with session_scope() as s:
        exists = (
            await s.execute(
                select(ModelVersionRow.id).where(ModelVersionRow.model_id == model_id)
            )
        ).scalar_one_or_none()
        if exists is None:
            return False
        await s.execute(update(ModelVersionRow).values(is_active=False))
        await s.execute(
            update(ModelVersionRow)
            .where(ModelVersionRow.model_id == model_id)
            .values(is_active=True)
        )
    return True


async def fetch_recent_features(limit: int = 100, symbol: str | None = None) -> list[dict]:
    stmt = select(FeatureRow).order_by(FeatureRow.ts.desc()).limit(limit)
    if symbol:
        stmt = stmt.where(FeatureRow.symbol == symbol)
    async with session_scope() as s:
        rows = (await s.execute(stmt)).scalars().all()
    return [_row_to_dict(r) for r in rows]


async def count_rows(table: Any, include_synthetic: bool = True) -> int:
    from sqlalchemy import func as sqlfunc

    stmt = select(sqlfunc.count()).select_from(table)
    if not include_synthetic and hasattr(table, "is_synthetic"):
        stmt = stmt.where(table.is_synthetic.is_(False))
    async with session_scope() as s:
        return int((await s.execute(stmt)).scalar_one())


async def fetch_candles(
    symbol: str,
    bucket_s: int,
    limit: int = 300,
    include_synthetic: bool = False,
) -> list[dict[str, Any]]:
    """OHLC bars built from mid-price ticks, newest `limit` buckets.

    Aggregated in PostgreSQL rather than in Python: an hour of 1-second bars is
    360k ticks, and shipping those to the API process to fold them would be
    slower than the query by an order of magnitude.

    The open/close use `first_value`/`last_value` over each bucket ordered by
    timestamp, so they are the real first and last prints of the bucket - not
    min/max standing in for them.
    """
    bucket_ms = bucket_s * 1000
    where_synth = "" if include_synthetic else "AND is_synthetic = false"
    sql = text(
        f"""
        SELECT bucket,
               open, high, low, close, ticks
        FROM (
            SELECT (ts / :bucket_ms) * :bucket_ms                    AS bucket,
                   (array_agg(mid ORDER BY ts ASC))[1]               AS open,
                   MAX(mid)                                          AS high,
                   MIN(mid)                                          AS low,
                   (array_agg(mid ORDER BY ts DESC))[1]              AS close,
                   COUNT(*)                                          AS ticks
            FROM market_ticks
            WHERE symbol = :symbol {where_synth}
            GROUP BY 1
            ORDER BY 1 DESC
            LIMIT :limit
        ) recent
        ORDER BY bucket ASC
        """
    )
    async with session_scope() as s:
        rows = (
            await s.execute(
                sql, {"bucket_ms": bucket_ms, "symbol": symbol, "limit": limit}
            )
        ).all()
    return [
        {
            # lightweight-charts wants seconds, not milliseconds.
            "time": int(r.bucket // 1000),
            "open": float(r.open),
            "high": float(r.high),
            "low": float(r.low),
            "close": float(r.close),
            "ticks": int(r.ticks),
        }
        for r in rows
    ]


async def table_counts() -> dict[str, int]:
    out: dict[str, int] = {}
    for table in (
        MarketTickRow, TradeRow, FeatureRow, SignalRow, PaperTradeRow,
        AgentPredictionRow, OrderBookSnapshotRow, OrderBookUpdateRow,
        SystemEventRow, ErrorRow, ModelVersionRow, PerformanceMetricRow,
        ShadowDecisionRow,
    ):
        try:
            out[table.__tablename__] = await count_rows(table)
        except Exception:  # noqa: BLE001
            out[table.__tablename__] = -1
    return out


async def purge_synthetic() -> dict[str, int]:
    """Delete every simulator-produced row. Used before real data collection."""
    deleted: dict[str, int] = {}
    async with session_scope() as s:
        for table in (
            MarketTickRow, TradeRow, FeatureRow, SignalRow, PaperTradeRow,
            AgentPredictionRow, OrderBookSnapshotRow, OrderBookUpdateRow,
            PerformanceMetricRow, ShadowDecisionRow,
        ):
            res = await s.execute(delete(table).where(table.is_synthetic.is_(True)))
            deleted[table.__tablename__] = res.rowcount or 0
    return deleted


def _row_to_dict(row: Any) -> dict:
    return {
        c.name: getattr(row, c.name)
        for c in row.__table__.columns
        if c.name != "created_at"
    }


__all__ = [
    "BatchWriter", "fetch_signals", "fetch_paper_trades", "fetch_all_paper_trades",
    "upsert_paper_trade", "update_signal_status", "record_model_version",
    "list_model_versions", "set_active_model", "fetch_recent_features",
    "count_rows", "table_counts", "purge_synthetic", "fetch_candles",
    "MarketTickRow", "TradeRow", "FeatureRow", "SignalRow", "PaperTradeRow",
    "AgentPredictionRow", "OrderBookSnapshotRow", "OrderBookUpdateRow",
    "SystemEventRow", "ErrorRow", "ModelVersionRow", "PerformanceMetricRow",
    "ShadowDecisionRow",
]
