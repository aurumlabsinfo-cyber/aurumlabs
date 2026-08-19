"""SQLite driver: WAL, indexed access, batched background writer, retention.

SQLite is acceptable for the MVP *if* it is configured for this workload, which
means four things: WAL so readers never block the writer, a single writer thread
so concurrent inserts never contend, batching so ten thousand market events cost
tens of transactions rather than ten thousand, and retention so the file does
not grow without bound.

The background writer owns the only write connection.  Readers get their own
read-only connection per thread; under WAL they see a consistent snapshot
without waiting for the writer to commit.
"""

from __future__ import annotations

import queue
import sqlite3
import threading
import time
from collections.abc import Iterable, Sequence
from pathlib import Path
from typing import Any

from ..logging_setup import get_logger
from .base import Database
from .schema import TABLES, TABLES_BY_NAME, insert_sql, render_ddl

log = get_logger("storage.sqlite")

_SHUTDOWN = object()


class SqliteDatabase(Database):
    dialect = "sqlite"

    def __init__(
        self,
        path: str | Path,
        *,
        batch_size: int = 500,
        flush_interval_ms: int = 500,
        queue_size: int = 50_000,
        timeout_s: float = 30.0,
    ) -> None:
        self.path = Path(path)
        self.batch_size = batch_size
        self.flush_interval_s = flush_interval_ms / 1000.0
        self.timeout_s = timeout_s
        self._queue: queue.Queue[Any] = queue.Queue(maxsize=queue_size)
        self._write_conn: sqlite3.Connection | None = None
        self._write_lock = threading.Lock()
        self._local = threading.local()
        self._writer: threading.Thread | None = None
        self._stop = threading.Event()
        self._flushed = threading.Event()
        self._counters = {
            "enqueued": 0,
            "written": 0,
            "dropped": 0,
            "batches": 0,
            "errors": 0,
            "sync_writes": 0,
        }
        self._counter_lock = threading.Lock()

    # ------------------------------------------------------------ lifecycle

    def connect(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._write_conn = self._new_connection()
        self._stop.clear()
        self._writer = threading.Thread(target=self._writer_loop, name="aurum-db-writer", daemon=True)
        self._writer.start()
        log.info("sqlite ready", extra={"path": str(self.path)})

    def _new_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, timeout=self.timeout_s, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute(f"PRAGMA busy_timeout={int(self.timeout_s * 1000)}")
        conn.execute("PRAGMA cache_size=-32000")  # ~32 MB page cache
        return conn

    def _read_connection(self) -> sqlite3.Connection:
        conn = getattr(self._local, "conn", None)
        if conn is None:
            conn = self._new_connection()
            self._local.conn = conn
        return conn

    def create_schema(self) -> None:
        assert self._write_conn is not None, "connect() first"
        with self._write_lock:
            for statement in render_ddl("sqlite"):
                self._write_conn.execute(statement)
            self._write_conn.commit()
        log.info("schema ready", extra={"tables": len(TABLES)})

    def close(self) -> None:
        self._stop.set()
        self._queue.put(_SHUTDOWN)
        if self._writer is not None:
            self._writer.join(timeout=10.0)
            self._writer = None
        if self._write_conn is not None:
            with self._write_lock:
                self._write_conn.commit()
                self._write_conn.close()
            self._write_conn = None
        conn = getattr(self._local, "conn", None)
        if conn is not None:
            conn.close()
            self._local.conn = None

    # --------------------------------------------------------------- writes

    def execute(self, sql: str, params: Sequence[Any] = ()) -> int:
        assert self._write_conn is not None, "connect() first"
        with self._write_lock:
            cursor = self._write_conn.execute(sql, tuple(params))
            self._write_conn.commit()
            self._bump("sync_writes")
            return cursor.rowcount

    def executemany(self, sql: str, rows: Iterable[Sequence[Any]]) -> int:
        assert self._write_conn is not None, "connect() first"
        payload = [tuple(r) for r in rows]
        if not payload:
            return 0
        with self._write_lock:
            cursor = self._write_conn.executemany(sql, payload)
            self._write_conn.commit()
            self._bump("sync_writes", len(payload))
            return cursor.rowcount

    def enqueue(self, table: str, row: dict[str, Any]) -> bool:
        """Hand a row to the background writer.  Never blocks, never raises."""
        if table not in TABLES_BY_NAME:
            raise KeyError(f"unknown table {table!r}")
        try:
            prepared = self.prepare_row(table, row)
        except KeyError:
            self._bump("errors")
            raise
        try:
            self._queue.put_nowait((table, prepared))
        except queue.Full:
            self._bump("dropped")
            return False
        self._bump("enqueued")
        self._flushed.clear()
        return True

    def flush(self, timeout: float = 5.0) -> int:
        """Block until the writer has drained what is queued right now."""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self._queue.empty() and self._flushed.is_set():
                break
            time.sleep(0.01)
        return self._counters["written"]

    def _writer_loop(self) -> None:
        pending: dict[str, list[dict[str, Any]]] = {}
        pending_count = 0
        last_flush = time.monotonic()
        while True:
            timeout = max(0.01, self.flush_interval_s - (time.monotonic() - last_flush))
            try:
                item = self._queue.get(timeout=timeout)
            except queue.Empty:
                item = None
            if item is _SHUTDOWN:
                self._commit(pending)
                self._flushed.set()
                return
            if item is not None:
                table, row = item  # type: ignore[misc]
                pending.setdefault(table, []).append(row)
                pending_count += 1
            due = pending_count >= self.batch_size or (time.monotonic() - last_flush) >= self.flush_interval_s
            if pending_count and due:
                self._commit(pending)
                pending = {}
                pending_count = 0
                last_flush = time.monotonic()
                if self._queue.empty():
                    self._flushed.set()
            elif not pending_count:
                last_flush = time.monotonic()
                if self._queue.empty():
                    self._flushed.set()

    def _commit(self, pending: dict[str, list[dict[str, Any]]]) -> None:
        if not pending or self._write_conn is None:
            return
        try:
            with self._write_lock:
                for table, rows in pending.items():
                    if not rows:
                        continue
                    columns = tuple(rows[0].keys())
                    sql = insert_sql(table, columns)
                    self._write_conn.executemany(sql, [tuple(r.get(c) for c in columns) for r in rows])
                    self._bump("written", len(rows))
                self._write_conn.commit()
                self._bump("batches")
        except sqlite3.Error as exc:
            # One malformed table must not cost the whole batch: retry the rest
            # table by table so a single bad row loses only its own table.
            self._bump("errors")
            log.error("batch write failed, retrying per table", extra={"error": str(exc)})
            for table, rows in pending.items():
                try:
                    with self._write_lock:
                        columns = tuple(rows[0].keys())
                        self._write_conn.executemany(
                            insert_sql(table, columns), [tuple(r.get(c) for c in columns) for r in rows]
                        )
                        self._write_conn.commit()
                        self._bump("written", len(rows))
                except sqlite3.Error as inner:
                    self._bump("errors")
                    self._bump("dropped", len(rows))
                    log.error("table write failed", extra={"table": table, "error": str(inner)})

    # ---------------------------------------------------------------- reads

    def query(self, sql: str, params: Sequence[Any] = ()) -> list[dict[str, Any]]:
        conn = self._read_connection()
        cursor = conn.execute(sql, tuple(params))
        try:
            return [dict(row) for row in cursor.fetchall()]
        finally:
            cursor.close()

    # ------------------------------------------------------------ utilities

    def _bump(self, key: str, amount: int = 1) -> None:
        with self._counter_lock:
            self._counters[key] = self._counters.get(key, 0) + amount

    def stats(self) -> dict[str, Any]:
        size_bytes = self.path.stat().st_size if self.path.exists() else 0
        return {
            "driver": "sqlite",
            "path": str(self.path),
            "size_bytes": size_bytes,
            "size_mb": round(size_bytes / (1024 * 1024), 3),
            "queue_depth": self._queue.qsize(),
            "queue_capacity": self._queue.maxsize,
            "writer_alive": bool(self._writer and self._writer.is_alive()),
            **dict(self._counters),
        }

    def table_counts(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        for table in TABLES:
            try:
                counts[table.name] = int(self.scalar(f"SELECT COUNT(*) FROM {table.name}", default=0) or 0)
            except sqlite3.Error:
                counts[table.name] = -1
        return counts

    def prune(self, retention: dict[str, float], *, now_ms: int | None = None) -> dict[str, int]:
        """Delete aged rows from the high-frequency tables.

        Only the tables that name a ``retention_key`` are touched — research,
        trades, wallet, cycles and post-mortems are history and are never pruned.
        """
        stamp = now_ms if now_ms is not None else int(time.time() * 1000)
        removed: dict[str, int] = {}
        for table in TABLES:
            if not table.retention_key:
                continue
            hours = retention.get(table.retention_key)
            if not hours:
                continue
            cutoff = stamp - int(hours * 3_600_000)
            removed[table.name] = max(0, self.execute(f"DELETE FROM {table.name} WHERE ts_ms < ?", (cutoff,)))
        return removed

    def vacuum(self) -> None:
        assert self._write_conn is not None
        with self._write_lock:
            self._write_conn.execute("VACUUM")
            self._write_conn.commit()


def open_database(url: str, **kwargs: Any) -> Database:
    """Build a driver from a URL.  ``sqlite:///abs/path`` or ``sqlite://relative``."""
    if url.startswith("sqlite:///"):
        return SqliteDatabase(url[len("sqlite:///") :], **kwargs)
    if url.startswith("sqlite://"):
        return SqliteDatabase(url[len("sqlite://") :], **kwargs)
    if url.startswith("postgres"):
        raise NotImplementedError(
            "The PostgreSQL driver is not part of v1. The schema and the Database "
            "abstraction are dialect-aware so it can be added without touching "
            "callers; see aurum/storage/schema.py:render_ddl."
        )
    return SqliteDatabase(url, **kwargs)
