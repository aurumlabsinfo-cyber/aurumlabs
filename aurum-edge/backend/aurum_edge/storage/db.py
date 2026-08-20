"""The one canonical database.

Design commitments, each one a bug that bit the previous programs:

* **one path only** - resolved to an absolute path once, printed at start-up and
  exposed on the API, so backend and frontend can never read different files;
* **versioned migrations** - applied inside a transaction, recorded in
  ``migrations``;
* **schema validation at start-up** - a drifted database refuses to run instead
  of throwing at the moment a trade must be written;
* **dead letter** - if a write ever fails anyway, the row is kept in
  ``write_failures`` with its payload and the health state degrades.  A trade is
  never lost in silence.
"""

from __future__ import annotations

import json
import sqlite3
import threading
import time
import uuid
from pathlib import Path
from typing import Any, Iterable, Sequence

from ..util.logging_setup import get_logger
from .schema import EXPECTED_SCHEMA, MIGRATIONS, SCHEMA_VERSION

log = get_logger("db")


class SchemaError(RuntimeError):
    """The database on disk does not match the schema this code needs."""


def new_run_id() -> str:
    return f"run_{time.strftime('%Y%m%dT%H%M%S')}_{uuid.uuid4().hex[:8]}"


class Database:
    def __init__(self, path: str, run_id: str | None = None) -> None:
        self.path = str(Path(path).expanduser().resolve())
        self.run_id = run_id or new_run_id()
        self._lock = threading.RLock()
        self._conn: sqlite3.Connection | None = None
        self.write_failures = 0
        self.last_write_error: str | None = None

    # ---------------------------------------------------------------- lifecycle
    def open(self) -> "Database":
        Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(self.path, check_same_thread=False, timeout=15.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA busy_timeout=15000")
        conn.execute("PRAGMA foreign_keys=ON")
        self._conn = conn
        self.migrate()
        self.validate()
        log.info("database ready at %s (schema v%s)", self.path, self.schema_version())
        return self

    def close(self) -> None:
        with self._lock:
            if self._conn is not None:
                self._conn.commit()
                self._conn.close()
                self._conn = None

    def __enter__(self) -> "Database":
        return self.open() if self._conn is None else self

    def __exit__(self, *exc: object) -> None:
        self.close()

    @property
    def conn(self) -> sqlite3.Connection:
        if self._conn is None:
            raise RuntimeError("database is not open")
        return self._conn

    # ---------------------------------------------------------------- migrations
    def applied_versions(self) -> set[int]:
        with self._lock:
            try:
                rows = self.conn.execute("SELECT version FROM migrations").fetchall()
            except sqlite3.OperationalError:
                return set()
        return {int(r["version"]) for r in rows}

    def migrate(self) -> list[int]:
        applied: list[int] = []
        done = self.applied_versions()
        with self._lock:
            for version, name, statements in MIGRATIONS:
                if version in done:
                    continue
                try:
                    with self.conn:  # implicit transaction
                        for sql in statements:
                            self.conn.execute(sql)
                        self.conn.execute(
                            "INSERT INTO migrations(version, name, applied_at) VALUES (?,?,?)",
                            (version, name, time.time()),
                        )
                except sqlite3.Error as exc:
                    raise SchemaError(f"migration {version} ({name}) failed: {exc}") from exc
                applied.append(version)
                log.info("applied migration %s (%s)", version, name)
            self.conn.execute(
                "INSERT INTO schema_meta(key, value) VALUES ('schema_version', ?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (str(SCHEMA_VERSION),),
            )
            self.conn.execute(
                "INSERT INTO schema_meta(key, value) VALUES ('db_path', ?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (self.path,),
            )
            self.conn.commit()
        return applied

    def schema_version(self) -> int:
        row = self.query_one("SELECT value FROM schema_meta WHERE key='schema_version'")
        return int(row["value"]) if row else 0

    def validate(self) -> None:
        """Fail loudly now rather than dropping a trade later."""
        problems: list[str] = []
        for table, columns in EXPECTED_SCHEMA.items():
            info = self.query(f"PRAGMA table_info({table})")
            if not info:
                problems.append(f"missing table: {table}")
                continue
            present = {row["name"] for row in info}
            missing = [c for c in columns if c not in present]
            if missing:
                problems.append(f"{table} missing columns: {', '.join(missing)}")
        if problems:
            raise SchemaError(
                "database schema does not match the code:\n  - " + "\n  - ".join(problems)
            )

    # ---------------------------------------------------------------- primitives
    def query(self, sql: str, params: Sequence[Any] = ()) -> list[dict[str, Any]]:
        with self._lock:
            rows = self.conn.execute(sql, params).fetchall()
        return [dict(r) for r in rows]

    def query_one(self, sql: str, params: Sequence[Any] = ()) -> dict[str, Any] | None:
        rows = self.query(sql, params)
        return rows[0] if rows else None

    def execute(self, sql: str, params: Sequence[Any] = ()) -> int | None:
        """Run a statement.  Returns lastrowid, or None if it was dead-lettered."""
        table = _table_of(sql)
        with self._lock:
            try:
                cur = self.conn.execute(sql, params)
                self.conn.commit()
                return int(cur.lastrowid) if cur.lastrowid is not None else 0
            except sqlite3.Error as exc:
                self._dead_letter(table, {"sql": sql, "params": list(params)}, exc)
                return None

    def insert(self, table: str, row: dict[str, Any]) -> int | None:
        cols = ", ".join(row)
        marks = ", ".join("?" for _ in row)
        sql = f"INSERT INTO {table} ({cols}) VALUES ({marks})"
        with self._lock:
            try:
                cur = self.conn.execute(sql, tuple(row.values()))
                self.conn.commit()
                return int(cur.lastrowid)
            except sqlite3.Error as exc:
                self._dead_letter(table, row, exc)
                return None

    def upsert(self, table: str, row: dict[str, Any], keys: Sequence[str]) -> int | None:
        cols = ", ".join(row)
        marks = ", ".join("?" for _ in row)
        updates = ", ".join(f"{c}=excluded.{c}" for c in row if c not in keys)
        conflict = ", ".join(keys)
        sql = (
            f"INSERT INTO {table} ({cols}) VALUES ({marks}) "
            f"ON CONFLICT({conflict}) DO UPDATE SET {updates}"
        )
        with self._lock:
            try:
                cur = self.conn.execute(sql, tuple(row.values()))
                self.conn.commit()
                return int(cur.lastrowid)
            except sqlite3.Error as exc:
                self._dead_letter(table, row, exc)
                return None

    def executemany(self, sql: str, rows: Iterable[Sequence[Any]]) -> bool:
        rows = list(rows)
        with self._lock:
            try:
                self.conn.executemany(sql, rows)
                self.conn.commit()
                return True
            except sqlite3.Error as exc:
                self._dead_letter(_table_of(sql), {"sql": sql, "rows": len(rows)}, exc)
                return False

    # ---------------------------------------------------------------- dead letter
    def _dead_letter(self, table: str, payload: dict[str, Any], exc: Exception) -> None:
        self.write_failures += 1
        self.last_write_error = f"{table}: {exc}"
        log.error("WRITE FAILED on %s: %s -- payload kept in write_failures", table, exc)
        try:
            self.conn.execute(
                "INSERT INTO write_failures(ts_ms, table_name, payload_json, error) "
                "VALUES (?,?,?,?)",
                (time.time() * 1000.0, table, json.dumps(payload, default=str), str(exc)),
            )
            self.conn.commit()
        except sqlite3.Error as inner:  # pragma: no cover - database is unusable
            log.critical("dead letter write also failed: %s", inner)

    def pending_write_failures(self) -> int:
        row = self.query_one("SELECT COUNT(*) AS n FROM write_failures WHERE replayed=0")
        return int(row["n"]) if row else 0

    def replay_write_failures(self) -> tuple[int, int]:
        """Retry dead-lettered statements after a schema repair.  (ok, still_failing)"""
        rows = self.query(
            "SELECT id, table_name, payload_json FROM write_failures WHERE replayed=0"
        )
        ok = failed = 0
        for row in rows:
            payload = json.loads(row["payload_json"])
            try:
                with self._lock:
                    if "sql" in payload and "params" in payload:
                        self.conn.execute(payload["sql"], payload["params"])
                    elif "sql" not in payload:
                        cols = ", ".join(payload)
                        marks = ", ".join("?" for _ in payload)
                        self.conn.execute(
                            f"INSERT INTO {row['table_name']} ({cols}) VALUES ({marks})",
                            tuple(payload.values()),
                        )
                    else:
                        raise sqlite3.Error("payload cannot be replayed")
                    self.conn.execute(
                        "UPDATE write_failures SET replayed=1 WHERE id=?", (row["id"],)
                    )
                    self.conn.commit()
                ok += 1
            except sqlite3.Error:
                failed += 1
        return ok, failed

    # ---------------------------------------------------------------- key/value
    def kv_get(self, key: str, default: str | None = None) -> str | None:
        row = self.query_one("SELECT value FROM kv WHERE key=?", (key,))
        return row["value"] if row else default

    def kv_set(self, key: str, value: str) -> None:
        self.upsert("kv", {"key": key, "value": value, "updated_at": time.time()}, ["key"])

    # ---------------------------------------------------------------- runs
    def start_run(
        self,
        mode: str,
        feed_source: str,
        model_version: str,
        config_json: str,
        code_version: str,
    ) -> str:
        self.insert(
            "runs",
            {
                "run_id": self.run_id,
                "started_at": time.time(),
                "mode": mode,
                "feed_source": feed_source,
                "model_version": model_version,
                "code_version": code_version,
                "schema_version": SCHEMA_VERSION,
                "config_json": config_json,
            },
        )
        return self.run_id

    def end_run(self) -> None:
        self.execute("UPDATE runs SET ended_at=? WHERE run_id=?", (time.time(), self.run_id))


def _table_of(sql: str) -> str:
    """Best-effort table name for dead-letter bookkeeping."""
    tokens = sql.replace("(", " ").split()
    upper = [t.upper() for t in tokens]
    for keyword in ("INTO", "UPDATE", "FROM"):
        if keyword in upper:
            idx = upper.index(keyword)
            if idx + 1 < len(tokens):
                return tokens[idx + 1].strip("`\"'")
    return "unknown"
