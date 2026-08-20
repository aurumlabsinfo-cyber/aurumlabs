"""Domain reads and writes on top of :class:`Database`.

The numbers the frontend shows are computed here, once, from the same rows the
trading core wrote - the dashboard never recomputes P&L its own way.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from typing import Any

from .db import Database


@dataclass
class Stats:
    trades: int = 0
    wins: int = 0
    losses: int = 0
    win_rate: float = 0.0
    avg_win_eur: float = 0.0
    avg_loss_eur: float = 0.0
    expectancy_eur: float = 0.0
    gross_pnl_eur: float = 0.0
    fees_eur: float = 0.0
    slippage_eur: float = 0.0
    net_pnl_eur: float = 0.0
    best_eur: float = 0.0
    worst_eur: float = 0.0
    avg_hold_s: float = 0.0
    trades_per_hour: float = 0.0
    net_per_hour_eur: float = 0.0
    max_drawdown_eur: float = 0.0
    profit_factor: float = 0.0
    first_ts: float = 0.0
    last_ts: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        return {k: round(v, 6) if isinstance(v, float) else v for k, v in self.__dict__.items()}


def compute_stats(rows: list[dict[str, Any]]) -> Stats:
    """Aggregate closed trades.  ``gross - fees - slippage == net`` by construction."""
    stats = Stats()
    if not rows:
        return stats
    stats.trades = len(rows)
    wins = [r for r in rows if r["net_pnl_eur"] > 0]
    losses = [r for r in rows if r["net_pnl_eur"] <= 0]
    stats.wins = len(wins)
    stats.losses = len(losses)
    stats.win_rate = stats.wins / stats.trades
    stats.avg_win_eur = sum(r["net_pnl_eur"] for r in wins) / len(wins) if wins else 0.0
    stats.avg_loss_eur = (
        sum(r["net_pnl_eur"] for r in losses) / len(losses) if losses else 0.0
    )
    stats.gross_pnl_eur = sum(r["gross_pnl_eur"] for r in rows)
    stats.fees_eur = sum(r["fees_eur"] for r in rows)
    stats.slippage_eur = sum(r["slippage_eur"] for r in rows)
    stats.net_pnl_eur = sum(r["net_pnl_eur"] for r in rows)
    stats.expectancy_eur = stats.net_pnl_eur / stats.trades
    stats.best_eur = max(r["net_pnl_eur"] for r in rows)
    stats.worst_eur = min(r["net_pnl_eur"] for r in rows)
    stats.avg_hold_s = sum(r["hold_s"] for r in rows) / stats.trades

    gross_win = sum(r["net_pnl_eur"] for r in wins)
    gross_loss = -sum(r["net_pnl_eur"] for r in losses)
    stats.profit_factor = gross_win / gross_loss if gross_loss > 0 else float(stats.trades and gross_win > 0) * 999.0

    ordered = sorted(rows, key=lambda r: r["exit_ts"])
    stats.first_ts = ordered[0]["entry_ts"]
    stats.last_ts = ordered[-1]["exit_ts"]
    running = peak = 0.0
    max_dd = 0.0
    for row in ordered:
        running += row["net_pnl_eur"]
        peak = max(peak, running)
        max_dd = max(max_dd, peak - running)
    stats.max_drawdown_eur = max_dd

    elapsed_h = max((stats.last_ts - stats.first_ts) / 3_600_000.0, 1e-9)
    stats.trades_per_hour = stats.trades / elapsed_h
    stats.net_per_hour_eur = stats.net_pnl_eur / elapsed_h
    return stats


class Repo:
    def __init__(self, db: Database) -> None:
        self.db = db

    # ---------------------------------------------------------------- writes
    def save_snapshot(self, symbol: str, ts_ms: float, source: str, payload: dict) -> int | None:
        return self.db.insert(
            "snapshots",
            {
                "run_id": self.db.run_id,
                "ts_ms": ts_ms,
                "symbol": symbol,
                "source": source,
                "data_json": json.dumps(payload, separators=(",", ":"), default=str),
            },
        )

    def save_decision(self, row: dict[str, Any]) -> int | None:
        row = dict(row)
        row["run_id"] = self.db.run_id
        return self.db.insert("decisions", row)

    def mark_decision_executed(self, decision_id: int) -> None:
        self.db.execute("UPDATE decisions SET executed=1 WHERE id=?", (decision_id,))

    def set_decision_outcome(
        self,
        decision_id: int,
        move_bps: float,
        cost_bps: float,
        label: int,
        ts_ms: float,
        horizon_s: float,
    ) -> None:
        """Record what the market did after a decision - taken or not."""
        self.db.execute(
            "UPDATE decisions SET outcome_move_bps=?, outcome_cost_bps=?, outcome_label=?, "
            "outcome_ts=?, outcome_horizon_s=? WHERE id=?",
            (move_bps, cost_bps, label, ts_ms, horizon_s, decision_id),
        )

    def labelled_decisions(self, limit: int = 200_000) -> list[dict[str, Any]]:
        return self.db.query(
            "SELECT id, ts_ms, symbol, side, action, features_json, outcome_move_bps, "
            "outcome_cost_bps, outcome_label, model_version, shadow, probability, quality "
            "FROM decisions WHERE outcome_label IS NOT NULL ORDER BY ts_ms ASC LIMIT ?",
            (limit,),
        )

    def labelled_decision_count(self) -> int:
        row = self.db.query_one(
            "SELECT COUNT(*) AS n FROM decisions WHERE outcome_label IS NOT NULL"
        )
        return int(row["n"]) if row else 0

    def bump_reject(self, reason: str, count: int, ts_ms: float) -> None:
        bucket = int(ts_ms // 60_000)
        self.db.execute(
            "INSERT INTO reject_counters(run_id, minute_bucket, reason, count) "
            "VALUES (?,?,?,?) ON CONFLICT(run_id, minute_bucket, reason) "
            "DO UPDATE SET count = count + excluded.count",
            (self.db.run_id, bucket, reason, count),
        )

    def save_order(self, row: dict[str, Any]) -> int | None:
        row = dict(row)
        row["run_id"] = self.db.run_id
        return self.db.upsert("orders", row, ["order_link_id"])

    def update_order(self, order_link_id: str, **fields: Any) -> None:
        if not fields:
            return
        assignments = ", ".join(f"{k}=?" for k in fields)
        self.db.execute(
            f"UPDATE orders SET {assignments} WHERE order_link_id=?",
            (*fields.values(), order_link_id),
        )

    def save_execution(self, row: dict[str, Any]) -> int | None:
        row = dict(row)
        row["run_id"] = self.db.run_id
        return self.db.upsert("executions", row, ["exec_id"])

    def execution_exists(self, exec_id: str) -> bool:
        return self.db.query_one("SELECT 1 FROM executions WHERE exec_id=?", (exec_id,)) is not None

    def save_position(self, row: dict[str, Any]) -> int | None:
        row = dict(row)
        row["run_id"] = self.db.run_id
        return self.db.upsert("positions", row, ["position_key"])

    def close_position(self, position_key: str, closed_ts: float) -> None:
        self.db.execute(
            "UPDATE positions SET status='CLOSED', closed_ts=? WHERE position_key=?",
            (closed_ts, position_key),
        )

    def save_trade(self, row: dict[str, Any]) -> int | None:
        row = dict(row)
        row["run_id"] = self.db.run_id
        trade_id = self.db.upsert("trades", row, ["position_key"])
        self._bump_symbol_stats(row)
        return trade_id

    def _bump_symbol_stats(self, trade: dict[str, Any]) -> None:
        symbol = trade["symbol"]
        prev = self.db.query_one("SELECT * FROM symbol_stats WHERE symbol=?", (symbol,))
        n = (prev["trades"] if prev else 0) + 1
        wins = (prev["wins"] if prev else 0) + (1 if trade["net_pnl_eur"] > 0 else 0)
        net = (prev["net_eur"] if prev else 0.0) + trade["net_pnl_eur"]
        slip = abs(trade["slippage_entry_bps"])
        mean_slip = (
            ((prev["slippage_bps_mean"] * (n - 1)) + slip) / n if prev else slip
        )
        self.db.upsert(
            "symbol_stats",
            {
                "symbol": symbol,
                "trades": n,
                "wins": wins,
                "net_eur": net,
                "slippage_bps_mean": mean_slip,
                "updated_at": time.time(),
            },
            ["symbol"],
        )

    def save_equity(self, row: dict[str, Any]) -> None:
        row = dict(row)
        row["run_id"] = self.db.run_id
        self.db.insert("equity", row)

    def save_health_event(self, component: str, state: str, detail: str, ts_ms: float) -> None:
        self.db.insert(
            "health_events",
            {
                "run_id": self.db.run_id,
                "ts_ms": ts_ms,
                "component": component,
                "state": state,
                "detail": detail,
            },
        )

    def save_reconciliation(self, row: dict[str, Any]) -> None:
        row = dict(row)
        row["run_id"] = self.db.run_id
        self.db.insert("reconciliations", row)

    # ---------------------------------------------------------------- reads
    def trades(self, run_id: str | None = None, limit: int = 500) -> list[dict[str, Any]]:
        if run_id == "*":
            return self.db.query("SELECT * FROM trades ORDER BY exit_ts DESC LIMIT ?", (limit,))
        return self.db.query(
            "SELECT * FROM trades WHERE run_id=? ORDER BY exit_ts DESC LIMIT ?",
            (run_id or self.db.run_id, limit),
        )

    def all_trades_for_training(self, limit: int = 50_000) -> list[dict[str, Any]]:
        return self.db.query(
            "SELECT * FROM trades ORDER BY entry_ts ASC LIMIT ?", (limit,)
        )

    def stats(self, run_id: str | None = None) -> Stats:
        rows = self.db.query(
            "SELECT * FROM trades WHERE run_id=?", (run_id or self.db.run_id,)
        )
        return compute_stats(rows)

    def recent_decisions(self, limit: int = 50, action: str | None = None) -> list[dict[str, Any]]:
        if action:
            return self.db.query(
                "SELECT * FROM decisions WHERE run_id=? AND action=? "
                "ORDER BY ts_ms DESC LIMIT ?",
                (self.db.run_id, action, limit),
            )
        return self.db.query(
            "SELECT * FROM decisions WHERE run_id=? ORDER BY ts_ms DESC LIMIT ?",
            (self.db.run_id, limit),
        )

    def reject_summary(self, window_minutes: int = 15) -> list[dict[str, Any]]:
        bucket = int(time.time() * 1000 // 60_000) - window_minutes
        return self.db.query(
            "SELECT reason, SUM(count) AS count FROM reject_counters "
            "WHERE run_id=? AND minute_bucket >= ? GROUP BY reason ORDER BY count DESC",
            (self.db.run_id, bucket),
        )

    def symbol_stats(self) -> list[dict[str, Any]]:
        return self.db.query("SELECT * FROM symbol_stats ORDER BY net_eur DESC")

    def recent_trades_for_model(self, version: str, limit: int) -> list[dict[str, Any]]:
        """The newest closed trades taken by one model version, across runs."""
        return self.db.query(
            "SELECT * FROM trades WHERE model_version=? ORDER BY exit_ts DESC LIMIT ?",
            (version, limit),
        )

    def regime_breakdown(self, limit: int = 5_000) -> list[dict[str, Any]]:
        """Performance by volatility regime, read off the entry snapshot.

        The bucket is the symbol's own volatility at entry, so "calm" and "wild"
        mean the same thing across a cheap altcoin and BTC.
        """
        rows = self.db.query(
            "SELECT volatility, side, hold_s, net_pnl_eur, fees_eur, slippage_eur, "
            "gross_pnl_eur FROM ("
            "  SELECT t.*, json_extract(s.data_json, '$.volatility_bps') AS volatility "
            "  FROM trades t LEFT JOIN snapshots s ON s.id = t.snapshot_id "
            "  ORDER BY t.exit_ts DESC LIMIT ?"
            ")",
            (limit,),
        )
        buckets: dict[str, list[dict[str, Any]]] = {}
        for row in rows:
            volatility = row["volatility"]
            if volatility is None:
                name = "unknown"
            elif volatility < 5:
                name = "calm (<5bps)"
            elif volatility < 12:
                name = "normal (5-12bps)"
            elif volatility < 25:
                name = "fast (12-25bps)"
            else:
                name = "wild (>25bps)"
            buckets.setdefault(name, []).append(row)

        out = []
        for name, group in buckets.items():
            stats = compute_stats(
                [{**r, "entry_ts": 0.0, "exit_ts": r["hold_s"] * 1000.0} for r in group]
            )
            out.append({
                "regime": name,
                "trades": stats.trades,
                "win_rate": round(stats.win_rate, 4),
                "expectancy_eur": round(stats.expectancy_eur, 4),
                "net_pnl_eur": round(stats.net_pnl_eur, 4),
                "avg_hold_s": round(stats.avg_hold_s, 2),
            })
        return sorted(out, key=lambda r: -r["trades"])

    def slippage_bps_for(self, symbol: str, default: float) -> float:
        row = self.db.query_one(
            "SELECT slippage_bps_mean, trades FROM symbol_stats WHERE symbol=?", (symbol,)
        )
        if not row or row["trades"] < 5:
            return default
        return max(float(row["slippage_bps_mean"]), 0.0)

    # ---------------------------------------------------------------- models
    def save_model_version(self, row: dict[str, Any]) -> None:
        self.db.upsert("model_versions", row, ["version"])

    def model_versions(self) -> list[dict[str, Any]]:
        return self.db.query("SELECT * FROM model_versions ORDER BY created_ts DESC")

    def get_model(self, version: str) -> dict[str, Any] | None:
        return self.db.query_one("SELECT * FROM model_versions WHERE version=?", (version,))

    def champion(self) -> dict[str, Any] | None:
        return self.db.query_one(
            "SELECT * FROM model_versions WHERE status='champion' ORDER BY promoted_ts DESC LIMIT 1"
        )

    def log_model_event(self, version: str, event: str, detail: dict[str, Any]) -> None:
        self.db.insert(
            "model_events",
            {
                "ts_ms": time.time() * 1000.0,
                "version": version,
                "event": event,
                "detail_json": json.dumps(detail, default=str),
            },
        )

    def model_events(self, limit: int = 30) -> list[dict[str, Any]]:
        return self.db.query(
            "SELECT * FROM model_events ORDER BY ts_ms DESC LIMIT ?", (limit,)
        )
