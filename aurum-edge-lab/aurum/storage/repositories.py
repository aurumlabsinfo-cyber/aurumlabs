"""Repositories: the only place in the system that writes SQL.

Each repository owns a small group of tables and speaks in domain objects.
Callers never see a cursor, a JSON blob or a dialect.
"""

from __future__ import annotations

from typing import Any

from ..domain import (
    FeatureSnapshot,
    MarketEvent,
    PaperTrade,
    Position,
    Signal,
    StrategyState,
    SymbolQuality,
    now_ms,
)
from .base import Database, decode_json


class MarketRepository:
    """market_events, orderbook_snapshots, features, data_quality."""

    def __init__(self, db: Database) -> None:
        self.db = db

    def record_event(self, event: MarketEvent) -> None:
        self.db.write(
            "market_events",
            {
                "symbol": event.symbol,
                "kind": event.kind.value,
                "ts_ms": event.ts_ms,
                "recv_ms": event.recv_ms,
                "latency_ms": event.latency_ms,
                "source": event.source,
                "payload": event.payload,
            },
        )

    def record_book(self, snapshot: Any, *, levels: int = 10) -> None:
        self.db.write(
            "orderbook_snapshots",
            {
                "symbol": snapshot.symbol,
                "ts_ms": snapshot.ts_ms,
                "last_update_id": snapshot.last_update_id,
                "best_bid": snapshot.best_bid,
                "best_ask": snapshot.best_ask,
                "mid": snapshot.mid,
                "spread_bps": snapshot.spread_bps(),
                "bid_depth_10": sum(level.qty for level in snapshot.bids[:10]),
                "ask_depth_10": sum(level.qty for level in snapshot.asks[:10]),
                "is_crossed": snapshot.is_crossed,
                "bids": [[level.price, level.qty] for level in snapshot.bids[:levels]],
                "asks": [[level.price, level.qty] for level in snapshot.asks[:levels]],
            },
        )

    def record_features(self, snapshot: FeatureSnapshot) -> None:
        self.db.write(
            "features",
            {
                "symbol": snapshot.symbol,
                "ts_ms": snapshot.ts_ms,
                "regime": snapshot.regime.value,
                "quality_score": snapshot.quality_score,
                "tradable": snapshot.tradable,
                "mid": snapshot.mid,
                "microprice": snapshot.microprice,
                "spread_bps": snapshot.spread_bps,
                "values_json": snapshot.values,
            },
        )

    def record_quality(self, quality: SymbolQuality) -> None:
        self.db.write(
            "data_quality",
            {
                "symbol": quality.symbol,
                "ts_ms": quality.ts_ms,
                "score": quality.score,
                "state": quality.state.value,
                "tradable": quality.tradable,
                "flags": [f.value for f in quality.flags],
                "latency_ms": quality.latency_ms,
                "spread_bps": quality.spread_bps,
                "events_per_min": quality.events_per_min,
                "sequence_gaps": quality.sequence_gaps,
                "resyncs": quality.resyncs,
            },
        )

    def recent_features(self, symbol: str, limit: int = 200) -> list[dict[str, Any]]:
        rows = self.db.query(
            "SELECT * FROM features WHERE symbol = ? ORDER BY ts_ms DESC LIMIT ?", (symbol, limit)
        )
        for row in rows:
            row["values_json"] = decode_json(row.get("values_json"), {})
        return rows

    def event_counts(self, since_ms: int) -> dict[str, int]:
        rows = self.db.query(
            "SELECT symbol, COUNT(*) AS n FROM market_events WHERE ts_ms >= ? GROUP BY symbol", (since_ms,)
        )
        return {row["symbol"]: int(row["n"]) for row in rows}


class ResearchRepository:
    """hypotheses, experiments, research_memory, agent_events."""

    def __init__(self, db: Database) -> None:
        self.db = db

    def save_hypothesis(self, row: dict[str, Any]) -> None:
        existing = self.db.query_one(
            "SELECT hypothesis_id FROM hypotheses WHERE hypothesis_id = ?", (row["hypothesis_id"],)
        )
        if existing:
            fields = [k for k in row if k != "hypothesis_id"]
            prepared = self.db.prepare_row("hypotheses", {k: row[k] for k in fields})
            assignments = ", ".join(f"{k} = ?" for k in prepared)
            self.db.execute(
                f"UPDATE hypotheses SET {assignments} WHERE hypothesis_id = ?",
                (*prepared.values(), row["hypothesis_id"]),
            )
        else:
            self.db.insert("hypotheses", row)

    def get_hypothesis(self, hypothesis_id: str) -> dict[str, Any] | None:
        row = self.db.query_one("SELECT * FROM hypotheses WHERE hypothesis_id = ?", (hypothesis_id,))
        if row:
            row["conditions"] = decode_json(row.get("conditions"), [])
        return row

    def list_hypotheses(
        self, *, status: str | None = None, agent: str | None = None, limit: int = 200
    ) -> list[dict[str, Any]]:
        clauses, params = [], []
        if status:
            clauses.append("validation_status = ?")
            params.append(status)
        if agent:
            clauses.append("agent = ?")
            params.append(agent)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        rows = self.db.query(
            f"SELECT * FROM hypotheses {where} ORDER BY updated_ms DESC, created_ms DESC LIMIT ?",
            (*params, limit),
        )
        for row in rows:
            row["conditions"] = decode_json(row.get("conditions"), [])
        return rows

    def status_counts(self) -> dict[str, int]:
        rows = self.db.query("SELECT validation_status AS s, COUNT(*) AS n FROM hypotheses GROUP BY s")
        return {row["s"]: int(row["n"]) for row in rows}

    def save_experiment(self, row: dict[str, Any]) -> None:
        self.db.insert("experiments", row)

    def list_experiments(self, hypothesis_id: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
        if hypothesis_id:
            rows = self.db.query(
                "SELECT * FROM experiments WHERE hypothesis_id = ? ORDER BY created_ms DESC LIMIT ?",
                (hypothesis_id, limit),
            )
        else:
            rows = self.db.query("SELECT * FROM experiments ORDER BY created_ms DESC LIMIT ?", (limit,))
        for row in rows:
            row["metrics"] = decode_json(row.get("metrics"), {})
            row["folds"] = decode_json(row.get("folds"), [])
        return rows

    def upsert_memory(self, row: dict[str, Any]) -> None:
        existing = self.db.query_one(
            "SELECT id, tests FROM research_memory WHERE fingerprint = ?", (row["fingerprint"],)
        )
        if existing:
            row = dict(row)
            row["tests"] = int(existing["tests"] or 0) + int(row.get("tests", 1))
            fields = [k for k in row if k != "fingerprint"]
            prepared = self.db.prepare_row("research_memory", {k: row[k] for k in fields})
            assignments = ", ".join(f"{k} = ?" for k in prepared)
            self.db.execute(
                f"UPDATE research_memory SET {assignments} WHERE fingerprint = ?",
                (*prepared.values(), row["fingerprint"]),
            )
        else:
            self.db.insert("research_memory", row)

    def get_memory(self, fingerprint: str) -> dict[str, Any] | None:
        row = self.db.query_one("SELECT * FROM research_memory WHERE fingerprint = ?", (fingerprint,))
        if row:
            row["conditions"] = decode_json(row.get("conditions"), [])
        return row

    def all_memory(self, limit: int = 5000) -> list[dict[str, Any]]:
        rows = self.db.query("SELECT * FROM research_memory ORDER BY last_seen_ms DESC LIMIT ?", (limit,))
        for row in rows:
            row["conditions"] = decode_json(row.get("conditions"), [])
        return rows

    def memory_counts(self) -> dict[str, int]:
        rows = self.db.query("SELECT outcome, COUNT(*) AS n FROM research_memory GROUP BY outcome")
        return {row["outcome"]: int(row["n"]) for row in rows}

    def log_agent_event(
        self, agent: str, kind: str, message: str, *, severity: str = "INFO", detail: dict[str, Any] | None = None
    ) -> None:
        self.db.write(
            "agent_events",
            {
                "ts_ms": now_ms(),
                "agent": agent,
                "kind": kind,
                "severity": severity,
                "message": message,
                "detail": detail or {},
            },
        )

    def recent_agent_events(self, agent: str | None = None, limit: int = 50) -> list[dict[str, Any]]:
        if agent:
            rows = self.db.query(
                "SELECT * FROM agent_events WHERE agent = ? ORDER BY ts_ms DESC LIMIT ?", (agent, limit)
            )
        else:
            rows = self.db.query("SELECT * FROM agent_events ORDER BY ts_ms DESC LIMIT ?", (limit,))
        for row in rows:
            row["detail"] = decode_json(row.get("detail"), {})
        return rows


class StrategyRepository:
    """strategies, strategy_versions, model_registry."""

    def __init__(self, db: Database) -> None:
        self.db = db

    def save_strategy(self, row: dict[str, Any]) -> None:
        existing = self.db.query_one("SELECT id FROM strategies WHERE strategy_id = ?", (row["strategy_id"],))
        if existing:
            fields = {k: v for k, v in row.items() if k != "strategy_id"}
            prepared = self.db.prepare_row("strategies", fields)
            assignments = ", ".join(f"{k} = ?" for k in prepared)
            self.db.execute(
                f"UPDATE strategies SET {assignments} WHERE strategy_id = ?",
                (*prepared.values(), row["strategy_id"]),
            )
        else:
            self.db.insert("strategies", row)

    def add_version(self, row: dict[str, Any]) -> None:
        self.db.insert("strategy_versions", row)

    def get_strategy(self, strategy_id: str) -> dict[str, Any] | None:
        row = self.db.query_one("SELECT * FROM strategies WHERE strategy_id = ?", (strategy_id,))
        if row:
            row["metrics"] = decode_json(row.get("metrics"), {})
        return row

    def list_strategies(self, state: StrategyState | str | None = None, limit: int = 200) -> list[dict[str, Any]]:
        value = state.value if isinstance(state, StrategyState) else state
        if value:
            rows = self.db.query(
                "SELECT * FROM strategies WHERE state = ? ORDER BY score DESC LIMIT ?", (value, limit)
            )
        else:
            rows = self.db.query("SELECT * FROM strategies ORDER BY score DESC LIMIT ?", (limit,))
        for row in rows:
            row["metrics"] = decode_json(row.get("metrics"), {})
        return rows

    def versions(self, strategy_id: str, limit: int = 100) -> list[dict[str, Any]]:
        rows = self.db.query(
            "SELECT * FROM strategy_versions WHERE strategy_id = ? ORDER BY version DESC LIMIT ?",
            (strategy_id, limit),
        )
        for row in rows:
            row["evidence"] = decode_json(row.get("evidence"), {})
            row["definition"] = decode_json(row.get("definition"), {})
        return rows

    def register_model(self, row: dict[str, Any]) -> None:
        self.db.insert("model_registry", row)

    def list_models(self, limit: int = 100) -> list[dict[str, Any]]:
        rows = self.db.query("SELECT * FROM model_registry ORDER BY created_ms DESC LIMIT ?", (limit,))
        for row in rows:
            row["params"] = decode_json(row.get("params"), {})
            row["calibration"] = decode_json(row.get("calibration"), {})
            row["metrics"] = decode_json(row.get("metrics"), {})
        return rows


class ExecutionRepository:
    """signals, positions, paper_trades."""

    def __init__(self, db: Database) -> None:
        self.db = db

    def record_signal(self, signal: Signal, cycle_id: int) -> None:
        self.db.write(
            "signals",
            {
                "signal_id": signal.signal_id,
                "ts_ms": signal.ts_ms,
                "strategy_id": signal.strategy_id,
                "hypothesis_id": signal.hypothesis_id,
                "symbol": signal.symbol,
                "direction": signal.direction.value,
                "confidence": signal.confidence,
                "expected_edge_bps": signal.expected_edge_bps,
                "expected_cost_bps": signal.expected_cost_bps,
                "net_edge_bps": signal.net_edge_bps,
                "horizon_ms": signal.horizon_ms,
                "regime": signal.regime.value,
                "accepted": signal.accepted,
                "shadow": signal.shadow,
                "rejection": signal.rejection.value if signal.rejection else None,
                "rejection_detail": signal.rejection_detail,
                "features": signal.features,
                "cycle_id": cycle_id,
            },
        )

    def open_position(self, position: Position) -> None:
        self.db.insert(
            "positions",
            {
                "position_id": position.position_id,
                "symbol": position.symbol,
                "direction": position.direction.value,
                "qty": position.qty,
                "entry_ts_ms": position.entry_ts_ms,
                "entry_price": position.entry_price,
                "requested_entry_price": position.requested_entry_price,
                "notional_eur": position.notional_eur,
                "margin_eur": position.margin_eur,
                "entry_fee_eur": position.entry_fee_eur,
                "entry_slippage_bps": position.entry_slippage_bps,
                "stop_bps": position.stop_bps,
                "target_bps": position.target_bps,
                "horizon_ms": position.horizon_ms,
                "strategy_id": position.strategy_id,
                "hypothesis_id": position.hypothesis_id,
                "signal_id": position.signal_id,
                "cycle_id": position.cycle_id,
                "status": "OPEN",
                "features": position.features,
            },
        )

    def close_position(self, position_id: str, closed_ts_ms: int) -> None:
        self.db.execute(
            "UPDATE positions SET status = 'CLOSED', closed_ts_ms = ? WHERE position_id = ?",
            (closed_ts_ms, position_id),
        )

    def record_trade(self, trade: PaperTrade) -> None:
        self.db.insert(
            "paper_trades",
            {
                "trade_id": trade.trade_id,
                "position_id": trade.position_id,
                "symbol": trade.symbol,
                "direction": trade.direction.value,
                "qty": trade.qty,
                "entry_ts_ms": trade.entry_ts_ms,
                "exit_ts_ms": trade.exit_ts_ms,
                "holding_ms": trade.holding_ms,
                "requested_entry_price": trade.requested_entry_price,
                "entry_price": trade.entry_price,
                "requested_exit_price": trade.requested_exit_price,
                "exit_price": trade.exit_price,
                "gross_pnl_eur": trade.gross_pnl_eur,
                "fees_eur": trade.fees_eur,
                "net_pnl_eur": trade.net_pnl_eur,
                "return_bps": trade.return_bps,
                "net_return_bps": trade.net_return_bps,
                "entry_slippage_bps": trade.entry_slippage_bps,
                "exit_slippage_bps": trade.exit_slippage_bps,
                "cost_bps": trade.cost_bps,
                "expected_edge_bps": trade.expected_edge_bps,
                "exit_reason": trade.exit_reason.value,
                "strategy_id": trade.strategy_id,
                "strategy_version": trade.strategy_version,
                "hypothesis_id": trade.hypothesis_id,
                "signal_id": trade.signal_id,
                "cycle_id": trade.cycle_id,
                "regime": trade.regime.value,
                "cost_model_version": trade.cost_model_version,
                "features": trade.features,
            },
        )

    def list_trades(
        self, *, cycle_id: int | None = None, strategy_id: str | None = None, limit: int = 100
    ) -> list[dict[str, Any]]:
        clauses, params = [], []
        if cycle_id is not None:
            clauses.append("cycle_id = ?")
            params.append(cycle_id)
        if strategy_id:
            clauses.append("strategy_id = ?")
            params.append(strategy_id)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        rows = self.db.query(
            f"SELECT * FROM paper_trades {where} ORDER BY exit_ts_ms DESC LIMIT ?", (*params, limit)
        )
        for row in rows:
            row["features"] = decode_json(row.get("features"), {})
        return rows

    def load_trade(self, trade_id: str) -> dict[str, Any] | None:
        row = self.db.query_one("SELECT * FROM paper_trades WHERE trade_id = ?", (trade_id,))
        if row:
            row["features"] = decode_json(row.get("features"), {})
        return row

    def list_signals(
        self,
        *,
        symbol: str | None = None,
        accepted: bool | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        clauses, params = [], []
        if symbol:
            clauses.append("symbol = ?")
            params.append(symbol)
        if accepted is not None:
            clauses.append("accepted = ?")
            params.append(1 if accepted else 0)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        rows = self.db.query(
            f"SELECT * FROM signals {where} ORDER BY ts_ms DESC LIMIT ?", (*params, limit)
        )
        for row in rows:
            row["features"] = decode_json(row.get("features"), {})
        return rows

    def rejection_counts(self, since_ms: int) -> list[dict[str, Any]]:
        return self.db.query(
            "SELECT rejection, COUNT(*) AS n FROM signals "
            "WHERE ts_ms >= ? AND rejection IS NOT NULL GROUP BY rejection ORDER BY n DESC",
            (since_ms,),
        )

    def signal_totals(self, since_ms: int) -> dict[str, int]:
        row = self.db.query_one(
            "SELECT COUNT(*) AS total, SUM(CASE WHEN accepted THEN 1 ELSE 0 END) AS accepted "
            "FROM signals WHERE ts_ms >= ?",
            (since_ms,),
        )
        return {
            "total": int(row["total"] or 0) if row else 0,
            "accepted": int(row["accepted"] or 0) if row else 0,
        }

    def restore_open_positions(self) -> list[dict[str, Any]]:
        rows = self.db.query("SELECT * FROM positions WHERE status = 'OPEN' ORDER BY entry_ts_ms ASC")
        for row in rows:
            row["features"] = decode_json(row.get("features"), {})
        return rows


class WalletRepository:
    """wallet, wallet_ledger, cycles, cycle_postmortems."""

    def __init__(self, db: Database) -> None:
        self.db = db

    def snapshot(self, row: dict[str, Any]) -> None:
        self.db.insert("wallet", row)

    def ledger(self, row: dict[str, Any]) -> None:
        self.db.insert("wallet_ledger", row)

    def list_ledger(self, cycle_id: int | None = None, limit: int = 200) -> list[dict[str, Any]]:
        if cycle_id is not None:
            return self.db.query(
                "SELECT * FROM wallet_ledger WHERE cycle_id = ? ORDER BY ts_ms DESC, id DESC LIMIT ?",
                (cycle_id, limit),
            )
        return self.db.query("SELECT * FROM wallet_ledger ORDER BY ts_ms DESC, id DESC LIMIT ?", (limit,))

    def equity_curve(self, cycle_id: int, limit: int = 500) -> list[dict[str, Any]]:
        rows = self.db.query(
            "SELECT ts_ms, equity, balance, drawdown_pct FROM wallet WHERE cycle_id = ? "
            "ORDER BY ts_ms DESC LIMIT ?",
            (cycle_id, limit),
        )
        return list(reversed(rows))

    def latest_wallet(self) -> dict[str, Any] | None:
        return self.db.query_one("SELECT * FROM wallet ORDER BY id DESC LIMIT 1")

    def save_cycle(self, row: dict[str, Any]) -> None:
        existing = self.db.query_one("SELECT id FROM cycles WHERE cycle_id = ?", (row["cycle_id"],))
        if existing:
            fields = {k: v for k, v in row.items() if k != "cycle_id"}
            prepared = self.db.prepare_row("cycles", fields)
            assignments = ", ".join(f"{k} = ?" for k in prepared)
            self.db.execute(
                f"UPDATE cycles SET {assignments} WHERE cycle_id = ?", (*prepared.values(), row["cycle_id"])
            )
        else:
            self.db.insert("cycles", row)

    def current_cycle(self) -> dict[str, Any] | None:
        return self.db.query_one("SELECT * FROM cycles ORDER BY cycle_id DESC LIMIT 1")

    def list_cycles(self, limit: int = 50) -> list[dict[str, Any]]:
        return self.db.query("SELECT * FROM cycles ORDER BY cycle_id DESC LIMIT ?", (limit,))

    def save_postmortem(self, row: dict[str, Any]) -> None:
        self.db.insert("cycle_postmortems", row)

    def postmortems(self, cycle_id: int | None = None, limit: int = 20) -> list[dict[str, Any]]:
        if cycle_id is not None:
            rows = self.db.query(
                "SELECT * FROM cycle_postmortems WHERE cycle_id = ? ORDER BY created_ms DESC", (cycle_id,)
            )
        else:
            rows = self.db.query("SELECT * FROM cycle_postmortems ORDER BY created_ms DESC LIMIT ?", (limit,))
        for row in rows:
            row["causes"] = decode_json(row.get("causes"), [])
            row["evidence"] = decode_json(row.get("evidence"), {})
            row["recommendations"] = decode_json(row.get("recommendations"), [])
        return rows


class SystemRepository:
    """system_events — the audit trail for anything operational."""

    def __init__(self, db: Database) -> None:
        self.db = db

    def log(
        self,
        component: str,
        kind: str,
        message: str,
        *,
        level: str = "INFO",
        detail: dict[str, Any] | None = None,
    ) -> None:
        self.db.insert(
            "system_events",
            {
                "ts_ms": now_ms(),
                "level": level,
                "component": component,
                "kind": kind,
                "message": message,
                "detail": detail or {},
            },
        )

    def recent(self, limit: int = 100, level: str | None = None) -> list[dict[str, Any]]:
        if level:
            rows = self.db.query(
                "SELECT * FROM system_events WHERE level = ? ORDER BY ts_ms DESC LIMIT ?", (level, limit)
            )
        else:
            rows = self.db.query("SELECT * FROM system_events ORDER BY ts_ms DESC LIMIT ?", (limit,))
        for row in rows:
            row["detail"] = decode_json(row.get("detail"), {})
        return rows

    def recent_errors(self, limit: int = 20) -> list[dict[str, Any]]:
        rows = self.db.query(
            "SELECT * FROM system_events WHERE level IN ('ERROR','CRITICAL') ORDER BY ts_ms DESC LIMIT ?",
            (limit,),
        )
        for row in rows:
            row["detail"] = decode_json(row.get("detail"), {})
        return rows


class Repositories:
    """One handle carrying every repository, built once at startup."""

    def __init__(self, db: Database) -> None:
        self.db = db
        self.market = MarketRepository(db)
        self.research = ResearchRepository(db)
        self.strategies = StrategyRepository(db)
        self.execution = ExecutionRepository(db)
        self.wallet = WalletRepository(db)
        self.system = SystemRepository(db)


__all__ = [
    "ExecutionRepository",
    "MarketRepository",
    "Repositories",
    "ResearchRepository",
    "StrategyRepository",
    "SystemRepository",
    "WalletRepository",
]
