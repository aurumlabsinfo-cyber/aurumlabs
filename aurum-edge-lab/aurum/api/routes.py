"""REST endpoints.

Every route in the blueprint's API contract, plus the two writes the Settings
page needs.  Handlers are thin: they read runtime state and shape it.  Anything
that decides something lives in the component that owns the decision, not here.

Two conventions worth stating:

* Nothing returns an empty body to mean "nothing happened".  ``/champion`` with
  no champion returns the *reason* there is none.  A frontend that renders "—"
  because the API said nothing cannot tell a healthy idle system from a broken
  one.
* Symbols are validated against the configured universe rather than passed
  through to a query, so a typo returns 404 instead of an empty list.
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request

from ..config import ConfigError, apply_setting, describe_symbols, settable_report
from ..diagnostics.collector import reason_catalogue
from ..domain import StrategyState, now_ms
from ..runtime import AurumRuntime

router = APIRouter()


def runtime_of(request: Request) -> AurumRuntime:
    runtime: AurumRuntime | None = getattr(request.app.state, "runtime", None)
    if runtime is None:
        raise HTTPException(status_code=503, detail="runtime is not started")
    return runtime


def _known_symbol(runtime: AurumRuntime, symbol: str) -> str:
    upper = symbol.upper()
    if upper not in runtime.data.states:
        raise HTTPException(
            status_code=404,
            detail=f"{upper} is not in the configured universe: {', '.join(runtime.data.symbols)}",
        )
    return upper


# --------------------------------------------------------------------- system


@router.get("/health")
def health(request: Request) -> dict[str, Any]:
    return runtime_of(request).health()


@router.get("/config")
def config(request: Request) -> dict[str, Any]:
    runtime = runtime_of(request)
    return {
        "config": runtime.config.public_dict(),
        "settable": settable_report(runtime.config),
        "symbols": list(describe_symbols(runtime.config)),
        "note": (
            "Only the values under 'settable' may be changed at runtime, and each is "
            "bounded server-side. Everything else requires a restart with a new config file."
        ),
    }


@router.post("/config/settings")
def update_settings(request: Request, payload: dict[str, Any]) -> dict[str, Any]:
    """Apply runtime settings.  Out-of-bounds values are refused, not clamped."""
    runtime = runtime_of(request)
    applied: dict[str, Any] = {}
    errors: dict[str, str] = {}
    for path, value in (payload or {}).items():
        try:
            applied[path] = apply_setting(runtime.config, path, value)
        except ConfigError as exc:
            errors[path] = str(exc)
    if applied:
        runtime.repos.system.log(
            "api", "settings_changed", f"{len(applied)} setting(s) updated", detail=applied
        )
    if errors and not applied:
        raise HTTPException(status_code=400, detail=errors)
    return {"applied": applied, "rejected": errors, "settable": settable_report(runtime.config)}


# --------------------------------------------------------------------- market


@router.get("/market")
def market(request: Request) -> dict[str, Any]:
    runtime = runtime_of(request)
    return {
        "venue": runtime.config.market.venue,
        "feed": runtime.config.market.feed,
        "live": runtime.feed.kind == "live",
        "state": runtime.feed.state.value,
        "symbols": runtime.data.market_summary(),
        "clock": runtime.clock.to_dict(),
        "endpoint_check": runtime.data.endpoint_check,
    }


@router.get("/markets")
def markets(request: Request) -> dict[str, Any]:
    runtime = runtime_of(request)
    return {"count": len(runtime.data.symbols), "symbols": runtime.data.market_summary()}


@router.get("/market/{symbol}")
def market_symbol(request: Request, symbol: str) -> dict[str, Any]:
    runtime = runtime_of(request)
    name = _known_symbol(runtime, symbol)
    state = runtime.data.states[name]
    snapshot = runtime.features.snapshot(name)
    quality = runtime.data.quality(name)
    return {
        "symbol": name,
        "book": state.book.to_dict(),
        "quality": quality.to_dict() if quality else None,
        "features": snapshot.to_dict() if snapshot else None,
        "regime": snapshot.regime.value if snapshot else "UNKNOWN",
        "mark_price": state.mark_price,
        "index_price": state.index_price,
        "funding_rate": state.funding_rate,
        "next_funding_ms": state.next_funding_ms,
        "latency": state.latency.to_dict(),
        "recent_trades": [
            {"ts_ms": t.ts_ms, "price": t.price, "qty": t.qty, "aggressor": t.aggressor.value}
            for t in list(state.trades)[-50:]
        ],
        "history": {
            "ts_ms": list(runtime.features.history[name].ts)[-300:],
            "mid": list(runtime.features.history[name].mid)[-300:],
        }
        if name in runtime.features.history
        else {},
    }


@router.get("/orderbook/{symbol}")
def orderbook(request: Request, symbol: str, depth: int = Query(20, ge=1, le=100)) -> dict[str, Any]:
    runtime = runtime_of(request)
    name = _known_symbol(runtime, symbol)
    state = runtime.data.states[name]
    view = state.book.top(depth)
    return {
        "symbol": name,
        "state": state.book.state.value,
        "ready": state.book.ready,
        "ts_ms": view.ts_ms,
        "last_update_id": view.last_update_id,
        "is_crossed": view.is_crossed,
        "mid": view.mid,
        "microprice": view.microprice(),
        "spread_bps": view.spread_bps(),
        "bids": [[level.price, level.qty] for level in view.bids],
        "asks": [[level.price, level.qty] for level in view.asks],
        "stats": state.book.stats.to_dict(),
    }


@router.get("/features/{symbol}")
def features(request: Request, symbol: str, history: int = Query(0, ge=0, le=500)) -> dict[str, Any]:
    runtime = runtime_of(request)
    name = _known_symbol(runtime, symbol)
    snapshot = runtime.features.snapshot(name)
    if snapshot is None:
        return {
            "symbol": name,
            "available": False,
            "reason": "no feature snapshot yet: the book is not ready or history is too short",
            "book_state": runtime.data.states[name].book.state.value,
        }
    payload: dict[str, Any] = {"symbol": name, "available": True, **snapshot.to_dict()}
    if history:
        payload["history"] = [s.to_dict() for s in list(runtime.features.snapshots[name])[-history:]]
    return payload


@router.get("/data-quality")
def data_quality(request: Request) -> dict[str, Any]:
    runtime = runtime_of(request)
    return {
        "summary": runtime.data.gate.summary(runtime.data.qualities),
        "thresholds": runtime.config.quality.model_dump(),
        "symbols": [q.to_dict() for q in runtime.data.qualities.values()],
    }


@router.get("/cross-market")
def cross_market(request: Request) -> dict[str, Any]:
    runtime = runtime_of(request)
    return {
        "matrix": runtime.cross_market.matrix(),
        "ranked": runtime.cross_market.ranked(limit=25),
        "stats": runtime.cross_market.stats(),
    }


# ------------------------------------------------------------------- research


@router.get("/agents")
def agents(request: Request) -> dict[str, Any]:
    runtime = runtime_of(request)
    states = runtime.director.agent_states()
    states.append(runtime.cycles.agent.state())
    return {
        "count": len(states),
        "agents": states,
        "recent_events": runtime.repos.research.recent_agent_events(limit=40),
    }


@router.get("/agents/{name}")
def agent(request: Request, name: str) -> dict[str, Any]:
    runtime = runtime_of(request)
    for state in runtime.director.agent_states():
        if state["name"] == name:
            return {**state, "recent_events": runtime.repos.research.recent_agent_events(name, limit=50)}
    if runtime.cycles.agent.name == name:
        return {
            **runtime.cycles.agent.state(),
            "recent_events": runtime.repos.research.recent_agent_events(name, limit=50),
        }
    raise HTTPException(status_code=404, detail=f"no agent named {name!r}")


@router.get("/research")
def research(request: Request) -> dict[str, Any]:
    runtime = runtime_of(request)
    counts = runtime.repos.research.status_counts()
    return {
        "director": runtime.director.state(),
        "hypothesis_status_counts": counts,
        "memory": runtime.director.memory.stats(),
        "memory_worst": runtime.director.memory.worst(limit=10),
        "validation": runtime.director.lab.stats(),
        "recent_cycles": [r.to_dict() for r in runtime.director.reports[-10:]],
        "recent_experiments": runtime.repos.research.list_experiments(limit=25),
    }


@router.get("/hypotheses")
def hypotheses(
    request: Request,
    status: str | None = None,
    agent: str | None = None,
    limit: int = Query(100, ge=1, le=1000),
) -> dict[str, Any]:
    runtime = runtime_of(request)
    rows = runtime.repos.research.list_hypotheses(status=status, agent=agent, limit=limit)
    return {
        "count": len(rows),
        "status_counts": runtime.repos.research.status_counts(),
        "hypotheses": rows,
    }


@router.get("/hypotheses/{hypothesis_id}")
def hypothesis(request: Request, hypothesis_id: str) -> dict[str, Any]:
    runtime = runtime_of(request)
    row = runtime.repos.research.get_hypothesis(hypothesis_id)
    if row is None:
        raise HTTPException(status_code=404, detail=f"no hypothesis {hypothesis_id!r}")
    return {"hypothesis": row, "experiments": runtime.repos.research.list_experiments(hypothesis_id)}


# ----------------------------------------------------------------- strategies


@router.get("/strategies")
def strategies(request: Request, state: str | None = None) -> dict[str, Any]:
    runtime = runtime_of(request)
    pool = runtime.lifecycle.ranked()
    if state:
        try:
            wanted = StrategyState(state.upper())
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=f"unknown state {state!r}") from exc
        pool = [s for s in pool if s.state is wanted]
    return {
        "counts": runtime.lifecycle.counts(),
        "transitions": runtime.lifecycle.transitions,
        "strategies": [s.to_dict() for s in pool],
    }


@router.get("/strategies/{strategy_id}")
def strategy(request: Request, strategy_id: str) -> dict[str, Any]:
    runtime = runtime_of(request)
    found = runtime.lifecycle.strategies.get(strategy_id)
    if found is None:
        raise HTTPException(status_code=404, detail=f"no strategy {strategy_id!r}")
    return {
        "strategy": found.to_dict(),
        "versions": runtime.repos.strategies.versions(strategy_id),
        "trades": runtime.repos.execution.list_trades(strategy_id=strategy_id, limit=50),
        "experiments": runtime.repos.research.list_experiments(found.hypothesis.hypothesis_id),
    }


@router.get("/champion")
def champion(request: Request) -> dict[str, Any]:
    runtime = runtime_of(request)
    current = runtime.lifecycle.champion()
    if current is None:
        return {
            "champion": None,
            "state": "NO_VALIDATED_EDGE",
            "reason": runtime.director.no_edge_reason,
            "shadows_in_evaluation": len(runtime.lifecycle.shadows()),
            "note": (
                "No champion is a valid state. The system does not promote a strategy to "
                "produce activity."
            ),
        }
    return {
        "champion": current.to_dict(),
        "state": "ACTIVE",
        "versions": runtime.repos.strategies.versions(current.strategy_id, limit=20),
        "trades": runtime.repos.execution.list_trades(strategy_id=current.strategy_id, limit=25),
    }


@router.get("/challengers")
def challengers(request: Request) -> dict[str, Any]:
    runtime = runtime_of(request)
    ranked = runtime.lifecycle.ranked(
        [StrategyState.CHALLENGER, StrategyState.SHADOW, StrategyState.CANDIDATE]
    )
    current = runtime.lifecycle.champion()
    return {
        "count": len(ranked),
        "champion_score": current.score if current else None,
        "replace_margin": runtime.config.validation.champion_replace_margin,
        "challengers": [s.to_dict() for s in ranked],
    }


# ------------------------------------------------------------------ execution


@router.get("/signals")
def signals(
    request: Request,
    symbol: str | None = None,
    accepted: bool | None = None,
    limit: int = Query(100, ge=1, le=1000),
) -> dict[str, Any]:
    runtime = runtime_of(request)
    if symbol:
        symbol = _known_symbol(runtime, symbol)
    rows = runtime.repos.execution.list_signals(symbol=symbol, accepted=accepted, limit=limit)
    window_ms = now_ms() - int(runtime.config.diagnostics.rejection_window_s * 1000)
    return {
        "count": len(rows),
        "totals": runtime.repos.execution.signal_totals(window_ms),
        "signals": rows,
    }


@router.get("/positions")
def positions(request: Request) -> dict[str, Any]:
    runtime = runtime_of(request)
    open_positions = runtime.broker.open_positions()
    return {
        "count": len(open_positions),
        "exposure_eur": round(runtime.broker.exposure_eur(), 4),
        "max_concurrent": runtime.config.risk.max_concurrent_positions,
        "positions": [p.to_dict() for p in open_positions.values()],
    }


@router.get("/trades")
def trades(
    request: Request,
    cycle_id: int | None = None,
    strategy_id: str | None = None,
    limit: int = Query(100, ge=1, le=1000),
) -> dict[str, Any]:
    runtime = runtime_of(request)
    rows = runtime.repos.execution.list_trades(cycle_id=cycle_id, strategy_id=strategy_id, limit=limit)
    return {"count": len(rows), "broker": runtime.broker.stats(), "trades": rows}


@router.get("/trades/{trade_id}")
def trade(request: Request, trade_id: str) -> dict[str, Any]:
    """The "WHY THIS TRADE" view: decision-time features, costs and lineage."""
    runtime = runtime_of(request)
    row = runtime.repos.execution.load_trade(trade_id)
    if row is None:
        raise HTTPException(status_code=404, detail=f"no trade {trade_id!r}")
    strategy = runtime.lifecycle.strategies.get(row.get("strategy_id") or "")
    hypothesis = runtime.repos.research.get_hypothesis(row.get("hypothesis_id") or "")
    signal_rows = runtime.repos.execution.list_signals(limit=1000)
    signal = next((s for s in signal_rows if s["signal_id"] == row.get("signal_id")), None)
    return {
        "trade": row,
        "why": {
            "hypothesis": hypothesis,
            "strategy": strategy.to_dict() if strategy else None,
            "signal": signal,
            "conditions_at_entry": [
                {
                    "condition": condition,
                    "value_at_entry": (row.get("features") or {}).get(condition.get("feature")),
                }
                for condition in ((hypothesis or {}).get("conditions") or [])
            ],
            "cost_breakdown": {
                "entry_slippage_bps": row.get("entry_slippage_bps"),
                "exit_slippage_bps": row.get("exit_slippage_bps"),
                "fees_eur": row.get("fees_eur"),
                "total_cost_bps": row.get("cost_bps"),
                "cost_model_version": row.get("cost_model_version"),
            },
            "outcome": {
                "expected_edge_bps": row.get("expected_edge_bps"),
                "realised_return_bps": row.get("return_bps"),
                "net_return_bps": row.get("net_return_bps"),
                "exit_reason": row.get("exit_reason"),
            },
        },
    }


# --------------------------------------------------------------------- wallet


@router.get("/wallet")
def wallet(request: Request) -> dict[str, Any]:
    runtime = runtime_of(request)
    cycle = runtime.cycles.cycle
    return {
        "wallet": runtime.wallet.to_dict(),
        "cycle": runtime.cycles.to_dict(),
        "ledger": runtime.repos.wallet.list_ledger(
            cycle_id=cycle.cycle_id if cycle else None, limit=100
        ),
        "equity_curve": runtime.repos.wallet.equity_curve(cycle.cycle_id, limit=500) if cycle else [],
        "risk": runtime.risk.circuit_breaker_state(),
    }


@router.get("/cycles")
def cycles(request: Request, limit: int = Query(50, ge=1, le=500)) -> dict[str, Any]:
    runtime = runtime_of(request)
    return {
        "current": runtime.cycles.to_dict(),
        "cycles": runtime.repos.wallet.list_cycles(limit=limit),
        "postmortems": runtime.repos.wallet.postmortems(limit=20),
    }


@router.get("/cycles/{cycle_id}")
def cycle_detail(request: Request, cycle_id: int) -> dict[str, Any]:
    runtime = runtime_of(request)
    rows = [c for c in runtime.repos.wallet.list_cycles(limit=500) if int(c["cycle_id"]) == cycle_id]
    if not rows:
        raise HTTPException(status_code=404, detail=f"no cycle {cycle_id}")
    return {
        "cycle": rows[0],
        "postmortems": runtime.repos.wallet.postmortems(cycle_id=cycle_id),
        "trades": runtime.repos.execution.list_trades(cycle_id=cycle_id, limit=500),
        "ledger": runtime.repos.wallet.list_ledger(cycle_id=cycle_id, limit=500),
        "equity_curve": runtime.repos.wallet.equity_curve(cycle_id, limit=1000),
    }


# ---------------------------------------------------------------- statistics


@router.get("/statistics")
def statistics(request: Request) -> dict[str, Any]:
    runtime = runtime_of(request)
    window_ms = now_ms() - int(runtime.config.diagnostics.rejection_window_s * 1000)
    return {
        "broker": runtime.broker.stats(),
        "wallet": runtime.wallet.to_dict(),
        "signals": runtime.repos.execution.signal_totals(window_ms),
        "research": {
            "cycles_run": runtime.director.cycles_run,
            "hypotheses": len(runtime.director.hypotheses),
            "validated": runtime.director.lab.validated,
            "rejected": runtime.director.lab.rejected,
            "rejections_by_stage": runtime.director.lab.stats()["rejections_by_stage"],
            "memory": runtime.director.memory.stats(),
        },
        "strategies": runtime.lifecycle.counts(),
        "features": runtime.features.stats(),
        "cross_market": runtime.cross_market.stats(),
        "market": {
            "events_processed": runtime.data.processed,
            "symbols_live": len(runtime.data.connected_symbols()),
        },
        "database": runtime.db.table_counts(),
    }


@router.get("/diagnostics")
def diagnostics(request: Request) -> dict[str, Any]:
    """Why did nothing trade?  Counts, percentages and what each gate means."""
    runtime = runtime_of(request)
    window_ms = now_ms() - int(runtime.config.diagnostics.rejection_window_s * 1000)
    report = runtime.diagnostics.report()
    return {
        "summary": runtime.diagnostics.summary_sentence(),
        "status": runtime.health()["status"],
        "no_edge_reason": runtime.director.no_edge_reason,
        "cycle_blocked_reason": runtime.cycles.blocked_reason,
        "risk_block_reason": runtime.risk.block_reason,
        **report,
        "persisted_rejections": runtime.repos.execution.rejection_counts(window_ms),
        "risk_rejections": dict(runtime.risk.rejections),
        "gate_catalogue": reason_catalogue(),
        "data_quality": runtime.data.gate.summary(runtime.data.qualities),
        "warmup": {
            "warmed_up": runtime.warmed_up(),
            "features_ready": runtime.features.warmed_up(),
            "books_ready": runtime.data.warmed_up(),
            "min_warmup_s": runtime.config.research.min_warmup_s,
        },
        "recent_errors": runtime.errors[-10:],
        "system_errors": runtime.repos.system.recent_errors(limit=10),
    }
