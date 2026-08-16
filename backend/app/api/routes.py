"""REST API.

Read endpoints are public (the underlying market data is public). Endpoints that
change engine state require the admin key.
"""

from __future__ import annotations

import asyncio
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from app.api.security import require_admin
from app.core.clock import now_ms
from app.db import repository as repo
from app.ml import montecarlo
from app.ml.runner import DEFAULT_HORIZONS, dataset_readiness, run_backtest
from app.services.container import get_services
from app.signals import statistics as stats

router = APIRouter()

# In-flight backtest, so a long run does not block the event loop or get
# launched twice by an impatient click.
_backtest_task: asyncio.Task | None = None
_backtest_result: dict[str, Any] | None = None


def _synthetic_banner() -> dict[str, Any]:
    svc = get_services()
    return {
        "is_synthetic": svc.market.is_synthetic,
        "source": svc.market.source.value,
        "warning": (
            "SYNTHETIC DATA - simulator output, not market data"
            if svc.market.is_synthetic else None
        ),
    }


# ----------------------------------------------------------------- health
@router.get("/health", tags=["system"])
async def health() -> dict[str, Any]:
    return await get_services().health()


@router.get("/health/live", tags=["system"])
async def liveness() -> dict[str, Any]:
    return {"status": "alive", "server_ts": now_ms()}


@router.get("/health/ready", tags=["system"])
async def readiness() -> dict[str, Any]:
    svc = get_services()
    quality = svc.market.data_quality()
    ready = bool(svc.market.last_tick) and quality["score"] > 0
    return {
        "ready": ready,
        "data_quality": quality,
        "book_synced": svc.market.book.synced,
        "server_ts": now_ms(),
    }


# ----------------------------------------------------------------- market
@router.get("/market", tags=["market"])
async def market() -> dict[str, Any]:
    svc = get_services()
    return {
        **svc.market.market_snapshot(),
        "server_ts": now_ms(),
        "data_quality": svc.market.data_quality(),
        **_synthetic_banner(),
    }


@router.get("/orderbook", tags=["market"])
async def orderbook(levels: int = Query(20, ge=1, le=200)) -> dict[str, Any]:
    svc = get_services()
    book = svc.market.book
    return {
        **book.snapshot_dict(levels=levels),
        "stats": {
            "applied_updates": book.stats.applied_updates,
            "gaps_detected": book.stats.gaps_detected,
            "resyncs": book.stats.resyncs,
            "dropped_stale": book.stats.dropped_stale,
        },
        **_synthetic_banner(),
    }


@router.get("/trades", tags=["market"])
async def trades(limit: int = Query(50, ge=1, le=500)) -> dict[str, Any]:
    svc = get_services()
    recent = list(svc.market.recent_trades)[-limit:][::-1]
    return {
        "count": len(recent),
        "trades": [
            {
                "ts": t.server_ts,
                "exchange_ts": t.exchange_ts,
                "latency_ms": t.latency_ms,
                "price": t.price,
                "quantity": t.quantity,
                "notional": t.notional,
                "side": t.aggressor.value,
                "trade_id": t.trade_id,
            }
            for t in recent
        ],
        **_synthetic_banner(),
    }


@router.get("/features", tags=["market"])
async def features(history: int = Query(0, ge=0, le=500)) -> dict[str, Any]:
    svc = get_services()
    out: dict[str, Any] = {
        "latest": svc.features.latest,
        "computed": svc.features.computed,
        "interval_ms": svc.settings.feature_interval_ms,
        **_synthetic_banner(),
    }
    if history:
        out["history"] = await repo.fetch_recent_features(
            limit=history, symbol=svc.settings.symbol
        )
    return out


@router.get("/agents", tags=["signals"])
async def agents() -> dict[str, Any]:
    svc = get_services()
    decision = svc.signals.last_decision
    if decision is None:
        return {"agents": [], "note": "no decision computed yet", **_synthetic_banner()}
    return {
        "ts": decision.ts,
        "regime": decision.regime.value,
        "agents": [a.to_dict() for a in decision.agents],
        "prob_up": round(decision.prob_up, 4),
        "prob_down": round(decision.prob_down, 4),
        "prob_neutral": round(decision.prob_neutral, 4),
        "aggregate_score": round(decision.aggregate_score, 4),
        "no_trade_reasons": decision.no_trade_reasons,
        "detail": decision.detail,
        **_synthetic_banner(),
    }


# ----------------------------------------------------------------- signals
@router.get("/signals", tags=["signals"])
async def signals(limit: int = Query(50, ge=1, le=500)) -> dict[str, Any]:
    svc = get_services()
    return {
        **svc.signals.snapshot(),
        "persisted": await repo.fetch_signals(limit=limit, symbol=svc.settings.symbol),
        **_synthetic_banner(),
    }


@router.get("/signals/current", tags=["signals"])
async def current_signal() -> dict[str, Any]:
    svc = get_services()
    return {
        "server_ts": now_ms(),
        "signal": svc.signals.current_signal(),
        "market": svc.market.market_snapshot(),
        "no_trade_reasons": (
            svc.signals.last_decision.no_trade_reasons
            if svc.signals.last_decision else []
        ),
        **_synthetic_banner(),
    }


@router.get("/paper-trading", tags=["paper"])
async def paper_trading(
    limit: int = Query(100, ge=1, le=1000),
    result: str | None = Query(None, pattern="^(WIN|LOSS|TIE|CANCELLED)$"),
) -> dict[str, Any]:
    svc = get_services()
    trades_ = await repo.fetch_paper_trades(
        limit=limit, symbol=svc.settings.symbol, result=result
    )
    return {
        "mode": "PAPER TRADING ONLY - no order is ever sent to any venue",
        "count": len(trades_),
        "trades": trades_,
        "live": svc.signals.snapshot(),
        **_synthetic_banner(),
    }


@router.get("/statistics", tags=["paper"])
async def statistics(
    include_synthetic: bool = Query(False),
    window_hours: int | None = Query(None, ge=1, le=24 * 30),
) -> dict[str, Any]:
    svc = get_services()
    since = now_ms() - window_hours * 3_600_000 if window_hours else None
    trades_ = await repo.fetch_all_paper_trades(
        include_synthetic=include_synthetic, since_ts=since
    )
    report = stats.full_report(
        trades_,
        payout=svc.settings.binary_payout,
        stake=svc.settings.paper_stake,
        no_trade_count=svc.signals.counters.get("no_trade"),
    )
    report["window_hours"] = window_hours
    report["include_synthetic"] = include_synthetic
    report["counters"] = svc.signals.counters
    report["agent_hit_rates"] = svc.agent_hit_rates()
    return report


@router.get("/statistics/calibration", tags=["paper"])
async def calibration(include_synthetic: bool = Query(False)) -> dict[str, Any]:
    trades_ = await repo.fetch_all_paper_trades(include_synthetic=include_synthetic)
    return stats.calibration(trades_)


@router.get("/statistics/montecarlo", tags=["paper"])
async def monte_carlo(
    simulations: int = Query(10_000, ge=100, le=100_000),
    bankroll_units: float = Query(20.0, gt=0),
    include_synthetic: bool = Query(False),
) -> dict[str, Any]:
    svc = get_services()
    trades_ = await repo.fetch_all_paper_trades(include_synthetic=include_synthetic)
    return montecarlo.from_trades(
        [t.get("result") or "" for t in trades_],
        payout=svc.settings.binary_payout,
        n_simulations=simulations,
        stake=svc.settings.paper_stake,
        starting_bankroll=bankroll_units,
    )


# ---------------------------------------------------------------- backtest
class BacktestRequest(BaseModel):
    horizons: list[float] = Field(default_factory=lambda: list(DEFAULT_HORIZONS))
    models: list[str] | None = None
    n_splits: int = Field(default=5, ge=2, le=20)
    include_synthetic: bool = False
    save_best: bool = False


@router.get("/backtest", tags=["research"])
async def backtest_status() -> dict[str, Any]:
    svc = get_services()
    counts = await repo.table_counts() if svc.db_ready else {}
    running = _backtest_task is not None and not _backtest_task.done()
    return {
        "running": running,
        "readiness": dataset_readiness(
            counts.get("features", 0), counts.get("market_ticks", 0),
            svc.settings.ml_min_samples,
        ),
        "table_counts": counts,
        "last_result": _backtest_result,
        "how_to_run": "POST /backtest with the admin API key",
    }


#: Chart intervals offered to the dashboard, in seconds.
CANDLE_INTERVALS: dict[str, int] = {
    "1s": 1, "5s": 5, "15s": 15, "1m": 60, "5m": 300, "10m": 600, "1h": 3600,
}


@router.get("/candles", tags=["market"])
async def candles(
    interval: str = Query(default="1m"),
    limit: int = Query(default=300, ge=10, le=1500),
    include_synthetic: bool = False,
) -> dict[str, Any]:
    """OHLC bars aggregated from the recorded mid-price ticks.

    The live chart builds its own bars from the WebSocket stream, which can
    only ever show the session it has been open for. History has to come from
    the database, and it is the same tick series the decision engine consumed -
    not a second feed that could disagree with it.
    """
    if interval not in CANDLE_INTERVALS:
        raise HTTPException(
            status_code=400,
            detail=f"unknown interval; use one of {sorted(CANDLE_INTERVALS)}",
        )
    svc = get_services()
    if not svc.db_ready:
        raise HTTPException(status_code=503, detail="database unavailable")
    bars = await repo.fetch_candles(
        symbol=svc.settings.symbol,
        bucket_s=CANDLE_INTERVALS[interval],
        limit=limit,
        include_synthetic=include_synthetic,
    )
    return {
        "symbol": svc.settings.symbol,
        "interval": interval,
        "bucket_seconds": CANDLE_INTERVALS[interval],
        "count": len(bars),
        "candles": bars,
        "note": (
            "Aggregated from recorded ticks: a bar only covers the time this "
            "engine was running. Gaps are real downtime, not missing data."
        ),
    }


@router.get("/retrain", tags=["research"])
async def retrain_status() -> dict[str, Any]:
    return get_services().retrain.status()


@router.post("/retrain", tags=["research"], dependencies=[Depends(require_admin)])
async def retrain_now() -> dict[str, Any]:
    """Run one collect -> validate -> maybe-activate cycle immediately.

    Activation still requires the walk-forward verdict to hold: this forces the
    schedule, not the outcome.
    """
    return await get_services().retrain.run_once()


@router.get("/shadow", tags=["research"])
async def shadow_report(include_synthetic: bool = False) -> dict[str, Any]:
    """Accuracy of the engine's lean on every window, not only signalled ones.

    Read-only research: it resolves outcomes from recorded ticks and never
    touches an exchange.
    """
    from app.ml.shadow import evaluate, load_shadow

    svc = get_services()
    if not svc.db_ready:
        raise HTTPException(status_code=503, detail="database unavailable")
    shadow, ticks = await load_shadow(
        svc.settings.symbol, include_synthetic=include_synthetic
    )
    if shadow.empty:
        return {
            "status": "NO DATA",
            "note": (
                "no shadow windows recorded yet - the engine records one per "
                f"{svc.settings.shadow_decision_interval_ms}ms while running."
            ),
        }
    return evaluate(shadow, ticks)


@router.post("/backtest", tags=["research"], dependencies=[Depends(require_admin)])
async def start_backtest(req: BacktestRequest) -> dict[str, Any]:
    global _backtest_task
    if _backtest_task is not None and not _backtest_task.done():
        raise HTTPException(status_code=409, detail="a backtest is already running")

    svc = get_services()

    async def _run() -> None:
        global _backtest_result
        _backtest_result = await run_backtest(
            svc.settings,
            horizons=req.horizons,
            models=req.models,
            n_splits=req.n_splits,
            include_synthetic=req.include_synthetic,
            save_best=req.save_best,
        )

    _backtest_task = asyncio.create_task(_run(), name="backtest")
    return {"started": True, "poll": "GET /backtest"}


# ------------------------------------------------------------------ models
@router.get("/models", tags=["research"])
async def models() -> dict[str, Any]:
    from app.ml.models import available

    svc = get_services()
    return {
        "available_algorithms": available(),
        "active": svc.model_provider.info(),
        "versions": await repo.list_model_versions() if svc.db_ready else [],
    }


class ActivateModel(BaseModel):
    model_id: str = Field(min_length=1, max_length=128)


@router.post("/models/activate", tags=["research"], dependencies=[Depends(require_admin)])
async def activate_model(req: ActivateModel) -> dict[str, Any]:
    svc = get_services()
    if not svc.model_provider.load(req.model_id):
        raise HTTPException(
            status_code=404,
            detail=f"model not loadable: {svc.model_provider.load_error}",
        )
    await repo.set_active_model(req.model_id)
    return {"activated": req.model_id, "info": svc.model_provider.info()}


@router.post("/models/deactivate", tags=["research"], dependencies=[Depends(require_admin)])
async def deactivate_model() -> dict[str, Any]:
    svc = get_services()
    svc.model_provider.unload()
    return {"active": None, "note": "decisions now use the rule ensemble only"}


# ---------------------------------------------------------------- settings
MUTABLE_SETTINGS = {
    "signal_enabled": bool,
    "signal_min_confidence": float,
    "signal_min_edge": float,
    "signal_cooldown_ms": int,
    "signal_max_concurrent": int,
    "signal_horizon_s": float,
    "signal_wait_timeout_s": float,
    "trigger_sigma_k": float,
    "trigger_max_bps": float,
    "max_spread_bps": float,
    "max_latency_ms": float,
    "min_data_quality": float,
    "binary_payout": float,
    "paper_stake": float,
}


@router.get("/settings", tags=["system"])
async def read_settings() -> dict[str, Any]:
    svc = get_services()
    s = svc.settings
    return {
        "runtime": {k: getattr(s, k) for k in MUTABLE_SETTINGS},
        "static": {
            "symbol": s.symbol,
            "exchanges": s.exchange_list,
            "feature_interval_ms": s.feature_interval_ms,
            "trigger_price_source": s.trigger_price_source,
            "tick_size": s.tick_size,
            "allow_synthetic_source": s.allow_synthetic_source,
            "model_dir": s.model_dir,
        },
        "mutable_keys": sorted(MUTABLE_SETTINGS),
        "note": (
            "Secrets are never exposed here. Static values are set through "
            "environment variables and require a restart."
        ),
        **_synthetic_banner(),
    }


class SettingsPatch(BaseModel):
    key: str
    value: Any


@router.patch("/settings", tags=["system"], dependencies=[Depends(require_admin)])
async def patch_settings(patch: SettingsPatch) -> dict[str, Any]:
    svc = get_services()
    if patch.key not in MUTABLE_SETTINGS:
        raise HTTPException(
            status_code=400,
            detail=f"'{patch.key}' is not runtime-mutable. "
                   f"Allowed: {sorted(MUTABLE_SETTINGS)}",
        )
    caster = MUTABLE_SETTINGS[patch.key]
    try:
        value = None if patch.value is None else caster(patch.value)
    except (TypeError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=f"invalid value: {exc}") from exc
    setattr(svc.settings, patch.key, value)
    return {"updated": {patch.key: value}}


@router.post("/admin/purge-synthetic", tags=["system"], dependencies=[Depends(require_admin)])
async def purge_synthetic() -> dict[str, Any]:
    deleted = await repo.purge_synthetic()
    return {
        "deleted": deleted,
        "note": "all simulator-produced rows removed; live data untouched",
    }


@router.get("/exchanges", tags=["market"])
async def exchanges() -> dict[str, Any]:
    from app.marketdata.registry import available_exchanges

    svc = get_services()
    return {
        "available": available_exchanges(),
        "configured": svc.settings.exchange_list,
        "primary": svc.market.primary_name,
        "capabilities": {
            name: sorted(c.value for c in a.capabilities)
            for name, a in svc.market.adapters.items()
        },
    }
