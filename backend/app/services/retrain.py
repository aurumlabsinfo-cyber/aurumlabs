"""Periodic walk-forward retraining.

The engine does not learn from a trade the moment it settles, and it should
not: updating on the last outcome is how a system ends up chasing noise with
growing confidence. Learning here means re-running the whole validated
pipeline on everything recorded so far, on a schedule.

The loop:

    collect  ->  walk-forward validate  ->  classify the edge  ->  activate

A model is only saved when its edge classifies as PROVEN or PROMISING, which
`run_backtest(save_best=True)` already enforces, and only a saved model is ever
activated. A run that concludes NO ROBUST EDGE leaves the live engine exactly
as it was - the previous model stays, or none does.

Note on cost: fitting blocks the thread it runs on, the same as the manual
POST /backtest path. It is deliberately infrequent, and the market feed is
resilient to a stalled consumer - the bus drops its oldest messages rather
than applying back-pressure to the socket.
"""

from __future__ import annotations

import asyncio
from typing import Any

from app.config import Settings
from app.core.clock import now_ms
from app.core.logging_conf import get_logger
from app.db import repository as repo
from app.ml.runner import dataset_readiness, run_backtest

log = get_logger(__name__)


class RetrainService:
    def __init__(self, settings: Settings, model_provider: Any) -> None:
        self.settings = settings
        self.model_provider = model_provider
        self._task: asyncio.Task | None = None
        self._running = False
        self.last_run_ts: int | None = None
        self.last_result: dict[str, Any] | None = None
        self.runs = 0
        self.activations = 0
        self.last_error: str | None = None

    async def start(self) -> None:
        if not self.settings.auto_retrain_enabled:
            log.info("retrain.disabled")
            return
        self._running = True
        self._task = asyncio.create_task(self._loop(), name="auto-retrain")

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

    async def _loop(self) -> None:
        # Never retrain immediately on boot: there is nothing new to learn from
        # in the first seconds, and a fit competing with warm-up helps nothing.
        await asyncio.sleep(self.settings.retrain_initial_delay_s)
        while self._running:
            try:
                await self.run_once()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                self.last_error = f"{type(exc).__name__}: {exc}"
                log.warning("retrain.failed", error=str(exc))
            await asyncio.sleep(self.settings.retrain_interval_s)

    async def run_once(self) -> dict[str, Any]:
        """One collect -> validate -> maybe activate cycle."""
        s = self.settings
        try:
            counts = await repo.table_counts()
        except Exception as exc:  # noqa: BLE001 - a DB outage is not a crash
            # The service used to be started only when the database answered at
            # boot, so a database that came up thirty seconds late meant the
            # engine never learned again until it was restarted. Now the loop
            # keeps running and simply reports why this cycle did nothing.
            self.last_run_ts = now_ms()
            self.last_error = f"{type(exc).__name__}: {exc}"
            self.last_result = {
                "skipped": "database unavailable",
                "error": self.last_error,
            }
            return self.last_result
        readiness = dataset_readiness(
            counts.get("features", 0), counts.get("market_ticks", 0), s.ml_min_samples
        )
        if not readiness.get("ready"):
            self.last_run_ts = now_ms()
            self.last_result = {"skipped": "insufficient data", "readiness": readiness}
            return self.last_result

        report = await run_backtest(
            s,
            horizons=[s.signal_horizon_s],
            n_splits=s.retrain_splits,
            include_synthetic=False,  # never learn from the simulator
            save_best=True,
        )
        self.runs += 1
        self.last_run_ts = now_ms()

        # The runner keys horizons as f"{h:g}s" - "900s", not "900.0". Getting
        # this wrong fails silently: no horizon found, so nothing is ever
        # activated and the loop looks like it is simply never finding an edge.
        horizon = report.get("horizons", {}).get(f"{s.signal_horizon_s:g}s") or {}
        saved = horizon.get("saved_model") or {}
        model_id = saved.get("model_id")
        edge = (horizon.get("edge") or {}).get("classification")

        activated = False
        if model_id and self.model_provider is not None:
            # `save_best` only writes a model whose edge cleared PROVEN or
            # PROMISING, so reaching here means the verdict held.
            if self.model_provider.load(model_id):
                await repo.set_active_model(model_id)
                activated = True
                self.activations += 1
                log.info("retrain.activated", model_id=model_id, edge=edge)
            else:
                self.last_error = (
                    f"trained {model_id} but could not load it: "
                    f"{self.model_provider.load_error}"
                )

        self.last_result = {
            "ran_at": self.last_run_ts,
            "edge": edge,
            "conclusion": report.get("conclusion"),
            "model_id": model_id,
            "activated": activated,
            "rows": horizon.get("rows"),
            "note": (
                "No model was activated: the run did not clear the edge "
                "classification, so the live engine is unchanged."
                if not activated
                else "A validated model replaced the previous one."
            ),
        }
        return self.last_result

    def status(self) -> dict[str, Any]:
        return {
            "enabled": self.settings.auto_retrain_enabled,
            "interval_s": self.settings.retrain_interval_s,
            "runs": self.runs,
            "activations": self.activations,
            "last_run_ts": self.last_run_ts,
            "last_result": self.last_result,
            "last_error": self.last_error,
            "policy": (
                "Retrains on every recorded window, not only on signalled "
                "trades, and activates only a model whose walk-forward edge "
                "classifies as PROVEN or PROMISING."
            ),
        }
