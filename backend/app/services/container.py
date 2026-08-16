"""Composition root: builds and owns every long-lived service."""

from __future__ import annotations

from typing import Any

from app.config import Settings, get_settings
from app.core.bus import EventBus
from app.core.clock import now_ms
from app.core.logging_conf import get_logger
from app.db.engine import create_schema, dispose_engine, init_engine, ping
from app.db.repository import BatchWriter
from app.features.engine import FeatureEngine
from app.marketdata.engine import MarketDataEngine
from app.ml.inference import ModelProvider
from app.services.persistence import PersistenceService
from app.services.retrain import RetrainService
from app.signals.decision import DecisionEngine
from app.signals.lifecycle import SignalEngine

log = get_logger(__name__)


class Services:
    """Everything the API talks to."""

    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or get_settings()
        self.bus = EventBus(maxsize=512)
        self.started_at = now_ms()
        self.db_ready = False
        self.startup_errors: list[str] = []

        self.writer = BatchWriter(self.settings)
        self.market = MarketDataEngine(self.settings, self.bus)
        self.features = FeatureEngine(self.settings, self.bus, self.market)
        self.model_provider = ModelProvider(
            self.settings.model_dir, self.settings.active_model_id
        )
        self.decisions = DecisionEngine(
            self.settings,
            model_provider=self.model_provider,
            performance_provider=self.agent_hit_rates,
        )
        self.signals = SignalEngine(
            self.settings, self.bus, self.market, self.features, self.decisions,
            self.writer,
        )
        self.persistence = PersistenceService(
            self.settings, self.bus, self.writer, self.market,
            counters_provider=lambda: self.signals.counters,
        )
        self.retrain = RetrainService(self.settings, self.model_provider)
        self._started = False

    # ------------------------------------------------------------- lifecycle
    async def start(self) -> None:
        if self._started:
            return
        init_engine(self.settings)
        try:
            await create_schema()
            self.db_ready = await ping()
        except Exception as exc:  # noqa: BLE001 - trading must survive a DB outage
            self.db_ready = False
            msg = f"database unavailable: {type(exc).__name__}: {exc}"
            self.startup_errors.append(msg)
            log.error("startup.db_failed", error=msg)

        await self.writer.start()
        await self.persistence.start()
        await self.features.start()
        await self.signals.start()
        await self.market.start()
        # Retraining reads only what has already been recorded, so it is
        # started last and never gates the feed coming up. It is started even
        # when the database is down right now: the loop re-checks on every
        # cycle, so a database that comes up late no longer costs the engine
        # its ability to learn until the next restart.
        await self.retrain.start()
        self._started = True
        log.info(
            "services.started",
            symbol=self.settings.symbol,
            exchanges=self.settings.exchange_list,
            synthetic=self.market.is_synthetic,
            db_ready=self.db_ready,
        )

    async def stop(self) -> None:
        if not self._started:
            return
        await self.retrain.stop()
        await self.market.stop()
        await self.signals.stop()
        await self.features.stop()
        await self.persistence.stop()
        await self.writer.stop()
        await dispose_engine()
        self._started = False

    # -------------------------------------------------------------- helpers
    def agent_hit_rates(self) -> dict[str, float]:
        """Per-agent hit rate over recent settled signals (in-memory).

        Feeds back into the ensemble weights. Only settled WIN/LOSS signals
        count, and an agent that abstained is not scored.
        """
        settled = [
            s for s in self.signals.history[-300:]
            if s.result in ("WIN", "LOSS") and s.agents
        ]
        if len(settled) < 20:
            return {}
        tally: dict[str, list[int]] = {}
        for sig in settled:
            actual_up = (
                sig.expiry_price is not None
                and sig.entry_price is not None
                and sig.expiry_price > sig.entry_price
            )
            for name, out in sig.agents.items():
                direction = out.get("direction")
                if direction not in ("UP", "DOWN"):
                    continue
                correct = int((direction == "UP") == actual_up)
                tally.setdefault(name, []).append(correct)
        return {
            name: sum(v) / len(v)
            for name, v in tally.items()
            if len(v) >= 10
        }

    async def health(self) -> dict[str, Any]:
        market = self.market.health()
        db_ok = await ping() if self.db_ready else False
        components = {
            "websocket": {
                "status": (
                    "UP" if any(
                        a["connected"] for a in market["adapters"].values()
                    ) else "DOWN"
                ),
                "detail": {k: v["connected"] for k, v in market["adapters"].items()},
            },
            "api": {"status": "UP"},
            "database": {
                "status": "UP" if db_ok else "DOWN",
                "detail": self.writer.health(),
            },
            "market_data": {
                "status": "UP" if (market["feed_age_ms"] or 1e9) < 5000 else "DOWN",
                "detail": {
                    "feed_age_ms": market["feed_age_ms"],
                    "counters": market["counters"],
                },
            },
            "order_book": {
                "status": "UP" if market["orderbook"]["synced"] else "DOWN",
                "detail": market["orderbook"],
            },
            "model": {
                "status": "UP" if self.model_provider.is_ready() else "DISABLED",
                "detail": self.model_provider.info(),
            },
            "latency": {
                "status": (
                    "UP"
                    if (market["tick_latency_ms"]["p95"] or 0)
                    <= self.settings.max_latency_ms
                    else "DEGRADED"
                ),
                "detail": market["tick_latency_ms"],
            },
            "error_rate": {
                "status": "UP" if len(self.market.errors) < 20 else "DEGRADED",
                "detail": {"recent_errors": len(self.market.errors)},
            },
        }
        overall = (
            "HEALTHY"
            if all(c["status"] in ("UP", "DISABLED") for c in components.values())
            else "DEGRADED"
        )
        return {
            "status": overall,
            "uptime_s": round((now_ms() - self.started_at) / 1000, 1),
            "server_ts": now_ms(),
            "symbol": self.settings.symbol,
            "source": self.market.source.value,
            "is_synthetic": self.market.is_synthetic,
            "synthetic_warning": (
                "SYNTHETIC DATA - this instance is running the simulator, not a "
                "live exchange feed. Nothing here describes the real market."
                if self.market.is_synthetic else None
            ),
            "components": components,
            "market": market,
            "signals": self.signals.counters,
            "bus": self.bus.stats(),
            "startup_errors": self.startup_errors,
        }


_services: Services | None = None


def get_services() -> Services:
    if _services is None:
        raise RuntimeError("services not initialised")
    return _services


def set_services(services: Services | None) -> None:
    global _services
    _services = services
