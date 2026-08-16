"""Signal lifecycle and paper-trading execution.

    ANALYSIS -> SIGNAL CREATED -> WAITING FOR TRIGGER -> TRIGGER HIT
             -> ACTIVE (horizon countdown) -> EXPIRED -> WIN / LOSS

Rules that matter:

* the **backend** owns the trigger. The browser only renders state.
* the countdown does **not** start when the signal is created. It starts when
  the live price touches the trigger, and `triggered_at` / `expires_at` are
  server timestamps broadcast to the client so its countdown cannot drift.
* nothing is executed on a real venue. Ever. This module writes paper trades.
"""

from __future__ import annotations

import asyncio
import re
from collections import Counter
from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any

from app.agents.base import Direction
from app.config import Settings
from app.core.bus import EventBus, Topic
from app.core.clock import now_ms
from app.core.logging_conf import get_logger
from app.db import repository as repo
from app.db.repository import BatchWriter
from app.marketdata.engine import MarketDataEngine
from app.signals.burst import BurstStrategy
from app.signals.decision import Decision, DecisionEngine, new_signal_id

log = get_logger(__name__)


class SignalStatus(str, Enum):
    WAITING = "WAITING"
    TRIGGERED = "TRIGGERED"
    ACTIVE = "ACTIVE"
    EXPIRED = "EXPIRED"
    WIN = "WIN"
    LOSS = "LOSS"
    TIE = "TIE"
    CANCELLED = "CANCELLED"


TERMINAL = {SignalStatus.WIN, SignalStatus.LOSS, SignalStatus.TIE,
            SignalStatus.CANCELLED}

#: Numbers inside a block reason are the measurement, not the category. They are
#: collapsed so the diagnostics counter answers "what keeps stopping us" rather
#: than listing ten thousand distinct one-off strings.
_NUMBERS = re.compile(r"[-+]?\d[\d_.,]*")


def gate_key(reason: str) -> str:
    """Normalise a no-trade reason into a countable category."""
    return _NUMBERS.sub("N", reason).strip()


@dataclass
class LiveSignal:
    signal_id: str
    symbol: str
    exchange: str
    direction: Direction
    status: SignalStatus
    reference_price: float
    trigger_price: float
    horizon_s: float
    confidence: float
    prob_up: float
    prob_down: float
    prob_neutral: float
    edge: float
    regime: str
    created_at: int
    expires_wait_at: int
    triggered_at: int | None = None
    expires_at: int | None = None
    settled_at: int | None = None
    entry_price: float | None = None
    expiry_price: float | None = None
    result: str | None = None
    pnl_units: float | None = None
    data_quality: float = 0.0
    model_id: str | None = None
    is_synthetic: bool = False
    features: dict[str, Any] = field(default_factory=dict)
    agents: dict[str, Any] = field(default_factory=dict)
    settle_delay_ms: int | None = None
    #: "TRIGGER" - wait for price to touch `trigger_price`.
    #: "DELAY"   - enter `entry_delay_ms` after creation, at the market.
    entry_mode: str = "TRIGGER"
    entry_delay_ms: int = 0
    strategy: str = "ensemble"

    def to_dict(self, server_ts: int | None = None) -> dict[str, Any]:
        d = asdict(self)
        d["direction"] = self.direction.value
        d["status"] = self.status.value
        ts = server_ts or now_ms()
        d["server_ts"] = ts
        # Backend-driven countdown: the client renders these, it does not invent
        # them. remaining_ms is authoritative at `server_ts`.
        if self.status in (SignalStatus.ACTIVE, SignalStatus.TRIGGERED) and self.expires_at:
            d["remaining_ms"] = max(0, self.expires_at - ts)
        elif self.status is SignalStatus.WAITING:
            d["remaining_ms"] = None
            d["wait_remaining_ms"] = max(0, self.expires_wait_at - ts)
        else:
            d["remaining_ms"] = 0
        return d


class SignalEngine:
    def __init__(
        self,
        settings: Settings,
        bus: EventBus,
        market: MarketDataEngine,
        features: Any,
        decision_engine: DecisionEngine,
        writer: BatchWriter,
    ) -> None:
        self.settings = settings
        self.bus = bus
        self.market = market
        self.features = features
        self.decisions = decision_engine
        self.writer = writer

        self.active: dict[str, LiveSignal] = {}
        self.history: list[LiveSignal] = []
        self.last_decision: Decision | None = None
        self.last_signal_ts: int = 0
        self._last_shadow_ts: int = 0
        self._last_no_trade_row_ts: int = 0
        self._last_no_trade_key: tuple[str, ...] = ()
        self.counters = {
            "decisions": 0, "signals": 0, "no_trade": 0, "triggered": 0,
            "expired": 0, "cancelled": 0, "wins": 0, "losses": 0, "ties": 0,
        }
        #: How often each gate blocked a decision, since start. This is the
        #: answer to "why am I not getting signals", and it is cheap: one
        #: counter increment per reason per evaluated window.
        self.gate_counter: Counter[str] = Counter()
        self.started_at = now_ms()
        #: BURST-15 owns its own session state; built regardless of the active
        #: strategy so /burst/session can report "not running" instead of 404.
        self.burst = BurstStrategy(settings)
        self._tasks: list[asyncio.Task] = []
        self._running = False

    @property
    def strategy_name(self) -> str:
        return self.settings.signal_strategy

    # ------------------------------------------------------------- lifecycle
    async def start(self) -> None:
        self._running = True
        self._tasks = [
            asyncio.create_task(self._decision_loop(), name="signal-decide"),
            asyncio.create_task(self._monitor_loop(), name="signal-monitor"),
        ]

    async def stop(self) -> None:
        self._running = False
        for t in self._tasks:
            t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()

    # ------------------------------------------------------------- decisions
    async def _decision_loop(self) -> None:
        sub = self.bus.subscribe(Topic.FEATURES, maxsize=64)
        try:
            while self._running:
                fv = await sub.queue.get()
                try:
                    self.evaluate(fv)
                except Exception as exc:  # noqa: BLE001
                    self.market.record_error(
                        "signal_engine", f"{type(exc).__name__}: {exc}"
                    )
        except asyncio.CancelledError:
            raise
        finally:
            sub.close()

    def evaluate(self, feature_vector: dict[str, Any]) -> Decision:
        market_state = self.market.market_snapshot()
        health = self.market.health()
        engine = (
            self.burst if self.strategy_name == "burst15" else self.decisions
        )
        decision = engine.decide(feature_vector, market_state, health)
        self.counters["decisions"] += 1
        self.last_decision = decision
        for reason in decision.no_trade_reasons:
            self.gate_counter[gate_key(reason)] += 1
        self._persist_agents(decision, signal_id=None)
        self.bus.publish(
            Topic.AGENTS,
            {
                "ts": decision.ts,
                "agents": [a.to_dict() for a in decision.agents],
                "regime": decision.regime.value,
                "prob_up": decision.prob_up,
                "prob_down": decision.prob_down,
                "prob_neutral": decision.prob_neutral,
                "no_trade_reasons": decision.no_trade_reasons,
            },
        )

        if not decision.is_trade:
            self.counters["no_trade"] += 1
            self._record_no_trade_row(decision)
            self._publish_shadow(
                decision, fv=feature_vector, emitted=False,
                blocked=decision.no_trade_reasons,
            )
            return decision

        if len(self.active) >= self.settings.signal_max_concurrent:
            self.gate_counter["max concurrent"] += 1
            self._publish_shadow(
                decision, fv=feature_vector, emitted=False, blocked=["max concurrent"]
            )
            return decision
        # BURST-15 runs its own cooldown per session; applying the ensemble's on
        # top would silently shorten or lengthen the strategy's own rule.
        if self.strategy_name != "burst15" and (
            decision.ts - self.last_signal_ts < self.settings.signal_cooldown_ms
        ):
            self.gate_counter["cooldown"] += 1
            self._publish_shadow(
                decision, fv=feature_vector, emitted=False, blocked=["cooldown"]
            )
            return decision

        self._publish_shadow(decision, fv=feature_vector, emitted=True, blocked=[])
        self._create_signal(decision, feature_vector)
        return decision

    def _publish_shadow(
        self,
        decision: Decision,
        *,
        fv: dict[str, Any],
        emitted: bool,
        blocked: list[str],
    ) -> None:
        """Record the lean on this window whether or not it became a signal.

        Throttled: the outcome at a minute-scale horizon does not change every
        100ms, and one row per second is already far more than the label
        resolution can use.
        """
        s = self.settings
        if not s.shadow_decisions_enabled:
            return
        if decision.ts - self._last_shadow_ts < s.shadow_decision_interval_ms:
            return
        self._last_shadow_ts = decision.ts
        self.bus.publish(
            Topic.DECISION,
            {
                "ts": decision.ts,
                "exchange": decision.exchange,
                "symbol": decision.symbol,
                "lean": decision.lean.value,
                "prob_up": decision.prob_up,
                "confidence": decision.lean_confidence,
                "edge": decision.edge,
                "horizon_s": decision.horizon_s,
                "reference_price": decision.reference_price,
                "market_regime": decision.regime.value,
                "emitted": emitted,
                "blocked_by": blocked,
                "data_quality": decision.data_quality,
                "model_id": decision.model_id,
                "source": fv.get("source", "LIVE"),
                "is_synthetic": bool(fv.get("is_synthetic", False)),
            },
        )

    def _create_signal(
        self, decision: Decision, feature_vector: dict[str, Any]
    ) -> LiveSignal:
        sid = new_signal_id()
        ts = now_ms()
        sig = LiveSignal(
            signal_id=sid,
            symbol=decision.symbol,
            exchange=decision.exchange,
            direction=decision.direction,
            status=SignalStatus.WAITING,
            reference_price=decision.reference_price,
            trigger_price=float(decision.trigger_price or 0.0),
            horizon_s=decision.horizon_s,
            confidence=decision.confidence,
            prob_up=decision.prob_up,
            prob_down=decision.prob_down,
            prob_neutral=decision.prob_neutral,
            edge=decision.edge,
            regime=decision.regime.value,
            created_at=ts,
            expires_wait_at=(
                ts + decision.entry_delay_ms
                if decision.entry_mode == "DELAY"
                else ts + int(self.settings.signal_wait_timeout_s * 1000)
            ),
            data_quality=decision.data_quality,
            model_id=decision.model_id,
            is_synthetic=self.market.is_synthetic,
            features=feature_vector.get("features", {}),
            agents={a.agent: a.to_dict() for a in decision.agents},
            entry_mode=decision.entry_mode,
            entry_delay_ms=decision.entry_delay_ms,
            strategy=self.strategy_name,
        )
        self.active[sid] = sig
        self.last_signal_ts = decision.ts
        self.counters["signals"] += 1
        if self.strategy_name == "burst15":
            self.burst.on_entry(sid, decision.ts)
        self._record_signal_row(decision, sid)
        self._persist_agents(decision, signal_id=sid)
        self._write_paper_trade(sig)
        self._broadcast(sig, "signal_created")
        log.info(
            "signal.created", signal_id=sid, direction=sig.direction.value,
            reference=sig.reference_price, trigger=sig.trigger_price,
            confidence=round(sig.confidence, 3),
        )
        return sig

    # -------------------------------------------------------------- monitor
    async def _monitor_loop(self) -> None:
        """Backend-side trigger and expiry checks, independent of the browser."""
        while self._running:
            await asyncio.sleep(0.02)  # 20 ms resolution
            try:
                self._tick_check()
            except Exception as exc:  # noqa: BLE001
                self.market.record_error(
                    "signal_monitor", f"{type(exc).__name__}: {exc}"
                )

    def _current_price(self) -> float | None:
        tick = self.market.last_tick
        if tick is None:
            return None
        src = self.settings.trigger_price_source
        if src == "last":
            return tick.last_price or tick.mid
        if src == "micro":
            return tick.micro_price
        return tick.mid

    def _tick_check(self) -> None:
        if not self.active:
            return
        price = self._current_price()
        ts = now_ms()
        for sid in list(self.active):
            sig = self.active.get(sid)
            if sig is None:
                continue
            if sig.status is SignalStatus.WAITING:
                if sig.entry_mode == "DELAY":
                    # Time-based entry: the clock, not a price level, decides.
                    if ts >= sig.created_at + sig.entry_delay_ms:
                        if price is None:
                            self._cancel(sig, ts, "no price at the scheduled entry")
                        else:
                            self._on_trigger(sig, price, ts)
                elif price is not None and self._touched(sig, price):
                    self._on_trigger(sig, price, ts)
                elif ts >= sig.expires_wait_at:
                    self._cancel(sig, ts, "trigger not reached within wait window")
            elif sig.status in (SignalStatus.TRIGGERED, SignalStatus.ACTIVE):
                if sig.expires_at and ts >= sig.expires_at:
                    self._on_expiry(sig, price, ts)

    def _touched(self, sig: LiveSignal, price: float) -> bool:
        if sig.direction is Direction.UP:
            return price >= sig.trigger_price
        return price <= sig.trigger_price

    def _on_trigger(self, sig: LiveSignal, price: float, ts: int) -> None:
        sig.status = SignalStatus.TRIGGERED
        sig.triggered_at = ts
        sig.expires_at = ts + int(sig.horizon_s * 1000)
        # Entry is the price actually observed at the touch. It can overshoot the
        # trigger on a fast move; recording both keeps the fill honest.
        sig.entry_price = price
        self.counters["triggered"] += 1
        self._broadcast(sig, "trigger_hit")
        # TRIGGERED is instantaneous; the countdown state is ACTIVE.
        sig.status = SignalStatus.ACTIVE
        self._broadcast(sig, "trade_active")
        self._write_paper_trade(sig)
        log.info(
            "signal.triggered", signal_id=sig.signal_id, entry=price,
            trigger=sig.trigger_price, expires_at=sig.expires_at,
        )

    def _on_expiry(self, sig: LiveSignal, price: float | None, ts: int) -> None:
        sig.status = SignalStatus.EXPIRED
        sig.expiry_price = price
        sig.settled_at = ts
        sig.settle_delay_ms = ts - (sig.expires_at or ts)
        self.counters["expired"] += 1
        self._broadcast(sig, "trade_expired")

        entry = sig.entry_price
        if price is None or entry is None:
            sig.result = SignalStatus.CANCELLED.value
            sig.status = SignalStatus.CANCELLED
            self.counters["cancelled"] += 1
        else:
            moved_up = price > entry
            moved_down = price < entry
            if not moved_up and not moved_down:
                sig.result = SignalStatus.TIE.value
                sig.status = SignalStatus.TIE
                self.counters["ties"] += 1
            elif (sig.direction is Direction.UP and moved_up) or (
                sig.direction is Direction.DOWN and moved_down
            ):
                sig.result = SignalStatus.WIN.value
                sig.status = SignalStatus.WIN
                self.counters["wins"] += 1
            else:
                sig.result = SignalStatus.LOSS.value
                sig.status = SignalStatus.LOSS
                self.counters["losses"] += 1
            sig.pnl_units = self._pnl_units(sig)

        self._finish(sig)

    def _cancel(self, sig: LiveSignal, ts: int, reason: str) -> None:
        sig.status = SignalStatus.CANCELLED
        sig.result = SignalStatus.CANCELLED.value
        sig.settled_at = ts
        self.counters["cancelled"] += 1
        self._broadcast(sig, "signal_cancelled", {"reason": reason})
        self._finish(sig)

    def _finish(self, sig: LiveSignal) -> None:
        self.active.pop(sig.signal_id, None)
        if sig.strategy == "burst15":
            # Session P&L, the stop-loss and the take-profit all move here.
            self.burst.on_settled(sig.signal_id, sig.result, sig.settled_at)
        self.history.append(sig)
        if len(self.history) > 1000:
            self.history = self.history[-1000:]
        self._broadcast(sig, "signal_settled")
        self._write_paper_trade(sig)
        asyncio.create_task(self._update_signal_row(sig))

    def _pnl_units(self, sig: LiveSignal) -> float | None:
        """P&L in stake units. None when the payout is unknown.

        A binary payout is a property of the broker, not of the market. Without
        it, monetary P&L is undefined and this returns None - the statistics
        layer then reports PAYOUT UNKNOWN instead of inventing a number.
        """
        payout = self.settings.binary_payout
        if payout is None or sig.result is None:
            return None
        if sig.result == SignalStatus.WIN.value:
            return self.settings.paper_stake * payout
        if sig.result == SignalStatus.LOSS.value:
            return -self.settings.paper_stake
        return 0.0

    # ------------------------------------------------------------ persistence
    def _broadcast(
        self, sig: LiveSignal, event: str, extra: dict[str, Any] | None = None
    ) -> None:
        payload = {"event": event, "signal": sig.to_dict(), **(extra or {})}
        self.bus.publish(Topic.SIGNAL, payload)

    def _record_no_trade_row(self, decision: Decision) -> None:
        """Heartbeat row for a NO TRADE window - not one per evaluation.

        At the default 100ms cadence the old behaviour wrote ~864k rows a day
        into `signals`, all of them saying nothing happened. That is what filled
        the batch writer's queue and made the real signals impossible to find.
        The learning record is `shadow_decisions`, which is written on its own
        (much slower) cadence, so nothing is lost by throttling here.
        """
        key = tuple(sorted({gate_key(r) for r in decision.no_trade_reasons}))
        interval = self.settings.no_trade_row_interval_ms
        if key == self._last_no_trade_key and (
            decision.ts - self._last_no_trade_row_ts < interval
        ):
            return
        self._last_no_trade_key = key
        self._last_no_trade_row_ts = decision.ts
        self._record_signal_row(decision, None)

    def _record_signal_row(self, decision: Decision, signal_id: str | None) -> None:
        self.writer.add(
            repo.SignalRow,
            {
                "signal_id": signal_id or f"nt-{new_signal_id()}",
                "ts": decision.ts,
                "exchange": decision.exchange,
                "symbol": decision.symbol,
                "direction": decision.direction.value,
                "status": (
                    SignalStatus.WAITING.value if signal_id
                    else SignalStatus.CANCELLED.value
                ),
                "reference_price": decision.reference_price,
                "trigger_price": decision.trigger_price,
                "horizon_s": decision.horizon_s,
                "confidence": decision.confidence,
                "prob_up": decision.prob_up,
                "prob_down": decision.prob_down,
                "prob_neutral": decision.prob_neutral,
                "edge": decision.edge,
                "market_regime": decision.regime.value,
                "no_trade_reasons": decision.no_trade_reasons,
                "decision": decision.detail,
                "model_id": decision.model_id,
                "data_quality": decision.data_quality,
                "source": self.market.source.value,
                "is_synthetic": self.market.is_synthetic,
            },
        )

    def _persist_agents(self, decision: Decision, signal_id: str | None) -> None:
        # Only persist the full agent panel for real signals; at 10 Hz the
        # no-trade panel would dominate the table without adding information.
        if signal_id is None:
            return
        self.writer.add_many(
            repo.AgentPredictionRow,
            [
                {
                    "ts": decision.ts,
                    "signal_id": signal_id,
                    "symbol": decision.symbol,
                    "agent": a.agent,
                    "direction": a.direction.value,
                    "confidence": a.confidence,
                    "score": a.score,
                    "reason": a.reason,
                    "features_used": a.features_used,
                    "data_quality": a.data_quality,
                    "source": self.market.source.value,
                    "is_synthetic": self.market.is_synthetic,
                }
                for a in decision.agents
            ],
        )

    def _write_paper_trade(self, sig: LiveSignal) -> None:
        asyncio.create_task(self._upsert_paper_trade(sig))

    async def _upsert_paper_trade(self, sig: LiveSignal) -> None:
        try:
            await repo.upsert_paper_trade(
                {
                    "signal_id": sig.signal_id,
                    "ts": sig.created_at,
                    "exchange": sig.exchange,
                    "symbol": sig.symbol,
                    "direction": sig.direction.value,
                    "status": sig.status.value,
                    "trigger_price": sig.trigger_price,
                    "entry_price": sig.entry_price,
                    "expiry_price": sig.expiry_price,
                    "confidence": sig.confidence,
                    "probability_up": sig.prob_up,
                    "probability_down": sig.prob_down,
                    "market_regime": sig.regime,
                    "horizon_s": sig.horizon_s,
                    "features": _jsonable(sig.features),
                    "agents": _jsonable(sig.agents),
                    "triggered_at": sig.triggered_at,
                    "expires_at": sig.expires_at,
                    "settled_at": sig.settled_at,
                    "result": sig.result,
                    "pnl_units": sig.pnl_units,
                    "payout": self.settings.binary_payout,
                    "stake": self.settings.paper_stake,
                    "latency_ms": (
                        self.market.last_tick.latency_ms
                        if self.market.last_tick else None
                    ),
                    "data_quality": sig.data_quality,
                    "source": self.market.source.value,
                    "is_synthetic": sig.is_synthetic,
                }
            )
        except Exception as exc:  # noqa: BLE001 - never break the engine on I/O
            self.market.record_error("paper_trade_write", str(exc))

    async def _update_signal_row(self, sig: LiveSignal) -> None:
        try:
            await repo.update_signal_status(sig.signal_id, status=sig.status.value)
        except Exception as exc:  # noqa: BLE001
            self.market.record_error("signal_row_update", str(exc))

    # ------------------------------------------------------------------ views
    def snapshot(self) -> dict[str, Any]:
        ts = now_ms()
        return {
            "server_ts": ts,
            "strategy": self.strategy_name,
            "active": [s.to_dict(ts) for s in self.active.values()],
            "last_settled": [s.to_dict(ts) for s in self.history[-20:]][::-1],
            "counters": dict(self.counters),
            "last_decision": (
                self.last_decision.to_dict() if self.last_decision else None
            ),
            "burst_session": (
                self.burst.session.to_dict(ts)
                if self.strategy_name == "burst15" and self.burst.session
                else None
            ),
            "is_synthetic": self.market.is_synthetic,
        }

    def diagnostics(self, top: int = 12) -> dict[str, Any]:
        """Why signals are or are not arriving, in one object.

        Built for the question every operator asks first. `blocking_gates` is
        ranked by how often each gate fired, so the binding constraint is the
        top row rather than something to be guessed at from a single snapshot.
        """
        ts = now_ms()
        decisions = max(self.counters["decisions"], 1)
        gates = [
            {
                "gate": key,
                "count": count,
                "share_of_decisions": round(count / decisions, 4),
            }
            for key, count in self.gate_counter.most_common(top)
        ]
        emitted = self.counters["signals"]
        uptime_s = max((ts - self.started_at) / 1000.0, 1e-9)
        return {
            "server_ts": ts,
            "strategy": self.strategy_name,
            "uptime_s": round(uptime_s, 1),
            "decisions_evaluated": self.counters["decisions"],
            "signals_emitted": emitted,
            "signals_per_hour": round(emitted / uptime_s * 3600.0, 2),
            "emission_rate": round(emitted / decisions, 5),
            "blocking_gates": gates,
            "binding_gate": gates[0]["gate"] if gates else None,
            "last_decision_reasons": (
                self.last_decision.no_trade_reasons if self.last_decision else []
            ),
            "thresholds": {
                "signal_min_agreement": self.settings.signal_min_agreement,
                "signal_min_confidence": self.settings.signal_min_confidence,
                "signal_min_edge": self.settings.signal_min_edge,
                "effective_min_confidence": round(
                    self.settings.effective_min_confidence, 4
                ),
                "min_data_quality": self.settings.min_data_quality,
                "max_spread_bps": self.settings.max_spread_bps,
                "min_expected_move_ticks": self.settings.min_expected_move_ticks,
                "max_zero_move_fraction": self.settings.max_zero_move_fraction,
                "anomaly_max_severity": self.settings.anomaly_max_severity,
            },
            "burst": (
                self.burst.status() if self.strategy_name == "burst15" else None
            ),
            "note": (
                "A gate firing often is not automatically wrong. Use GET "
                "/shadow to check whether the windows a gate rejected would "
                "have won: that is the only evidence that says whether it is "
                "protecting you or costing you."
            ),
        }

    def current_signal(self) -> dict[str, Any] | None:
        ts = now_ms()
        if self.active:
            return next(iter(self.active.values())).to_dict(ts)
        if self.history:
            return self.history[-1].to_dict(ts)
        return None


def _jsonable(value: Any) -> Any:
    """Strip non-finite floats so JSONB serialisation cannot fail."""
    import math

    if isinstance(value, dict):
        return {k: _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(v) for v in value]
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    return value
