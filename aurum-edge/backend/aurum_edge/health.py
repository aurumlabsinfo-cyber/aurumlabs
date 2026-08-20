"""System health and the trading gate.

If the public stream, the private stream, the books, the account state or the
database are not healthy, **new entries stop** and the reason is stated in
words - on the API, on the dashboard and in ``diagnose``.  "0 signals" without
an explanation is a bug, not a state.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable

from .config import Config
from .util.clock import Clock


class ComponentState(str, Enum):
    OK = "OK"
    DEGRADED = "DEGRADED"
    DOWN = "DOWN"


class SystemState(str, Enum):
    BOOTING = "BOOTING"
    RECONCILING = "RECONCILING"
    LIVE_READY = "LIVE_READY"
    BLOCKED = "BLOCKED"
    HALTED = "HALTED"


@dataclass
class Component:
    name: str
    state: ComponentState
    detail: str
    critical: bool = True
    metrics: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "state": self.state.value,
            "detail": self.detail,
            "critical": self.critical,
            "metrics": self.metrics,
        }


class HealthMonitor:
    def __init__(
        self,
        cfg: Config,
        clock: Clock,
        market_health: Callable[[], dict[str, Any]],
        broker_health: Callable[[], dict[str, Any]],
        db_health: Callable[[], dict[str, Any]],
        execution_health: Callable[[], dict[str, Any]],
    ) -> None:
        self.cfg = cfg
        self.clock = clock
        self.market_health = market_health
        self.broker_health = broker_health
        self.db_health = db_health
        self.execution_health = execution_health
        self.system_state = SystemState.BOOTING
        self.state_detail = "starting up"
        self.transitions: list[dict[str, Any]] = []
        self._last_states: dict[str, ComponentState] = {}
        self.on_transition: Callable[[str, str, str], None] | None = None

    # ---------------------------------------------------------------- state
    def set_state(self, state: SystemState, detail: str = "") -> None:
        if state is not self.system_state:
            self.transitions.append(
                {
                    "ts_ms": self.clock.now_ms(),
                    "from": self.system_state.value,
                    "to": state.value,
                    "detail": detail,
                }
            )
            del self.transitions[:-50]
            if self.on_transition is not None:
                self.on_transition("system", state.value, detail)
        self.system_state = state
        self.state_detail = detail

    # ---------------------------------------------------------------- checks
    def components(self) -> list[Component]:
        market = self.market_health()
        broker = self.broker_health()
        db = self.db_health()
        execution = self.execution_health()
        out: list[Component] = []

        # public stream ----------------------------------------------------
        live = market.get("connections_live", 0)
        total = market.get("connections_total", 0)
        if total == 0:
            state, detail = ComponentState.DOWN, "no public connection started"
        elif live == 0:
            state, detail = ComponentState.DOWN, f"all {total} public connections are down"
        elif live < total:
            state, detail = (
                ComponentState.DEGRADED,
                f"{live}/{total} public connections live",
            )
        else:
            state, detail = ComponentState.OK, f"{live}/{total} public connections live"
        out.append(Component("public_ws", state, detail, True, {
            "live": live, "total": total, "messages": market.get("messages", 0),
        }))

        # private stream ---------------------------------------------------
        if self.cfg.is_live:
            connected = bool(broker.get("connected"))
            has_wallet = bool(broker.get("has_wallet"))
            if not connected:
                state, detail = ComponentState.DOWN, "private websocket not connected"
            elif not has_wallet:
                state, detail = ComponentState.DEGRADED, "no wallet snapshot received yet"
            else:
                state, detail = ComponentState.OK, "private websocket live, wallet known"
        else:
            state, detail = (
                ComponentState.OK,
                "PAPER mode: no private stream needed (simulated venue)",
            )
        out.append(Component("private_ws", state, detail, self.cfg.is_live, broker))

        # books ------------------------------------------------------------
        focus_ok = market.get("focus_books_ok", 0)
        focus_size = market.get("focus_size", 0)
        if focus_size == 0:
            state, detail = ComponentState.DOWN, "no focus symbols selected"
        elif focus_ok == 0:
            state, detail = ComponentState.DOWN, "no synchronised order book in the focus set"
        elif focus_ok < max(focus_size // 2, 1):
            state, detail = (
                ComponentState.DEGRADED,
                f"only {focus_ok}/{focus_size} focus books in sync",
            )
        else:
            state, detail = ComponentState.OK, f"{focus_ok}/{focus_size} focus books in sync"
        out.append(Component("books", state, detail, True, {
            "focus_ok": focus_ok, "focus_size": focus_size,
            "books_ok": market.get("books_ok", 0), "symbols": market.get("symbols", 0),
        }))

        # latency ----------------------------------------------------------
        p95 = market.get("latency_ms_p95")
        if p95 is None:
            state, detail = ComponentState.DEGRADED, "latency not measured yet"
        elif p95 > self.cfg.scan.max_latency_ms:
            state, detail = (
                ComponentState.DEGRADED,
                f"p95 latency {p95:.0f}ms above {self.cfg.scan.max_latency_ms:.0f}ms",
            )
        else:
            state, detail = ComponentState.OK, f"p95 latency {p95:.0f}ms"
        out.append(Component("latency", state, detail, False, {
            "p50": market.get("latency_ms_p50"), "p95": p95,
        }))

        # account ----------------------------------------------------------
        blocked = execution.get("entries_blocked")
        if blocked:
            state = ComponentState.DEGRADED
            detail = execution.get("entries_blocked_reason", "entries blocked")
        else:
            state, detail = ComponentState.OK, "account state confirmed"
        out.append(Component("account", state, detail, True, {
            "open_positions": execution.get("open_positions", 0),
        }))

        # database ---------------------------------------------------------
        failures = db.get("write_failures", 0)
        if not db.get("open"):
            state, detail = ComponentState.DOWN, "database is not open"
        elif failures:
            state, detail = (
                ComponentState.DOWN,
                f"{failures} write failures - rows are parked in write_failures: "
                f"{db.get('last_error', '')}",
            )
        else:
            state, detail = ComponentState.OK, f"schema v{db.get('schema_version')} at {db.get('path')}"
        out.append(Component("database", state, detail, True, db))

        # feed authenticity -------------------------------------------------
        source = market.get("source", "unknown")
        if self.cfg.is_live and source != "bybit":
            state, detail = (
                ComponentState.DOWN,
                f"LIVE mode requires the real Bybit feed, current source is '{source}'",
            )
        else:
            state, detail = ComponentState.OK, f"feed source: {source}"
        out.append(Component("feed_source", state, detail, True, {"source": source}))

        self._record_transitions(out)
        return out

    def _record_transitions(self, components: list[Component]) -> None:
        for component in components:
            previous = self._last_states.get(component.name)
            if previous is not None and previous is not component.state:
                if self.on_transition is not None:
                    self.on_transition(component.name, component.state.value, component.detail)
            self._last_states[component.name] = component.state

    # ---------------------------------------------------------------- gate
    def gate(self) -> tuple[bool, list[str]]:
        """May the system open new positions right now?"""
        reasons: list[str] = []
        for component in self.components():
            if not component.critical:
                continue
            if component.state is ComponentState.DOWN:
                reasons.append(f"{component.name} DOWN: {component.detail}")
            elif component.state is ComponentState.DEGRADED and component.name in (
                "account", "private_ws", "books",
            ):
                reasons.append(f"{component.name} degraded: {component.detail}")
        if self.system_state in (SystemState.HALTED, SystemState.RECONCILING, SystemState.BOOTING):
            reasons.append(f"system state is {self.system_state.value}: {self.state_detail}")
        return (not reasons), reasons

    def snapshot(self) -> dict[str, Any]:
        components = [c.to_dict() for c in self.components()]
        allowed, reasons = self.gate()
        return {
            "system_state": self.system_state.value,
            "system_detail": self.state_detail,
            "trading_allowed": allowed,
            "blocking_reasons": reasons,
            "components": components,
            "transitions": self.transitions[-10:],
        }
