"""BLOCK 3 - EXECUTE.  One core owns the life of a position.

    MarketSnapshot -> Decision -> OrderIntent -> Bybit -> ACK -> Execution -> Fill
                                                            -> Position Confirmed

A position exists when fills say so, never when an ack says so.  Exits are
driven by the same snapshots that opened the trade: the position lives only
while the conditions that created it still hold and the risk stays bounded.
There is no "wait until it comes back".

P&L is decomposed so it always adds up exactly:

    net = gross(reference prices) - slippage(vs reference) - fees(actual)
"""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable

from ..config import Config
from ..decide.decision_core import Decision
from ..decide.model import Model
from ..decide.risk import AccountState, RiskEngine
from ..scan.snapshot import MarketSnapshot
from ..storage.repo import Repo
from ..util.clock import Clock
from ..util.logging_setup import get_logger
from .broker import Broker, ExecutionEvent, OrderIntent, make_order_link_id

log = get_logger("execute.core")


class PositionState(str, Enum):
    PENDING_ENTRY = "PENDING_ENTRY"
    OPEN = "OPEN"
    EXITING = "EXITING"
    CLOSED = "CLOSED"
    ABORTED = "ABORTED"


class ExitReason(str, Enum):
    TARGET = "TARGET"
    TRAIL = "TRAIL"
    STOP = "STOP"
    EDGE_GONE = "EDGE_GONE"
    MAX_HOLD = "MAX_HOLD"
    HEALTH = "HEALTH"
    KILL_SWITCH = "KILL_SWITCH"
    FLATTEN = "FLATTEN"
    RECONCILE = "RECONCILE"


@dataclass
class Position:
    position_key: str
    symbol: str
    side: str                        # LONG | SHORT
    qty: float
    leverage: float
    margin_eur: float
    notional_eur: float
    entry_ref_price: float           # mid at the moment of the decision
    entry_price: float = 0.0         # volume weighted actual fill
    entry_fee_usdt: float = 0.0
    exit_ref_price: float = 0.0
    exit_price: float = 0.0
    exit_fee_usdt: float = 0.0
    exit_qty: float = 0.0
    filled_qty: float = 0.0
    state: PositionState = PositionState.PENDING_ENTRY
    opened_ms: float = 0.0
    closed_ms: float = 0.0
    intent_ms: float = 0.0
    entry_link_id: str = ""
    exit_link_id: str = ""
    decision_id: int | None = None
    snapshot_id: int | None = None
    model_version: str = ""
    target_move_bps: float = 0.0
    stop_bps: float = 0.0
    max_hold_s: float = 0.0
    target_eur: float = 0.0
    expected_cost_eur: float = 0.0
    entry_reason: str = ""
    exit_reason: str = ""
    prediction: dict[str, Any] = field(default_factory=dict)
    features: dict[str, float] = field(default_factory=dict)
    evolution: list[dict[str, Any]] = field(default_factory=list)
    mfe_eur: float = 0.0
    mae_eur: float = 0.0
    best_profit_eur: float = 0.0
    last_sample_ms: float = 0.0
    exit_requested_ms: float = 0.0

    @property
    def direction(self) -> float:
        return 1.0 if self.side == "LONG" else -1.0

    def unrealized_gross_eur(self, price: float, eur_per_usdt: float) -> float:
        if self.entry_price <= 0 or self.filled_qty <= 0:
            return 0.0
        return (price - self.entry_price) * self.direction * self.filled_qty * eur_per_usdt

    def to_dict(self, mark_price: float | None = None, eur_per_usdt: float = 1.0) -> dict[str, Any]:
        unrealized = (
            self.unrealized_gross_eur(mark_price, eur_per_usdt) if mark_price else 0.0
        )
        return {
            "position_key": self.position_key,
            "symbol": self.symbol,
            "side": self.side,
            "state": self.state.value,
            "qty": self.filled_qty or self.qty,
            "entry_price": self.entry_price,
            "entry_ref_price": self.entry_ref_price,
            "mark_price": mark_price,
            "leverage": round(self.leverage, 2),
            "margin_eur": round(self.margin_eur, 2),
            "notional_eur": round(self.notional_eur, 2),
            "unrealized_eur": round(unrealized, 3),
            "target_eur": round(self.target_eur, 3),
            "target_move_bps": round(self.target_move_bps, 2),
            "stop_bps": round(self.stop_bps, 2),
            "max_hold_s": round(self.max_hold_s, 1),
            "age_s": round((self.last_sample_ms - self.opened_ms) / 1000.0, 1)
            if self.opened_ms else 0.0,
            "mfe_eur": round(self.mfe_eur, 3),
            "mae_eur": round(self.mae_eur, 3),
            "entry_reason": self.entry_reason,
            "model_version": self.model_version,
        }


class ExecutionCore:
    def __init__(
        self,
        cfg: Config,
        clock: Clock,
        broker: Broker,
        repo: Repo,
        risk: RiskEngine,
        gate: Callable[[], tuple[bool, list[str]]] | None = None,
    ) -> None:
        self.cfg = cfg
        self.clock = clock
        self.broker = broker
        self.repo = repo
        self.risk = risk
        self.gate = gate or (lambda: (True, []))
        self.positions: dict[str, Position] = {}      # by symbol
        self.by_link: dict[str, Position] = {}        # by order link id
        self.closed: list[Position] = []
        self.entries_blocked_reason: str = ""
        self.entries_blocked: bool = False
        self._seq = 0
        self._lock = asyncio.Lock()
        self.trades_opened = 0
        self.trades_closed = 0
        self.entry_failures = 0
        self.seen_exec_ids: set[str] = set()
        broker.set_execution_handler(self.on_execution)

    # ---------------------------------------------------------------- gating
    def block_entries(self, reason: str) -> None:
        if not self.entries_blocked:
            log.warning("new entries BLOCKED: %s", reason)
        self.entries_blocked = True
        self.entries_blocked_reason = reason

    def allow_entries(self) -> None:
        if self.entries_blocked:
            log.info("new entries allowed again")
        self.entries_blocked = False
        self.entries_blocked_reason = ""

    # ---------------------------------------------------------------- entry
    async def open_position(self, decision: Decision) -> Position | None:
        """Turn an accepted decision into a confirmed position, or nothing."""
        if not decision.is_trade:
            return None
        if self.entries_blocked:
            log.info("entry skipped (%s): %s", decision.symbol, self.entries_blocked_reason)
            return None
        healthy, reasons = self.gate()
        if not healthy:
            log.info("entry skipped (%s): %s", decision.symbol, "; ".join(reasons))
            return None

        async with self._lock:
            if decision.symbol in self.positions:
                return None
            self._seq += 1
            position_key = f"{self.repo.db.run_id}:{decision.symbol}:{self._seq:05d}"
            link_id = make_order_link_id(self.repo.db.run_id, position_key, "ENTRY")
            snap = decision.snapshot
            position = Position(
                position_key=position_key,
                symbol=decision.symbol,
                side=decision.side,
                qty=decision.qty,
                leverage=decision.leverage,
                margin_eur=decision.margin_eur,
                notional_eur=decision.notional_eur,
                entry_ref_price=snap.mid,
                intent_ms=self.clock.now_ms(),
                entry_link_id=link_id,
                decision_id=decision_id_of(decision),
                model_version=decision.model_version,
                target_move_bps=decision.target_move_bps,
                stop_bps=decision.stop_bps,
                max_hold_s=decision.max_hold_s,
                target_eur=decision.target_eur,
                expected_cost_eur=decision.expected_cost_eur,
                entry_reason=decision.reason,
                features=decision.features,
                prediction={
                    "probability": decision.probability,
                    "quality": decision.quality,
                    "expected_move_bps": decision.expected_move_bps,
                    "required_move_bps": decision.required_move_bps,
                    "target_move_bps": decision.target_move_bps,
                    "expected_cost_eur": decision.expected_cost_eur,
                    "expectancy_eur": decision.expectancy_eur,
                    "est_slippage_bps": decision.est_slippage_bps,
                    "scan_score": decision.scan_score,
                    "model_version": decision.model_version,
                },
            )
            self.positions[decision.symbol] = position
            self.by_link[link_id] = position

        intent = OrderIntent(
            order_link_id=link_id,
            symbol=decision.symbol,
            side="Buy" if decision.side == "LONG" else "Sell",
            qty=decision.qty,
            qty_str=_format_qty(decision.qty, decision.snapshot.qty_step),
            purpose="ENTRY",
            position_key=position_key,
            leverage=decision.leverage,
            margin_eur=decision.margin_eur,
            decision_id=position.decision_id,
            created_ms=position.intent_ms,
            snapshot=decision.snapshot,
        )
        self.repo.save_order(intent.to_row("INTENT"))

        ack = await self.broker.submit(intent)
        self.repo.update_order(
            link_id,
            status="ACK" if ack.accepted else "REJECTED",
            ack_ts=ack.ts_ms,
            exchange_order_id=ack.exchange_order_id,
            reject_reason=ack.reject_reason or None,
            raw_json=json.dumps(ack.raw, default=str),
        )
        if not ack.accepted:
            self.entry_failures += 1
            log.warning("entry rejected on %s: %s", decision.symbol, ack.reject_reason)
            async with self._lock:
                position.state = PositionState.ABORTED
                self.positions.pop(decision.symbol, None)
                self.by_link.pop(link_id, None)
            return None

        # ACK is not a fill.  Wait for the private stream to confirm.
        confirmed = await self._await_fill(position)
        if not confirmed:
            self.entry_failures += 1
            log.error(
                "no fill for %s within %.1fs after ACK - reconciliation required",
                decision.symbol, self.cfg.execute.fill_timeout_s,
            )
            self.repo.update_order(link_id, status="UNCONFIRMED")
            async with self._lock:
                position.state = PositionState.ABORTED
                self.positions.pop(decision.symbol, None)
            return None

        self.trades_opened += 1
        self.repo.save_position(
            {
                "position_key": position.position_key,
                "symbol": position.symbol,
                "side": position.side,
                "qty": position.filled_qty,
                "entry_price": position.entry_price,
                "leverage": position.leverage,
                "margin_eur": position.margin_eur,
                "opened_ts": position.opened_ms,
                "closed_ts": None,
                "status": "OPEN",
                "decision_id": position.decision_id,
            }
        )
        if position.decision_id:
            self.repo.mark_decision_executed(position.decision_id)
        log.info(
            "POSITION CONFIRMED %s %s qty=%g @ %.6f (ref %.6f, slip %.2fbps) margin=%.2f EUR lev=%.1fx",
            position.side, position.symbol, position.filled_qty, position.entry_price,
            position.entry_ref_price, _slippage_bps(
                position.entry_ref_price, position.entry_price, position.direction
            ),
            position.margin_eur, position.leverage,
        )
        return position

    async def _await_fill(self, position: Position) -> bool:
        deadline = self.clock.mono() + self.cfg.execute.fill_timeout_s
        while self.clock.mono() < deadline:
            if position.state is PositionState.OPEN:
                return True
            await asyncio.sleep(0.02)
        return position.state is PositionState.OPEN

    # ---------------------------------------------------------------- fills in
    async def on_execution(self, event: ExecutionEvent) -> None:
        """Single entry point for every fill, paper or live."""
        if event.exec_id in self.seen_exec_ids:
            return                      # duplicate delivery after a reconnect
        self.seen_exec_ids.add(event.exec_id)
        if self.repo.execution_exists(event.exec_id):
            return
        self.repo.save_execution(
            {
                "exec_id": event.exec_id,
                "order_link_id": event.order_link_id,
                "exchange_order_id": event.exchange_order_id,
                "symbol": event.symbol,
                "side": event.side,
                "price": event.price,
                "qty": event.qty,
                "fee": event.fee,
                "is_maker": 1 if event.is_maker else 0,
                "ts_ms": event.ts_ms,
                "raw_json": json.dumps(event.raw, default=str),
            }
        )
        position = self.by_link.get(event.order_link_id)
        if position is None:
            log.warning(
                "execution %s for unknown order %s (%s) - reconciliation will pick it up",
                event.exec_id, event.order_link_id, event.symbol,
            )
            return

        if event.order_link_id == position.entry_link_id:
            await self._apply_entry_fill(position, event)
        elif event.order_link_id == position.exit_link_id:
            await self._apply_exit_fill(position, event)

    async def _apply_entry_fill(self, position: Position, event: ExecutionEvent) -> None:
        total_qty = position.filled_qty + event.qty
        if total_qty <= 0:
            return
        position.entry_price = (
            position.entry_price * position.filled_qty + event.price * event.qty
        ) / total_qty
        position.filled_qty = total_qty
        position.entry_fee_usdt += event.fee
        position.opened_ms = position.opened_ms or event.ts_ms
        position.last_sample_ms = event.ts_ms
        position.state = PositionState.OPEN
        position.notional_eur = position.filled_qty * position.entry_price * self.cfg.eur_per_usdt
        self.repo.update_order(
            position.entry_link_id,
            status="FILLED",
            filled_ts=event.ts_ms,
            filled_qty=position.filled_qty,
            avg_price=position.entry_price,
            fee_usdt=position.entry_fee_usdt,
        )

    async def _apply_exit_fill(self, position: Position, event: ExecutionEvent) -> None:
        total = position.exit_qty + event.qty
        position.exit_price = (
            position.exit_price * position.exit_qty + event.price * event.qty
        ) / total
        position.exit_qty = total
        position.exit_fee_usdt += event.fee
        position.closed_ms = event.ts_ms
        self.repo.update_order(
            position.exit_link_id,
            status="FILLED",
            filled_ts=event.ts_ms,
            filled_qty=position.exit_qty,
            avg_price=position.exit_price,
            fee_usdt=position.exit_fee_usdt,
        )
        if position.exit_qty + 1e-12 >= position.filled_qty:
            await self._finalise(position)

    # ---------------------------------------------------------------- management
    async def manage(self, snapshots: dict[str, MarketSnapshot], model: Model) -> None:
        """Re-evaluate every open position against the newest snapshots."""
        healthy, health_reasons = self.gate()
        killed, kill_reason = self.risk.kill_switch_active()

        for position in list(self.positions.values()):
            if position.state is not PositionState.OPEN:
                continue
            snap = snapshots.get(position.symbol)
            if snap is None:
                continue
            self._sample(position, snap, model)

            reason = self._exit_reason(position, snap, model, healthy, health_reasons, killed)
            if reason is not None:
                await self.close_position(position, reason[0], snap, detail=reason[1])

    def _sample(self, position: Position, snap: MarketSnapshot, model: Model) -> None:
        mark = snap.bid if position.side == "LONG" else snap.ask
        gross = position.unrealized_gross_eur(mark, self.cfg.eur_per_usdt)
        net_now = gross - position.expected_cost_eur
        position.mfe_eur = max(position.mfe_eur, net_now)
        position.mae_eur = min(position.mae_eur, net_now)
        position.best_profit_eur = max(position.best_profit_eur, net_now)
        position.last_sample_ms = snap.ts_local_ms
        if snap.ts_local_ms - (position.evolution[-1]["t"] if position.evolution else 0) >= 500:
            if len(position.evolution) < 120:
                position.evolution.append(
                    {
                        "t": snap.ts_local_ms,
                        "mid": snap.mid,
                        "net_eur": round(net_now, 4),
                        "p": round(model.probability(snap.features(position.side)), 4),
                        "ofi_5s": round(snap.ofi_5s, 3),
                    }
                )

    def _exit_reason(
        self,
        position: Position,
        snap: MarketSnapshot,
        model: Model,
        healthy: bool,
        health_reasons: list[str],
        killed: bool,
    ) -> tuple[ExitReason, str] | None:
        cfg = self.cfg.decide
        age_s = (snap.ts_local_ms - position.opened_ms) / 1000.0
        mark = snap.bid if position.side == "LONG" else snap.ask
        gross = position.unrealized_gross_eur(mark, self.cfg.eur_per_usdt)
        net_now = gross - position.expected_cost_eur
        move_bps = (
            (mark / position.entry_price - 1.0) * position.direction * 10_000.0
            if position.entry_price > 0 else 0.0
        )

        if killed:
            return ExitReason.KILL_SWITCH, self.risk.kill_switch_active()[1]
        if not healthy:
            return ExitReason.HEALTH, "; ".join(health_reasons)
        if move_bps <= -position.stop_bps:
            return ExitReason.STOP, f"stop hit: {move_bps:.1f}bps <= -{position.stop_bps:.1f}bps"
        if age_s >= position.max_hold_s:
            # A winner still being pushed our way earns extra time, once, and
            # never past the global cap.  Everything else goes out on the clock.
            probability = model.probability(snap.features(position.side))
            can_extend = (
                net_now >= position.target_eur
                and probability >= cfg.min_probability
                and position.max_hold_s < cfg.max_hold_s
            )
            if can_extend:
                position.max_hold_s = min(position.max_hold_s * 1.5, cfg.max_hold_s)
                return None
            return ExitReason.MAX_HOLD, f"held {age_s:.1f}s >= {position.max_hold_s:.1f}s"
        if age_s < cfg.min_hold_s:
            return None

        # Target reached: let it run only while the flow still pushes our way.
        if net_now >= position.target_eur * cfg.trail_activate_fraction:
            probability = model.probability(snap.features(position.side))
            give_back = position.best_profit_eur * cfg.trail_give_back_fraction
            if net_now <= position.best_profit_eur - give_back:
                return (
                    ExitReason.TRAIL,
                    f"gave back {position.best_profit_eur - net_now:.2f} EUR of "
                    f"{position.best_profit_eur:.2f} EUR peak",
                )
            if probability < cfg.edge_exit_probability:
                return (
                    ExitReason.TARGET,
                    f"target reached ({net_now:.2f} EUR) and edge faded (p={probability:.3f})",
                )
            return None   # in profit and the push continues: stay

        # Below target: the trade is only allowed to live while its thesis holds.
        probability = model.probability(snap.features(position.side))
        if probability < cfg.edge_exit_probability:
            return (
                ExitReason.EDGE_GONE,
                f"edge gone: p={probability:.3f} < {cfg.edge_exit_probability:.3f} "
                f"(entry p={position.prediction.get('probability', 0):.3f})",
            )
        return None

    # ---------------------------------------------------------------- exit
    async def close_position(
        self,
        position: Position,
        reason: ExitReason,
        snap: MarketSnapshot | None,
        detail: str = "",
    ) -> None:
        if position.state is not PositionState.OPEN:
            return
        position.state = PositionState.EXITING
        position.exit_reason = f"{reason.value}: {detail}" if detail else reason.value
        position.exit_ref_price = (
            (snap.bid if position.side == "LONG" else snap.ask) if snap else position.entry_price
        )
        position.exit_requested_ms = self.clock.now_ms()
        link_id = make_order_link_id(self.repo.db.run_id, position.position_key, "EXIT")
        position.exit_link_id = link_id
        self.by_link[link_id] = position

        intent = OrderIntent(
            order_link_id=link_id,
            symbol=position.symbol,
            side="Sell" if position.side == "LONG" else "Buy",
            qty=position.filled_qty,
            qty_str=_format_qty(position.filled_qty, snap.qty_step if snap else 0.0),
            purpose="EXIT",
            position_key=position.position_key,
            reduce_only=True,
            leverage=position.leverage,
            margin_eur=position.margin_eur,
            decision_id=position.decision_id,
            created_ms=position.exit_requested_ms,
            snapshot=snap,
        )
        self.repo.save_order(intent.to_row("INTENT"))
        log.info("EXIT %s %s: %s", position.side, position.symbol, position.exit_reason)

        ack = await self.broker.submit(intent)
        self.repo.update_order(
            link_id,
            status="ACK" if ack.accepted else "REJECTED",
            ack_ts=ack.ts_ms,
            exchange_order_id=ack.exchange_order_id,
            reject_reason=ack.reject_reason or None,
            raw_json=json.dumps(ack.raw, default=str),
        )
        if not ack.accepted:
            log.error(
                "EXIT REJECTED on %s (%s) - position still open, will retry",
                position.symbol, ack.reject_reason,
            )
            position.state = PositionState.OPEN     # retry on the next pass
            position.exit_link_id = ""
            self.by_link.pop(link_id, None)

    async def _finalise(self, position: Position) -> None:
        """Fills complete: write the trade with an exact cost decomposition."""
        eur = self.cfg.eur_per_usdt
        qty = position.filled_qty
        direction = position.direction

        gross = (position.exit_ref_price - position.entry_ref_price) * direction * qty * eur
        slip_entry_eur = (position.entry_price - position.entry_ref_price) * direction * qty * eur
        slip_exit_eur = (position.exit_ref_price - position.exit_price) * direction * qty * eur
        slippage_eur = slip_entry_eur + slip_exit_eur
        fees_eur = (position.entry_fee_usdt + position.exit_fee_usdt) * eur
        net = gross - slippage_eur - fees_eur

        notional = position.notional_eur or (qty * position.entry_price * eur)
        hold_s = max((position.closed_ms - position.opened_ms) / 1000.0, 0.0)

        position.state = PositionState.CLOSED
        self.positions.pop(position.symbol, None)
        self.by_link.pop(position.entry_link_id, None)
        self.by_link.pop(position.exit_link_id, None)
        self.closed.append(position)
        self.trades_closed += 1

        self.repo.close_position(position.position_key, position.closed_ms)
        self.repo.save_trade(
            {
                "position_key": position.position_key,
                "symbol": position.symbol,
                "side": position.side,
                "decision_id": position.decision_id,
                "snapshot_id": position.snapshot_id,
                "model_version": position.model_version,
                "mode": self.cfg.mode,
                "entry_ts": position.opened_ms,
                "exit_ts": position.closed_ms,
                "hold_s": hold_s,
                "entry_price": position.entry_price,
                "exit_price": position.exit_price,
                "entry_ref_price": position.entry_ref_price,
                "exit_ref_price": position.exit_ref_price,
                "qty": qty,
                "notional_eur": notional,
                "leverage": position.leverage,
                "margin_eur": position.margin_eur,
                "expected_cost_eur": position.expected_cost_eur,
                "entry_fee_eur": position.entry_fee_usdt * eur,
                "exit_fee_eur": position.exit_fee_usdt * eur,
                "fees_eur": fees_eur,
                "slippage_entry_bps": _bps(slip_entry_eur, notional),
                "slippage_exit_bps": _bps(slip_exit_eur, notional),
                "slippage_eur": slippage_eur,
                "gross_pnl_eur": gross,
                "net_pnl_eur": net,
                "mfe_eur": position.mfe_eur,
                "mae_eur": position.mae_eur,
                "entry_reason": position.entry_reason,
                "exit_reason": position.exit_reason,
                "prediction_json": json.dumps(position.prediction, default=str),
                "evolution_json": json.dumps(position.evolution, default=str),
                "features_json": json.dumps(
                    {k: round(v, 6) for k, v in position.features.items()}
                ),
                "label": 1 if net > 0 else 0,
            }
        )
        self.risk.record_realized(net, position.symbol, self.clock.mono())
        log.info(
            "TRADE CLOSED %s %s: net %.3f EUR (gross %.3f - slippage %.3f - fees %.3f) "
            "in %.1fs [%s]",
            position.side, position.symbol, net, gross, slippage_eur, fees_eur,
            hold_s, position.exit_reason,
        )

    async def flatten_all(self, reason: ExitReason, snapshots: dict[str, MarketSnapshot]) -> int:
        count = 0
        for position in list(self.positions.values()):
            if position.state is PositionState.OPEN:
                await self.close_position(position, reason, snapshots.get(position.symbol))
                count += 1
        return count

    # ---------------------------------------------------------------- views
    def account_state(self, snapshots: dict[str, MarketSnapshot]) -> AccountState:
        raw = self.broker.account()
        eur = self.cfg.eur_per_usdt
        unrealized = 0.0
        exposure = 0.0
        for position in self.positions.values():
            if position.state is not PositionState.OPEN:
                continue
            snap = snapshots.get(position.symbol)
            mark = (snap.bid if position.side == "LONG" else snap.ask) if snap else position.entry_price
            unrealized += position.unrealized_gross_eur(mark, eur)
            exposure += position.filled_qty * (mark or position.entry_price) * eur
        state = AccountState(
            source=raw.get("source", self.broker.name),
            equity_eur=raw.get("equity_eur", 0.0),
            available_eur=raw.get("available_eur", 0.0),
            used_margin_eur=raw.get("used_margin_eur", 0.0),
            exposure_eur=exposure,
            unrealized_eur=unrealized,
            realized_today_eur=self.risk.realized_today_eur,
            open_positions=sum(
                1 for p in self.positions.values() if p.state is PositionState.OPEN
            ),
            symbols_open={
                p.symbol for p in self.positions.values()
                if p.state in (PositionState.OPEN, PositionState.PENDING_ENTRY)
            },
            last_update_ms=self.clock.now_ms(),
            confirmed=not self.entries_blocked,
        )
        return state

    def open_positions(self, snapshots: dict[str, MarketSnapshot]) -> list[dict[str, Any]]:
        out = []
        for position in self.positions.values():
            snap = snapshots.get(position.symbol)
            mark = (snap.bid if position.side == "LONG" else snap.ask) if snap else None
            out.append(position.to_dict(mark, self.cfg.eur_per_usdt))
        return out

    def health(self) -> dict[str, Any]:
        return {
            "broker": self.broker.health(),
            "entries_blocked": self.entries_blocked,
            "entries_blocked_reason": self.entries_blocked_reason,
            "open_positions": len(self.positions),
            "trades_opened": self.trades_opened,
            "trades_closed": self.trades_closed,
            "entry_failures": self.entry_failures,
        }


def decision_id_of(decision: Decision) -> int | None:
    return getattr(decision, "_db_id", None)


def _format_qty(qty: float, step: float) -> str:
    if step and step > 0:
        import math

        decimals = max(0, -int(math.floor(math.log10(step))))
        return f"{qty:.{decimals}f}"
    return f"{qty:.8f}".rstrip("0").rstrip(".")


def _slippage_bps(reference: float, actual: float, direction: float) -> float:
    if reference <= 0:
        return 0.0
    return (actual - reference) * direction / reference * 10_000.0


def _bps(amount_eur: float, notional_eur: float) -> float:
    if notional_eur <= 0:
        return 0.0
    return amount_eur / notional_eur * 10_000.0
