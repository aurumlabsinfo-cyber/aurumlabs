"""Reconciliation: make local state match Bybit, or refuse to trade.

Run at start-up, after every reconnect, and periodically.  The sequence is
fixed:

    block entries -> wallet -> open orders -> executions -> positions
                  -> diff against local state -> repair -> LIVE_READY

Nothing here guesses.  If a difference cannot be explained, entries stay blocked
and the reason is visible on the dashboard and in ``diagnose``.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any

from ..config import Config
from ..scan.bybit_rest import BybitError, BybitRest, BybitTransportError
from ..storage.repo import Repo
from ..util.clock import Clock
from ..util.logging_setup import get_logger
from .execution_core import ExecutionCore, ExitReason, Position, PositionState

log = get_logger("execute.reconcile")


@dataclass
class ReconcileResult:
    ok: bool
    trigger: str
    ts_ms: float
    wallet: dict[str, Any] = field(default_factory=dict)
    positions: list[dict[str, Any]] = field(default_factory=list)
    orders: list[dict[str, Any]] = field(default_factory=list)
    differences: list[str] = field(default_factory=list)
    actions: list[str] = field(default_factory=list)
    error: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "trigger": self.trigger,
            "ts_ms": self.ts_ms,
            "equity": self.wallet.get("totalEquity") or self.wallet.get("equity_eur"),
            "exchange_positions": len(self.positions),
            "open_orders": len(self.orders),
            "differences": self.differences,
            "actions": self.actions,
            "error": self.error,
        }


class Reconciler:
    def __init__(
        self,
        cfg: Config,
        clock: Clock,
        execution: ExecutionCore,
        repo: Repo,
        rest: BybitRest | None = None,
    ) -> None:
        self.cfg = cfg
        self.clock = clock
        self.execution = execution
        self.repo = repo
        self.rest = rest
        self.last_result: ReconcileResult | None = None
        self.runs = 0

    async def reconcile(self, trigger: str) -> ReconcileResult:
        """Block entries, read the truth from Bybit, repair, then unblock."""
        self.runs += 1
        self.execution.block_entries(f"reconciling ({trigger})")
        result = ReconcileResult(ok=False, trigger=trigger, ts_ms=self.clock.now_ms())
        try:
            if self.cfg.is_live and self.rest is not None:
                await self._read_live(result)
            else:
                self._read_paper(result)
            await self._repair(result)
            result.ok = not result.error
        except (BybitError, BybitTransportError) as exc:
            result.error = f"could not read account state from Bybit: {exc}"
            log.error("%s", result.error)
        except Exception as exc:  # noqa: BLE001
            result.error = f"reconciliation failed: {type(exc).__name__}: {exc}"
            log.exception("reconciliation failed")

        self.repo.save_reconciliation(
            {
                "ts_ms": result.ts_ms,
                "trigger": trigger,
                "wallet_json": json.dumps(result.wallet, default=str),
                "positions_json": json.dumps(result.positions, default=str),
                "orders_json": json.dumps(result.orders, default=str),
                "diff_json": json.dumps(
                    {"differences": result.differences, "actions": result.actions}
                ),
                "outcome": "OK" if result.ok else f"FAILED: {result.error}",
            }
        )
        self.last_result = result
        if result.ok:
            self.execution.allow_entries()
            log.info(
                "reconciliation OK (%s): %d exchange positions, %d open orders, %d differences",
                trigger, len(result.positions), len(result.orders), len(result.differences),
            )
        else:
            self.execution.block_entries(f"reconciliation failed: {result.error}")
        return result

    # ---------------------------------------------------------------- reads
    async def _read_live(self, result: ReconcileResult) -> None:
        assert self.rest is not None
        result.wallet = await self.rest.wallet_balance()
        result.orders = await self.rest.open_orders()
        result.positions = [
            p for p in await self.rest.positions() if float(p.get("size", 0) or 0) > 0
        ]
        executions = await self.rest.executions(limit=100)
        for row in executions:
            exec_id = str(row.get("execId") or "")
            if exec_id and not self.repo.execution_exists(exec_id):
                # A fill we never saw on the stream: record it so nothing is lost.
                self.repo.save_execution(
                    {
                        "exec_id": exec_id,
                        "order_link_id": str(row.get("orderLinkId") or ""),
                        "exchange_order_id": str(row.get("orderId") or ""),
                        "symbol": str(row.get("symbol") or ""),
                        "side": str(row.get("side") or ""),
                        "price": float(row.get("execPrice", 0) or 0),
                        "qty": float(row.get("execQty", 0) or 0),
                        "fee": float(row.get("execFee", 0) or 0),
                        "is_maker": 1 if row.get("isMaker") else 0,
                        "ts_ms": float(row.get("execTime", 0) or 0),
                        "raw_json": json.dumps(row, default=str),
                    }
                )
                result.actions.append(f"recovered missed execution {exec_id} ({row.get('symbol')})")

    def _read_paper(self, result: ReconcileResult) -> None:
        account = self.execution.broker.account()
        result.wallet = {
            "equity_eur": account.get("equity_eur"),
            "available_eur": account.get("available_eur"),
            "source": account.get("source"),
        }
        result.positions = [
            {"symbol": symbol, **data} for symbol, data in account.get("positions", {}).items()
        ]
        result.orders = []

    # ---------------------------------------------------------------- repair
    async def _repair(self, result: ReconcileResult) -> None:
        exchange = {
            str(row.get("symbol")): row
            for row in result.positions
            if row.get("symbol")
        }
        local = {
            symbol: position
            for symbol, position in self.execution.positions.items()
            if position.state in (PositionState.OPEN, PositionState.EXITING)
        }

        for symbol, position in local.items():
            if symbol not in exchange:
                result.differences.append(
                    f"{symbol}: open locally ({position.side} {position.filled_qty:g}) "
                    f"but flat on the exchange"
                )
                await self._close_orphan_local(position, result)
                continue
            row = exchange[symbol]
            remote_qty = float(row.get("size", row.get("qty", 0)) or 0)
            if abs(remote_qty - position.filled_qty) > max(position.filled_qty * 0.01, 1e-9):
                result.differences.append(
                    f"{symbol}: quantity mismatch local {position.filled_qty:g} vs "
                    f"exchange {remote_qty:g} - adopting the exchange value"
                )
                position.filled_qty = remote_qty
                result.actions.append(f"{symbol}: local quantity set to exchange value")

        for symbol, row in exchange.items():
            if symbol in local:
                continue
            result.differences.append(
                f"{symbol}: open on the exchange but unknown locally - adopting it"
            )
            self._adopt(symbol, row, result)

        # Orders of ours still resting somewhere: cancel, we only use IOC.
        for row in result.orders:
            link = str(row.get("orderLinkId") or "")
            if link.startswith("AE") and self.rest is not None:
                try:
                    await self.rest.cancel_order(str(row.get("symbol")), link)
                    result.actions.append(f"cancelled stale order {link}")
                except (BybitError, BybitTransportError) as exc:
                    result.differences.append(f"could not cancel {link}: {exc}")

    async def _close_orphan_local(self, position: Position, result: ReconcileResult) -> None:
        """Locally open, exchange flat: the exit happened without us seeing it."""
        exits = self.repo.db.query(
            "SELECT * FROM executions WHERE order_link_id=? ORDER BY ts_ms",
            (position.exit_link_id,),
        ) if position.exit_link_id else []
        if exits:
            qty = sum(row["qty"] for row in exits)
            notional = sum(row["qty"] * row["price"] for row in exits)
            position.exit_price = notional / qty if qty else position.entry_price
            position.exit_qty = qty
            position.exit_fee_usdt = sum(row["fee"] for row in exits)
            position.closed_ms = max(row["ts_ms"] for row in exits)
            if not position.exit_ref_price:
                position.exit_ref_price = position.exit_price
            position.exit_reason = position.exit_reason or ExitReason.RECONCILE.value
            await self.execution._finalise(position)
            result.actions.append(
                f"{position.symbol}: closed from recovered executions at {position.exit_price:.6f}"
            )
            return

        # No trace at all: record it at the entry reference so the books stay
        # honest, and say plainly that the exit price is unknown.
        position.exit_price = position.exit_price or position.entry_price
        position.exit_ref_price = position.exit_ref_price or position.entry_ref_price
        position.exit_qty = position.filled_qty
        position.closed_ms = self.clock.now_ms()
        position.exit_reason = (
            f"{ExitReason.RECONCILE.value}: exchange reports flat, no execution found"
        )
        await self.execution._finalise(position)
        result.actions.append(
            f"{position.symbol}: force-closed at entry reference (no execution found)"
        )

    def _adopt(self, symbol: str, row: dict[str, Any], result: ReconcileResult) -> None:
        side = "LONG" if str(row.get("side")) in ("Buy", "LONG") else "SHORT"
        qty = float(row.get("size", row.get("qty", 0)) or 0)
        entry = float(row.get("entryPrice", row.get("entry_price", 0)) or 0)
        leverage = float(row.get("leverage", 0) or self.cfg.decide.leverage_default)
        notional = qty * entry * self.cfg.eur_per_usdt
        self.execution._seq += 1
        key = f"{self.repo.db.run_id}:{symbol}:adopted:{self.execution._seq:05d}"
        position = Position(
            position_key=key,
            symbol=symbol,
            side=side,
            qty=qty,
            filled_qty=qty,
            leverage=leverage,
            margin_eur=notional / max(leverage, 1.0),
            notional_eur=notional,
            entry_ref_price=entry,
            entry_price=entry,
            state=PositionState.OPEN,
            opened_ms=float(row.get("createdTime", self.clock.now_ms()) or self.clock.now_ms()),
            model_version="adopted",
            target_move_bps=self.cfg.decide.target_net_eur / max(notional, 1e-9) * 10_000.0,
            stop_bps=self.cfg.decide.stop_vol_multiple * 10.0,
            max_hold_s=self.cfg.decide.max_hold_s,
            target_eur=self.cfg.decide.target_net_eur,
            entry_reason="adopted during reconciliation",
        )
        self.execution.positions[symbol] = position
        self.repo.save_position(
            {
                "position_key": key,
                "symbol": symbol,
                "side": side,
                "qty": qty,
                "entry_price": entry,
                "leverage": leverage,
                "margin_eur": position.margin_eur,
                "opened_ts": position.opened_ms,
                "closed_ts": None,
                "status": "OPEN",
                "decision_id": None,
            }
        )
        result.actions.append(f"{symbol}: adopted exchange position {side} {qty:g}")
