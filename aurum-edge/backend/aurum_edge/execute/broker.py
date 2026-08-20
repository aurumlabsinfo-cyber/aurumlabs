"""Order types shared by paper and live, plus the paper broker.

The contract both brokers honour:

    OrderIntent -> submit() -> OrderAck -> (asynchronously) ExecutionEvent

An ``OrderAck`` means Bybit accepted the request.  It is **not** a fill, it does
not open a position, and nothing downstream may treat it as one.  A position
exists only once execution events have been received for it.  The paper broker
deliberately takes the same two steps, with latency in between, so the state
machine that runs in LIVE is the one exercised in PAPER.
"""

from __future__ import annotations

import asyncio
import hashlib
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Protocol

from ..config import Config
from ..scan.snapshot import MarketSnapshot
from ..util.clock import Clock
from ..util.logging_setup import get_logger

log = get_logger("execute.broker")


def make_order_link_id(run_id: str, position_key: str, purpose: str, attempt: int = 0) -> str:
    """Deterministic, idempotent client order id.

    The same (run, position, purpose, attempt) always produces the same id, so a
    retry after a reconnect can never open a second position: Bybit rejects the
    duplicate ``orderLinkId``.  Bybit allows 36 characters.
    """
    digest = hashlib.sha1(
        f"{run_id}|{position_key}|{purpose}|{attempt}".encode()
    ).hexdigest()[:18]
    prefix = "AE"
    tag = "E" if purpose == "ENTRY" else "X"
    return f"{prefix}{tag}{digest}"


@dataclass
class OrderIntent:
    order_link_id: str
    symbol: str
    side: str                       # Buy | Sell (exchange convention)
    qty: float
    qty_str: str
    purpose: str                    # ENTRY | EXIT
    position_key: str
    reduce_only: bool = False
    order_type: str = "Market"
    price: float | None = None
    leverage: float = 1.0
    margin_eur: float = 0.0
    decision_id: int | None = None
    created_ms: float = 0.0
    snapshot: MarketSnapshot | None = None

    def to_row(self, status: str) -> dict[str, Any]:
        return {
            "order_link_id": self.order_link_id,
            "exchange_order_id": None,
            "decision_id": self.decision_id,
            "symbol": self.symbol,
            "side": self.side,
            "order_type": self.order_type,
            "qty": self.qty,
            "price": self.price,
            "reduce_only": 1 if self.reduce_only else 0,
            "status": status,
            "intent_ts": self.created_ms,
            "ack_ts": None,
            "filled_ts": None,
            "filled_qty": 0.0,
            "avg_price": None,
            "fee_usdt": 0.0,
            "reject_reason": None,
            "raw_json": None,
        }


@dataclass
class OrderAck:
    order_link_id: str
    accepted: bool
    ts_ms: float
    exchange_order_id: str | None = None
    reject_reason: str = ""
    raw: dict[str, Any] = field(default_factory=dict)


@dataclass
class ExecutionEvent:
    exec_id: str
    order_link_id: str
    symbol: str
    side: str
    price: float
    qty: float
    fee: float                       # in quote currency (USDT)
    ts_ms: float
    is_maker: bool = False
    exchange_order_id: str | None = None
    raw: dict[str, Any] = field(default_factory=dict)


ExecutionHandler = Callable[[ExecutionEvent], Awaitable[None]]


class Broker(Protocol):
    name: str

    async def start(self) -> None: ...
    async def stop(self) -> None: ...
    async def submit(self, intent: OrderIntent) -> OrderAck: ...
    def set_execution_handler(self, handler: ExecutionHandler) -> None: ...
    def account(self) -> dict[str, Any]: ...
    def health(self) -> dict[str, Any]: ...


# ---------------------------------------------------------------------------
# paper broker
# ---------------------------------------------------------------------------

@dataclass
class PaperPosition:
    symbol: str
    side: str
    qty: float
    entry_price: float
    margin_eur: float
    leverage: float


class PaperBroker:
    """A simulated Bybit that fills against the snapshot's own depth curve.

    It is honest about being simulated: ``name`` is ``paper`` and every event it
    emits is tagged, so nothing downstream can mistake a paper fill for a real
    one.  Fill prices come from the real book that was in the snapshot, plus the
    configured latency, so costs are not flattering.
    """

    name = "paper"

    def __init__(self, cfg: Config, clock: Clock) -> None:
        self.cfg = cfg
        self.clock = clock
        self.equity_eur = cfg.paper_start_equity_eur
        self.used_margin_eur = 0.0
        self.positions: dict[str, PaperPosition] = {}
        self.realized_eur = 0.0
        self.fees_paid_eur = 0.0
        self._handler: ExecutionHandler | None = None
        self._tasks: set[asyncio.Task[None]] = set()
        self._seq = 0
        self.submitted = 0
        self.rejected = 0
        self.filled = 0

    # ---------------------------------------------------------------- lifecycle
    async def start(self) -> None:
        log.info(
            "PAPER broker ready: equity %.2f EUR (simulated fills against the real book)",
            self.equity_eur,
        )

    async def stop(self) -> None:
        for task in list(self._tasks):
            task.cancel()
        for task in list(self._tasks):
            try:
                await task
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
                pass
        self._tasks.clear()

    def set_execution_handler(self, handler: ExecutionHandler) -> None:
        self._handler = handler

    # ---------------------------------------------------------------- orders
    async def submit(self, intent: OrderIntent) -> OrderAck:
        self.submitted += 1
        now = self.clock.now_ms()
        snap = intent.snapshot
        if snap is None:
            self.rejected += 1
            return OrderAck(intent.order_link_id, False, now, reject_reason="no snapshot attached")
        if snap.book_state != "OK":
            self.rejected += 1
            return OrderAck(
                intent.order_link_id, False, now,
                reject_reason=f"book not in sync ({snap.book_state})",
            )
        if intent.qty <= 0:
            self.rejected += 1
            return OrderAck(intent.order_link_id, False, now, reject_reason="qty is zero")

        self._seq += 1
        ack = OrderAck(
            order_link_id=intent.order_link_id,
            accepted=True,
            ts_ms=now,
            exchange_order_id=f"paper-{self._seq:08d}",
            raw={"simulated": True},
        )
        task = asyncio.create_task(self._fill_later(intent, ack))
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        return ack

    async def _fill_later(self, intent: OrderIntent, ack: OrderAck) -> None:
        """Fills arrive after latency, exactly like the private stream."""
        await asyncio.sleep(self.cfg.execute.paper_latency_ms / 1000.0)
        snap = intent.snapshot
        assert snap is not None
        side_hint = "LONG" if intent.side == "Buy" else "SHORT"
        notional_usd = intent.qty * snap.mid
        slip_bps = snap.slippage_bps_for(side_hint, notional_usd)
        direction = 1.0 if intent.side == "Buy" else -1.0
        price = snap.mid * (1.0 + direction * slip_bps / 10_000.0)
        fee = notional_usd * self.cfg.decide.taker_fee_rate

        event = ExecutionEvent(
            exec_id=f"paper-{intent.order_link_id}-{self._seq}",
            order_link_id=intent.order_link_id,
            symbol=intent.symbol,
            side=intent.side,
            price=price,
            qty=intent.qty,
            fee=fee,
            ts_ms=self.clock.now_ms(),
            is_maker=False,
            exchange_order_id=ack.exchange_order_id,
            raw={"simulated": True, "slippage_bps": round(slip_bps, 4)},
        )
        self._apply_fill(intent, event)
        self.filled += 1
        if self._handler is not None:
            await self._handler(event)

    def _apply_fill(self, intent: OrderIntent, event: ExecutionEvent) -> None:
        eur = self.cfg.eur_per_usdt
        fee_eur = event.fee * eur
        self.equity_eur -= fee_eur
        self.fees_paid_eur += fee_eur
        existing = self.positions.get(intent.symbol)

        if intent.reduce_only or (existing and existing.side != _side_word(intent.side)):
            if existing is None:
                return
            direction = 1.0 if existing.side == "LONG" else -1.0
            gross = (event.price - existing.entry_price) * direction * min(event.qty, existing.qty)
            self.equity_eur += gross * eur
            self.realized_eur += gross * eur
            self.used_margin_eur = max(self.used_margin_eur - existing.margin_eur, 0.0)
            remaining = existing.qty - event.qty
            if remaining <= 1e-12:
                self.positions.pop(intent.symbol, None)
            else:
                existing.qty = remaining
        else:
            self.used_margin_eur += intent.margin_eur
            self.positions[intent.symbol] = PaperPosition(
                symbol=intent.symbol,
                side=_side_word(intent.side),
                qty=event.qty,
                entry_price=event.price,
                margin_eur=intent.margin_eur,
                leverage=intent.leverage,
            )

    # ---------------------------------------------------------------- account
    def account(self) -> dict[str, Any]:
        return {
            "source": "paper",
            "equity_eur": self.equity_eur,
            "available_eur": max(self.equity_eur - self.used_margin_eur, 0.0),
            "used_margin_eur": self.used_margin_eur,
            "realized_eur": self.realized_eur,
            "fees_eur": self.fees_paid_eur,
            "positions": {
                symbol: {
                    "side": p.side,
                    "qty": p.qty,
                    "entry_price": p.entry_price,
                    "margin_eur": p.margin_eur,
                    "leverage": p.leverage,
                }
                for symbol, p in self.positions.items()
            },
        }

    def health(self) -> dict[str, Any]:
        return {
            "broker": "paper",
            "connected": True,
            "submitted": self.submitted,
            "filled": self.filled,
            "rejected": self.rejected,
            "note": "simulated venue - fills are computed from the real Bybit book",
        }


def _side_word(exchange_side: str) -> str:
    return "LONG" if exchange_side == "Buy" else "SHORT"
