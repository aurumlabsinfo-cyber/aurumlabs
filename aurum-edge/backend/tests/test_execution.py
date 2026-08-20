"""The Execution Core: an ACK is not a fill, and the money always adds up."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from aurum_edge.config import Config
from aurum_edge.decide.decision_core import DecisionCore
from aurum_edge.decide.model import champion_v1
from aurum_edge.decide.risk import RiskEngine
from aurum_edge.execute.broker import (
    ExecutionEvent, OrderAck, OrderIntent, PaperBroker, make_order_link_id,
)
from aurum_edge.execute.execution_core import ExecutionCore, ExitReason, PositionState
from aurum_edge.storage.repo import Repo
from aurum_edge.util.clock import Clock

from .test_decision import make_snapshot, opportunity, rich_account


class ManualBroker:
    """Acks orders; fills only when the test says so."""

    name = "manual"

    def __init__(self, clock: Clock, accept: bool = True) -> None:
        self.clock = clock
        self.accept = accept
        self.intents: list[OrderIntent] = []
        self.handler = None
        self.equity_eur = 1000.0
        self.used_margin_eur = 0.0

    async def start(self) -> None: ...
    async def stop(self) -> None: ...

    def set_execution_handler(self, handler) -> None:
        self.handler = handler

    async def submit(self, intent: OrderIntent) -> OrderAck:
        self.intents.append(intent)
        return OrderAck(
            intent.order_link_id, self.accept, self.clock.now_ms(),
            exchange_order_id="manual-1" if self.accept else None,
            reject_reason="" if self.accept else "rejected by test",
        )

    async def fill(self, intent: OrderIntent, price: float, fee: float = 0.0,
                   qty: float | None = None, exec_id: str | None = None) -> None:
        assert self.handler is not None
        await self.handler(
            ExecutionEvent(
                exec_id=exec_id or f"exec-{len(self.intents)}-{price}",
                order_link_id=intent.order_link_id,
                symbol=intent.symbol,
                side=intent.side,
                price=price,
                qty=qty if qty is not None else intent.qty,
                fee=fee,
                ts_ms=self.clock.now_ms(),
            )
        )

    def account(self) -> dict[str, Any]:
        return {
            "source": "manual", "equity_eur": self.equity_eur,
            "available_eur": self.equity_eur - self.used_margin_eur,
            "used_margin_eur": self.used_margin_eur, "positions": {},
        }

    def health(self) -> dict[str, Any]:
        return {"broker": "manual", "connected": True}


def build(cfg: Config, repo: Repo, broker: Any, allow: bool = True) -> ExecutionCore:
    execution = ExecutionCore(
        cfg, Clock(), broker, repo, RiskEngine(cfg),
        gate=(lambda: (allow, [] if allow else ["test gate closed"])),
    )
    return execution


def make_decision(cfg: Config, **snapshot_overrides):
    decision_core = DecisionCore(cfg, RiskEngine(cfg))
    snap = make_snapshot(**snapshot_overrides)
    decision = decision_core.evaluate(
        opportunity(snap), champion_v1(), rich_account(), now_mono=100.0
    )
    assert decision.is_trade, decision.reasons
    return decision


# ----------------------------------------------------------- ACK is not a fill

async def test_ack_alone_does_not_open_a_position(cfg: Config, repo: Repo) -> None:
    broker = ManualBroker(Clock())
    execution = build(cfg, repo, broker)
    decision = make_decision(cfg)

    position = await execution.open_position(decision)     # acked, never filled
    assert position is None, "an ACK must not create a position"
    assert execution.positions == {}
    assert execution.trades_opened == 0

    order = repo.db.query_one("SELECT * FROM orders")
    assert order["status"] == "UNCONFIRMED"
    assert order["ack_ts"] is not None
    assert order["filled_qty"] == 0
    assert repo.db.query_one("SELECT COUNT(*) AS n FROM executions")["n"] == 0


async def test_a_rejected_order_leaves_no_position(cfg: Config, repo: Repo) -> None:
    broker = ManualBroker(Clock(), accept=False)
    execution = build(cfg, repo, broker)
    position = await execution.open_position(make_decision(cfg))
    assert position is None
    assert execution.entry_failures == 1
    assert repo.db.query_one("SELECT status FROM orders")["status"] == "REJECTED"


async def test_a_fill_confirms_the_position(cfg: Config, repo: Repo) -> None:
    broker = ManualBroker(Clock())
    execution = build(cfg, repo, broker)
    decision = make_decision(cfg)

    task = asyncio.create_task(execution.open_position(decision))
    await asyncio.sleep(0.05)
    assert broker.intents, "no order was submitted"
    await broker.fill(broker.intents[0], price=100.02, fee=0.05)
    position = await task

    assert position is not None
    assert position.state is PositionState.OPEN
    assert position.entry_price == pytest.approx(100.02)
    assert position.filled_qty == pytest.approx(decision.qty)
    assert execution.trades_opened == 1
    assert repo.db.query_one("SELECT status FROM orders WHERE order_link_id=?",
                             (position.entry_link_id,))["status"] == "FILLED"
    assert repo.db.query_one("SELECT COUNT(*) AS n FROM executions")["n"] == 1
    assert repo.db.query_one("SELECT status FROM positions")["status"] == "OPEN"


async def test_partial_fills_average_into_the_entry(cfg: Config, repo: Repo) -> None:
    broker = ManualBroker(Clock())
    execution = build(cfg, repo, broker)
    decision = make_decision(cfg)
    task = asyncio.create_task(execution.open_position(decision))
    await asyncio.sleep(0.05)
    half = decision.qty / 2
    await broker.fill(broker.intents[0], price=100.0, qty=half, exec_id="a")
    await broker.fill(broker.intents[0], price=100.10, qty=half, exec_id="b")
    position = await task
    assert position is not None
    assert position.filled_qty == pytest.approx(decision.qty)
    assert position.entry_price == pytest.approx(100.05, rel=1e-6)


async def test_duplicate_executions_are_ignored(cfg: Config, repo: Repo) -> None:
    """After a reconnect Bybit can resend a fill: it must not double the position."""
    broker = ManualBroker(Clock())
    execution = build(cfg, repo, broker)
    decision = make_decision(cfg)
    task = asyncio.create_task(execution.open_position(decision))
    await asyncio.sleep(0.05)
    await broker.fill(broker.intents[0], price=100.0, exec_id="same-exec")
    position = await task
    assert position is not None
    quantity = position.filled_qty
    await broker.fill(broker.intents[0], price=100.0, exec_id="same-exec")
    assert position.filled_qty == pytest.approx(quantity)
    assert repo.db.query_one("SELECT COUNT(*) AS n FROM executions")["n"] == 1


def test_order_link_ids_are_deterministic_and_idempotent() -> None:
    first = make_order_link_id("run-1", "run-1:BTCUSDT:00001", "ENTRY")
    again = make_order_link_id("run-1", "run-1:BTCUSDT:00001", "ENTRY")
    exit_id = make_order_link_id("run-1", "run-1:BTCUSDT:00001", "EXIT")
    other = make_order_link_id("run-2", "run-1:BTCUSDT:00001", "ENTRY")
    assert first == again, "a retry must reuse the same id"
    assert first != exit_id != other
    assert len(first) <= 36 and first.startswith("AE")


# -------------------------------------------------------------- exits and P&L

async def open_position(cfg: Config, repo: Repo, entry_price: float = 100.0,
                        fee: float = 0.05, **snapshot_overrides):
    broker = ManualBroker(Clock())
    execution = build(cfg, repo, broker)
    decision = make_decision(cfg, **snapshot_overrides)
    task = asyncio.create_task(execution.open_position(decision))
    await asyncio.sleep(0.05)
    await broker.fill(broker.intents[0], price=entry_price, fee=fee)
    position = await task
    assert position is not None
    return execution, broker, position, decision


async def test_pnl_decomposition_is_exact(cfg: Config, repo: Repo) -> None:
    execution, broker, position, decision = await open_position(cfg, repo, entry_price=100.02)
    exit_snapshot = make_snapshot(bid=100.60, ask=100.62, mid=100.61)

    await execution.close_position(position, ExitReason.TARGET, exit_snapshot, "test")
    await asyncio.sleep(0.02)
    await broker.fill(broker.intents[1], price=100.58, fee=0.06)
    await asyncio.sleep(0.02)

    trade = repo.db.query_one("SELECT * FROM trades")
    assert trade is not None
    identity = (
        trade["gross_pnl_eur"] - trade["slippage_eur"] - trade["fees_eur"]
    )
    assert identity == pytest.approx(trade["net_pnl_eur"], abs=1e-12)
    # and the net equals what the fills actually produced, minus fees
    realised = (trade["exit_price"] - trade["entry_price"]) * trade["qty"] - (
        trade["entry_fee_eur"] + trade["exit_fee_eur"]
    )
    assert trade["net_pnl_eur"] == pytest.approx(realised, rel=1e-9)
    assert trade["fees_eur"] == pytest.approx(0.11, abs=1e-9)
    assert trade["hold_s"] >= 0
    assert trade["exit_reason"].startswith("TARGET")
    assert trade["entry_reason"]
    assert trade["prediction_json"] and trade["features_json"]
    assert repo.db.query_one("SELECT status FROM positions")["status"] == "CLOSED"


async def test_a_loss_is_recorded_as_a_loss(cfg: Config, repo: Repo) -> None:
    execution, broker, position, _ = await open_position(cfg, repo, entry_price=100.0)
    exit_snapshot = make_snapshot(bid=99.50, ask=99.52, mid=99.51)
    await execution.close_position(position, ExitReason.STOP, exit_snapshot, "stop hit")
    await asyncio.sleep(0.02)
    await broker.fill(broker.intents[1], price=99.48, fee=0.06)
    await asyncio.sleep(0.02)
    trade = repo.db.query_one("SELECT * FROM trades")
    assert trade["net_pnl_eur"] < 0
    assert trade["label"] == 0
    assert execution.risk.realized_today_eur < 0
    assert execution.risk.cooldown_remaining("BTCUSDT", Clock().mono()) > 0


async def test_stop_exit_triggers_on_an_adverse_move(cfg: Config, repo: Repo) -> None:
    execution, broker, position, _ = await open_position(cfg, repo, entry_price=100.0)
    stop_move = position.stop_bps / 10_000.0
    bad_price = 100.0 * (1 - stop_move * 1.5)
    adverse = make_snapshot(bid=bad_price, ask=bad_price + 0.02, mid=bad_price + 0.01)
    position.opened_ms = adverse.ts_local_ms - 5_000

    await execution.manage({"BTCUSDT": adverse}, champion_v1())
    assert position.state is PositionState.EXITING
    assert "STOP" in position.exit_reason


async def test_edge_gone_closes_before_the_target(cfg: Config, repo: Repo) -> None:
    execution, broker, position, _ = await open_position(cfg, repo, entry_price=100.0)
    # flow reverses hard while the price has barely moved
    reversed_flow = make_snapshot(
        ret_250ms_bps=-4.0, ret_1s_bps=-14.0, ret_3s_bps=-26.0, ret_5s_bps=-30.0,
        ret_15s_bps=-20.0, ret_60s_bps=-10.0, ofi_1s=-0.7, ofi_5s=-0.8,
        imbalance_top=-0.5, imbalance_depth=-0.45, aggression_5s=-0.7,
        aggression_60s=-0.5, microprice_edge_bps=-0.6, bid=99.99, ask=100.01, mid=100.0,
    )
    position.opened_ms = reversed_flow.ts_local_ms - 5_000
    await execution.manage({"BTCUSDT": reversed_flow}, champion_v1())
    assert position.state is PositionState.EXITING
    assert "EDGE_GONE" in position.exit_reason


async def test_a_position_is_never_held_waiting_to_come_back(cfg: Config, repo: Repo) -> None:
    """Time and edge both bound the trade; neither waits for a recovery."""
    execution, broker, position, _ = await open_position(cfg, repo, entry_price=100.0)
    flat = make_snapshot(
        ret_1s_bps=-0.5, ret_3s_bps=-1.0, ret_5s_bps=-1.5, ofi_1s=-0.1, ofi_5s=-0.15,
        aggression_5s=-0.1, imbalance_top=-0.05, bid=99.90, ask=99.92, mid=99.91,
    )
    position.opened_ms = flat.ts_local_ms - (position.max_hold_s + 5) * 1000
    await execution.manage({"BTCUSDT": flat}, champion_v1())
    assert position.state is PositionState.EXITING
    assert position.exit_reason.split(":")[0] in {"MAX_HOLD", "EDGE_GONE", "STOP"}


async def test_health_failure_flattens_and_blocks(cfg: Config, repo: Repo) -> None:
    broker = ManualBroker(Clock())
    execution = ExecutionCore(
        cfg, Clock(), broker, repo, RiskEngine(cfg), gate=lambda: (True, [])
    )
    decision = make_decision(cfg)
    task = asyncio.create_task(execution.open_position(decision))
    await asyncio.sleep(0.05)
    await broker.fill(broker.intents[0], price=100.0, fee=0.05)
    position = await task
    assert position is not None

    execution.gate = lambda: (False, ["public_ws DOWN: all connections are down"])
    snapshot = make_snapshot()
    position.opened_ms = snapshot.ts_local_ms - 3_000
    await execution.manage({"BTCUSDT": snapshot}, champion_v1())
    assert position.state is PositionState.EXITING
    assert "HEALTH" in position.exit_reason

    blocked = await execution.open_position(make_decision(cfg))
    assert blocked is None


async def test_blocked_entries_are_respected(cfg: Config, repo: Repo) -> None:
    broker = ManualBroker(Clock())
    execution = build(cfg, repo, broker)
    execution.block_entries("reconciling")
    assert await execution.open_position(make_decision(cfg)) is None
    assert broker.intents == []


async def test_flatten_all_closes_everything(cfg: Config, repo: Repo) -> None:
    execution, broker, position, _ = await open_position(cfg, repo)
    closed = await execution.flatten_all(ExitReason.FLATTEN, {"BTCUSDT": make_snapshot()})
    assert closed == 1
    assert position.state is PositionState.EXITING


# ------------------------------------------------------------------- paper broker

async def test_paper_broker_walks_the_real_book(cfg: Config, repo: Repo) -> None:
    clock = Clock()
    broker = PaperBroker(cfg, clock)
    await broker.start()
    execution = build(cfg, repo, broker)
    decision = make_decision(cfg)

    position = await execution.open_position(decision)
    assert position is not None
    assert position.state is PositionState.OPEN
    # a buy fills above the mid, never at it
    assert position.entry_price > decision.snapshot.mid
    assert position.entry_fee_usdt > 0
    account = broker.account()
    assert account["source"] == "paper"
    assert account["used_margin_eur"] > 0
    assert broker.health()["broker"] == "paper"

    exit_snapshot = make_snapshot(bid=100.50, ask=100.52, mid=100.51)
    await execution.close_position(position, ExitReason.TARGET, exit_snapshot)
    await asyncio.sleep(0.05)
    trade = repo.db.query_one("SELECT * FROM trades")
    assert trade is not None
    assert trade["gross_pnl_eur"] - trade["slippage_eur"] - trade["fees_eur"] == pytest.approx(
        trade["net_pnl_eur"], abs=1e-12
    )
    assert trade["slippage_entry_bps"] >= 0, "a taker entry cannot beat the mid"
    await broker.stop()


async def test_paper_broker_refuses_to_fill_on_a_broken_book(cfg: Config, repo: Repo) -> None:
    clock = Clock()
    broker = PaperBroker(cfg, clock)
    await broker.start()
    intent = OrderIntent(
        order_link_id="AEtest", symbol="BTCUSDT", side="Buy", qty=1.0, qty_str="1",
        purpose="ENTRY", position_key="k", snapshot=make_snapshot(book_state="RESYNC"),
    )
    ack = await broker.submit(intent)
    assert ack.accepted is False
    assert "not in sync" in ack.reject_reason
    await broker.stop()
