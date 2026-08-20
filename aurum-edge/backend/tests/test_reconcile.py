"""Reconciliation: local state must match Bybit, or trading stops."""

from __future__ import annotations

import asyncio
import dataclasses

import pytest

from aurum_edge.config import BybitConfig, Config
from aurum_edge.decide.risk import RiskEngine
from aurum_edge.execute.broker import PaperBroker
from aurum_edge.execute.execution_core import ExecutionCore, PositionState
from aurum_edge.execute.reconcile import Reconciler
from aurum_edge.scan.bybit_rest import BybitRest
from aurum_edge.storage.repo import Repo
from aurum_edge.util.clock import Clock

from .fakebybit.server import FakeBybit
from .test_execution import ManualBroker, build, make_decision


async def opened_position(cfg: Config, repo: Repo):
    broker = ManualBroker(Clock())
    execution = build(cfg, repo, broker)
    decision = make_decision(cfg)
    task = asyncio.create_task(execution.open_position(decision))
    await asyncio.sleep(0.05)
    await broker.fill(broker.intents[0], price=100.0, fee=0.05)
    position = await task
    assert position is not None
    return execution, broker, position


async def test_paper_reconcile_succeeds_and_unblocks(cfg: Config, repo: Repo) -> None:
    clock = Clock()
    broker = PaperBroker(cfg, clock)
    execution = ExecutionCore(cfg, clock, broker, repo, RiskEngine(cfg))
    reconciler = Reconciler(cfg, clock, execution, repo)

    execution.block_entries("startup")
    result = await reconciler.reconcile("startup")

    assert result.ok is True
    assert execution.entries_blocked is False
    row = repo.db.query_one("SELECT * FROM reconciliations")
    assert row["trigger"] == "startup"
    assert row["outcome"] == "OK"


async def test_local_position_missing_on_the_exchange_is_closed(
    cfg: Config, repo: Repo
) -> None:
    execution, broker, position = await opened_position(cfg, repo)
    reconciler = Reconciler(cfg, Clock(), execution, repo)

    # the paper venue has no position under that symbol
    broker_account = {"source": "manual", "positions": {}}
    execution.broker.account = lambda: {**broker_account, "equity_eur": 1000.0,
                                        "available_eur": 1000.0, "used_margin_eur": 0.0}

    result = await reconciler.reconcile("test")
    assert result.ok is True
    assert any("flat on the exchange" in d for d in result.differences)
    assert position.state is PositionState.CLOSED
    trade = repo.db.query_one("SELECT * FROM trades")
    assert trade is not None
    assert "RECONCILE" in trade["exit_reason"]
    assert execution.positions == {}


async def test_exchange_position_unknown_locally_is_adopted(cfg: Config, repo: Repo) -> None:
    clock = Clock()
    broker = PaperBroker(cfg, clock)
    execution = ExecutionCore(cfg, clock, broker, repo, RiskEngine(cfg))
    reconciler = Reconciler(cfg, clock, execution, repo)

    broker.account = lambda: {
        "source": "paper", "equity_eur": 1000.0, "available_eur": 900.0,
        "used_margin_eur": 100.0,
        "positions": {"ETHUSDT": {"side": "LONG", "qty": 0.5, "entry_price": 2000.0,
                                  "margin_eur": 100.0, "leverage": 10.0}},
    }
    result = await reconciler.reconcile("test")

    assert result.ok is True
    assert any("unknown locally" in d for d in result.differences)
    assert "ETHUSDT" in execution.positions
    adopted = execution.positions["ETHUSDT"]
    assert adopted.state is PositionState.OPEN
    assert adopted.filled_qty == 0.5
    assert repo.db.query_one("SELECT * FROM positions WHERE symbol='ETHUSDT'") is not None


async def test_quantity_mismatch_takes_the_exchange_value(cfg: Config, repo: Repo) -> None:
    execution, broker, position = await opened_position(cfg, repo)
    reconciler = Reconciler(cfg, Clock(), execution, repo)
    exchange_qty = position.filled_qty * 0.5
    execution.broker.account = lambda: {
        "source": "manual", "equity_eur": 1000.0, "available_eur": 900.0,
        "used_margin_eur": 100.0,
        "positions": {"BTCUSDT": {"side": position.side, "qty": exchange_qty,
                                  "entry_price": position.entry_price,
                                  "margin_eur": 50.0, "leverage": 10.0}},
    }
    result = await reconciler.reconcile("test")
    assert any("quantity mismatch" in d for d in result.differences)
    assert position.filled_qty == pytest.approx(exchange_qty)


async def test_a_failed_reconciliation_keeps_entries_blocked(cfg: Config, repo: Repo) -> None:
    clock = Clock()
    broker = PaperBroker(cfg, clock)
    execution = ExecutionCore(cfg, clock, broker, repo, RiskEngine(cfg))
    reconciler = Reconciler(cfg, clock, execution, repo)

    def explode() -> dict:
        raise RuntimeError("wallet unavailable")

    broker.account = explode
    result = await reconciler.reconcile("test")

    assert result.ok is False
    assert "wallet unavailable" in result.error
    assert execution.entries_blocked is True
    assert "reconciliation failed" in execution.entries_blocked_reason
    row = repo.db.query_one("SELECT outcome FROM reconciliations")
    assert row["outcome"].startswith("FAILED")


async def test_live_reconcile_reads_bybit_and_recovers_missed_fills(
    cfg: Config, repo: Repo, fake_bybit: FakeBybit
) -> None:
    live_cfg = dataclasses.replace(cfg, mode="live")
    clock = Clock()
    rest = BybitRest(
        BybitConfig(api_key=fake_bybit.api_key, api_secret=fake_bybit.api_secret),
        base_url=fake_bybit.rest_url,
    )
    broker = PaperBroker(live_cfg, clock)     # only the reads matter here
    execution = ExecutionCore(live_cfg, clock, broker, repo, RiskEngine(live_cfg))
    reconciler = Reconciler(live_cfg, clock, execution, repo, rest=rest)

    # a fill that happened while we were disconnected
    await fake_bybit.push_execution(
        symbol="BTCUSDT", side="Buy", qty=0.01, price=100.0, order_link_id="AEmissed",
        exec_id="missed-1",
    )
    await fake_bybit.push_position("BTCUSDT", "Buy", 0.01, 100.0)

    result = await reconciler.reconcile("reconnect")

    assert result.ok is True
    assert result.wallet.get("totalEquity") == "1000"
    assert any("recovered missed execution" in action for action in result.actions)
    assert repo.execution_exists("missed-1"), "a fill seen only by REST must still be stored"
    assert "BTCUSDT" in execution.positions, "the exchange position must be adopted"
    await rest.close()


async def test_reconcile_is_recorded_for_audit(cfg: Config, repo: Repo) -> None:
    clock = Clock()
    broker = PaperBroker(cfg, clock)
    execution = ExecutionCore(cfg, clock, broker, repo, RiskEngine(cfg))
    reconciler = Reconciler(cfg, clock, execution, repo)
    await reconciler.reconcile("one")
    await reconciler.reconcile("two")
    rows = repo.db.query("SELECT trigger FROM reconciliations ORDER BY id")
    assert [r["trigger"] for r in rows] == ["one", "two"]
    assert reconciler.runs == 2
