"""Strategy lifecycle, post-mortem and the conditional reset.

Three blueprint acceptance checks live here:

* a forced wallet depletion triggers cycle end, post-mortem and research state
  without deleting history;
* no automatic reset to €100 occurs before post-mortem and strategy eligibility;
* state changes are persisted, timestamped, and reversible only through a new
  version.
"""

from __future__ import annotations

import pytest

from aurum.agents.postmortem import Cause, CyclePostMortemAgent
from aurum.config import Config
from aurum.domain import (
    CycleState,
    Direction,
    ExitReason,
    MetricSet,
    PaperTrade,
    Regime,
    StrategyState,
)
from aurum.research.hypotheses import Hypothesis, PercentileCondition, new_hypothesis_id
from aurum.storage.repositories import Repositories
from aurum.strategies.lifecycle import IllegalTransition, StrategyLifecycle
from aurum.wallet.postmortem import CycleManager
from aurum.wallet.virtual_wallet import VirtualWallet


def hypothesis(symbol: str = "BTCUSDT") -> Hypothesis:
    return Hypothesis(
        hypothesis_id=new_hypothesis_id("test"),
        agent="microstructure",
        family="microstructure",
        signal_symbol=symbol,
        execution_symbol=symbol,
        direction=Direction.LONG,
        conditions=[PercentileCondition("ofi_norm_1s", ">=", 80.0, threshold=0.4)],
        horizon_ms=5_000,
    )


def trade(
    net_eur: float,
    *,
    symbol: str = "BTCUSDT",
    gross_bps: float = 5.0,
    cost_bps: float = 11.0,
    slippage: float = 0.5,
    regime: Regime = Regime.NORMAL_RANGE,
    expected_bps: float = 20.0,
    index: int = 0,
) -> PaperTrade:
    return PaperTrade(
        trade_id=f"trd-{index}", position_id=f"pos-{index}", symbol=symbol,
        direction=Direction.LONG, qty=0.001,
        entry_ts_ms=1_000 + index * 10_000, exit_ts_ms=6_000 + index * 10_000,
        requested_entry_price=100.0, entry_price=100.05,
        requested_exit_price=100.1, exit_price=100.1,
        gross_pnl_eur=net_eur + 0.02, fees_eur=0.02, net_pnl_eur=net_eur,
        return_bps=gross_bps, net_return_bps=gross_bps - cost_bps,
        entry_slippage_bps=slippage, exit_slippage_bps=slippage, cost_bps=cost_bps,
        exit_reason=ExitReason.HORIZON, strategy_id="str-1", strategy_version=1,
        hypothesis_id="hyp-1", signal_id="sig-1", cycle_id=1, regime=regime,
        cost_model_version="cost-v1", expected_edge_bps=expected_bps,
    )


@pytest.fixture
def lifecycle(repos: Repositories) -> StrategyLifecycle:
    return StrategyLifecycle(repos.strategies)


@pytest.fixture
def manager(config: Config, repos: Repositories, lifecycle: StrategyLifecycle):
    wallet = VirtualWallet(repos.wallet, starting_balance=config.wallet.starting_balance_eur)
    cycle_manager = CycleManager(config, wallet, lifecycle, repos)
    cycle_manager.start()
    return cycle_manager, wallet, lifecycle, repos, config


# ------------------------------------------------------------------ lifecycle


def test_only_a_champion_may_trade(lifecycle: StrategyLifecycle) -> None:
    strategy = lifecycle.create(hypothesis(), MetricSet(samples=100), 1.0, cycle_id=1)
    for state in (StrategyState.CANDIDATE, StrategyState.CHALLENGER, StrategyState.SHADOW):
        lifecycle.transition(strategy, state, "advancing")
        assert not strategy.can_trade, f"{state.value} must not reach the wallet"
    lifecycle.transition(strategy, StrategyState.CHAMPION, "promoted")
    assert strategy.can_trade
    assert lifecycle.champion() is strategy


def test_illegal_transitions_are_refused(lifecycle: StrategyLifecycle) -> None:
    strategy = lifecycle.create(hypothesis(), MetricSet(samples=100), 1.0, cycle_id=1)
    with pytest.raises(IllegalTransition, match="not a legal transition"):
        lifecycle.transition(strategy, StrategyState.CHAMPION, "skipping every gate")
    assert strategy.state is StrategyState.RESEARCH

    lifecycle.transition(strategy, StrategyState.REJECTED, "no")
    with pytest.raises(IllegalTransition):
        lifecycle.transition(strategy, StrategyState.CANDIDATE, "reviving a rejection")


def test_every_transition_is_versioned_and_persisted(
    lifecycle: StrategyLifecycle, repos: Repositories
) -> None:
    strategy = lifecycle.create(hypothesis(), MetricSet(samples=100), 1.0, cycle_id=1)
    lifecycle.transition(strategy, StrategyState.CANDIDATE, "passed validation")
    lifecycle.transition(strategy, StrategyState.CHALLENGER, "ranked above the incumbent")

    versions = repos.strategies.versions(strategy.strategy_id)
    assert len(versions) == 3           # creation plus two transitions
    assert versions[0]["state"] == "CHALLENGER"
    assert versions[0]["previous_state"] == "CANDIDATE"
    assert versions[0]["reason"] == "ranked above the incumbent"
    assert versions[0]["created_ms"] > 0
    assert versions[0]["definition"]["signal_symbol"] == "BTCUSDT"
    # Versions increase and never overwrite.
    assert [v["version"] for v in versions] == [3, 2, 1]


def test_a_strategy_survives_a_restart(lifecycle: StrategyLifecycle, repos: Repositories) -> None:
    h = hypothesis()
    strategy = lifecycle.create(h, MetricSet(samples=200, net_edge_bps=3.0), 2.5, cycle_id=1)
    for state in (StrategyState.CANDIDATE, StrategyState.CHALLENGER, StrategyState.SHADOW,
                  StrategyState.CHAMPION):
        lifecycle.transition(strategy, state, "advancing")

    reloaded = StrategyLifecycle(repos.strategies)
    restored = reloaded.restore(repos.strategies.list_strategies(), {h.hypothesis_id: h})
    assert restored == 1
    champion = reloaded.champion()
    assert champion is not None
    assert champion.strategy_id == strategy.strategy_id
    assert champion.validated_metrics.net_edge_bps == 3.0


# ----------------------------------------------------------------- post-mortem


def test_post_mortem_refuses_to_attribute_without_evidence(repos: Repositories) -> None:
    agent = CyclePostMortemAgent(repos.research)
    post = agent.analyse(cycle_id=1, trades=[trade(-1.0, index=i) for i in range(3)],
                         starting_balance=100.0, final_equity=97.0)
    assert post.primary_cause == Cause.INSUFFICIENT_EVIDENCE
    assert "too few to attribute" in post.causes[0].detail


def test_post_mortem_identifies_costs_eating_a_real_edge(repos: Repositories) -> None:
    agent = CyclePostMortemAgent(repos.research)
    # Direction right (+5 bps gross), costs 11 bps: a losing cycle with a real edge.
    trades = [trade(-0.4, gross_bps=5.0, cost_bps=11.0, index=i) for i in range(20)]
    post = agent.analyse(cycle_id=1, trades=trades, starting_balance=100.0, final_equity=92.0)
    assert post.primary_cause == Cause.EXECUTION_COST
    assert "direction was right" in post.causes[0].detail
    assert any("larger validated edge" in r for r in post.recommendations)


def test_post_mortem_identifies_concentration(repos: Repositories) -> None:
    agent = CyclePostMortemAgent(repos.research)
    trades = [trade(0.05, symbol="BTCUSDT", gross_bps=12.0, index=i) for i in range(15)]
    trades += [trade(-3.0, symbol="DOGEUSDT", gross_bps=-40.0, index=100 + i) for i in range(5)]
    post = agent.analyse(cycle_id=1, trades=trades, starting_balance=100.0, final_equity=85.0)
    causes = {c.cause for c in post.causes}
    assert Cause.CONCENTRATION in causes or Cause.SIZING in causes
    assert "DOGEUSDT" in post.evidence["by_symbol"]


def test_post_mortem_never_recommends_lowering_a_gate(repos: Repositories) -> None:
    agent = CyclePostMortemAgent(repos.research)
    trades = [trade(-0.5, gross_bps=3.0, cost_bps=11.0, index=i) for i in range(20)]
    post = agent.analyse(cycle_id=1, trades=trades, starting_balance=100.0, final_equity=90.0)
    joined = " ".join(post.recommendations).lower()
    for forbidden in ("lower the threshold", "reduce the minimum edge", "loosen"):
        assert forbidden not in joined


# ----------------------------------------------------------- the reset ritual


def test_wallet_depletion_ends_the_cycle_and_keeps_history(manager) -> None:
    cycle_manager, wallet, lifecycle, repos, config = manager
    assert cycle_manager.active
    assert wallet.state.balance == 100.0

    # Force depletion below the failure floor.
    wallet.settle(-65.0, "pos-blowup")
    reason = cycle_manager.check_failure()
    assert reason is not None and "equity" in reason

    trades = [trade(-3.25, index=i) for i in range(20)]
    post = cycle_manager.fail_cycle(reason, trades)

    assert cycle_manager.cycle.state is CycleState.AWAITING_EDGE
    assert post.trades_analyzed == 20
    # History is preserved, not deleted.
    cycles = repos.wallet.list_cycles()
    assert len(cycles) == 1 and cycles[0]["end_reason"] == reason
    assert repos.wallet.postmortems(cycle_id=1), "the post-mortem must be persisted"
    assert repos.wallet.list_ledger(cycle_id=1), "the cycle's ledger must survive"


def test_no_reset_happens_before_the_post_mortem(manager) -> None:
    cycle_manager, wallet, *_ = manager
    wallet.settle(-65.0, "pos-blowup")
    # Skipping fail_cycle entirely: nothing may reset.
    assert cycle_manager.try_reset() is None
    assert cycle_manager.cycle.cycle_id == 1


def test_no_reset_without_an_eligible_strategy(manager) -> None:
    cycle_manager, wallet, lifecycle, repos, config = manager
    wallet.settle(-65.0, "pos-blowup")
    cycle_manager.fail_cycle("forced", [trade(-3.25, index=i) for i in range(20)])

    assert cycle_manager.try_reset() is None
    assert "NO VALIDATED EDGE" in cycle_manager.blocked_reason
    assert cycle_manager.cycle.cycle_id == 1
    assert wallet.state.balance != 100.0, "the wallet must not be quietly topped up"

    # A CANDIDATE is not enough: it has passed history, not live conditions.
    strategy = lifecycle.create(hypothesis(), MetricSet(samples=200), 2.0, cycle_id=1)
    lifecycle.transition(strategy, StrategyState.CANDIDATE, "passed validation")
    assert cycle_manager.try_reset() is None


def test_reset_happens_once_a_strategy_has_passed_the_shadow_gate(manager) -> None:
    cycle_manager, wallet, lifecycle, repos, config = manager
    wallet.settle(-65.0, "pos-blowup")
    cycle_manager.fail_cycle("forced", [trade(-3.25, index=i) for i in range(20)])
    assert cycle_manager.try_reset() is None

    strategy = lifecycle.create(hypothesis("ETHUSDT"), MetricSet(samples=300), 3.0, cycle_id=1)
    for state in (StrategyState.CANDIDATE, StrategyState.CHALLENGER, StrategyState.SHADOW):
        lifecycle.transition(strategy, state, "advancing")
    strategy.shadow_metrics = MetricSet(
        samples=config.validation.shadow_min_signals, net_edge_bps=4.0
    )

    new_cycle = cycle_manager.try_reset()
    assert new_cycle is not None
    assert new_cycle.cycle_id == 2
    assert wallet.state.balance == 100.00, "a new cycle starts at exactly EUR 100.00"
    assert wallet.state.cycle_id == 2

    # Cycle 1 is closed, not erased.
    cycles = {c["cycle_id"]: c for c in repos.wallet.list_cycles()}
    assert cycles[1]["state"] == "CLOSED"
    assert cycles[2]["state"] == "ACTIVE"
    assert cycles[1]["end_reason"] == "forced"


def test_the_champion_is_frozen_not_deleted_on_failure(manager) -> None:
    cycle_manager, wallet, lifecycle, repos, config = manager
    strategy = lifecycle.create(hypothesis(), MetricSet(samples=200, net_edge_bps=5.0), 3.0, cycle_id=1)
    for state in (StrategyState.CANDIDATE, StrategyState.CHALLENGER, StrategyState.SHADOW,
                  StrategyState.CHAMPION):
        lifecycle.transition(strategy, state, "advancing")

    wallet.settle(-65.0, "pos-blowup")
    cycle_manager.fail_cycle("forced", [trade(-3.25, index=i) for i in range(20)])

    assert strategy.state is StrategyState.DEGRADED
    assert lifecycle.champion() is None
    versions = repos.strategies.versions(strategy.strategy_id)
    assert versions[0]["state"] == "DEGRADED"
    assert "cycle 1 failed" in versions[0]["reason"]
