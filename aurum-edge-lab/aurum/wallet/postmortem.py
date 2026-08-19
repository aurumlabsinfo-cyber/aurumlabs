"""Cycle manager: failure detection, post-mortem, and the conditional reset.

The blueprint is emphatic that a reset is *not* automatic.  When a cycle fails,
this is the sequence, and every step must complete before the next begins:

1. Block new entries.
2. Freeze the champion version and preserve all logs and trades.
3. Run the CyclePostMortemAgent.
4. Classify root causes and update research memory.
5. Generate or reactivate challenger hypotheses only if justified.
6. Re-run validation and shadow gates.
7. Start a new cycle at €100 **only** once an eligible strategy exists.
   Otherwise the system stays in NO VALIDATED EDGE.

Step 7 is the one that is tempting to skip.  A system that resets to €100 the
moment it goes broke will cheerfully lose €100 every hour forever and produce a
dashboard full of activity.  This one sits in AWAITING_EDGE with an explanation
until research has actually produced something that passed every gate — and
"nothing passed" is a legitimate terminal state, not a bug to be worked around.

Nothing is deleted on reset.  A new cycle is a new row beside the old one.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

from ..agents.postmortem import CyclePostMortemAgent, PostMortem
from ..config import Config
from ..domain import CycleState, PaperTrade, StrategyState, now_ms
from ..logging_setup import get_logger
from ..storage.repositories import Repositories
from ..strategies.lifecycle import StrategyLifecycle
from .virtual_wallet import VirtualWallet

log = get_logger("wallet.cycle")


@dataclass
class Cycle:
    cycle_id: int
    state: CycleState
    started_ms: int
    starting_balance: float
    ended_ms: int = 0
    final_balance: float = 0.0
    peak_equity: float = 0.0
    max_drawdown_pct: float = 0.0
    trades: int = 0
    wins: int = 0
    losses: int = 0
    net_pnl: float = 0.0
    champion_strategy_id: str = ""
    end_reason: str = ""

    def to_row(self) -> dict[str, Any]:
        return {
            "cycle_id": self.cycle_id,
            "state": self.state.value,
            "started_ms": self.started_ms,
            "ended_ms": self.ended_ms or None,
            "starting_balance": self.starting_balance,
            "final_balance": self.final_balance or None,
            "peak_equity": self.peak_equity,
            "max_drawdown_pct": self.max_drawdown_pct,
            "trades": self.trades,
            "wins": self.wins,
            "losses": self.losses,
            "net_pnl": self.net_pnl,
            "champion_strategy_id": self.champion_strategy_id or None,
            "end_reason": self.end_reason,
        }

    def to_dict(self) -> dict[str, Any]:
        data = self.to_row()
        data["duration_ms"] = max(0, (self.ended_ms or now_ms()) - self.started_ms)
        data["return_pct"] = (
            round((self.final_balance - self.starting_balance) / self.starting_balance * 100.0, 4)
            if self.final_balance and self.starting_balance
            else 0.0
        )
        return data


class CycleManager:
    def __init__(
        self,
        config: Config,
        wallet: VirtualWallet,
        lifecycle: StrategyLifecycle,
        repos: Repositories,
    ) -> None:
        self.config = config
        self.wallet = wallet
        self.lifecycle = lifecycle
        self.repos = repos
        self.agent = CyclePostMortemAgent(repos.research)
        self.cycle: Cycle | None = None
        self.postmortems: list[PostMortem] = []
        #: Set while the system is in AWAITING_EDGE, and reported by /diagnostics.
        self.blocked_reason = ""
        self.resets = 0

    # -------------------------------------------------------------- lifecycle

    def start(self, *, at_ms: int | None = None) -> Cycle:
        """Open cycle 1, or resume the cycle recorded in the database."""
        existing = self.repos.wallet.current_cycle()
        if existing:
            self.cycle = Cycle(
                cycle_id=int(existing["cycle_id"]),
                state=CycleState(existing["state"]),
                started_ms=int(existing["started_ms"]),
                starting_balance=float(existing["starting_balance"]),
                ended_ms=int(existing.get("ended_ms") or 0),
                final_balance=float(existing.get("final_balance") or 0.0),
                peak_equity=float(existing.get("peak_equity") or 0.0),
                max_drawdown_pct=float(existing.get("max_drawdown_pct") or 0.0),
                trades=int(existing.get("trades") or 0),
                wins=int(existing.get("wins") or 0),
                losses=int(existing.get("losses") or 0),
                net_pnl=float(existing.get("net_pnl") or 0.0),
                champion_strategy_id=existing.get("champion_strategy_id") or "",
                end_reason=existing.get("end_reason") or "",
            )
            latest_wallet = self.repos.wallet.latest_wallet()
            if latest_wallet and int(latest_wallet["cycle_id"]) == self.cycle.cycle_id:
                self.wallet.restore(latest_wallet)
            log.info("cycle resumed", extra={"cycle": self.cycle.cycle_id, "state": self.cycle.state.value})
            if self.cycle.state is CycleState.AWAITING_EDGE:
                self.blocked_reason = "resumed into AWAITING_EDGE: no eligible strategy yet"
            return self.cycle

        return self._open(1, at_ms=at_ms)

    def _open(self, cycle_id: int, *, at_ms: int | None = None) -> Cycle:
        stamp = at_ms if at_ms is not None else now_ms()
        self.wallet.open_cycle(cycle_id)
        self.cycle = Cycle(
            cycle_id=cycle_id,
            state=CycleState.ACTIVE,
            started_ms=stamp,
            starting_balance=self.wallet.starting_balance,
            peak_equity=self.wallet.starting_balance,
        )
        self.repos.wallet.save_cycle(self.cycle.to_row())
        self.repos.system.log(
            "cycle", "opened", f"cycle {cycle_id} opened at {self.wallet.starting_balance:.2f} EUR"
        )
        self.blocked_reason = ""
        return self.cycle

    # ------------------------------------------------------------- monitoring

    @property
    def active(self) -> bool:
        return self.cycle is not None and self.cycle.state is CycleState.ACTIVE

    def state_name(self) -> str:
        return self.cycle.state.value if self.cycle else "NONE"

    def check_failure(self, *, at_ms: int | None = None) -> str | None:
        """Has this cycle failed?  Returns the reason, or None."""
        if not self.active or self.cycle is None:
            return None
        state = self.wallet.state
        failure_floor = self.wallet.starting_balance * self.config.risk.wallet_failure_equity_pct / 100.0
        if state.equity <= failure_floor:
            return (
                f"equity {state.equity:.2f} EUR fell to or below {failure_floor:.2f} "
                f"({self.config.risk.wallet_failure_equity_pct:.0f}% of the "
                f"{self.wallet.starting_balance:.2f} start)"
            )
        if state.drawdown_pct >= self.config.risk.max_drawdown_pct:
            return (
                f"drawdown {state.drawdown_pct:.2f}% reached the limit "
                f"{self.config.risk.max_drawdown_pct:.2f}%"
            )
        return None

    # ------------------------------------------------------------- the ritual

    def fail_cycle(
        self,
        reason: str,
        trades: Sequence[PaperTrade],
        *,
        at_ms: int | None = None,
    ) -> PostMortem:
        """Steps 1-4: block, freeze, analyse, record."""
        stamp = at_ms if at_ms is not None else now_ms()
        assert self.cycle is not None, "no cycle to fail"

        # 1. Block new entries (the caller holds the risk manager and does this
        #    too; recording it here makes the order auditable).
        self.cycle.state = CycleState.POST_MORTEM
        self.cycle.ended_ms = stamp
        self.cycle.end_reason = reason
        self.cycle.final_balance = self.wallet.state.balance
        self.cycle.peak_equity = self.wallet.state.peak_equity
        self.cycle.max_drawdown_pct = self.wallet.state.drawdown_pct
        self.cycle.trades = self.wallet.state.trades
        self.cycle.wins = self.wallet.state.wins
        self.cycle.losses = self.wallet.state.losses
        self.cycle.net_pnl = self.wallet.state.balance - self.cycle.starting_balance

        # 2. Freeze the champion. It is not deleted; it is retired with its
        #    evidence, so the next cycle can see what was tried and why it lost.
        champion = self.lifecycle.champion()
        validated_edge = 0.0
        if champion is not None:
            self.cycle.champion_strategy_id = champion.strategy_id
            validated_edge = champion.validated_metrics.net_edge_bps
            self.lifecycle.transition(
                champion,
                StrategyState.DEGRADED,
                f"cycle {self.cycle.cycle_id} failed: {reason}",
                evidence={"cycle": self.cycle.to_dict()},
                at_ms=stamp,
            )
        self.repos.wallet.save_cycle(self.cycle.to_row())

        # 3-4. Analyse and record.
        post = self.agent.analyse(
            cycle_id=self.cycle.cycle_id,
            trades=trades,
            starting_balance=self.cycle.starting_balance,
            final_equity=self.wallet.state.equity,
            validated_edge_bps=validated_edge,
            end_reason=reason,
            at_ms=stamp,
        )
        self.repos.wallet.save_postmortem(post.to_row())
        self.postmortems.append(post)

        self.cycle.state = CycleState.AWAITING_EDGE
        self.repos.wallet.save_cycle(self.cycle.to_row())
        self.blocked_reason = (
            f"cycle {self.cycle.cycle_id} ended ({post.primary_cause}); a new cycle opens only "
            f"once a strategy passes every validation and shadow gate"
        )
        self.repos.system.log(
            "cycle",
            "failed",
            f"cycle {self.cycle.cycle_id} failed: {reason}",
            level="WARNING",
            detail={"postmortem": post.to_dict()},
        )
        return post

    def eligible_strategy(self) -> Any | None:
        """Steps 5-6: is there a strategy that has actually earned a new cycle?

        A CHAMPION is eligible.  So is a SHADOW that has completed its live
        evaluation — the shadow gate is what step 6 refers to.  A CANDIDATE is
        not: it has passed history, not live conditions.
        """
        champion = self.lifecycle.champion()
        if champion is not None:
            return champion
        for strategy in self.lifecycle.shadows():
            if (
                strategy.shadow_metrics.samples >= self.config.validation.shadow_min_signals
                and strategy.shadow_metrics.net_edge_bps >= self.config.validation.min_net_edge_bps
            ):
                return strategy
        return None

    def try_reset(self, *, at_ms: int | None = None) -> Cycle | None:
        """Step 7: open a new cycle, but only if one has been earned."""
        if self.cycle is None or self.cycle.state is not CycleState.AWAITING_EDGE:
            return None
        if not self.postmortems:
            self.blocked_reason = "post-mortem has not completed"
            return None

        eligible = self.eligible_strategy()
        if eligible is None:
            self.blocked_reason = (
                "NO VALIDATED EDGE: post-mortem is complete, but no strategy has passed "
                "validation and live shadow evaluation. The wallet stays closed."
            )
            return None

        self.cycle.state = CycleState.CLOSED
        self.repos.wallet.save_cycle(self.cycle.to_row())
        new_cycle = self._open(self.cycle.cycle_id + 1, at_ms=at_ms)
        self.resets += 1
        self.repos.system.log(
            "cycle",
            "reset",
            f"cycle {new_cycle.cycle_id} opened after post-mortem; eligible strategy "
            f"{getattr(eligible, 'strategy_id', '?')}",
        )
        log.info(
            "cycle reset",
            extra={"cycle": new_cycle.cycle_id, "eligible": getattr(eligible, "strategy_id", "?")},
        )
        return new_cycle

    # ---------------------------------------------------------------- reporting

    def sync(self) -> None:
        """Keep the persisted cycle row in step with the wallet."""
        if self.cycle is None:
            return
        state = self.wallet.state
        self.cycle.peak_equity = state.peak_equity
        self.cycle.max_drawdown_pct = state.drawdown_pct
        self.cycle.trades = state.trades
        self.cycle.wins = state.wins
        self.cycle.losses = state.losses
        self.cycle.net_pnl = state.balance - self.cycle.starting_balance
        champion = self.lifecycle.champion()
        if champion is not None:
            self.cycle.champion_strategy_id = champion.strategy_id
        self.repos.wallet.save_cycle(self.cycle.to_row())

    def to_dict(self) -> dict[str, Any]:
        return {
            "cycle": self.cycle.to_dict() if self.cycle else None,
            "state": self.state_name(),
            "active": self.active,
            "blocked_reason": self.blocked_reason,
            "resets": self.resets,
            "postmortems": len(self.postmortems),
            "last_postmortem": self.postmortems[-1].to_dict() if self.postmortems else None,
            "failure_floor_eur": round(
                self.wallet.starting_balance * self.config.risk.wallet_failure_equity_pct / 100.0, 2
            ),
        }
