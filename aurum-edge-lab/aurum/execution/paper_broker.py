"""PaperBroker: simulated execution against the live book.

There is no live-order path in this class and no credential it could use to
create one.  It holds a reference to the order book and the cost model, and
nothing else that touches a venue.

What it refuses to do is flatter itself:

* The **requested** price and the **filled** price are stored separately on
  every position and every trade.  A broker that records only the fill price
  hides its own slippage.
* Fills walk the resting depth.  A 200 EUR order in a book with 30 EUR at the
  touch does not get the touch price.
* Fees are charged to the wallet at entry and at exit, as they are on the venue.
* The **feature snapshot at the moment of the decision** is stored on the trade.
  That is what makes the "WHY THIS TRADE" view possible, and what lets a
  post-mortem attribute a loss to the conditions rather than to a hunch.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import Any, Iterable

from ..config import Config
from ..domain import (
    BookSnapshot,
    Direction,
    ExitReason,
    FeatureSnapshot,
    PaperTrade,
    Position,
    Regime,
    Side,
    Signal,
    now_ms,
)
from ..logging_setup import get_logger
from ..risk.manager import SizingPlan
from ..storage.repositories import ExecutionRepository
from ..wallet.virtual_wallet import LedgerKind, VirtualWallet
from .cost_model import CostModel

log = get_logger("execution.paper")


@dataclass
class ExitCheck:
    should_exit: bool
    reason: ExitReason | None = None
    detail: str = ""


class PaperBroker:
    #: Proof, asserted by the selftest, that this class cannot reach a venue.
    can_place_live_orders = False

    def __init__(
        self,
        config: Config,
        wallet: VirtualWallet,
        cost_model: CostModel,
        repo: ExecutionRepository,
    ) -> None:
        self.config = config
        self.wallet = wallet
        self.costs = cost_model
        self.repo = repo
        self.positions: dict[str, Position] = {}
        self.closed: list[PaperTrade] = []
        self.opened_count = 0
        self.closed_count = 0
        # Side tables keyed by position id: these belong on the trade record
        # rather than on the live position, which is a slots dataclass sized for
        # the hot path.
        self._strategy_versions: dict[str, int] = {}
        self._regimes: dict[str, Regime] = {}
        self._expected_edge: dict[str, float] = {}

    @property
    def usdt_per_eur(self) -> float:
        return self.config.fx.usdt_per_eur

    # ------------------------------------------------------------------ entry

    def open(
        self,
        signal: Signal,
        plan: SizingPlan,
        book: BookSnapshot,
        snapshot: FeatureSnapshot,
        *,
        cycle_id: int,
        strategy_version: int = 1,
        at_ms: int | None = None,
    ) -> Position | None:
        """Simulate an entry.  Returns None when the book could not fill it."""
        stamp = at_ms if at_ms is not None else now_ms()
        side = signal.direction.entry_side
        fill = self.costs.simulate_market_order(
            book, side, plan.qty, reference_price=plan.entry_price
        )
        if not fill.fully_filled or fill.filled_qty <= 0:
            log.info(
                "entry not filled",
                extra={"symbol": signal.symbol, "qty": plan.qty, "filled": fill.filled_qty},
            )
            return None

        notional_quote = fill.fill_price * fill.filled_qty
        notional_eur = notional_quote / self.usdt_per_eur
        margin_eur = notional_eur / self.config.risk.max_leverage
        position_id = f"pos-{uuid.uuid4().hex[:12]}"

        self.wallet.reserve(margin_eur, reference=position_id)
        fee_eur = self.costs.fee_eur(notional_eur)
        self.wallet.charge_fee(fee_eur, reference=position_id, kind=LedgerKind.ENTRY_FEE)

        position = Position(
            position_id=position_id,
            symbol=signal.symbol,
            direction=signal.direction,
            qty=fill.filled_qty,
            entry_ts_ms=stamp,
            entry_price=fill.fill_price,
            requested_entry_price=fill.requested_price,
            notional_eur=notional_eur,
            margin_eur=margin_eur,
            strategy_id=signal.strategy_id,
            hypothesis_id=signal.hypothesis_id,
            signal_id=signal.signal_id,
            horizon_ms=signal.horizon_ms,
            entry_fee_eur=fee_eur,
            entry_slippage_bps=fill.slippage_bps,
            stop_bps=plan.stop_bps,
            target_bps=plan.target_bps,
            cycle_id=cycle_id,
            features=dict(snapshot.values),
            mark_price=fill.fill_price,
        )
        self._strategy_versions[position_id] = strategy_version
        self._regimes[position_id] = snapshot.regime
        self._expected_edge[position_id] = signal.expected_edge_bps

        self.positions[signal.symbol] = position
        self.opened_count += 1
        self.repo.open_position(position)
        self.wallet.snapshot()
        log.info(
            "position opened",
            extra={
                "symbol": position.symbol,
                "direction": position.direction.value,
                "qty": position.qty,
                "requested": position.requested_entry_price,
                "filled": position.entry_price,
                "slippage_bps": round(position.entry_slippage_bps, 3),
                "notional_eur": round(notional_eur, 2),
            },
        )
        return position

    # ------------------------------------------------------------------- mark

    def mark_to_market(self, books: dict[str, BookSnapshot | None]) -> float:
        """Revalue open positions at what they could actually be closed at."""
        total = 0.0
        for symbol, position in self.positions.items():
            book = books.get(symbol)
            if book is None:
                total += position.unrealized_pnl_eur
                continue
            # Mark at the price that would *close* the position, not the mid:
            # a long is worth what a bid will pay for it.
            exit_price = (book.best_bid if position.direction is Direction.LONG else book.best_ask)
            if exit_price is None:
                total += position.unrealized_pnl_eur
                continue
            total += position.update_mark(exit_price, self.usdt_per_eur)
        self.wallet.mark_unrealized(total)
        return total

    # ------------------------------------------------------------------- exit

    def check_exit(
        self, position: Position, book: BookSnapshot | None, *, at_ms: int | None = None,
        quality_ok: bool = True, quality_detail: str = ""
    ) -> ExitCheck:
        stamp = at_ms if at_ms is not None else now_ms()
        if book is None or book.best_bid is None or book.best_ask is None:
            return ExitCheck(False)

        if not quality_ok:
            return ExitCheck(True, ExitReason.DATA_QUALITY, quality_detail or "data quality lost")

        exit_price = book.best_bid if position.direction is Direction.LONG else book.best_ask
        move_bps = position.return_bps(exit_price)

        if move_bps <= -position.stop_bps:
            return ExitCheck(True, ExitReason.STOP_LOSS, f"{move_bps:.2f} bps <= -{position.stop_bps:.2f}")
        if move_bps >= position.target_bps:
            return ExitCheck(True, ExitReason.TAKE_PROFIT, f"{move_bps:.2f} bps >= {position.target_bps:.2f}")

        held = stamp - position.entry_ts_ms
        if held >= position.horizon_ms:
            return ExitCheck(True, ExitReason.HORIZON, f"held {held} ms >= horizon {position.horizon_ms} ms")
        if held >= self.config.risk.max_holding_s * 1000:
            return ExitCheck(
                True, ExitReason.MAX_HOLDING, f"held {held} ms >= max {self.config.risk.max_holding_s}s"
            )
        return ExitCheck(False)

    def close(
        self,
        position: Position,
        book: BookSnapshot,
        reason: ExitReason,
        *,
        at_ms: int | None = None,
    ) -> PaperTrade:
        stamp = at_ms if at_ms is not None else now_ms()
        side = position.direction.entry_side.opposite
        requested = (book.best_bid if position.direction is Direction.LONG else book.best_ask) or 0.0
        fill = self.costs.simulate_market_order(book, side, position.qty, reference_price=requested)
        # A book too thin to close into still closes — at the worst price it can
        # give. Refusing to exit would silently turn a stop into an open bet.
        exit_price = fill.fill_price if fill.filled_qty > 0 else requested

        gross_quote = (exit_price - position.entry_price) * position.direction.sign * position.qty
        gross_pnl_eur = gross_quote / self.usdt_per_eur
        exit_notional_eur = exit_price * position.qty / self.usdt_per_eur
        exit_fee = self.costs.fee_eur(exit_notional_eur)

        self.wallet.release(position.margin_eur, reference=position.position_id)
        self.wallet.charge_fee(exit_fee, reference=position.position_id, kind=LedgerKind.EXIT_FEE)
        fees_eur = position.entry_fee_eur + exit_fee
        net_pnl_eur = gross_pnl_eur - fees_eur
        self.wallet.settle(gross_pnl_eur, reference=position.position_id, won=net_pnl_eur > 0)

        return_bps = position.return_bps(exit_price)
        fee_bps = (fees_eur / position.notional_eur * 10_000.0) if position.notional_eur > 0 else 0.0
        trade = PaperTrade(
            trade_id=f"trd-{uuid.uuid4().hex[:12]}",
            position_id=position.position_id,
            symbol=position.symbol,
            direction=position.direction,
            qty=position.qty,
            entry_ts_ms=position.entry_ts_ms,
            exit_ts_ms=stamp,
            requested_entry_price=position.requested_entry_price,
            entry_price=position.entry_price,
            requested_exit_price=requested,
            exit_price=exit_price,
            gross_pnl_eur=gross_pnl_eur,
            fees_eur=fees_eur,
            net_pnl_eur=net_pnl_eur,
            return_bps=return_bps,
            net_return_bps=return_bps - fee_bps,
            entry_slippage_bps=position.entry_slippage_bps,
            exit_slippage_bps=fill.slippage_bps,
            cost_bps=fee_bps + position.entry_slippage_bps + fill.slippage_bps,
            exit_reason=reason,
            strategy_id=position.strategy_id,
            strategy_version=self._strategy_versions.pop(position.position_id, 1),
            hypothesis_id=position.hypothesis_id,
            signal_id=position.signal_id,
            cycle_id=position.cycle_id,
            regime=self._regimes.pop(position.position_id, Regime.UNKNOWN),
            features=position.features,
            cost_model_version=self.costs.version,
            expected_edge_bps=self._expected_edge.pop(position.position_id, 0.0),
        )

        self.positions.pop(position.symbol, None)
        self.closed.append(trade)
        self.closed_count += 1
        self.repo.close_position(position.position_id, stamp)
        self.repo.record_trade(trade)
        self.wallet.snapshot()
        log.info(
            "position closed",
            extra={
                "symbol": trade.symbol,
                "reason": reason.value,
                "net_pnl_eur": round(trade.net_pnl_eur, 4),
                "return_bps": round(trade.return_bps, 2),
                "net_return_bps": round(trade.net_return_bps, 2),
                "held_ms": trade.holding_ms,
            },
        )
        return trade

    def close_all(
        self, books: dict[str, BookSnapshot | None], reason: ExitReason, *, at_ms: int | None = None
    ) -> list[PaperTrade]:
        trades = []
        for symbol in list(self.positions):
            book = books.get(symbol)
            if book is None:
                continue
            trades.append(self.close(self.positions[symbol], book, reason, at_ms=at_ms))
        return trades

    # ----------------------------------------------------------------- access

    def open_positions(self) -> dict[str, Position]:
        return dict(self.positions)

    def exposure_eur(self) -> float:
        return sum(p.notional_eur for p in self.positions.values())

    def recent_trades(self, limit: int = 50) -> list[PaperTrade]:
        return self.closed[-limit:][::-1]

    def stats(self) -> dict[str, Any]:
        wins = sum(1 for t in self.closed if t.net_pnl_eur > 0)
        losses = sum(1 for t in self.closed if t.net_pnl_eur <= 0)
        gross_win = sum(t.net_pnl_eur for t in self.closed if t.net_pnl_eur > 0)
        gross_loss = -sum(t.net_pnl_eur for t in self.closed if t.net_pnl_eur <= 0)
        return {
            "live_orders_possible": self.can_place_live_orders,
            "open_positions": len(self.positions),
            "opened": self.opened_count,
            "closed": self.closed_count,
            "wins": wins,
            "losses": losses,
            "win_rate": round(wins / (wins + losses), 4) if (wins + losses) else 0.0,
            "profit_factor": round(gross_win / gross_loss, 4) if gross_loss > 0 else 0.0,
            "net_pnl_eur": round(sum(t.net_pnl_eur for t in self.closed), 6),
            "fees_eur": round(sum(t.fees_eur for t in self.closed), 6),
            "exposure_eur": round(self.exposure_eur(), 4),
            "mean_entry_slippage_bps": round(
                sum(t.entry_slippage_bps for t in self.closed) / len(self.closed), 4
            )
            if self.closed
            else 0.0,
            "exit_reasons": self._exit_reason_counts(),
        }

    def _exit_reason_counts(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        for trade in self.closed:
            counts[trade.exit_reason.value] = counts.get(trade.exit_reason.value, 0) + 1
        return counts

    def restore(self, rows: Iterable[dict[str, Any]]) -> int:
        """Reload open positions after a restart so history is never lost."""
        restored = 0
        for row in rows:
            position = Position(
                position_id=row["position_id"],
                symbol=row["symbol"],
                direction=Direction(row["direction"]),
                qty=float(row["qty"]),
                entry_ts_ms=int(row["entry_ts_ms"]),
                entry_price=float(row["entry_price"]),
                requested_entry_price=float(row.get("requested_entry_price") or row["entry_price"]),
                notional_eur=float(row.get("notional_eur") or 0.0),
                margin_eur=float(row.get("margin_eur") or 0.0),
                strategy_id=row.get("strategy_id") or "",
                hypothesis_id=row.get("hypothesis_id") or "",
                signal_id=row.get("signal_id") or "",
                horizon_ms=int(row.get("horizon_ms") or 0),
                entry_fee_eur=float(row.get("entry_fee_eur") or 0.0),
                entry_slippage_bps=float(row.get("entry_slippage_bps") or 0.0),
                stop_bps=float(row.get("stop_bps") or 0.0),
                target_bps=float(row.get("target_bps") or 0.0),
                cycle_id=int(row.get("cycle_id") or 0),
                features=row.get("features") or {},
            )
            self.positions[position.symbol] = position
            restored += 1
        return restored
