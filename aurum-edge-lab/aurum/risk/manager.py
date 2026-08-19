"""Risk manager: the last gate before a signal becomes a position.

Ordering matters and is deliberate.  The cheapest, most absolute checks run
first — is the cycle even active, is the feed trustworthy — so that a signal
blocked by a circuit breaker is never *also* charged against a cooldown, and the
diagnostics attribute each rejection to the first reason that actually applies.
That is what makes "0 signals today" a sentence rather than a mystery.

Every rejection returns a :class:`RejectionReason` plus a human-readable detail.
Both are persisted with the signal, so ``/diagnostics`` can report counts by
reason and the UI can say *why* nothing traded.

Position sizing is risk-first, not notional-first: 1% of equity is what may be
lost if the stop is hit, and the notional follows from that and the stop
distance.  Sizing by notional instead would make every position's real risk a
function of whatever the volatility happened to be.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any

from ..config import Config
from ..domain import (
    BookSnapshot,
    Direction,
    FeatureSnapshot,
    RejectionReason,
    Side,
    now_ms,
)
from ..execution.cost_model import CostModel
from ..logging_setup import get_logger
from ..wallet.virtual_wallet import VirtualWallet

log = get_logger("risk")


@dataclass(slots=True)
class SizingPlan:
    """What the risk manager authorises, and the arithmetic behind it.

    ``risk_eur_target`` is 1% of equity; ``risk_eur_effective`` is what is
    actually at stake once the exposure caps have had their say.  They differ
    whenever a stop is tight enough that full risk-based sizing would breach a
    cap, and reporting only the target would overstate how much the system is
    really risking.
    """

    qty: float
    notional_eur: float
    notional_quote: float
    margin_eur: float
    risk_eur_target: float
    risk_eur_effective: float
    stop_bps: float
    target_bps: float
    entry_price: float
    leverage: float
    capped_by: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "qty": self.qty,
            "notional_eur": round(self.notional_eur, 4),
            "notional_quote": round(self.notional_quote, 4),
            "margin_eur": round(self.margin_eur, 4),
            "risk_eur_target": round(self.risk_eur_target, 4),
            "risk_eur_effective": round(self.risk_eur_effective, 4),
            "stop_bps": round(self.stop_bps, 2),
            "target_bps": round(self.target_bps, 2),
            "entry_price": self.entry_price,
            "leverage": round(self.leverage, 3),
            "capped_by": self.capped_by,
        }


@dataclass(slots=True)
class RiskDecision:
    allowed: bool
    reason: RejectionReason | None = None
    detail: str = ""
    plan: SizingPlan | None = None


class RiskManager:
    def __init__(self, config: Config, wallet: VirtualWallet, cost_model: CostModel) -> None:
        self.config = config
        self.wallet = wallet
        self.costs = cost_model
        self.entries_blocked = False
        self.block_reason = ""
        self._last_signal_ms: dict[str, int] = {}
        self._last_direction: dict[str, Direction] = {}
        self.rejections: dict[str, int] = {}

    # ------------------------------------------------------------------ gates

    def evaluate(
        self,
        *,
        symbol: str,
        direction: Direction,
        expected_edge_bps: float,
        horizon_ms: int,
        snapshot: FeatureSnapshot,
        book: BookSnapshot | None,
        open_positions: dict[str, Any],
        cycle_active: bool,
        quality_ok: bool,
        quality_detail: str,
        usdt_per_eur: float,
        at_ms: int | None = None,
        require_edge: bool = True,
        risk_pct_override: float | None = None,
    ) -> RiskDecision:
        """Decide whether this entry may happen, and at what size.

        ``require_edge=False`` is used by the exploration scheduler, which
        trades on a timer with no edge to claim. It waives *only* the
        edge-versus-costs gate. Every other gate — cycle state, drawdown and
        daily-loss breakers, data quality, spread, crossed book, position and
        exposure caps, cooldown, sizing, liquidity, available margin — still
        applies, because those protect against damage rather than against
        trading without an edge.
        """
        stamp = at_ms if at_ms is not None else now_ms()

        if not cycle_active:
            return self._reject(RejectionReason.CYCLE_NOT_ACTIVE, "cycle is not accepting entries")

        if self.entries_blocked:
            return self._reject(RejectionReason.ENTRIES_BLOCKED, self.block_reason or "entries blocked")

        # --- circuit breakers, before anything that costs money -------------
        drawdown = self.wallet.state.drawdown_pct
        if drawdown >= self.config.risk.max_drawdown_pct:
            return self._reject(
                RejectionReason.MAX_DRAWDOWN,
                f"drawdown {drawdown:.2f}% at or beyond limit {self.config.risk.max_drawdown_pct:.2f}%",
            )
        daily = self.wallet.daily_pnl_pct(at_ms=stamp)
        if daily <= -self.config.risk.daily_loss_limit_pct:
            return self._reject(
                RejectionReason.DAILY_LOSS_LIMIT,
                f"daily P&L {daily:.2f}% at or beyond limit -{self.config.risk.daily_loss_limit_pct:.2f}%",
            )

        # --- data quality is not negotiable by strategy confidence -----------
        if not quality_ok:
            return self._reject(RejectionReason.DATA_QUALITY, quality_detail)
        if book is None or book.best_bid is None or book.best_ask is None:
            return self._reject(RejectionReason.DATA_QUALITY, "no usable book")
        if book.is_crossed:
            return self._reject(RejectionReason.CROSSED_BOOK, "book is crossed")

        spread_bps = book.spread_bps() or 0.0
        if spread_bps > self.config.quality.max_spread_bps:
            return self._reject(
                RejectionReason.SPREAD_TOO_WIDE,
                f"spread {spread_bps:.2f} bps over limit {self.config.quality.max_spread_bps:.2f}",
            )

        # --- portfolio constraints ------------------------------------------
        if symbol in open_positions:
            return self._reject(RejectionReason.POSITION_ALREADY_OPEN, f"already long/short {symbol}")
        if len(open_positions) >= self.config.risk.max_concurrent_positions:
            return self._reject(
                RejectionReason.MAX_POSITIONS,
                f"{len(open_positions)} open, limit {self.config.risk.max_concurrent_positions}",
            )

        last = self._last_signal_ms.get(symbol)
        if last is not None:
            elapsed = (stamp - last) / 1000.0
            if elapsed < self.config.risk.cooldown_s:
                return self._reject(
                    RejectionReason.COOLDOWN,
                    f"{elapsed:.1f}s since last signal, cooldown {self.config.risk.cooldown_s:.0f}s",
                )
            if (
                elapsed < self.config.risk.duplicate_window_s
                and self._last_direction.get(symbol) is direction
            ):
                return self._reject(
                    RejectionReason.DUPLICATE_SIGNAL, f"same direction within {elapsed:.1f}s"
                )

        # --- the edge has to survive what it costs ---------------------------
        entry_cost = self.costs.entry_cost_bps(spread_bps)
        round_trip = self.costs.round_trip_bps(spread_bps)
        if require_edge and expected_edge_bps <= round_trip:
            return self._reject(
                RejectionReason.EDGE_BELOW_COSTS,
                f"expected {expected_edge_bps:.2f} bps <= round trip {round_trip:.2f} bps",
            )

        # --- sizing -----------------------------------------------------------
        equity = max(1e-9, self.wallet.state.equity)
        current_exposure = sum(p.notional_eur for p in open_positions.values())
        headroom = equity * self.config.risk.max_exposure_pct / 100.0 - current_exposure
        if headroom <= 0:
            return self._reject(
                RejectionReason.MAX_EXPOSURE,
                f"exposure {current_exposure / equity * 100.0:.1f}% already at limit "
                f"{self.config.risk.max_exposure_pct:.1f}%",
            )

        plan = self.size(
            symbol=symbol,
            direction=direction,
            expected_edge_bps=expected_edge_bps,
            horizon_ms=horizon_ms,
            snapshot=snapshot,
            book=book,
            round_trip_bps=round_trip,
            usdt_per_eur=usdt_per_eur,
            exposure_headroom_eur=headroom,
            risk_pct_override=risk_pct_override,
        )
        if plan is None:
            return self._reject(RejectionReason.NOTIONAL_TOO_SMALL, "computed size rounds to zero")

        if plan.notional_eur < self.config.risk.min_notional_eur:
            return self._reject(
                RejectionReason.NOTIONAL_TOO_SMALL,
                f"notional EUR {plan.notional_eur:.2f} under minimum "
                f"{self.config.risk.min_notional_eur:.2f}"
                + (f" after being capped by {plan.capped_by}" if plan.capped_by else ""),
            )

        if plan.margin_eur > self.wallet.state.available:
            return self._reject(
                RejectionReason.INSUFFICIENT_BALANCE,
                f"margin {plan.margin_eur:.2f} over available {self.wallet.state.available:.2f}",
            )

        # --- liquidity: can the book absorb it at all? ------------------------
        side = direction.entry_side
        available_qty = book.depth(10, Side.SELL if side is Side.BUY else Side.BUY)
        if available_qty < plan.qty:
            return self._reject(
                RejectionReason.INSUFFICIENT_LIQUIDITY,
                f"top 10 levels hold {available_qty:.6f}, order needs {plan.qty:.6f}",
            )

        _ = entry_cost  # already folded into round_trip; kept for readability above
        return RiskDecision(allowed=True, plan=plan)

    def size(
        self,
        *,
        symbol: str,
        direction: Direction,
        expected_edge_bps: float,
        horizon_ms: int,
        snapshot: FeatureSnapshot,
        book: BookSnapshot,
        round_trip_bps: float,
        usdt_per_eur: float,
        exposure_headroom_eur: float | None = None,
        risk_pct_override: float | None = None,
    ) -> SizingPlan | None:
        """Risk-first sizing, then capped.

        ``risk_per_trade_pct`` of equity is what may be lost if the stop is hit,
        so the notional is ``risk / stop_distance``.  The stop is set from the
        volatility actually observed over the signal's own horizon, floored at
        twice the round trip — a stop tighter than the cost of trading is not a
        stop, it is a guarantee of paying the spread twice.

        At these horizons the stop is a fraction of a percent, so risk-based
        sizing routinely asks for more notional than the account may carry.  The
        caps then bind and the position is *sized down*, not refused: a signal
        that survived every other gate should trade small rather than not at
        all.  What the caps cost is recorded in ``risk_eur_effective``.
        """
        entry_price = (book.best_ask if direction is Direction.LONG else book.best_bid) or 0.0
        if entry_price <= 0:
            return None

        expected_move = self._expected_move_bps(snapshot, horizon_ms)
        stop_bps = max(2.0 * round_trip_bps, 1.5 * expected_move)
        target_bps = max(expected_edge_bps, round_trip_bps * 1.5)

        equity = self.wallet.state.equity
        risk_pct = (
            self.config.risk.risk_per_trade_pct if risk_pct_override is None else risk_pct_override
        )
        risk_target = equity * risk_pct / 100.0
        notional_eur = risk_target / (stop_bps / 10_000.0)
        capped_by = ""

        caps = [
            ("max_leverage", equity * self.config.risk.max_leverage),
            ("max_symbol_exposure_pct", equity * self.config.risk.max_symbol_exposure_pct / 100.0),
        ]
        if exposure_headroom_eur is not None:
            caps.append(("max_exposure_pct", exposure_headroom_eur))
        # Margin must fit in what is actually free, whatever the percentage caps say.
        caps.append(("available_margin", self.wallet.state.available * self.config.risk.max_leverage))

        for name, limit in caps:
            if limit < notional_eur:
                notional_eur = limit
                capped_by = name

        if notional_eur <= 0:
            return None

        qty = self._round_qty(notional_eur * usdt_per_eur / entry_price)
        if qty <= 0:
            return None

        notional_quote = qty * entry_price
        notional_eur = notional_quote / usdt_per_eur
        margin_eur = notional_eur / self.config.risk.max_leverage
        leverage = notional_eur / equity if equity > 0 else 0.0

        return SizingPlan(
            qty=qty,
            notional_eur=notional_eur,
            notional_quote=notional_quote,
            margin_eur=margin_eur,
            risk_eur_target=risk_target,
            risk_eur_effective=notional_eur * stop_bps / 10_000.0,
            stop_bps=stop_bps,
            target_bps=target_bps,
            entry_price=entry_price,
            leverage=leverage,
            capped_by=capped_by,
        )

    @staticmethod
    def _expected_move_bps(snapshot: FeatureSnapshot, horizon_ms: int) -> float:
        """Realized volatility over the window closest to the signal's horizon."""
        best_key, best_gap = None, None
        for key in snapshot.values:
            if not key.startswith("vol_"):
                continue
            label = key[4:]
            try:
                window = int(label[:-2]) if label.endswith("ms") else int(label[:-1]) * 1000
            except ValueError:
                continue
            gap = abs(window - horizon_ms)
            if best_gap is None or gap < best_gap:
                best_key, best_gap = key, gap
        value = snapshot.values.get(best_key or "", 0.0)
        return max(1.0, abs(value))

    @staticmethod
    def _round_qty(qty: float) -> float:
        """Round to a plausible lot without pretending to know the venue's
        exact filters.  Six significant figures is finer than any USD-M
        step size and never rounds a valid order up."""
        if qty <= 0:
            return 0.0
        magnitude = math.floor(math.log10(qty)) if qty > 0 else 0
        decimals = max(0, 5 - magnitude)
        return math.floor(qty * 10**decimals) / 10**decimals

    # ---------------------------------------------------------------- signals

    def record_signal(self, symbol: str, direction: Direction, at_ms: int | None = None) -> None:
        """Start the cooldown.  Called only for signals that were *accepted*, so
        a rejected signal never suppresses the next real one."""
        self._last_signal_ms[symbol] = at_ms if at_ms is not None else now_ms()
        self._last_direction[symbol] = direction

    def block_entries(self, reason: str) -> None:
        self.entries_blocked = True
        self.block_reason = reason
        log.warning("entries blocked", extra={"reason": reason})

    def unblock_entries(self) -> None:
        self.entries_blocked = False
        self.block_reason = ""

    def _reject(self, reason: RejectionReason, detail: str) -> RiskDecision:
        self.rejections[reason.value] = self.rejections.get(reason.value, 0) + 1
        return RiskDecision(allowed=False, reason=reason, detail=detail)

    def circuit_breaker_state(self) -> dict[str, Any]:
        return {
            "entries_blocked": self.entries_blocked,
            "block_reason": self.block_reason,
            "drawdown_pct": round(self.wallet.state.drawdown_pct, 4),
            "max_drawdown_pct": self.config.risk.max_drawdown_pct,
            "daily_pnl_pct": round(self.wallet.daily_pnl_pct(), 4),
            "daily_loss_limit_pct": self.config.risk.daily_loss_limit_pct,
            "risk_per_trade_pct": self.config.risk.risk_per_trade_pct,
            "max_concurrent_positions": self.config.risk.max_concurrent_positions,
            "max_exposure_pct": self.config.risk.max_exposure_pct,
            "rejections": dict(self.rejections),
        }
