"""Cost model.

Every number this system reports as an edge is net of what it would cost to
capture it.  That is the difference between research and wishful thinking at
these horizons: a 3 bps signal is a good signal and a losing trade, because
crossing the spread twice plus taker fees on both sides is already more than
that.

Four components, each measured rather than assumed away:

``spread``     crossing the book costs half the spread per side, and more when
               the order is larger than the inside level.
``commission`` taker fee per side, in basis points of notional.
``slippage``   walked through the actual resting depth, not a flat guess.
``latency``    the price moves during the round trip; charged as a penalty
               proportional to the configured latency.

The model is versioned.  A hypothesis records which version validated it, so
changing the fee schedule invalidates the research it was measured under
instead of silently reinterpreting it.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from ..config import CostsConfig
from ..domain import BookSnapshot, Side


@dataclass(slots=True)
class FillEstimate:
    """What an order would actually get, versus what it asked for."""

    requested_price: float
    fill_price: float
    qty: float
    filled_qty: float
    levels_consumed: int
    slippage_bps: float
    fee_bps: float
    latency_bps: float
    total_cost_bps: float
    fully_filled: bool

    def to_dict(self) -> dict[str, Any]:
        return {
            "requested_price": self.requested_price,
            "fill_price": self.fill_price,
            "qty": self.qty,
            "filled_qty": self.filled_qty,
            "levels_consumed": self.levels_consumed,
            "slippage_bps": round(self.slippage_bps, 4),
            "fee_bps": round(self.fee_bps, 4),
            "latency_bps": round(self.latency_bps, 4),
            "total_cost_bps": round(self.total_cost_bps, 4),
            "fully_filled": self.fully_filled,
        }


class CostModel:
    def __init__(self, config: CostsConfig) -> None:
        self.config = config

    @property
    def version(self) -> str:
        return self.config.version

    # ----------------------------------------------------------- estimation

    def latency_bps(self) -> float:
        return self.config.latency_ms / 100.0 * self.config.latency_penalty_bps_per_100ms

    def round_trip_bps(self, spread_bps: float) -> float:
        """Total cost of entering and exiting, both sides taker.

        This is the number a hypothesis has to beat before it is worth a single
        further minute of anyone's attention.
        """
        crossing = spread_bps  # half the spread on entry plus half on exit
        commission = 2.0 * self.config.taker_fee_bps
        slippage = 2.0 * self.config.extra_slippage_bps
        latency = 2.0 * self.latency_bps()
        return crossing + commission + slippage + latency

    def entry_cost_bps(self, spread_bps: float) -> float:
        return (
            spread_bps / 2.0
            + self.config.taker_fee_bps
            + self.config.extra_slippage_bps
            + self.latency_bps()
        )

    # ------------------------------------------------------------ simulation

    def simulate_market_order(
        self, book: BookSnapshot, side: Side, qty: float, *, reference_price: float | None = None
    ) -> FillEstimate:
        """Walk the resting depth to price a market order.

        A taker order does not get the touch price for the whole size — it eats
        levels until it is filled.  Pricing it at the touch is the single most
        common way a paper broker flatters itself, so the walk is explicit and
        the number of levels consumed is recorded on the trade.
        """
        levels = book.asks if side is Side.BUY else book.bids
        touch = (book.best_ask if side is Side.BUY else book.best_bid) or 0.0
        requested = reference_price if reference_price is not None else touch
        if not levels or touch <= 0 or qty <= 0:
            return FillEstimate(requested, requested, qty, 0.0, 0, 0.0, 0.0, 0.0, 0.0, False)

        remaining = qty
        notional = 0.0
        consumed = 0
        for level in levels:
            if remaining <= 0:
                break
            take = min(remaining, level.qty)
            notional += take * level.price
            remaining -= take
            consumed += 1

        filled = qty - remaining
        if filled <= 0:
            return FillEstimate(requested, requested, qty, 0.0, 0, 0.0, 0.0, 0.0, 0.0, False)

        walked_price = notional / filled
        if self.config.impact_model == "fixed":
            walked_price = touch

        # A fixed extra slippage covers what the snapshot cannot see: the queue
        # ahead of us and the levels that vanish between decision and arrival.
        drift = self.config.extra_slippage_bps / 10_000.0
        fill_price = walked_price * (1.0 + drift) if side is Side.BUY else walked_price * (1.0 - drift)

        slippage_bps = abs(fill_price - requested) / requested * 10_000.0 if requested > 0 else 0.0
        fee_bps = self.config.taker_fee_bps
        latency_bps = self.latency_bps()
        return FillEstimate(
            requested_price=requested,
            fill_price=fill_price,
            qty=qty,
            filled_qty=filled,
            levels_consumed=consumed,
            slippage_bps=slippage_bps,
            fee_bps=fee_bps,
            latency_bps=latency_bps,
            total_cost_bps=slippage_bps + fee_bps + latency_bps,
            fully_filled=remaining <= 1e-12,
        )

    def fee_eur(self, notional_eur: float) -> float:
        return notional_eur * self.config.taker_fee_bps / 10_000.0

    def to_dict(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "taker_fee_bps": self.config.taker_fee_bps,
            "maker_fee_bps": self.config.maker_fee_bps,
            "extra_slippage_bps": self.config.extra_slippage_bps,
            "latency_ms": self.config.latency_ms,
            "latency_bps": round(self.latency_bps(), 4),
            "impact_model": self.config.impact_model,
            "round_trip_at_1bps_spread": round(self.round_trip_bps(1.0), 4),
        }
