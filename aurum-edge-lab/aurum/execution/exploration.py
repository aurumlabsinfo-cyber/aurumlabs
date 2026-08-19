"""Scheduled entries with no validated edge, to exercise the live path.

Research can tell you a hypothesis does not survive its costs.  It cannot tell
you whether the code that would have traded it works, because that code never
runs.  A system whose execution path has never executed has an untested
execution path, and the first real signal is the worst possible time to find
out.

So this opens positions on a timer.  It is not a strategy and nothing here
pretends otherwise:

* The direction is a seeded coin flip, deliberately.  Anything cleverer —
  momentum, order-flow sign, the best-scoring rejected hypothesis — would make
  the resulting P&L look like a claim about the market, and it would be a claim
  the validation lab has already rejected.  A coin flip cannot be mistaken for
  an edge, which makes the measurement readable: what comes back is the cost of
  trading, and nothing else.
* Every signal and every position is tagged ``exploration``, all the way into
  the database, so validated performance figures never absorb these trades.
* Research does not learn from them.  They produce no hypotheses and never
  enter research memory.

The expected result is a loss of about one round trip per trade.  That is the
measurement, not a malfunction: if forced trading made money, the research
gates would have found the edge and promoted a champion.

What it does prove, run against a live venue: the fill model against real
books, wallet and margin arithmetic under real turnover, the exit machinery,
the post-mortem, and the whole persistence path.
"""

from __future__ import annotations

import random
import uuid

from ..config import Config
from ..domain import Direction, FeatureSnapshot, RejectionReason, Signal, now_ms
from ..logging_setup import get_logger

log = get_logger("exploration")

#: Strategy identifier carried by every exploration signal. It is not a real
#: strategy id and will not resolve in the lifecycle registry — that is the
#: point: nothing can look this up and find a validated track record.
STRATEGY_ID = "exploration"


class ExplorationTrader:
    """Paces entries to a target daily rate and reports what it did."""

    def __init__(self, config: Config) -> None:
        self.config = config
        self.settings = config.exploration
        self._rng = random.Random(self.settings.seed)
        self._next_due_ms = 0
        self._cursor = 0
        self.attempted = 0
        self.opened = 0
        self.rejected = 0
        self.last_rejection: str = ""

        configured = [s.symbol for s in config.market.symbols]
        wanted = [s.upper() for s in self.settings.symbols]
        self.symbols = [s for s in configured if not wanted or s in wanted] or configured

    # ---------------------------------------------------------------- pacing

    @property
    def enabled(self) -> bool:
        return self.settings.enabled

    def due(self, at_ms: int) -> bool:
        """Has enough time passed to open the next exploration position?

        The first call arms the schedule rather than firing, so enabling
        exploration does not open a position on the very first feature
        snapshot, before the books have settled.
        """
        if not self.enabled:
            return False
        if self._next_due_ms == 0:
            self._next_due_ms = at_ms + int(self.settings.interval_s * 1000)
            return False
        return at_ms >= self._next_due_ms

    def arm_next(self, at_ms: int) -> None:
        self._next_due_ms = at_ms + int(self.settings.interval_s * 1000)

    def next_symbol(self, open_symbols: set[str]) -> str | None:
        """Round-robin over the eligible symbols, skipping those already open.

        Round-robin rather than random so that a day's exploration spreads
        evenly across the book instead of clustering, which would leave some
        symbols' execution paths just as untested as before.
        """
        for _ in range(len(self.symbols)):
            symbol = self.symbols[self._cursor % len(self.symbols)]
            self._cursor += 1
            if symbol not in open_symbols:
                return symbol
        return None

    def direction(self) -> Direction:
        return Direction.LONG if self._rng.random() < 0.5 else Direction.SHORT

    # ---------------------------------------------------------------- signal

    def build_signal(self, symbol: str, snapshot: FeatureSnapshot, cost_bps: float) -> Signal:
        """A signal that states plainly it has no edge.

        ``expected_edge_bps`` is zero rather than something flattering: the
        stored record has to read as what it is, because that record is what a
        post-mortem and any later analysis will believe.
        """
        return Signal(
            signal_id=f"exp-{uuid.uuid4().hex[:12]}",
            ts_ms=snapshot.ts_ms,
            strategy_id=STRATEGY_ID,
            hypothesis_id="",
            symbol=symbol,
            direction=self.direction(),
            confidence=0.0,
            expected_edge_bps=0.0,
            expected_cost_bps=cost_bps,
            horizon_ms=int(self.settings.hold_s * 1000),
            regime=snapshot.regime,
            features=dict(snapshot.values),
            exploration=True,
        )

    def record_rejection(self, reason: RejectionReason | None, detail: str) -> None:
        self.rejected += 1
        self.last_rejection = f"{reason.value if reason else 'UNKNOWN'}: {detail}"

    def record_open(self) -> None:
        self.opened += 1

    # ----------------------------------------------------------------- report

    def to_dict(self, at_ms: int | None = None) -> dict[str, object]:
        stamp = at_ms if at_ms is not None else now_ms()
        return {
            "enabled": self.enabled,
            "trades_per_day_target": self.settings.trades_per_day,
            "interval_s": round(self.settings.interval_s, 1),
            "hold_s": self.settings.hold_s,
            "risk_per_trade_pct": self.settings.risk_per_trade_pct,
            "symbols": list(self.symbols),
            "attempted": self.attempted,
            "opened": self.opened,
            "rejected": self.rejected,
            "last_rejection": self.last_rejection,
            "next_due_in_s": (
                round(max(0, self._next_due_ms - stamp) / 1000.0, 1) if self._next_due_ms else None
            ),
            "warning": (
                "EXPLORATION MODE — these trades have no validated edge. The direction is a "
                "coin flip and the expected result is a loss of about one round trip per "
                "trade. They exercise the execution path; they are not evidence of anything "
                "about the market."
            ),
        }
