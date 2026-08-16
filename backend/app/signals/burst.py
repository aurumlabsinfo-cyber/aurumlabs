"""AURUM BURST-15: a 15-minute operating window over a 5-second horizon.

Ported from the standalone `strategies/aurum_burst15.py` and wired into the
live engine, the persistence layer and the research tooling so the same rule
can be run live, replayed offline and validated walk-forward.

The idea, unchanged from the original:

    It does not try to predict where price will be in fifteen minutes - with
    this data that is not predictable. It opens a fifteen-minute WINDOW and,
    inside it, takes only bursts of tape on a five-second horizon.

Trigger, evaluated on each feature vector:

    n5    = trades in the last 5s                    >= BURST_N5_MIN
    |r10| = |10-second return| in bps                >= BURST_R10_MIN_BPS
    ofi5  = notional order-flow imbalance over 5s    same sign as r10
    direction = sign(r10)                            (momentum)

Entry is TIME-based, not a price touch: BURST_ENTRY_DELAY_MS after the trigger,
at whatever the market is then. That is what the original script does
(`entry t+1s, expiry entry+5s`) and it is materially different from the
ensemble path, where a signal waits for price to come to a trigger level.

Session control is the other half of the strategy and is not optional: a
cooldown between entries, a cap on trades, and a stop-loss / take-profit that
close the window early. A session that hits its stop is finished - the engine
stands down until the next one opens.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from app.agents.base import Direction, Regime
from app.config import Settings
from app.core.clock import now_ms
from app.core.logging_conf import get_logger
from app.signals.decision import Decision

log = get_logger(__name__)

#: Features the rule reads. Kept in one place so the research path
#: (app/ml/strategies.py, app/ml/burst_backtest.py) and the live path cannot
#: drift apart.
N5_FEATURE = "trade_count_5s"
R10_FEATURE = "return_10000ms"
OFI_FEATURE = "ofi_notional_5s"


@dataclass
class BurstSession:
    """One 15-minute operating window."""

    start_ts: int
    end_ts: int
    pnl_units: float = 0.0
    trades: int = 0
    wins: int = 0
    losses: int = 0
    ties: int = 0
    last_entry_ts: int = -10**18
    closed_reason: str | None = None
    open_signals: set[str] = field(default_factory=set)

    def is_open(self, ts: int) -> bool:
        return self.closed_reason is None and ts < self.end_ts

    def to_dict(self, ts: int | None = None) -> dict[str, Any]:
        ts = ts or now_ms()
        return {
            "start_ts": self.start_ts,
            "end_ts": self.end_ts,
            "remaining_ms": max(0, self.end_ts - ts),
            "pnl_units": round(self.pnl_units, 4),
            "trades": self.trades,
            "wins": self.wins,
            "losses": self.losses,
            "ties": self.ties,
            "open_signals": len(self.open_signals),
            "closed_reason": self.closed_reason,
            "is_open": self.is_open(ts),
        }


class BurstStrategy:
    """Live implementation of BURST-15.

    Produces the same `Decision` object the ensemble produces, so persistence,
    the WebSocket fan-out, paper trading and the statistics layer are untouched.
    """

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.session: BurstSession | None = None
        self.sessions_completed: int = 0
        self.history: list[dict[str, Any]] = []

    # ------------------------------------------------------------- sessions
    @property
    def payout(self) -> float:
        """Payout used for the session's own risk arithmetic.

        `BINARY_PAYOUT` when the operator has set it. When they have not, the
        session still needs a number to run a stop-loss against, so it falls
        back to BURST_ASSUMED_PAYOUT and says so in the API. Reported monetary
        P&L elsewhere stays PAYOUT UNKNOWN - this number never leaks into it.
        """
        if self.settings.binary_payout is not None:
            return float(self.settings.binary_payout)
        return float(self.settings.burst_assumed_payout)

    @property
    def payout_is_assumed(self) -> bool:
        return self.settings.binary_payout is None

    def open_session(self, ts: int | None = None) -> BurstSession:
        ts = ts or now_ms()
        self.session = BurstSession(
            start_ts=ts, end_ts=ts + self.settings.burst_session_s * 1000
        )
        log.info(
            "burst.session_opened",
            start_ts=ts, end_ts=self.session.end_ts,
            duration_s=self.settings.burst_session_s,
        )
        return self.session

    def close_session(self, reason: str, ts: int | None = None) -> None:
        if self.session is None or self.session.closed_reason:
            return
        ts = ts or now_ms()
        self.session.closed_reason = reason
        self.sessions_completed += 1
        self.history.append({**self.session.to_dict(ts), "closed_at": ts})
        self.history = self.history[-50:]
        log.info(
            "burst.session_closed",
            reason=reason, pnl_units=round(self.session.pnl_units, 3),
            trades=self.session.trades,
        )

    def _current_session(self, ts: int) -> BurstSession | None:
        """The session to trade in, opening the next one when allowed."""
        s = self.session
        if s is not None and s.closed_reason is None and ts >= s.end_ts:
            self.close_session("session window elapsed", ts)
            s = self.session
        if s is None:
            return self.open_session(ts) if self.settings.burst_auto_restart else None
        if s.closed_reason:
            # A session that stopped out does not reopen until its window would
            # have ended anyway: restarting immediately turns a stop-loss into
            # a suggestion.
            if self.settings.burst_auto_restart and ts >= s.end_ts:
                return self.open_session(ts)
            return None
        return s

    # -------------------------------------------------------------- outcome
    def on_settled(
        self, signal_id: str, result: str | None, ts: int | None = None
    ) -> None:
        """Fold a settled paper trade back into the session's P&L."""
        s = self.session
        if s is None:
            return
        s.open_signals.discard(signal_id)
        if result is None:
            return
        if result == "WIN":
            s.pnl_units += self.payout * self.settings.paper_stake
            s.wins += 1
        elif result == "LOSS":
            s.pnl_units -= self.settings.paper_stake
            s.losses += 1
        elif result == "TIE":
            s.ties += 1
        else:  # CANCELLED - never entered, so it costs nothing and counts as nothing
            return

        ts = ts or now_ms()
        if s.pnl_units <= self.settings.burst_stop_loss_units:
            self.close_session("session stop loss", ts)
        elif s.pnl_units >= self.settings.burst_take_profit_units:
            self.close_session("session take profit", ts)

    def on_entry(self, signal_id: str, ts: int) -> None:
        """Record that a signal was emitted into the current session."""
        if self.session is None:
            return
        self.session.trades += 1
        self.session.last_entry_ts = ts
        self.session.open_signals.add(signal_id)

    # ------------------------------------------------------------- decision
    def decide(
        self,
        feature_vector: dict[str, Any],
        market_state: dict[str, Any],
        health: dict[str, Any],
    ) -> Decision:
        s = self.settings
        f: dict[str, Any] = feature_vector.get("features", {})
        ts = int(feature_vector.get("ts") or now_ms())
        quality = health.get("data_quality", {})
        dq = float(quality.get("score", 0.0))
        ref_price = market_state.get("price") or f.get("mid") or 0.0

        reasons: list[str] = []
        if not s.signal_enabled:
            reasons.append("signal engine disabled by configuration")
        if not quality.get("warmup_complete", False):
            reasons.append("engine still warming up")

        session = self._current_session(ts)
        if session is None:
            closed = self.session.closed_reason if self.session else "no session"
            reasons.append(f"no open session: {closed}")
        else:
            if session.trades >= s.burst_max_trades_session:
                self.close_session("max trades reached", ts)
                reasons.append("max trades for this session reached")
            elif ts - session.last_entry_ts < s.burst_cooldown_ms:
                left = s.burst_cooldown_ms - (ts - session.last_entry_ts)
                reasons.append(f"cooldown, {left}ms left")
            elif session.open_signals:
                reasons.append("a trade from this session is still open")

        # ---------------------------------------------------------- guards
        staleness = health.get("feed_age_ms")
        if staleness is None:
            reasons.append("no market data received yet")
        elif staleness > s.burst_max_staleness_ms:
            reasons.append(f"feed stale: {staleness:.0f}ms")
        if dq < s.burst_min_data_quality:
            reasons.append(f"data quality {dq:.2f} < {s.burst_min_data_quality:.2f}")
        for r in quality.get("reasons", []):
            if r not in reasons:
                reasons.append(r)

        # --------------------------------------------------------- trigger
        n5 = f.get(N5_FEATURE)
        r10 = f.get(R10_FEATURE)
        ofi5 = f.get(OFI_FEATURE)
        if n5 is None or r10 is None:
            reasons.append("burst features unavailable (need 10s of history)")
        else:
            if n5 < s.burst_n5_min:
                reasons.append(
                    f"tape too quiet: {n5:.0f} trades/5s < {s.burst_n5_min}"
                )
            if abs(r10) < s.burst_r10_min_bps:
                reasons.append(
                    f"move {abs(r10):.2f}bps < {s.burst_r10_min_bps:g}bps over 10s"
                )
            if s.burst_require_ofi_agree and ofi5 is not None and r10 != 0:
                if (ofi5 > 0) != (r10 > 0):
                    reasons.append(
                        f"flow disagrees: ofi5 {ofi5:+.2f} vs r10 {r10:+.2f}bps"
                    )
            if r10 == 0:
                reasons.append("no 10s move to follow")

        direction = Direction.NO_TRADE
        lean = Direction.NO_TRADE
        if r10 is not None and r10 != 0:
            lean = Direction.UP if r10 > 0 else Direction.DOWN
        if not reasons and ref_price > 0 and lean is not Direction.NO_TRADE:
            direction = lean
        elif not reasons:
            reasons.append("no reference price")

        # BURST-15 is a rule, not a probability model: it does not claim a
        # calibrated P(up). What it does report is the strength of the trigger,
        # so the UI has something honest to render in the confidence slot and
        # the calibration report can measure what it is worth.
        strength = _strength(n5, r10, ofi5, s)
        confidence = 0.5 + 0.5 * strength if direction is not Direction.NO_TRADE else 0.0
        p_up = confidence if lean is Direction.UP else (1.0 - confidence)

        return Decision(
            ts=ts,
            symbol=feature_vector.get("symbol", s.symbol),
            exchange=feature_vector.get("exchange", ""),
            direction=direction,
            prob_up=p_up if direction is not Direction.NO_TRADE else 0.0,
            prob_down=(1.0 - p_up) if direction is not Direction.NO_TRADE else 0.0,
            prob_neutral=1.0 if direction is Direction.NO_TRADE else 0.0,
            confidence=confidence,
            edge=abs(confidence - 0.5),
            regime=Regime.BREAKOUT if direction is not Direction.NO_TRADE else Regime.RANGE,
            reference_price=ref_price,
            # Entry is time-based; there is no level to wait for. The reference
            # price is carried through so the UI has something to show.
            trigger_price=ref_price if direction is not Direction.NO_TRADE else None,
            horizon_s=s.signal_horizon_s,
            no_trade_reasons=reasons,
            agents=[],
            aggregate_score=strength if lean is Direction.UP else -strength,
            data_quality=dq,
            lean=lean,
            lean_confidence=0.5 + 0.5 * strength,
            entry_mode="DELAY",
            entry_delay_ms=s.burst_entry_delay_ms,
            detail={
                "strategy": "burst15",
                "n5": n5,
                "r10_bps": r10,
                "ofi5": ofi5,
                "thresholds": {
                    "n5_min": s.burst_n5_min,
                    "r10_min_bps": s.burst_r10_min_bps,
                    "require_ofi_agree": s.burst_require_ofi_agree,
                },
                "session": session.to_dict(ts) if session else None,
                "payout_assumed": self.payout_is_assumed,
            },
        )

    # ----------------------------------------------------------------- views
    def status(self) -> dict[str, Any]:
        ts = now_ms()
        return {
            "strategy": "burst15",
            "session": self.session.to_dict(ts) if self.session else None,
            "sessions_completed": self.sessions_completed,
            "recent_sessions": self.history[-10:][::-1],
            "config": {
                "n5_min": self.settings.burst_n5_min,
                "r10_min_bps": self.settings.burst_r10_min_bps,
                "require_ofi_agree": self.settings.burst_require_ofi_agree,
                "horizon_s": self.settings.signal_horizon_s,
                "entry_delay_ms": self.settings.burst_entry_delay_ms,
                "session_s": self.settings.burst_session_s,
                "cooldown_ms": self.settings.burst_cooldown_ms,
                "max_trades_session": self.settings.burst_max_trades_session,
                "stop_loss_units": self.settings.burst_stop_loss_units,
                "take_profit_units": self.settings.burst_take_profit_units,
                "auto_restart": self.settings.burst_auto_restart,
            },
            "payout_used_for_session_risk": self.payout,
            "payout_is_assumed": self.payout_is_assumed,
            "note": (
                "Session P&L is in stake units on paper trades only. When "
                "BINARY_PAYOUT is unset the stop-loss arithmetic uses "
                "BURST_ASSUMED_PAYOUT; monetary P&L elsewhere still reports "
                "PAYOUT UNKNOWN rather than inventing one."
            ),
        }


def _strength(
    n5: float | None, r10: float | None, ofi5: float | None, s: Settings
) -> float:
    """0..1 summary of how far past its thresholds the trigger fired.

    Not a probability, and never presented as one: it exists so a stronger
    burst is visibly stronger, and so the calibration report has a value to
    bucket by.
    """
    if n5 is None or r10 is None:
        return 0.0
    import math

    move = abs(r10) / max(s.burst_r10_min_bps, 1e-9)
    tape = n5 / max(s.burst_n5_min, 1)
    flow = abs(ofi5) if ofi5 is not None else 0.0
    raw = 0.5 * math.tanh(move - 1.0) + 0.3 * math.tanh(tape - 1.0) + 0.2 * flow
    return max(0.0, min(1.0, raw))
