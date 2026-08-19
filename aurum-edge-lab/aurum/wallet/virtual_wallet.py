"""The €100 virtual wallet.

Every cycle starts at exactly €100.00 and every movement of that money is
written to a ledger before it is reflected in a balance.  The ledger is the
record; the balance is a cache of it.  That ordering is what makes a post-mortem
possible months later, and it is why a reset does not delete anything — it
opens a new cycle beside the old one.

Balances tracked separately, because conflating them hides the failure that
matters:

``balance``     realised money.  Moves only when a trade closes or a fee is paid.
``reserved``    margin committed to open positions.
``available``   ``balance - reserved``.  What a new position may draw on.
``equity``      ``balance + unrealised``.  What the cycle is actually worth.

Drawdown is measured against peak *equity*, not peak balance: a cycle that ran
to €140 unrealised and closed at €95 drew down 32%, and a balance-only view
would call it a 5% loss.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from ..domain import now_ms
from ..logging_setup import get_logger
from ..storage.repositories import WalletRepository

log = get_logger("wallet")


class LedgerKind:
    CYCLE_START = "CYCLE_START"
    RESERVE = "RESERVE"
    RELEASE = "RELEASE"
    ENTRY_FEE = "ENTRY_FEE"
    EXIT_FEE = "EXIT_FEE"
    REALIZED_PNL = "REALIZED_PNL"
    ADJUSTMENT = "ADJUSTMENT"


@dataclass
class WalletState:
    cycle_id: int = 1
    starting_balance: float = 100.0
    balance: float = 100.0
    reserved: float = 0.0
    realized_pnl: float = 0.0
    unrealized_pnl: float = 0.0
    peak_equity: float = 100.0
    trades: int = 0
    wins: int = 0
    losses: int = 0
    fees_paid: float = 0.0
    currency: str = "EUR"
    opened_ms: int = 0

    @property
    def available(self) -> float:
        return max(0.0, self.balance - self.reserved)

    @property
    def equity(self) -> float:
        return self.balance + self.unrealized_pnl

    @property
    def drawdown_pct(self) -> float:
        if self.peak_equity <= 0:
            return 0.0
        return max(0.0, (self.peak_equity - self.equity) / self.peak_equity * 100.0)

    @property
    def return_pct(self) -> float:
        if self.starting_balance <= 0:
            return 0.0
        return (self.equity - self.starting_balance) / self.starting_balance * 100.0

    @property
    def win_rate(self) -> float:
        decided = self.wins + self.losses
        return self.wins / decided if decided else 0.0

    def to_dict(self) -> dict[str, Any]:
        return {
            "cycle_id": self.cycle_id,
            "currency": self.currency,
            "starting_balance": round(self.starting_balance, 2),
            "balance": round(self.balance, 6),
            "available": round(self.available, 6),
            "reserved": round(self.reserved, 6),
            "equity": round(self.equity, 6),
            "realized_pnl": round(self.realized_pnl, 6),
            "unrealized_pnl": round(self.unrealized_pnl, 6),
            "fees_paid": round(self.fees_paid, 6),
            "peak_equity": round(self.peak_equity, 6),
            "drawdown_pct": round(self.drawdown_pct, 4),
            "return_pct": round(self.return_pct, 4),
            "trades": self.trades,
            "wins": self.wins,
            "losses": self.losses,
            "win_rate": round(self.win_rate, 4),
            "opened_ms": self.opened_ms,
        }


class InsufficientFunds(Exception):
    """Raised when a reservation exceeds what is available.

    Deliberately an exception rather than a silent clamp: a broker that quietly
    halves a position size produces trades whose P&L cannot be reconciled with
    the strategy that asked for them.
    """


class VirtualWallet:
    def __init__(
        self,
        repo: WalletRepository,
        *,
        starting_balance: float = 100.0,
        currency: str = "EUR",
        cycle_id: int = 1,
    ) -> None:
        self.repo = repo
        self.starting_balance = round(starting_balance, 2)
        self.state = WalletState(
            cycle_id=cycle_id,
            starting_balance=self.starting_balance,
            balance=self.starting_balance,
            peak_equity=self.starting_balance,
            currency=currency,
            opened_ms=now_ms(),
        )
        self._daily_start_equity = self.starting_balance
        self._daily_key = self._day_key(now_ms())

    # -------------------------------------------------------------- lifecycle

    def open_cycle(self, cycle_id: int) -> WalletState:
        """Start a fresh cycle at exactly the configured starting balance."""
        self.state = WalletState(
            cycle_id=cycle_id,
            starting_balance=self.starting_balance,
            balance=self.starting_balance,
            peak_equity=self.starting_balance,
            currency=self.state.currency,
            opened_ms=now_ms(),
        )
        self._daily_start_equity = self.starting_balance
        self._daily_key = self._day_key(now_ms())
        self._ledger(LedgerKind.CYCLE_START, self.starting_balance, reference=f"cycle-{cycle_id}",
                     detail="cycle opened")
        self.snapshot()
        log.info("cycle opened", extra={"cycle": cycle_id, "balance": self.starting_balance})
        return self.state

    def restore(self, row: dict[str, Any]) -> None:
        """Rebuild in-memory state from the last persisted snapshot."""
        self.state = WalletState(
            cycle_id=int(row["cycle_id"]),
            starting_balance=float(row["starting_balance"]),
            balance=float(row["balance"]),
            reserved=float(row.get("reserved") or 0.0),
            realized_pnl=float(row.get("realized_pnl") or 0.0),
            unrealized_pnl=float(row.get("unrealized_pnl") or 0.0),
            peak_equity=float(row.get("peak_equity") or row["balance"]),
            trades=int(row.get("trades") or 0),
            wins=int(row.get("wins") or 0),
            losses=int(row.get("losses") or 0),
            currency=row.get("currency") or "EUR",
        )
        self._daily_start_equity = self.state.equity
        self._daily_key = self._day_key(now_ms())

    # ----------------------------------------------------------------- money

    def reserve(self, amount: float, reference: str) -> float:
        amount = round(amount, 8)
        if amount <= 0:
            raise InsufficientFunds("reservation must be positive")
        if amount > self.state.available + 1e-9:
            raise InsufficientFunds(
                f"reserve {amount:.4f} exceeds available {self.state.available:.4f}"
            )
        self.state.reserved += amount
        self._ledger(LedgerKind.RESERVE, -amount, reference=reference, detail="margin reserved")
        return amount

    def release(self, amount: float, reference: str) -> None:
        amount = round(min(amount, self.state.reserved), 8)
        if amount <= 0:
            return
        self.state.reserved -= amount
        self._ledger(LedgerKind.RELEASE, amount, reference=reference, detail="margin released")

    def charge_fee(self, amount: float, reference: str, *, kind: str = LedgerKind.ENTRY_FEE) -> None:
        amount = round(amount, 8)
        if amount <= 0:
            return
        self.state.balance -= amount
        self.state.fees_paid += amount
        self._ledger(kind, -amount, reference=reference, detail="execution fee")

    def settle(self, gross_pnl: float, reference: str, *, won: bool | None = None) -> float:
        """Book a closed trade's gross P&L.  Fees are charged separately so the
        ledger shows what the market gave and what the venue took."""
        gross_pnl = round(gross_pnl, 8)
        self.state.balance += gross_pnl
        self.state.realized_pnl += gross_pnl
        self.state.trades += 1
        if won is None:
            won = gross_pnl > 0
        if won:
            self.state.wins += 1
        else:
            self.state.losses += 1
        self._ledger(LedgerKind.REALIZED_PNL, gross_pnl, reference=reference, detail="trade settled")
        self._touch_peak()
        return self.state.balance

    def mark_unrealized(self, total: float) -> None:
        self.state.unrealized_pnl = round(total, 8)
        self._touch_peak()

    def _touch_peak(self) -> None:
        if self.state.equity > self.state.peak_equity:
            self.state.peak_equity = self.state.equity

    # ------------------------------------------------------------ daily view

    @staticmethod
    def _day_key(ms: int) -> int:
        return ms // 86_400_000

    def daily_pnl_pct(self, *, at_ms: int | None = None) -> float:
        stamp = at_ms if at_ms is not None else now_ms()
        key = self._day_key(stamp)
        if key != self._daily_key:
            self._daily_key = key
            self._daily_start_equity = self.state.equity
        if self._daily_start_equity <= 0:
            return 0.0
        return (self.state.equity - self._daily_start_equity) / self._daily_start_equity * 100.0

    # ----------------------------------------------------------- persistence

    def _ledger(self, kind: str, amount: float, *, reference: str, detail: str = "") -> None:
        self.repo.ledger(
            {
                "ts_ms": now_ms(),
                "cycle_id": self.state.cycle_id,
                "kind": kind,
                "amount": round(amount, 8),
                "balance_after": round(self.state.balance, 8),
                "reference": reference,
                "detail": detail,
            }
        )

    def snapshot(self) -> None:
        state = self.state
        self.repo.snapshot(
            {
                "cycle_id": state.cycle_id,
                "ts_ms": now_ms(),
                "starting_balance": state.starting_balance,
                "balance": state.balance,
                "available": state.available,
                "reserved": state.reserved,
                "equity": state.equity,
                "realized_pnl": state.realized_pnl,
                "unrealized_pnl": state.unrealized_pnl,
                "peak_equity": state.peak_equity,
                "drawdown_pct": state.drawdown_pct,
                "trades": state.trades,
                "wins": state.wins,
                "losses": state.losses,
                "currency": state.currency,
            }
        )

    def to_dict(self) -> dict[str, Any]:
        data = self.state.to_dict()
        data["daily_pnl_pct"] = round(self.daily_pnl_pct(), 4)
        return data
