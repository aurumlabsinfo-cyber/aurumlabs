"""Portafoglio virtuale e cicli.

Non rappresenta il conto reale di nessun broker: e' uno **strumento di
misura**. Serve a rispondere a una domanda che il tasso di vittoria da solo non
risponde — quanto dura questa strategia prima di consumare 500 euro a 25 per
operazione, e i cicli si allungano man mano che il sistema impara?

Tre regole non negoziabili.

**Il saldo e' la somma del registro**, non un contatore in memoria. Ogni
movimento e' una riga con il saldo risultante: il conto si ricostruisce e si
verifica, anche dopo un riavvio.

**La puntata si fissa all'ingresso.** Calcolarla alla chiusura significherebbe
pagare le perdite col saldo di prima e incassare le vincite con quello di dopo.

**Ricominciare non recupera niente.** Il capitale del ciclo bruciato e' perso.
Quello che i cicli misurano e' quanti ne servono e quanto durano, non una
seconda possibilita' sulla stessa puntata.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from typing import Any, Callable

from .signals import DRAW, LOSS, WIN

OPERATIVE, STUDY, CLOSED = "OPERATIVE", "STUDY", "CLOSED"


@dataclass
class CycleStats:
    cycle_id: int
    started_ts: int
    starting_balance: float
    ended_ts: int | None = None
    ending_balance: float | None = None
    trades: int = 0
    wins: int = 0
    losses: int = 0
    draws: int = 0
    peak_balance: float = 0.0
    max_drawdown: float = 0.0
    win_streak: int = 0
    loss_streak: int = 0
    best_win_streak: int = 0
    worst_loss_streak: int = 0
    gross_profit: float = 0.0
    gross_loss: float = 0.0
    reason_closed: str | None = None
    strategy_version: str = ""
    model_version: str = ""
    booster_version: str = ""

    @property
    def decided(self) -> int:
        return self.wins + self.losses

    @property
    def win_rate(self) -> float | None:
        return (self.wins / self.decided) if self.decided else None

    @property
    def roi(self) -> float | None:
        if not self.starting_balance or self.ending_balance is None:
            return None
        return (self.ending_balance - self.starting_balance) / self.starting_balance

    @property
    def profit_factor(self) -> float | None:
        """Quanto si guadagna per ogni euro perso. Sotto 1 si sta perdendo.

        Senza perdite il rapporto sarebbe infinito, e infinito qui non e' un
        risultato: e' l'assenza del denominatore. Restituiva `float("inf")`, e
        siccome JSON non ammette l'infinito la rotta `/wallet` rispondeva 500
        appena arrivava la prima vittoria prima della prima perdita — cioe' nel
        caso piu' banale possibile. `None` significa "non calcolabile", ed e'
        cio' che l'interfaccia gia' sa mostrare.
        """
        if self.gross_loss <= 0:
            return None
        return self.gross_profit / self.gross_loss

    @property
    def expectancy(self) -> float | None:
        """Guadagno atteso per operazione, in euro. E' il numero che conta."""
        if not self.trades:
            return None
        return (self.gross_profit - self.gross_loss) / self.trades

    def to_dict(self) -> dict[str, Any]:
        return {
            "cycle_id": self.cycle_id, "started_ts": self.started_ts,
            "ended_ts": self.ended_ts,
            "starting_balance": round(self.starting_balance, 2),
            "ending_balance": (round(self.ending_balance, 2)
                               if self.ending_balance is not None else None),
            "trades": self.trades, "wins": self.wins, "losses": self.losses,
            "draws": self.draws, "decided": self.decided,
            "win_rate": round(self.win_rate, 4) if self.win_rate is not None else None,
            "roi": round(self.roi, 4) if self.roi is not None else None,
            "max_drawdown": round(self.max_drawdown, 2),
            "peak_balance": round(self.peak_balance, 2),
            "profit_factor": (round(self.profit_factor, 3)
                              if self.profit_factor not in (None, float("inf"))
                              else self.profit_factor),
            "expectancy": (round(self.expectancy, 4)
                           if self.expectancy is not None else None),
            "best_win_streak": self.best_win_streak,
            "worst_loss_streak": self.worst_loss_streak,
            "reason_closed": self.reason_closed,
            "strategy_version": self.strategy_version,
            "model_version": self.model_version,
            "booster_version": self.booster_version,
        }

    def to_row(self) -> dict[str, Any]:
        d = self.to_dict()
        d.pop("decided", None)
        d.pop("peak_balance", None)
        d.pop("profit_factor", None)
        d.pop("expectancy", None)
        d.pop("best_win_streak", None)
        d.pop("worst_loss_streak", None)
        return d


class VirtualWallet:
    """500 euro, 25 a operazione, e un ciclo che finisce quando finiscono."""

    def __init__(self, cfg, db=None,
                 on_cycle_failed: Callable[["VirtualWallet"], None] | None = None) -> None:
        self.cfg = cfg
        self.db = db
        self.on_cycle_failed = on_cycle_failed
        self.state = OPERATIVE
        self.cycle_id = 1
        self.balance = float(cfg.virtual_capital)
        self.cycles: list[CycleStats] = []
        self.current = CycleStats(cycle_id=1, started_ts=self._now(),
                                  starting_balance=self.balance,
                                  peak_balance=self.balance)
        self._reserved: dict[str, float] = {}
        self.equity: list[tuple[int, float]] = [(self._now(), self.balance)]
        self.study_report: dict[str, Any] | None = None
        self._load()

    def _now(self) -> int:
        return int(time.time() * 1000)

    # ------------------------------------------------------------- capitale
    @property
    def stake(self) -> float:
        return round(float(self.cfg.virtual_stake), 2)

    @property
    def exposure(self) -> float:
        return round(sum(self._reserved.values()), 2)

    def can_trade(self) -> tuple[bool, str]:
        if self.state == STUDY:
            return False, f"ciclo {self.cycle_id} in studio dopo il fallimento"
        if self.state == CLOSED:
            return False, f"ciclo {self.cycle_id} chiuso"
        if self.balance < self.stake:
            return False, (f"saldo {self.balance:.2f} sotto la puntata "
                           f"{self.stake:.2f}")
        return True, ""

    def is_failed(self) -> bool:
        """Il ciclo e' finito quando il conto non regge piu' una puntata."""
        return self.balance < self.stake

    # -------------------------------------------------------------- movimenti
    def reserve(self, signal_id: str, ts: int | None = None) -> float:
        """Blocca la puntata all'ingresso: e' capitale a rischio."""
        ok, _ = self.can_trade()
        if not ok:
            return 0.0
        stake = self.stake
        self._reserved[signal_id] = stake
        return stake

    def release(self, signal_id: str) -> None:
        """Libera la puntata di un segnale che non e' MAI entrato a mercato.

        Un segnale annullato non e' un'operazione: non ha esito, non muove il
        saldo e non deve comparire nel conteggio. Contarlo gonfierebbe il
        numero di operazioni e sballerebbe ogni tasso calcolato su di esso.
        """
        self._reserved.pop(signal_id, None)

    def settle(self, signal_id: str, result: str, ts: int | None = None) -> float | None:
        """Applica l'esito e scrive la riga di registro."""
        stake = self._reserved.pop(signal_id, None)
        if stake is None:
            return None
        ts = ts or self._now()
        payout = float(self.cfg.payout)
        if result == WIN:
            amount = stake * payout
            self.current.gross_profit += amount
            self.current.wins += 1
            self.current.win_streak += 1
            self.current.loss_streak = 0
            self.current.best_win_streak = max(self.current.best_win_streak,
                                               self.current.win_streak)
        elif result == LOSS:
            amount = -stake
            self.current.gross_loss += stake
            self.current.losses += 1
            self.current.loss_streak += 1
            self.current.win_streak = 0
            self.current.worst_loss_streak = max(self.current.worst_loss_streak,
                                                 self.current.loss_streak)
        else:                                  # DRAW o UNKNOWN
            amount = 0.0
            self.current.draws += 1

        self.balance = round(self.balance + amount, 2)
        self.current.trades += 1
        self.current.peak_balance = max(self.current.peak_balance, self.balance)
        self.current.max_drawdown = min(
            self.current.max_drawdown, self.balance - self.current.peak_balance)
        self.equity.append((ts, self.balance))
        self.equity = self.equity[-5000:]
        self._write("TRADE", ts, signal_id=signal_id, result=result,
                    stake=stake, amount=round(amount, 2))

        if self.is_failed() and self.state == OPERATIVE:
            self.fail_cycle(ts, f"saldo {self.balance:.2f} sotto la puntata "
                                f"{self.stake:.2f}")
        return amount

    # ---------------------------------------------------------------- cicli
    def fail_cycle(self, ts: int | None = None, reason: str = "") -> CycleStats:
        """Chiude il ciclo. **Non cancella niente**: salva tutto e passa allo studio."""
        ts = ts or self._now()
        c = self.current
        c.ended_ts = ts
        c.ending_balance = self.balance
        c.reason_closed = reason or "capitale esaurito"
        self.cycles.append(c)
        self.state = STUDY
        self._write("CLOSE", ts, note=c.reason_closed)
        if self.db is not None:
            self.db.upsert("wallet_cycles", c.to_row())
            self.db.event("wallet", "WARNING",
                          f"ciclo {c.cycle_id} fallito", c.to_dict())
        if self.on_cycle_failed is not None:
            try:
                self.on_cycle_failed(self)
            except Exception as exc:  # noqa: BLE001 - lo studio non blocca il motore
                self.study_report = {"error": f"{type(exc).__name__}: {exc}"}
                self.start_new_cycle("studio fallito: si riparte comunque")
        return c

    def start_new_cycle(self, note: str = "",
                        versions: dict[str, str] | None = None) -> None:
        """Riapre con il capitale iniziale. Il ciclo precedente resta perso."""
        ts = self._now()
        self.cycle_id += 1
        self.balance = float(self.cfg.virtual_capital)
        self._reserved.clear()
        self.state = OPERATIVE
        v = versions or {}
        self.current = CycleStats(
            cycle_id=self.cycle_id, started_ts=ts,
            starting_balance=self.balance, peak_balance=self.balance,
            strategy_version=v.get("strategy", ""),
            model_version=v.get("model", ""),
            booster_version=v.get("booster", ""))
        self.equity.append((ts, self.balance))
        self._write("OPEN", ts, note=note or f"ciclo {self.cycle_id}")
        if self.db is not None:
            self.db.upsert("wallet_cycles", self.current.to_row())

    # ------------------------------------------------------------- registro
    def _write(self, kind: str, ts: int, **fields: Any) -> None:
        if self.db is None:
            return
        row = {"ts": ts, "cycle_id": self.cycle_id, "kind": kind,
               "signal_id": None, "result": None, "stake": None,
               "amount": 0.0, "balance_after": self.balance, "note": None}
        row.update(fields)
        self.db.add("wallet_ledger", row)

    def _load(self) -> None:
        """Ricostruisce saldo e ciclo dal registro, se esiste gia'."""
        if self.db is None:
            self._write("OPEN", self._now(), note="ciclo 1")
            return
        self.db.flush()
        rows = self.db.query(
            "SELECT * FROM wallet_ledger ORDER BY id ASC")
        if not rows:
            self._write("OPEN", self._now(), note="ciclo 1")
            if self.db is not None:
                self.db.upsert("wallet_cycles", self.current.to_row())
            return
        last = rows[-1]
        self.balance = float(last["balance_after"])
        self.cycle_id = int(last["cycle_id"] or 1)
        self.equity = [(int(r["ts"]), float(r["balance_after"])) for r in rows]
        cycles = self.db.query(
            "SELECT * FROM wallet_cycles ORDER BY cycle_id ASC")
        for c in cycles:
            if c["ended_ts"]:
                self.cycles.append(CycleStats(
                    cycle_id=int(c["cycle_id"]), started_ts=int(c["started_ts"]),
                    starting_balance=float(c["starting_balance"] or 0),
                    ended_ts=int(c["ended_ts"]),
                    ending_balance=float(c["ending_balance"] or 0),
                    trades=int(c["trades"] or 0), wins=int(c["wins"] or 0),
                    losses=int(c["losses"] or 0), draws=int(c["draws"] or 0),
                    max_drawdown=float(c["max_drawdown"] or 0),
                    reason_closed=c["reason_closed"]))
        # Ricostruisce il ciclo in corso dalle sue operazioni.
        current_rows = [r for r in rows
                        if int(r["cycle_id"] or 1) == self.cycle_id
                        and r["kind"] == "TRADE"]
        self.current = CycleStats(
            cycle_id=self.cycle_id,
            started_ts=int(rows[0]["ts"]),
            starting_balance=float(self.cfg.virtual_capital),
            peak_balance=max([float(r["balance_after"]) for r in current_rows]
                             + [self.balance]))
        for r in current_rows:
            self.current.trades += 1
            res = r["result"]
            amount = float(r["amount"] or 0)
            if res == WIN:
                self.current.wins += 1
                self.current.gross_profit += amount
            elif res == LOSS:
                self.current.losses += 1
                self.current.gross_loss += abs(amount)
            else:
                self.current.draws += 1
        if self.is_failed():
            self.state = STUDY

    # ---------------------------------------------------------------- viste
    def status(self) -> dict[str, Any]:
        c = self.current
        pnl = self.balance - c.starting_balance
        closed = [x for x in self.cycles if x.ended_ts]
        return {
            "state": self.state,
            "cycle_id": self.cycle_id,
            "balance": round(self.balance, 2),
            "starting_capital": round(c.starting_balance, 2),
            "stake": self.stake,
            "payout": self.cfg.payout,
            "breakeven_win_rate": round(self.cfg.breakeven_win_rate, 4),
            "exposure": self.exposure,
            "open_trades": len(self._reserved),
            "profit": round(pnl, 2),
            "roi": round(pnl / c.starting_balance, 4) if c.starting_balance else None,
            "trades": c.trades, "wins": c.wins, "losses": c.losses,
            "draws": c.draws, "decided": c.decided,
            "win_rate": round(c.win_rate, 4) if c.win_rate is not None else None,
            "expectancy": (round(c.expectancy, 4)
                           if c.expectancy is not None else None),
            "profit_factor": (round(c.profit_factor, 3)
                              if c.profit_factor not in (None, float("inf"))
                              else c.profit_factor),
            "peak_balance": round(c.peak_balance, 2),
            "max_drawdown": round(c.max_drawdown, 2),
            "win_streak": c.win_streak, "loss_streak": c.loss_streak,
            "best_win_streak": c.best_win_streak,
            "worst_loss_streak": c.worst_loss_streak,
            "trades_to_zero": int(self.balance // self.stake) if self.stake else 0,
            "cycles_failed": len(closed),
            "avg_cycle_trades": (round(sum(x.trades for x in closed) / len(closed), 1)
                                 if closed else None),
            "last_study": self.study_report,
            "note": ("Denaro FINTO: non rappresenta il saldo di alcun broker. "
                     "Ricominciare dopo un fallimento non recupera il capitale "
                     "del ciclo bruciato."),
        }

    def cycle_history(self) -> list[dict[str, Any]]:
        return [c.to_dict() for c in reversed(self.cycles)] + \
               ([self.current.to_dict()] if self.current.ended_ts is None else [])

    def equity_curve(self, points: int = 300) -> list[dict[str, Any]]:
        rows = self.equity
        if len(rows) > points:
            step = len(rows) / points
            rows = [rows[int(i * step)] for i in range(points)] + [rows[-1]]
        return [{"t": t, "balance": round(b, 2)} for t, b in rows]
