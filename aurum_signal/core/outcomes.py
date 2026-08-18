"""Determinazione dell'esito e misurazione delle latenze.

Sull'esito c'e' una sola regola che conta e va dichiarata una volta per tutte:
**si usa sempre lo stesso prezzo**, il mid, sia all'ingresso sia alla scadenza.
Mescolare — entrare sul mid e chiudere sul bid — produrrebbe uno svantaggio
sistematico di mezzo spread che non viene dal mercato ma dal codice, e che
sposterebbe ogni statistica costruita sopra.

Il pareggio esiste ed e' importante: su una binaria che non lo paga, una
chiusura identica all'apertura e' capitale immobilizzato per niente. Contarlo
come vittoria o sconfitta falserebbe il tasso di vittoria proprio nella zona
che decide se si e' sopra o sotto il pareggio economico.
"""

from __future__ import annotations

import statistics
from collections import defaultdict, deque
from typing import Any

from .signals import DRAW, LOSS, PRICE_BASIS, UNKNOWN, WIN


class OutcomeEngine:
    """Chiude i segnali scaduti e riporta l'esito a chi deve impararlo."""

    def __init__(self, cfg, db, signals, wallet) -> None:
        self.cfg = cfg
        self.db = db
        self.signals = signals
        self.wallet = wallet
        self.listeners: list = []

    def settle_due(self, now: int, price: float | None) -> list:
        """Chiude tutto cio' che e' scaduto e propaga l'esito."""
        closed = []
        for sig in self.signals.due_for_settlement(now):
            result = self.signals.settle(sig, price, now)
            pnl = self.wallet.settle(sig.signal_id, result, now)
            sig.pnl = pnl
            sig.balance_after = self.wallet.balance
            if self.db is not None:
                self.db.upsert("signals", sig.to_row())
            for fn in self.listeners:
                try:
                    fn(sig, result)
                except Exception:  # noqa: BLE001 - un ascoltatore non ferma il resto
                    pass
            closed.append(sig)
        return closed

    def settle_shadow(self, now: int, price_at: Any) -> int:
        """Valuta a posteriori le decisioni BLOCCATE.

        E' la parte che quasi nessun sistema fa, ed e' quella che dice se un
        filtro protegge o costa. Senza, si puo' solo contare quante volte un
        cancello e' scattato — che non e' la stessa domanda.
        """
        rows = self.db.query(
            "SELECT decision_id, entry_ts, expiry_ts, direction, entry_price "
            "FROM shadow_decisions WHERE result IS NULL AND expiry_ts <= ?",
            (now,))
        n = 0
        for r in rows:
            entry = r["entry_price"]
            exit_price = price_at(r["expiry_ts"])
            if entry is None or exit_price is None:
                result = UNKNOWN
            elif exit_price == entry:
                result = DRAW
            elif (exit_price > entry) == (r["direction"] == "CALL"):
                result = WIN
            else:
                result = LOSS
            self.db.upsert("shadow_decisions", {
                "decision_id": r["decision_id"], "ts": r["entry_ts"],
                "expiry_price": exit_price, "result": result})
            n += 1
        return n


class LatencyTracker:
    """Misura ogni stadio della pipeline e ne riporta p50, p95 e p99.

    Le percentuali alte contano piu' della media: un sistema che decide in 40
    ms in media ma in 900 ms una volta su venti sbaglia proprio quando il
    mercato si muove, ed e' la media a nasconderlo.
    """

    STAGES = ("market_data", "features", "agents", "model", "booster",
              "decision", "notification", "total")

    def __init__(self, db=None, window: int = 2000) -> None:
        self.db = db
        self.samples: dict[str, deque] = defaultdict(lambda: deque(maxlen=window))
        self.counts: dict[str, int] = defaultdict(int)

    def record(self, stage: str, ms: float, ts: int | None = None) -> None:
        self.samples[stage].append(float(ms))
        self.counts[stage] += 1
        if self.db is not None and ts is not None and self.counts[stage] % 20 == 0:
            # Non si scrive ogni campione: registrare 10 righe al secondo di
            # latenza costerebbe piu' della latenza che si sta misurando.
            self.db.add("latency_metrics", {"ts": ts, "stage": stage, "ms": ms})

    def record_many(self, stages: dict[str, float], ts: int | None = None) -> None:
        for stage, ms in stages.items():
            self.record(stage.replace("_ms", ""), ms, ts)

    @staticmethod
    def _pct(values: list[float], q: float) -> float | None:
        if not values:
            return None
        s = sorted(values)
        if len(s) == 1:
            return s[0]
        pos = q * (len(s) - 1)
        lo = int(pos)
        hi = min(lo + 1, len(s) - 1)
        return s[lo] + (s[hi] - s[lo]) * (pos - lo)

    def report(self) -> dict[str, Any]:
        out: dict[str, Any] = {"stages": {}}
        total_p50 = 0.0
        for stage, vals in self.samples.items():
            v = list(vals)
            if not v:
                continue
            p50 = self._pct(v, 0.50)
            out["stages"][stage] = {
                "samples": len(v),
                "last": round(v[-1], 3),
                "mean": round(statistics.fmean(v), 3),
                "p50": round(p50, 3) if p50 is not None else None,
                "p95": round(self._pct(v, 0.95), 3),
                "p99": round(self._pct(v, 0.99), 3),
                "max": round(max(v), 3),
            }
            if stage != "total" and p50:
                total_p50 += p50
        out["pipeline_p50_ms"] = round(total_p50, 3)
        budget = 500.0
        out["budget_ms"] = budget
        out["within_budget"] = total_p50 < budget
        out["note"] = ("Un sistema semplice che decide in 50 ms vale piu' di uno "
                       "sofisticato che decide in 5 secondi: su un orizzonte da "
                       "60 secondi il ritardo e' una risposta a un'altra domanda.")
        return out
