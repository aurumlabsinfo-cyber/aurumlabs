"""Il ciclo di vita del segnale, da T-lead a T+60.

Questo file implementa il principio centrale del progetto: **non si aspetta il
momento dell'entrata per decidere**. Il motore studia l'opportunita' futura in
continuazione e il segnale nasce con almeno `signal_lead_seconds` di anticipo,
perche' l'operazione la esegue una persona e una persona ha bisogno di tempo.

    T-60   osservazione, nessun impegno
    T-30   PRE_SIGNAL: c'e' un candidato, arriva la notifica
    T-20   monitoraggio: la direzione regge?
    T-10   CONFIRMED oppure CANCELLED
    T-5    congelato: da qui non cambia piu'
    T      entrata (manuale)
    T+60   scadenza ed esito

Il costo statistico dell'anticipo va detto, perche' e' reale: a T-30 il motore
sta predicendo t+90, non t+60. Trenta secondi in piu' di futuro su un cambio
che a 60 secondi e' gia' quasi imprevedibile. Il sistema misura quel costo —
`lead_ms` finisce su ogni riga — invece di far finta che sia gratis.
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable

from .agents import CALL, PUT
from .decision import Decision

# Stati del segnale.
WATCH = "WATCH"
PRE_SIGNAL = "PRE_SIGNAL"
CONFIRMED = "CONFIRMED_SIGNAL"
FROZEN = "FROZEN"
ENTERED = "ENTERED"
CANCELLED = "CANCELLED"
EXPIRED = "EXPIRED"

# Esiti.
WIN, LOSS, DRAW, UNKNOWN = "WIN", "LOSS", "DRAW", "UNKNOWN"

#: Il prezzo che decide l'esito. Dichiararlo una volta ed usarlo sempre e'
#: l'unico modo di avere esiti confrontabili: mescolare mid all'ingresso e bid
#: alla scadenza produrrebbe un vantaggio o uno svantaggio sistematico che non
#: viene dal mercato ma dal codice.
PRICE_BASIS = "mid"

#: Offset delle fotografie salvate prima dell'entrata, in secondi.
PREVIEW_OFFSETS = (-30, -25, -20, -15, -10, -5, 0)

#: Blocchi che un segnale gia' aperto causa da solo.
#:
#: Con `max_concurrent_signals = 1`, il segnale aperto occupa lo slot: da quel
#: momento OGNI valutazione successiva porta MAX_CONCURRENT e quindi
#: `direction = NO_TRADE`. Leggerli come deterioramento significava annullare
#: il segnale perche' esiste — ed e' esattamente il difetto che si vedeva:
#: 84 segnali creati, 84 annullati, 0 entrate. Lo stesso vale per il
#: portafoglio, che ha gia' impegnato la puntata di questo segnale.
STRUCTURAL_BLOCKERS = frozenset({"MAX_CONCURRENT", "WALLET_EXHAUSTED"})

#: Caduta minima di probabilita', rispetto a quella di partenza, perche' si
#: parli di deterioramento e non di rumore.
CANCEL_MIN_DROP = 0.06

#: Per quanto il deterioramento deve persistere prima di annullare.
#:
#: Il motore valuta quattro volte al secondo: senza questa attesa, un solo tick
#: sfortunato basterebbe ad annullare un segnale valido. Due secondi sono
#: abbastanza per distinguere una tendenza che cede da un sussulto.
CANCEL_PERSIST_MS = 2_000


@dataclass
class SignalUpdate:
    ts: int
    seconds_to_entry: float
    state: str
    direction: str
    probability: float
    confidence: float
    edge: float
    order_flow: float | None = None
    ml_probability: float | None = None
    booster_score: float | None = None
    note: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "ts": self.ts, "seconds_to_entry": round(self.seconds_to_entry, 1),
            "state": self.state, "direction": self.direction,
            "probability": round(self.probability, 4),
            "confidence": round(self.confidence, 4), "edge": round(self.edge, 4),
            "order_flow": self.order_flow, "ml_probability": self.ml_probability,
            "booster_score": self.booster_score, "note": self.note,
        }


@dataclass
class Signal:
    signal_id: str
    decision_id: str
    cycle_id: int
    created_ts: int
    entry_ts: int
    expiry_ts: int
    direction: str
    mode: str
    state: str = PRE_SIGNAL
    probability: float = 0.5
    #: La probabilita' a favore al momento della creazione. Serve come metro:
    #: il deterioramento si misura rispetto a cio' che il segnale prometteva,
    #: non rispetto a una soglia assoluta uguale per tutti.
    created_probability: float = 0.5
    confidence: float = 0.0
    edge: float = 0.0
    regime: str = ""
    news_risk: str = ""
    entry_price: float | None = None
    expiry_price: float | None = None
    result: str | None = None
    stake: float = 0.0
    payout: float = 0.0
    pnl: float | None = None
    balance_after: float | None = None
    cancelled_reason: str | None = None
    strategy_version: str = ""
    model_version: str = ""
    booster_version: str = ""
    updates: list[SignalUpdate] = field(default_factory=list)
    previews: dict[int, dict] = field(default_factory=dict)
    #: Da quando la probabilita' a favore e' sotto la soglia di annullamento.
    #: `None` quando il segnale sta reggendo. Non finisce sul database: e' uno
    #: stato di lavoro, non un fatto da conservare.
    deteriorating_since: int | None = None

    @property
    def lead_ms(self) -> int:
        return self.entry_ts - self.created_ts

    def seconds_to_entry(self, now: int) -> float:
        return (self.entry_ts - now) / 1000.0

    def is_open(self) -> bool:
        return self.state in (WATCH, PRE_SIGNAL, CONFIRMED, FROZEN, ENTERED)

    def aligned_probability(self, probability: float) -> float:
        """La probabilita' letta DALLA PARTE del segnale.

        Il motore riporta sempre la probabilita' che il prezzo salga. Per un
        PUT la lettura utile e' quella complementare: senza questa conversione
        un PUT che si rafforza sembrerebbe un PUT che peggiora.
        """
        return probability if self.direction == CALL else 1.0 - probability

    def to_dict(self, now: int | None = None) -> dict[str, Any]:
        d = {
            "signal_id": self.signal_id, "decision_id": self.decision_id,
            "cycle_id": self.cycle_id, "created_ts": self.created_ts,
            "entry_ts": self.entry_ts, "expiry_ts": self.expiry_ts,
            "lead_ms": self.lead_ms, "direction": self.direction,
            "state": self.state, "probability": round(self.probability, 4),
            "created_probability": round(self.created_probability, 4),
            "confidence": round(self.confidence, 4), "edge": round(self.edge, 4),
            "regime": self.regime, "news_risk": self.news_risk,
            "entry_price": self.entry_price, "expiry_price": self.expiry_price,
            "price_basis": PRICE_BASIS, "result": self.result,
            "stake": self.stake, "payout": self.payout, "pnl": self.pnl,
            "balance_after": self.balance_after,
            "cancelled_reason": self.cancelled_reason, "mode": self.mode,
            "updates": [u.to_dict() for u in self.updates],
        }
        if now is not None:
            d["seconds_to_entry"] = round(self.seconds_to_entry(now), 1)
            d["seconds_to_expiry"] = round((self.expiry_ts - now) / 1000.0, 1)
        return d

    def to_row(self) -> dict[str, Any]:
        return {
            "signal_id": self.signal_id, "decision_id": self.decision_id,
            "cycle_id": self.cycle_id, "created_ts": self.created_ts,
            "entry_ts": self.entry_ts, "expiry_ts": self.expiry_ts,
            "lead_ms": self.lead_ms, "direction": self.direction,
            "state": self.state, "probability": self.probability,
            "confidence": self.confidence, "edge": self.edge,
            "regime": self.regime, "news_risk": self.news_risk,
            "entry_price": self.entry_price, "expiry_price": self.expiry_price,
            "price_basis": PRICE_BASIS, "result": self.result,
            "stake": self.stake, "payout": self.payout, "pnl": self.pnl,
            "balance_after": self.balance_after,
            "strategy_version": self.strategy_version,
            "model_version": self.model_version,
            "booster_version": self.booster_version,
            "cancelled_reason": self.cancelled_reason, "mode": self.mode,
        }


class SignalEngine:
    """Crea i segnali in anticipo, li sorveglia, li chiude.

    Il motore possiede il tempo: l'interfaccia disegna il countdown, non lo
    inventa. Cosi' due schermi aperti sulla stessa sessione mostrano lo stesso
    numero, e un browser lento non produce un'entrata anticipata.
    """

    def __init__(self, cfg, db=None, on_event: Callable | None = None) -> None:
        self.cfg = cfg
        self.db = db
        self.on_event = on_event
        self.active: dict[str, Signal] = {}
        self.history: list[Signal] = []
        self.counters = {"created": 0, "confirmed": 0, "cancelled": 0,
                         "entered": 0, "wins": 0, "losses": 0, "draws": 0,
                         "unknown": 0}
        self.last_signal: Signal | None = None

    # ------------------------------------------------------------ creazione
    def next_entry_ts(self, now: int) -> int:
        """Il prossimo istante di entrata utile, con l'anticipo richiesto.

        Le entrate sono allineate al minuto perche' e' cosi' che si opera su
        una binaria a scadenza fissa: si sceglie il minuto, non il secondo.
        """
        lead = self.cfg.lead_ms
        earliest = now + lead
        if not self.cfg.align_entries_to_minute:
            return earliest
        minute = 60_000
        return ((earliest + minute - 1) // minute) * minute

    def can_create(self, now: int) -> tuple[bool, str]:
        if len(self.active) >= self.cfg.max_concurrent_signals:
            return False, "MAX_CONCURRENT"
        return True, ""

    def create(self, decision: Decision, cycle_id: int, stake: float,
               now: int) -> Signal | None:
        """Nasce un PRE_SIGNAL con l'anticipo richiesto."""
        ok, _ = self.can_create(now)
        if not ok or decision.direction not in (CALL, PUT):
            return None
        entry_ts = self.next_entry_ts(now)
        sig = Signal(
            signal_id=uuid.uuid4().hex[:16], decision_id=decision.decision_id,
            cycle_id=cycle_id, created_ts=now, entry_ts=entry_ts,
            expiry_ts=entry_ts + self.cfg.horizon_ms,
            direction=decision.direction, mode=decision.mode,
            state=PRE_SIGNAL, probability=decision.probability,
            confidence=decision.confidence, edge=decision.edge,
            created_probability=(decision.probability
                                 if decision.direction == CALL
                                 else 1.0 - decision.probability),
            regime=decision.regime, news_risk=decision.news_risk,
            stake=stake, payout=self.cfg.payout,
            model_version=decision.ml_model_id or "",
        )
        self.active[sig.signal_id] = sig
        self.last_signal = sig
        self.counters["created"] += 1
        self._record_update(sig, now, PRE_SIGNAL, decision, "segnale creato")
        self._persist(sig)
        self._emit("pre_signal", sig)
        return sig

    # ------------------------------------------------------------ evoluzione
    def update(self, decision: Decision, now: int,
               price: float | None) -> list[Signal]:
        """Aggiorna i segnali aperti con la valutazione corrente.

        E' qui che un segnale si rafforza, si conferma o muore: la stessa
        macchina che lo ha creato continua a guardarlo, e se le condizioni si
        deteriorano lo annulla PRIMA dell'entrata — che e' l'unico momento in
        cui annullare ha ancora valore.
        """
        changed: list[Signal] = []
        for sid in list(self.active):
            sig = self.active.get(sid)
            if sig is None:
                continue
            secs = sig.seconds_to_entry(now)

            # --- congelato: nessun ripensamento ---------------------------
            if sig.state in (CONFIRMED, PRE_SIGNAL) and secs <= self.cfg.signal_freeze_seconds:
                sig.state = FROZEN
                self._record_update(sig, now, FROZEN, decision, "segnale congelato")
                self._persist(sig)
                self._emit("frozen", sig)
                changed.append(sig)
                continue

            # --- entrata --------------------------------------------------
            if sig.state == FROZEN and now >= sig.entry_ts:
                if price is None:
                    self.cancel(sig, now, "nessun prezzo all'ora di entrata")
                else:
                    sig.state = ENTERED
                    sig.entry_price = price
                    self.counters["entered"] += 1
                    self._record_update(sig, now, ENTERED, decision,
                                        f"entrata a {price:.5f}")
                    self._persist(sig)
                    self._emit("entered", sig)
                changed.append(sig)
                continue

            if sig.state == ENTERED:
                continue          # l'esito lo decide l'OutcomeEngine

            # --- sorveglianza fra creazione e congelamento -----------------
            if sig.state in (PRE_SIGNAL, CONFIRMED):
                self._supervise(sig, decision, now, secs, changed)
        return changed

    def _supervise(self, sig: Signal, decision: Decision, now: int,
                   secs: float, changed: list[Signal]) -> None:
        """Il segnale regge, si conferma, o si deteriora fino all'annullamento.

        Il criterio e' un DETERIORAMENTO misurato rispetto al segnale stesso —
        "70% che diventa 55%" — non "la valutazione di adesso non basterebbe a
        creare un segnale nuovo". Le due cose sembrano simili e non lo sono: la
        seconda annulla praticamente sempre, perche' un segnale aperto occupa
        lo slot e da quel momento ogni decisione successiva e' NO_TRADE.
        """
        breakeven = self.cfg.breakeven_win_rate
        # La probabilita' letta dalla parte del segnale, con i blocchi che il
        # segnale causa da se' esclusi dal giudizio.
        aligned = sig.aligned_probability(decision.probability)
        blockers = [b for b in decision.blockers if b not in STRUCTURAL_BLOCKERS]

        # Un dato macro comparso dopo la creazione: si annulla subito, senza
        # aspettare conferme. Qui il rischio non e' statistico, e' un salto.
        if "NEWS_HIGH_IMPACT" in decision.blockers:
            self.cancel(sig, now, "dato macro ad alto impatto in uscita")
            changed.append(sig)
            return

        # La direzione si e' ribaltata: sotto il 50% il mercato favorisce
        # l'altro lato, e questo non e' piu' quel segnale.
        flipped = aligned < 0.5
        # Oppure e' semplicemente sceso: sotto il pareggio E abbastanza sotto
        # il valore con cui era nato.
        drop = sig.created_probability - aligned
        faded = aligned < breakeven and drop >= CANCEL_MIN_DROP

        if flipped or faded:
            if sig.deteriorating_since is None:
                sig.deteriorating_since = now
            elif now - sig.deteriorating_since >= CANCEL_PERSIST_MS:
                self.cancel(sig, now, (
                    f"probabilita' a favore scesa da "
                    f"{sig.created_probability:.0%} a {aligned:.0%}"
                    + (" (direzione invertita)" if flipped
                       else f", sotto il pareggio {breakeven:.0%}")
                    + f", per {(now - sig.deteriorating_since) / 1000:.0f}s"))
                changed.append(sig)
                return
        else:
            sig.deteriorating_since = None      # ha ripreso: si riparte da zero

        previous = sig.state
        sig.probability = decision.probability
        sig.confidence = decision.confidence
        sig.edge = decision.edge
        # Conferma: la probabilita' a favore regge la soglia e non c'e' nessun
        # blocco che non sia il segnale stesso.
        holds = (aligned >= self.cfg.effective_min_probability and not blockers)
        if sig.state == PRE_SIGNAL and holds:
            sig.state = CONFIRMED
            self.counters["confirmed"] += 1
            self._emit("confirmed", sig)
        self._record_update(sig, now, sig.state, decision,
                            "conferma" if sig.state != previous else "")
        self._snapshot_preview(sig, decision, now, secs)
        if sig.state != previous:
            self._persist(sig)
            changed.append(sig)

    # -------------------------------------------------------------- chiusure
    def cancel(self, sig: Signal, now: int, reason: str) -> None:
        sig.state = CANCELLED
        sig.result = None
        sig.cancelled_reason = reason
        self.counters["cancelled"] += 1
        self.active.pop(sig.signal_id, None)
        self.history.append(sig)
        self._persist(sig)
        self._emit("cancelled", sig)

    def settle(self, sig: Signal, price: float | None, now: int) -> str:
        """Determina l'esito. `DRAW` quando il prezzo non si e' mosso.

        Il pareggio non e' un dettaglio: su una binaria che non lo paga, una
        chiusura identica all'apertura e' capitale immobilizzato per niente, e
        contarlo come vittoria o sconfitta falserebbe ogni statistica.
        """
        sig.expiry_price = price
        if price is None or sig.entry_price is None:
            sig.result = UNKNOWN
            self.counters["unknown"] += 1
        elif price == sig.entry_price:
            sig.result = DRAW
            self.counters["draws"] += 1
        elif (price > sig.entry_price) == (sig.direction == CALL):
            sig.result = WIN
            self.counters["wins"] += 1
        else:
            sig.result = LOSS
            self.counters["losses"] += 1
        sig.state = EXPIRED
        self.active.pop(sig.signal_id, None)
        self.history.append(sig)
        self.history = self.history[-2000:]
        self._persist(sig)
        self._emit("settled", sig)
        return sig.result

    def due_for_settlement(self, now: int) -> list[Signal]:
        return [s for s in self.active.values()
                if s.state == ENTERED and now >= s.expiry_ts]

    # ------------------------------------------------------------- supporto
    def _record_update(self, sig: Signal, now: int, state: str,
                       decision: Decision, note: str) -> None:
        u = SignalUpdate(
            ts=now, seconds_to_entry=sig.seconds_to_entry(now), state=state,
            direction=sig.direction, probability=decision.probability,
            confidence=decision.confidence, edge=decision.edge,
            order_flow=decision.features.get("tick_imbalance_5s"),
            ml_probability=decision.ml_probability,
            booster_score=decision.booster_score, note=note)
        sig.updates.append(u)
        sig.updates = sig.updates[-200:]
        if self.db is not None:
            self.db.add("signal_updates", {"signal_id": sig.signal_id, **u.to_dict()})

    def _snapshot_preview(self, sig: Signal, decision: Decision, now: int,
                          secs: float) -> None:
        """Fotografie a T-30, T-25, ... Servono alla ricerca sugli anticipatori:
        senza, non si potrebbe mai chiedere "com'era il mercato 20 secondi
        prima delle operazioni vincenti?"."""
        offset = -int(round(secs / 5.0) * 5)
        if offset not in PREVIEW_OFFSETS or offset in sig.previews:
            return
        payload = {
            "probability": round(decision.probability, 4),
            "confidence": round(decision.confidence, 4),
            "edge": round(decision.edge, 4),
            "regime": decision.regime,
            "agreement": round(decision.agreement, 4),
            "ml_probability": decision.ml_probability,
            "booster_score": decision.booster_score,
            "agents": {o.agent: round(o.direction, 3) for o in decision.opinions},
            "order_flow": decision.features.get("tick_imbalance_5s"),
            "momentum": decision.features.get("momentum"),
            "volatility": decision.features.get("realized_vol_5s_bps"),
            "spread_bps": decision.features.get("spread_bps"),
        }
        sig.previews[offset] = payload
        if self.db is not None:
            self.db.add("preview_snapshots", {
                "signal_id": sig.signal_id, "offset_s": offset, "ts": now,
                "payload": json.dumps(payload, default=str)})

    def _persist(self, sig: Signal) -> None:
        if self.db is not None:
            self.db.upsert("signals", sig.to_row())

    def _emit(self, event: str, sig: Signal) -> None:
        if self.on_event is not None:
            try:
                self.on_event(event, sig)
            except Exception:  # noqa: BLE001 - una notifica non ferma il motore
                pass

    def stats(self) -> dict[str, Any]:
        decided = self.counters["wins"] + self.counters["losses"]
        return {
            **self.counters,
            "open": len(self.active),
            "decided": decided,
            "win_rate": (round(self.counters["wins"] / decided, 4)
                         if decided else None),
            "breakeven_win_rate": round(self.cfg.breakeven_win_rate, 4),
            "price_basis": PRICE_BASIS,
        }
