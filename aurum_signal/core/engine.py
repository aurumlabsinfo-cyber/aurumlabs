"""L'orchestratore: il loop realtime e i lavoratori in secondo piano.

La separazione che conta piu' di tutte:

    quotazione -> feature -> agenti -> modello -> decisione -> segnale
                                                                 |
                                                              notifica

gira nel loop veloce e non deve mai aspettare nessuno. Tutto il resto — la
ricerca, l'addestramento, lo studio dopo un fallimento, le notifiche — vive su
thread separati e comunica per code. Un addestramento che impiega dieci secondi
non puo' ritardare un segnale che vale trenta.

L'altra regola: **la ricerca non tocca il motore live**. Produce candidati, li
valida, e al massimo propone. La promozione e' un atto esplicito con dei
requisiti, non un effetto collaterale.
"""

from __future__ import annotations

import asyncio
import json
import threading
import time
import uuid
from typing import Any, Callable

from ..config import MODE_LIVE, MODE_REPLAY, MODE_SIMULATION, VERSION
from ..market.base import Quote
from ..market.fallback import MarketFeed
from ..notification.telegram import TelegramNotifier
from ..research.booster import SignalBooster
from ..research.engine import ResearchEngine
from ..research.validation import Dataset
from ..storage.database import Database, now_ms
from .agents import CALL, PUT
from .decision import MetaDecisionEngine, NO_TRADE
from .features import CANDLE_BUCKETS_S, FeatureEngine
from .outcomes import LatencyTracker, OutcomeEngine
from .regime import ReliabilityTracker
from .signals import (CANCELLED, ENTERED, LOSS, PRICE_BASIS, SignalEngine, WIN)
from .wallet import VirtualWallet


class AurumEngine:
    """Mette insieme i pezzi e li fa girare."""

    def __init__(self, cfg, feed: MarketFeed | None = None,
                 db: Database | None = None) -> None:
        self.cfg = cfg
        self.db = db or Database(cfg.database_path, cfg.db_flush_ms)
        self.feed = feed or MarketFeed(cfg)
        self.features = FeatureEngine(cfg)
        self.reliability = ReliabilityTracker()
        self.decisions = MetaDecisionEngine(cfg, reliability=self.reliability)
        self.signals = SignalEngine(cfg, self.db, on_event=self._on_signal_event)
        self.wallet = VirtualWallet(cfg, self.db,
                                    on_cycle_failed=self._on_cycle_failed)
        self.outcomes = OutcomeEngine(cfg, self.db, self.signals, self.wallet)
        self.latency = LatencyTracker(self.db)
        self.research = ResearchEngine(cfg, self.db)
        self.booster = SignalBooster(cfg)
        self.telegram = TelegramNotifier(cfg)

        self.started_ts = now_ms()
        self.running = False
        self.safe_mode = False
        self.safe_reason: str | None = None
        self.last_decision = None
        self.last_quote: Quote | None = None
        self._last_decision_ts = 0
        self._price_history: list[tuple[int, float]] = []
        self._subscribers: list[Callable[[str, dict], None]] = []
        self._research_thread: threading.Thread | None = None
        self._research_lock = threading.Lock()
        #: Il giudizio del booster per ogni segnale aperto, tenuto fino
        #: all'esito. Senza, l'esito verrebbe registrato con punteggio zero e
        #: il booster misurerebbe se stesso su un dato che non ha prodotto.
        self._booster_results: dict[str, Any] = {}
        self._last_flow_snapshot = 0
        #: Osservazioni sullo stream in attesa del proprio futuro. Non sono
        #: decisioni: e' materiale di ricerca sul comportamento dei tick, e
        #: nessun segnale dipende da loro.
        self._pending_patterns: list[dict[str, Any]] = []
        self._last_pattern_ts = 0
        self.strategy_id = f"s_{uuid.uuid4().hex[:10]}"
        self.outcomes.listeners.append(self._on_outcome)

    # ------------------------------------------------------------- orologio
    def now(self) -> int:
        """In replay l'ora la decide il feed, non il muro."""
        adapter = self.feed.adapter
        clock = getattr(adapter, "clock", None)
        return clock.now_ms() if clock is not None else now_ms()

    @property
    def mode(self) -> str:
        return self.feed.mode

    @property
    def warmed_up(self) -> bool:
        """Serve abbastanza storia per misurare l'orizzonte, non solo per esistere."""
        return self.features.mid.span_ms >= self.cfg.horizon_ms * 3

    # -------------------------------------------------------------- il loop
    async def run(self, max_seconds: float | None = None) -> None:
        self.db.start()
        self.telegram.start()
        self.running = True
        if not await self.feed.connect():
            self.db.event("feed", "ERROR", "nessuna sorgente disponibile",
                          self.feed.tried)
            raise RuntimeError(
                "Nessun adapter di mercato si e' connesso. "
                f"Provati: {json.dumps(self.feed.tried)}")
        self.db.event("engine", "INFO",
                      f"avvio {VERSION} in modalita' {self.mode}",
                      {"symbol": self.cfg.symbol, "adapter": self.feed.adapter.name})
        # La strategia in esecuzione, con i parametri che la definiscono.
        # Serve a rispondere alla domanda che si pone dopo un ciclo fallito:
        # "con quali soglie stava girando quando e' andata cosi'?".
        self.db.upsert("strategy_versions", {
            "strategy_id": self.strategy_id, "ts": self.started_ts,
            "parent_id": None, "status": "CHAMPION",
            "promoted_ts": self.started_ts, "retired_ts": None,
            "reason": "parametri di avvio",
            "params": json.dumps({
                "min_confidence": self.cfg.min_confidence,
                "min_edge": self.cfg.min_edge,
                "effective_min_probability": self.cfg.effective_min_probability,
                "payout": self.cfg.payout,
                "breakeven": self.cfg.breakeven_win_rate,
                "lead_s": self.cfg.signal_lead_seconds,
                "freeze_s": self.cfg.signal_freeze_seconds,
                "max_concurrent": self.cfg.max_concurrent_signals,
                "heuristic_shrink": self.decisions.heuristic_shrink,
            })})
        self.db.upsert("booster_versions", {
            "booster_id": self.booster.booster_id, "ts": self.started_ts,
            "mode": self.booster.mode,
            "params": json.dumps({"min_samples": self.cfg.booster_min_samples}),
            "metrics": None, "validated": int(self.booster.validated)})
        self._start_research_worker()

        deadline = (time.time() + max_seconds) if max_seconds else None
        try:
            async for quote in self.feed.quotes():
                self._on_quote(quote)
                if deadline and time.time() > deadline:
                    break
                if not self.running:
                    break
        finally:
            self.running = False
            self.telegram.stop()
            self.db.flush()

    def _on_quote(self, quote: Quote) -> None:
        """Un tick: aggiorna i buffer e, quando e' ora, decide."""
        t0 = time.perf_counter()
        self.last_quote = quote
        now = quote.received_ts
        self._price_history.append((now, quote.mid))
        if len(self._price_history) > 20_000:
            del self._price_history[:5_000]

        if self.cfg.persist_ticks:
            self.db.add("market_ticks", quote.row())
        closed = self.features.observe(quote)
        for bucket, candle in closed:
            self.db.upsert("candles", {"bucket_s": bucket, "mode": self.mode,
                                       **candle.to_dict()})
        self.latency.record("market_data", (time.perf_counter() - t0) * 1000.0, now)

        # Il ciclo di vita dei segnali gira a ogni tick: e' cio' che rende il
        # countdown e l'annullamento reattivi quanto il mercato.
        self.outcomes.settle_due(now, quote.mid)
        self.outcomes.settle_shadow(now, self._price_at)

        if now - self._last_decision_ts < self.cfg.decision_interval_ms:
            return
        self._last_decision_ts = now
        self._decide(now)

    def _decide(self, now: int) -> None:
        t0 = time.perf_counter()
        vec = self.features.compute(now, self.mode)
        if vec is None:
            return
        self.latency.record("features", (time.perf_counter() - t0) * 1000.0, now)

        quality = self.feed.quality.assess(now)
        self._update_safe_mode(quality)
        vec.values["quality_score"] = quality.quality_score
        vec.values.update(self._news_features())

        can_trade, _ = self.wallet.can_trade()
        ctx = {
            "quality_score": quality.quality_score,
            "blocking": quality.blocking,
            "warmed_up": self.warmed_up,
            "open_signals": len(self.signals.active),
            "wallet_can_trade": can_trade,
            "safe_mode": self.safe_mode,
        }
        d = self.decisions.decide(vec, ctx)
        d.planned_entry_ts = self.signals.next_entry_ts(now)
        d.expiry_ts = d.planned_entry_ts + self.cfg.horizon_ms
        d.lead_ms = d.planned_entry_ts - now
        self.last_decision = d
        self.latency.record_many(d.latency, now)
        self.latency.record("total", (time.perf_counter() - t0) * 1000.0, now)

        if self.cfg.persist_features:
            self.db.add("features", {
                "ts": now, "mode": self.mode, "mid": vec.values.get("mid"),
                "spread_bps": vec.values.get("spread_bps"), "regime": d.regime,
                "quality": quality.quality_score,
                "payload": json.dumps(vec.predictive())})
        self.db.upsert("decisions", d.to_row())
        self.db.add("regime_history", {
            "ts": now, "regime": d.regime, "confidence": d.regime_confidence,
            "volatility_bps": vec.values.get("sigma_horizon_bps")})
        self._record_order_flow(vec, now)
        self._observe_stream_pattern(vec, d, now)

        # I segnali aperti vengono sorvegliati con la valutazione corrente.
        self.signals.update(d, now, self.last_quote.mid if self.last_quote else None)

        if d.is_tradeable:
            self._create_signal(d, now)
        else:
            self._record_shadow(d, now)
        self._publish("decision", {"decision": d.explain(),
                                   "regime": d.regime, "ts": now})

    def _create_signal(self, d, now: int) -> None:
        ok, _ = self.signals.can_create(now)
        if not ok:
            return
        stake = self.wallet.reserve("pending")
        if stake <= 0:
            return
        self.wallet.release("pending")
        sig = self.signals.create(d, self.wallet.cycle_id, stake, now)
        if sig is None:
            return
        self.wallet._reserved[sig.signal_id] = stake
        self.db.upsert("decisions", {**d.to_row(), "emitted": 1})
        self._record_similarity(d, now)

    def _evaluate_booster(self, sig) -> None:
        """Il giudizio del booster, al congelamento e non alla creazione.

        Momento scelto di proposito: il booster legge gli ULTIMI secondi, e
        alla creazione — trenta secondi prima dell'entrata — quegli ultimi
        secondi non sono ancora accaduti. Valutarlo li' significava dargli in
        pasto una storia vuota e registrarne il verdetto come se fosse
        informato.

        In ombra il risultato non tocca il segnale: si salva e basta.
        """
        d = self.last_decision
        if d is None or sig.signal_id in self._booster_results:
            return
        now = self.now()
        try:
            t = time.perf_counter()
            history = [sig.previews[k] for k in sorted(sig.previews)]
            res = self.booster.evaluate(sig, d, history)
            self._booster_results[sig.signal_id] = res
            self.latency.record("booster", (time.perf_counter() - t) * 1000.0, now)
            self.db.upsert("booster_decisions", {
                "decision_id": d.decision_id, "signal_id": sig.signal_id,
                "ts": now, "booster_id": self.booster.booster_id,
                "base_probability": res.base_probability,
                "booster_score": res.score,
                "booster_probability": res.boosted_probability,
                "action": res.action, "reasons": json.dumps(res.reasons),
                "applied": int(res.applied), "latency_ms": res.latency_ms})
        except Exception as exc:  # noqa: BLE001 - il booster non ferma il motore
            self.db.event("booster", "WARNING", str(exc))

    def _record_similarity(self, d, now: int) -> None:
        """I casi passati piu' simili a questo, salvati accanto alla decisione.

        Serve a rispondere dopo, non adesso: "quando il mercato somigliava a
        questo, com'e' andata?". Senza salvarlo al momento della decisione la
        domanda diventa impossibile, perche' la libreria nel frattempo cambia.
        """
        try:
            similar = self.research.library.similar(d.features, d.regime, limit=3)
        except Exception:  # noqa: BLE001 - la libreria non ferma il motore
            return
        for s in similar:
            self.db.add("similarity_results", {
                "decision_id": d.decision_id, "setup_id": s.get("setup_id"),
                "ts": now, "similarity": s.get("similarity"),
                "historical_win_rate": s.get("win_rate"),
                "samples": s.get("samples")})

    def _record_strategy_metrics(self, wallet) -> None:
        """Il risultato del ciclo appena chiuso, attribuito alla strategia.

        L'intervallo di confidenza e' li' per un motivo: un ciclo si chiude
        dopo venti operazioni, e su venti operazioni un tasso del 40% e uno
        del 60% sono spesso lo stesso tasso. Senza l'intervallo si finisce a
        cambiare strategia per inseguire il rumore.
        """
        c = wallet.current
        if c is None:
            return
        from ..ml.calibration import wilson_interval
        from ..research.validation import binomial_p_value
        n, wins = c.decided, c.wins
        lo, hi = wilson_interval(wins, n) if n else (0.0, 1.0)
        self.db.add("strategy_metrics", {
            "strategy_id": self.strategy_id, "ts": self.now(),
            "scope": f"cycle:{c.cycle_id}", "samples": n,
            "win_rate": c.win_rate, "expectancy": c.expectancy,
            "profit_factor": c.profit_factor, "max_drawdown": c.max_drawdown,
            "ci_low": round(lo, 4), "ci_high": round(hi, 4),
            "p_value": (round(binomial_p_value(wins, n,
                                               self.cfg.breakeven_win_rate), 6)
                        if n else None)})

    def _observe_stream_pattern(self, vec, d, now: int) -> None:
        """Registra lo stato dello stream e, un minuto dopo, cosa e' successo.

        Non e' una decisione e nessun segnale ne dipende: e' materiale grezzo
        per la ricerca, che permette di chiedersi in seguito "dopo questa
        configurazione di flusso, il prezzo dove e' andato a +10, +20, +30 e
        +60 secondi?". Registrarlo mentre accade e' l'unico modo di averlo:
        ricostruirlo dopo significherebbe ricalcolare le feature con la storia
        di oggi, cioe' guardare il passato con informazioni che allora non
        c'erano.
        """
        if now - self._last_pattern_ts >= 5_000:
            self._last_pattern_ts = now
            flow = vec.values.get("tick_imbalance_5s")
            if flow is not None and self.last_quote is not None:
                self._pending_patterns.append({
                    "ts": now, "price": self.last_quote.mid, "regime": d.regime,
                    "order_flow": flow,
                    "direction": CALL if flow > 0 else PUT,
                    "features": {k: vec.values.get(k) for k in (
                        "momentum", "acceleration", "realized_vol_5s_bps",
                        "move_over_noise", "spread_bps", "quote_velocity_10s",
                        "trend_strength", "zscore_60s")},
                })
            if len(self._pending_patterns) > 5_000:
                del self._pending_patterns[:1_000]

        # Chiude quelle mature: il futuro di 60 secondi fa e' adesso.
        matured = [p for p in self._pending_patterns
                   if now - p["ts"] >= self.cfg.horizon_ms]
        if not matured:
            return
        self._pending_patterns = [p for p in self._pending_patterns
                                  if now - p["ts"] < self.cfg.horizon_ms]
        for p in matured:
            base = p["price"]
            if not base:
                continue

            def ret(offset_s: int, _p=p, _b=base) -> float | None:
                px = self._price_at(_p["ts"] + offset_s * 1000)
                return round((px - _b) / _b * 10_000.0, 4) if px else None

            r60 = ret(60)
            self.db.add("stream_patterns", {
                "pattern_id": f"flow_{p['direction']}_{p['regime']}",
                "ts": p["ts"], "regime": p["regime"],
                "features_before": json.dumps(p["features"], default=str),
                "order_flow": p["order_flow"], "direction": p["direction"],
                "future_return_10s": ret(10), "future_return_20s": ret(20),
                "future_return_30s": ret(30), "future_return_60s": r60,
                "success": (None if r60 is None else
                            int((r60 > 0) == (p["direction"] == CALL)))})

    def _record_order_flow(self, vec, now: int) -> None:
        """La microstruttura misurata, con l'onesta' di dire cos'e' e cosa non e'.

        Su un feed FX gratuito NON esiste un book di livello 2: qui c'e' solo
        cio' che il provider fornisce davvero — direzione dei tick, velocita'
        delle quotazioni, spread. `available` distingue una misura reale da
        un'astensione, cosi' chi studiera' questi dati fra un mese sapra' quali
        righe significano qualcosa. Un book inventato sarebbe peggio di
        nessun book.
        """
        if now - self._last_flow_snapshot < 1000:      # una al secondo basta
            return
        self._last_flow_snapshot = now
        caps = self.feed.adapter.capabilities if self.feed.adapter else None
        components = {k: vec.values.get(k) for k in (
            "tick_imbalance_1s", "tick_imbalance_5s", "tick_imbalance_15s",
            "tick_imbalance_30s", "quote_velocity_10s", "spread_bps",
            "spread_vs_average")}
        measured = [v for v in components.values() if v is not None]
        score = vec.values.get("tick_imbalance_5s")
        self.db.add("order_flow_snapshots", {
            "ts": now, "score": score,
            "components": json.dumps(components, default=str),
            "available": int(bool(measured) and score is not None),
            "note": ("nessun book L2: il provider fornisce solo tick e spread"
                     if caps is None or not getattr(caps, "has_depth", False)
                     else "book disponibile dal provider")})

    def _record_shadow(self, d, now: int) -> None:
        """Anche un NO_TRADE va valutato: e' una scelta che si puo' sbagliare."""
        if d.probability == 0.5 or not d.blockers:
            return
        entry_ts = d.planned_entry_ts or (now + self.cfg.lead_ms)
        self.db.upsert("shadow_decisions", {
            "decision_id": d.decision_id, "ts": now,
            "entry_ts": entry_ts, "expiry_ts": entry_ts + self.cfg.horizon_ms,
            "direction": CALL if d.probability > 0.5 else PUT,
            "probability": d.probability, "confidence": d.confidence,
            "edge": d.edge, "blocking_reason": d.primary_blocker,
            "entry_price": self.last_quote.mid if self.last_quote else None,
            "regime": d.regime, "mode": self.mode})

    # -------------------------------------------------------------- eventi
    def _on_signal_event(self, event: str, sig) -> None:
        if event == "pre_signal":
            self.telegram.send(self.telegram.signal_message(sig, self.cfg))
        elif event == "frozen":
            self._evaluate_booster(sig)
        elif event == "cancelled":
            self.wallet.release(sig.signal_id)
            self._booster_results.pop(sig.signal_id, None)
            self.telegram.send(self.telegram.cancel_message(sig))
        self._publish(event, sig.to_dict(self.now()))

    def _on_outcome(self, sig, result: str) -> None:
        """L'esito alimenta TUTTO cio' che impara. E' il cuore del progetto."""
        won = result == WIN
        if result in (WIN, LOSS):
            self.decisions.record_outcome(sig.probability, won)
            regime = sig.regime or "UNCERTAIN"
            row = self.db.query(
                "SELECT agent_payload, feature_snapshot FROM decisions "
                "WHERE decision_id=?", (sig.decision_id,))
            if row:
                try:
                    agents = json.loads(row[0]["agent_payload"] or "[]")
                    for a in agents:
                        if a.get("abstained") or not a.get("direction"):
                            continue
                        agent_call = a["direction"] > 0
                        actual_up = (sig.expiry_price or 0) > (sig.entry_price or 0)
                        self.reliability.record(a["agent"], regime,
                                                agent_call == actual_up)
                    feats = json.loads(row[0]["feature_snapshot"] or "{}")
                    self.research.library.observe(feats, regime, result)
                except (ValueError, TypeError):
                    pass
            # Il punteggio VERO che il booster aveva dato a questo segnale.
            # Registrare uno zero fisso — com'era prima — significava misurare
            # il booster su un dato che non aveva prodotto: ogni osservazione
            # finiva nella stessa fascia, nessun segnale risultava scartato, e
            # il rapporto non poteva dire niente per costruzione.
            res = self._booster_results.pop(sig.signal_id, None)
            if res is not None:
                self.booster.record(res, won)
                self.db.upsert("booster_decisions", {
                    "decision_id": sig.decision_id,
                    "base_result": result,
                    "boosted_simulated_result": (
                        result if res.action != "VETO" else "EVITATA")})
            self._record_booster_metrics()
        self.telegram.send(self.telegram.result_message(sig, self.wallet.balance))
        self._publish("settled", sig.to_dict(self.now()))

    def _record_booster_metrics(self) -> None:
        """Una fotografia periodica di quanto vale il booster, sul database.

        Ogni cinquanta esiti: piu' spesso sarebbe rumore salvato con cura.
        """
        n = len(self.booster.observations)
        if n < 50 or n % 50 != 0:
            return
        rep = self.booster.report(self.cfg.breakeven_win_rate)
        if rep.get("status") != "OK":
            return
        self.db.add("booster_metrics", {
            "booster_id": self.booster.booster_id, "ts": self.now(),
            "samples": rep["samples"], "base_win_rate": rep["base_win_rate"],
            "boosted_win_rate": rep["boosted_win_rate"],
            "base_expectancy": rep["base_expectancy"],
            "boosted_expectancy": rep["boosted_expectancy"],
            "brier_base": None, "brier_boosted": None,
            "verdict": rep["verdict"][:500]})

    def _on_cycle_failed(self, wallet) -> None:
        """Il ciclo e' finito: si studia PRIMA di ricominciare."""
        self._record_strategy_metrics(wallet)

        def study() -> None:
            try:
                report = self.research.study(wallet, self.signals,
                                             self.decisions, self.booster)
                wallet.study_report = report
                self.db.event("wallet", "INFO",
                              f"studio del ciclo {wallet.cycle_id} completato",
                              {"diagnosis": report.get("diagnosis")})
            except Exception as exc:  # noqa: BLE001
                wallet.study_report = {"error": f"{type(exc).__name__}: {exc}"}
            finally:
                # Si riparte SOLO dopo lo studio, e senza cambiare parametri a
                # caso: la strategia nuova deve passare dalla validazione.
                wallet.start_new_cycle(
                    "nuovo ciclo dopo lo studio",
                    versions={"strategy": self.research.champion_id or "base",
                              "booster": self.booster.booster_id})
                self._publish("cycle", wallet.status())
        threading.Thread(target=study, name="study", daemon=True).start()

    # -------------------------------------------------------- modalita' sicura
    def _update_safe_mode(self, quality) -> None:
        """Se i dati non sono affidabili, non si emette. Punto.

        E' l'unico posto dove un cancello duro e' la risposta giusta: un
        segnale costruito su un feed morto non e' un segnale debole, e' un
        numero casuale con un'etichetta sopra.
        """
        was = self.safe_mode
        self.safe_mode = bool(quality.blocking) or not self.db.healthy
        self.safe_reason = (", ".join(quality.blocking) if quality.blocking
                            else ("database non utilizzabile"
                                  if not self.db.healthy else None))
        if self.safe_mode != was:
            self.db.event("engine", "WARNING" if self.safe_mode else "INFO",
                          "modalita' sicura attivata" if self.safe_mode
                          else "modalita' sicura disattivata",
                          {"reason": self.safe_reason})

    def _news_features(self) -> dict[str, Any]:
        """Il calendario non e' ancora collegato: si dichiara, non si inventa."""
        return {"news_available": 0.0, "news_in_blackout": 0.0,
                "news_minutes_to_next": None}

    def _price_at(self, ts: int) -> float | None:
        """Il prezzo piu' recente non successivo a `ts`. Sempre lo stesso tipo."""
        best = None
        for t, p in reversed(self._price_history):
            if t <= ts:
                best = p
                break
        return best

    # ------------------------------------------------------------- ricerca
    def _start_research_worker(self) -> None:
        if not self.cfg.research_enabled:
            return

        def loop() -> None:
            # Non si studia subito: nei primi minuti non c'e' niente da imparare.
            time.sleep(min(60.0, self.cfg.research_interval_seconds / 4))
            while self.running:
                try:
                    with self._research_lock:
                        ds = self.build_dataset()
                        if len(ds) >= self.cfg.research_min_samples:
                            self.research.run_once(ds)
                            self._adopt_champion()
                except Exception as exc:  # noqa: BLE001 - la ricerca non ferma nulla
                    self.db.event("research", "ERROR",
                                  f"{type(exc).__name__}: {exc}")
                for _ in range(int(self.cfg.research_interval_seconds * 2)):
                    if not self.running:
                        return
                    time.sleep(0.5)

        self._research_thread = threading.Thread(target=loop, name="research",
                                                 daemon=True)
        self._research_thread.start()

    def _adopt_champion(self) -> None:
        """Il modello promosso dalla ricerca entra nella decisione.

        Senza questo passo il percorso di validazione non cambierebbe niente:
        il sistema studierebbe, concluderebbe, e continuerebbe a decidere
        esattamente come prima. E' un'assegnazione di riferimento, quindi il
        loop realtime non si ferma: alla decisione successiva il modello c'e'
        gia', e nel frattempo continua con le sole euristiche.

        Il modello NON sostituisce gli agenti: entra con peso 0.5 se calibrato
        e 0.25 se no, come qualunque altra opinione (vedi `MetaDecisionEngine`).
        """
        champ = self.research.champion_model
        current = getattr(self.decisions.model, "model_id", None)
        if champ is None or getattr(champ, "model_id", None) == current:
            return
        self.decisions.model = champ
        self.db.event("engine", "INFO",
                      f"modello {champ.model_id} adottato dal motore decisionale",
                      {"previous": current, "calibrated": champ.calibrated,
                       "features": champ.feature_names})
        self._publish("model", {"model_id": champ.model_id,
                                "calibrated": champ.calibrated,
                                "metrics": champ.metrics})

    def rollback_model(self, reason: str) -> dict[str, Any]:
        """Ritira il modello in produzione e torna alle sole euristiche."""
        out = self.research.demote(reason)
        self.decisions.model = None
        self._publish("model", {"model_id": None, "reason": reason})
        return out

    def build_dataset(self, include_simulation: bool | None = None) -> Dataset:
        """Righe causali con l'etichetta a +orizzonte, prese dal database.

        Le righe simulate sono escluse per difetto: un vantaggio "dimostrato"
        su dati generati da un modello non e' un vantaggio, e lasciarle entrare
        renderebbe ogni conclusione priva di valore.
        """
        include_sim = (include_simulation if include_simulation is not None
                       else self.mode == MODE_SIMULATION)
        where = "" if include_sim else " WHERE mode != 'SIMULATION'"
        rows = self.db.query(
            f"SELECT ts, mid, payload FROM features{where} ORDER BY ts ASC")
        ticks = self.db.query(
            f"SELECT ts, mid FROM market_ticks{where} ORDER BY ts ASC")
        if len(rows) < 50 or len(ticks) < 50:
            return Dataset([], [], [], [], self.cfg.horizon_seconds,
                           notes={"error": "dati insufficienti"})

        tick_ts = [r["ts"] for r in ticks]
        tick_mid = [r["mid"] for r in ticks]
        horizon_ms = self.cfg.horizon_ms

        import bisect
        names: set[str] = set()
        parsed: list[tuple[int, float, dict]] = []
        # Campionamento a 1 Hz: dieci righe al secondo sono dieci copie quasi
        # identiche dello stesso stato di mercato.
        last_kept = -10 ** 18
        for r in rows:
            ts = int(r["ts"])
            if ts - last_kept < 1000:
                continue
            last_kept = ts
            try:
                payload = json.loads(r["payload"] or "{}")
            except ValueError:
                continue
            parsed.append((ts, float(r["mid"] or 0), payload))
            names.update(payload)

        cols = sorted(names)
        X: list[list[float]] = []
        y: list[int] = []
        ts_list: list[int] = []
        entry: list[float] = []
        exit_: list[float] = []
        ties = 0
        for ts, mid, payload in parsed:
            i = bisect.bisect_left(tick_ts, ts + horizon_ms)
            if i >= len(tick_ts) or tick_ts[i] - (ts + horizon_ms) > 5_000:
                continue
            future = tick_mid[i]
            if mid <= 0 or future is None:
                continue
            if future == mid:
                ties += 1
                continue                 # il pareggio non e' ne' su ne' giu'
            X.append([float(payload.get(c, float("nan"))) for c in cols])
            y.append(1 if future > mid else 0)
            ts_list.append(ts)
            entry.append(mid)
            exit_.append(future)

        return Dataset(X, y, ts_list, cols, self.cfg.horizon_seconds,
                       entry, exit_,
                       {"rows_scanned": len(rows), "ties_dropped": ties,
                        "include_simulation": include_sim})

    # ------------------------------------------------------------ pubblicazione
    def subscribe(self, fn: Callable[[str, dict], None]) -> None:
        self._subscribers.append(fn)

    def _publish(self, event: str, payload: dict) -> None:
        for fn in list(self._subscribers):
            try:
                fn(event, payload)
            except Exception:  # noqa: BLE001
                pass

    # -------------------------------------------------------------- diagnosi
    def health(self) -> dict[str, Any]:
        q = self.feed.quality.assess(self.now())
        return {
            "status": ("safe_mode" if self.safe_mode
                       else "ok" if self.running else "stopped"),
            "version": VERSION,
            "mode": self.mode,
            "symbol": self.cfg.symbol,
            "market_feed": ("connected" if self.feed.adapter
                            and self.feed.adapter.health_state.connected
                            else "disconnected"),
            "database": "ok" if self.db.healthy else "degraded",
            "model": "loaded" if self.decisions.model else "none",
            "signal_engine": "running" if self.running else "stopped",
            "research_engine": ("running" if self._research_thread
                                and self._research_thread.is_alive() else "off"),
            "booster": self.booster.mode,
            "telegram": ("connected" if self.telegram.enabled else "off"),
            "safe_reason": self.safe_reason,
            "uptime_s": round((self.now() - self.started_ts) / 1000.0, 1),
            "warmed_up": self.warmed_up,
            "data_quality": q.to_dict(),
        }

    def diagnostics(self) -> dict[str, Any]:
        d = self.last_decision
        stats = self.decisions.stats()
        sig_stats = self.signals.stats()
        return {
            "ts": self.now(),
            "mode": self.mode,
            "feed": self.feed.health(),
            "last_tick_age_ms": (self.now() - self.last_quote.received_ts
                                 if self.last_quote else None),
            "decisions": stats,
            "signals": sig_stats,
            "top_blockers": list(stats["blockers"].items())[:8],
            "average_confidence": (round(d.confidence, 4) if d else None),
            "average_edge": (round(d.edge, 4) if d else None),
            "last_decision": d.explain() if d else None,
            "calibration": self.decisions.calibration_state(),
            "latency": self.latency.report(),
            "research": self.research.status(),
            "booster": self.booster.report(self.cfg.breakeven_win_rate),
            "wallet": self.wallet.status(),
            "notification": self.telegram.status(),
            "database": self.db.health(),
            "safe_mode": self.safe_mode,
            "safe_reason": self.safe_reason,
            "price_basis": PRICE_BASIS,
        }

    def stop(self) -> None:
        self.running = False
