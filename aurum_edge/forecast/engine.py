"""Il motore di previsione. Mette insieme tutto e, quasi sempre, dice WAIT.

Questo file non contiene, e non deve mai contenere, una sola chiamata capace di
piazzare, modificare o annullare un ordine. Produce un giudizio; il giudizio lo
legge una persona. E' l'intero perimetro del prodotto.

La catena di una previsione:

    barre e serie live -> riga causale -> modello -> edge validati
        -> cancello della decisione -> intervallo di prezzo dagli analoghi
        -> durata e invalidazione -> qualita' -> verdetto

Il cancello e' la parte che conta. Perche' esca una direzione servono, tutte
insieme:

* una probabilita' sopra la soglia;
* un margine sufficiente sulla direzione opposta;
* una qualita' del segnale accettabile;
* un modello che ha superato l'holdout, **oppure** un edge validato che si e'
  attivato adesso.

Se manca anche uno solo, il verdetto e' WAIT, e la dashboard dice quale
mancava. Un WAIT spiegato e' un'informazione; una direzione non spiegata e' una
scommessa con una faccia sicura.
"""

from __future__ import annotations

import json
import math
import uuid
from dataclasses import dataclass, field
from typing import Any

from .. import config
from ..data.store import Store
from ..features import builder
from ..features.dataset import Dataset
from ..features.labels import band_bps, horizon_sigma_bps
from ..model.logistic import LogisticModel
from ..research import edges as edges_mod
from ..research import lifecycle
from ..util import timeutil
from ..util.numeric import clamp, median, quantile
from . import quality as quality_mod
from .regime import Regime, bucket

LONG, SHORT, FLAT, WAIT = "LONG", "SHORT", "FLAT", "WAIT"


@dataclass
class Forecast:
    """Il prodotto. Tutto quello che la dashboard mostra sta qui dentro."""

    forecast_id: str
    ts: int
    symbol: str
    horizon_min: int
    verdict: str
    p_long: float
    p_short: float
    p_flat: float
    price: float
    target_low: float | None
    target_high: float | None
    expected_move_pct: float | None
    expected_duration_min: float | None
    invalidation: float | None
    quality: dict[str, Any]
    regime: dict[str, Any]
    reasons: list[dict[str, Any]]
    blockers: list[str]
    edge: dict[str, Any] | None
    model_id: str | None
    features: dict[str, float | None] = field(default_factory=dict)
    panels: dict[str, Any] = field(default_factory=dict)
    diagnostics: dict[str, Any] = field(default_factory=dict)

    def to_dict(self, with_features: bool = False) -> dict[str, Any]:
        out: dict[str, Any] = {
            "forecast_id": self.forecast_id,
            "ts": self.ts,
            "at": timeutil.iso(self.ts),
            "symbol": self.symbol,
            "horizon_min": self.horizon_min,
            "verdict": self.verdict,
            "probabilities": {
                "LONG": round(self.p_long, 4),
                "SHORT": round(self.p_short, 4),
                "FLAT": round(self.p_flat, 4),
            },
            "price": self.price,
            "target_low": self.target_low,
            "target_high": self.target_high,
            "expected_move_pct": self.expected_move_pct,
            "expected_duration_min": self.expected_duration_min,
            "expected_duration": timeutil.humanize_minutes(
                self.expected_duration_min),
            "invalidation": self.invalidation,
            "quality": self.quality,
            "regime": self.regime,
            "reasons": self.reasons,
            "blockers": self.blockers,
            "edge": self.edge,
            "model_id": self.model_id,
            "panels": self.panels,
            "diagnostics": self.diagnostics,
        }
        if with_features:
            out["features"] = self.features
        return out

    def to_row(self) -> dict[str, Any]:
        return {
            "forecast_id": self.forecast_id, "ts": self.ts,
            "symbol": self.symbol, "horizon_min": self.horizon_min,
            "verdict": self.verdict, "p_long": self.p_long,
            "p_short": self.p_short, "p_flat": self.p_flat,
            "price": self.price, "target_low": self.target_low,
            "target_high": self.target_high,
            "expected_move_pct": self.expected_move_pct,
            "expected_duration_min": self.expected_duration_min,
            "invalidation": self.invalidation,
            "quality": self.quality.get("score"),
            "regime": self.regime.get("name"),
            "model_id": self.model_id,
            "edge_id": (self.edge or {}).get("edge_id"),
            "reasons": json.dumps(self.reasons, default=str),
            "features": json.dumps(self.features, default=str),
        }


class ForecastEngine:
    """Costruisce una previsione dallo stato corrente dell'archivio."""

    def __init__(self, store: Store | None = None) -> None:
        self.store = store or Store()
        self._model: LogisticModel | None = None
        self._model_id: str | None = None
        self._model_metrics: dict[str, Any] = {}
        self._model_loaded_ts = 0
        self._analogues_cache: dict[str, Any] = {}
        self._analogues_ts = 0

    # ------------------------------------------------------------- modello
    def load_model(self, force: bool = False) -> LogisticModel | None:
        """Carica il campione. Si ricarica ogni cinque minuti, non a ogni giro."""
        now = timeutil.now_ms()
        if not force and self._model is not None and \
                now - self._model_loaded_ts < 300_000:
            return self._model
        row = self.store.model_by_role("CHAMPION")
        if row is None:
            self._model, self._model_id, self._model_metrics = None, None, {}
        else:
            try:
                self._model = LogisticModel.from_json(row["params"])
                self._model_id = row["model_id"]
                self._model_metrics = json.loads(row["metrics"] or "{}")
            except (ValueError, KeyError, TypeError):
                self._model, self._model_id, self._model_metrics = None, None, {}
        self._model_loaded_ts = now
        return self._model

    # ------------------------------------------------------------ previsione
    def forecast(self, *, persist: bool = True,
                 horizon_min: int | None = None) -> Forecast:
        horizon_min = horizon_min or config.PRIMARY_HORIZON_MIN
        now = timeutil.now_ms()
        symbol = config.SYMBOL

        # Una finestra di storia sufficiente per le finestre lunghe, senza
        # ricaricare sei mesi a ogni previsione.
        start = now - 3 * timeutil.DAY_MS
        frame = builder.build_frame(self.store, symbol, start_ms=start)
        live = builder.live_row(frame)

        if live is None:
            return self._empty(now, symbol, horizon_min,
                               "l'archivio non contiene barre: eseguire "
                               "backfill e collector")

        row, bar_ts, price, regime = live
        snapshot = self.store.latest_snapshot(symbol)
        data_age = None
        if snapshot is not None:
            data_age = (now - snapshot["ts"]) / 1000.0
        bar_age = (now - bar_ts) / 1000.0

        # Il prezzo mostrato e' l'ultimo prezzo live se e' fresco, altrimenti
        # la chiusura dell'ultima barra. Non si mescolano: si dice quale.
        price_source = "ultima barra chiusa"
        if snapshot is not None and snapshot["last_price"] and \
                data_age is not None and data_age <= config.STALE_SECONDS:
            price = float(snapshot["last_price"])
            price_source = "ticker live"

        coverage = self._coverage(row)
        blockers: list[str] = []
        reasons: list[dict[str, Any]] = []

        # ------------------------------------------------------- 1. modello
        model = self.load_model()
        probs = {LONG: 1 / 3, SHORT: 1 / 3, FLAT: 1 / 3}
        contributions: list[dict[str, Any]] = []
        model_holdout_ok = False

        if model is None:
            blockers.append(
                "nessun modello addestrato: la ricerca non ha ancora prodotto "
                "un campione")
        else:
            dense = self._dense_row(model, row)
            probs = model.predict_row(dense)
            contributions = model.contributions(dense)[:6]
            model_holdout_ok = self._holdout_ok()
            if not model_holdout_ok:
                blockers.append(
                    "il modello in carica non ha superato l'holdout fresco: le "
                    "sue probabilita' non sono confermate fuori campione")

        # --------------------------------------------------------- 2. edge
        active_edges = self._active_edges(row)
        edge_support = bool(active_edges)
        if active_edges:
            probs = self._blend(probs, active_edges)

        # ----------------------------------------------------- 3. qualita'
        q = quality_mod.assess(
            data_age_seconds=data_age,
            feature_coverage=coverage,
            regime_confidence=regime.confidence,
            model_available=model is not None,
            model_holdout_ok=model_holdout_ok,
            spread_bps=(snapshot["spread_bps"] if snapshot else None),
            bars_available=len(frame),
            edge_support=edge_support)

        if bar_age > 15 * 60:
            q.warnings.append(
                f"l'ultima barra ha {bar_age / 60:.0f} minuti: il collector "
                "potrebbe essere fermo")

        # ------------------------------------------------- 4. il cancello
        p_long, p_short, p_flat = probs[LONG], probs[SHORT], probs[FLAT]
        direction = LONG if p_long >= p_short else SHORT
        best = max(p_long, p_short)
        margin = abs(p_long - p_short)

        if best < config.MIN_DIRECTIONAL_PROB:
            blockers.append(
                f"probabilita' massima {best:.1%} sotto la soglia "
                f"{config.MIN_DIRECTIONAL_PROB:.0%}")
        if margin < config.MIN_PROB_MARGIN:
            blockers.append(
                f"margine fra le due direzioni {margin:.1%} sotto "
                f"{config.MIN_PROB_MARGIN:.0%}: non e' una direzione, e' un "
                "pareggio")
        if p_flat > best:
            blockers.append(
                f"lo scenario piu' probabile e' che non succeda niente "
                f"({p_flat:.1%})")
        if q.score < config.MIN_SIGNAL_QUALITY:
            blockers.append(
                f"qualita' del segnale {q.score:.2f} sotto "
                f"{config.MIN_SIGNAL_QUALITY:.2f}")
        if model is None and not edge_support:
            blockers.append(
                "nessuna base validata: ne' un modello promosso ne' un edge "
                "validato attivo")

        verdict = WAIT if blockers else direction

        # -------------------------------------- 5. prezzo, durata, invalidita'
        analogues = self._analogues(frame, regime, direction, horizon_min)
        targets = self._targets(price, direction, analogues, frame,
                                horizon_min, verdict)
        invalidation = self._invalidation(price, direction, frame, verdict)

        # ------------------------------------------------------ 6. i motivi
        reasons = self._reasons(contributions, active_edges, regime, row,
                                analogues)

        forecast = Forecast(
            forecast_id=uuid.uuid4().hex[:16],
            ts=now, symbol=symbol, horizon_min=horizon_min,
            verdict=verdict,
            p_long=p_long, p_short=p_short, p_flat=p_flat,
            price=price,
            target_low=targets.get("low"), target_high=targets.get("high"),
            expected_move_pct=targets.get("expected_move_pct"),
            # Su WAIT non si dichiara una durata. Non e' pudore: una durata
            # attesa senza una direzione attesa e' un numero senza referente,
            # e sulla dashboard verrebbe letta come se una previsione ci fosse.
            expected_duration_min=(None if verdict == WAIT
                                   else analogues.get("median_time_to_mfe_min")),
            invalidation=invalidation,
            quality=q.to_dict(),
            regime=regime.to_dict(),
            reasons=reasons,
            blockers=blockers,
            edge=(active_edges[0] if active_edges else None),
            model_id=self._model_id,
            features={k: v for k, v in row.items()},
            panels=self._panels(frame, row, snapshot, regime),
            diagnostics={
                "price_source": price_source,
                "bar_age_seconds": round(bar_age, 1),
                "live_age_seconds": (round(data_age, 1)
                                     if data_age is not None else None),
                "bars_loaded": len(frame),
                "feature_coverage": round(coverage, 3),
                "active_edges": len(active_edges),
                "analogues": analogues,
                "model_metrics": self._model_metrics.get("holdout"),
                "execution_enabled": config.EXECUTION_ENABLED,
            })

        if persist:
            self.store.add_forecast(forecast.to_row())
            self._record_edge_observations(forecast, active_edges)
        return forecast

    # ------------------------------------------------------------- aiutanti
    def _empty(self, now: int, symbol: str, horizon: int,
               reason: str) -> Forecast:
        return Forecast(
            forecast_id=uuid.uuid4().hex[:16], ts=now, symbol=symbol,
            horizon_min=horizon, verdict=WAIT,
            p_long=1 / 3, p_short=1 / 3, p_flat=1 / 3, price=0.0,
            target_low=None, target_high=None, expected_move_pct=None,
            expected_duration_min=None, invalidation=None,
            quality={"score": 0.0, "label": "INSUFFICIENTE",
                     "components": {}, "warnings": [reason]},
            regime={"name": "SCONOSCIUTO"}, reasons=[], blockers=[reason],
            edge=None, model_id=None)

    def _coverage(self, row: dict[str, float | None]) -> float:
        present = sum(1 for v in row.values()
                      if v is not None and math.isfinite(v))
        return present / len(row) if row else 0.0

    def _dense_row(self, model: LogisticModel,
                   row: dict[str, float | None]) -> list[float]:
        """La riga nell'ordine del modello, con le mediane al posto dei buchi."""
        out: list[float] = []
        for name in model.feature_names:
            v = row.get(name)
            if v is None or not math.isfinite(v):
                out.append(model.medians.get(name, 0.0))
            else:
                out.append(float(v))
        return out

    def _holdout_ok(self) -> bool:
        ho = (self._model_metrics or {}).get("holdout") or {}
        d = ho.get("directional") or {}
        acc = d.get("accuracy")
        auc = ho.get("auc_directional")
        return bool(acc is not None and auc is not None and auc >= 0.52)

    def _active_edges(self, row: dict[str, float | None]) -> list[dict[str, Any]]:
        """Gli edge VALIDATI che si attivano adesso.

        Solo VALIDATI: quelli in ombra vengono osservati e registrati, ma non
        influenzano il numero mostrato. E' la differenza fra studiare e usare.
        """
        out: list[dict[str, Any]] = []
        names = sorted(row.keys())
        values = [row.get(n) for n in names]
        pseudo = Dataset(names=names, rows=[values], ts=[0], labels=[None],
                         prices=[0.0])
        index = {n: j for j, n in enumerate(names)}

        for db_row in self.store.edges([lifecycle.VALIDATED]):
            record = lifecycle.EdgeRecord.from_row(db_row)
            fitted_def = (record.metrics or {}).get("fitted") or {}
            thresholds = fitted_def.get("thresholds")
            if not thresholds:
                continue
            try:
                rule = edges_mod.EdgeRule.from_dict(record.definition)
            except (KeyError, TypeError):
                continue
            fitted = edges_mod.FittedEdge(
                rule=rule, thresholds=list(thresholds),
                p_long=fitted_def.get("p_long", 1 / 3),
                p_short=fitted_def.get("p_short", 1 / 3),
                p_flat=fitted_def.get("p_flat", 1 / 3),
                train_activations=fitted_def.get("train_activations", 0))
            if not fitted.activates(values, index):
                continue
            wf = (record.metrics or {}).get("walk_forward") or {}
            out.append({
                "edge_id": record.edge_id,
                "label": record.label,
                "family": record.family,
                "direction": fitted.direction,
                "state": record.state,
                "description": fitted_def.get("description"),
                "probabilities": fitted.probabilities(),
                "historical_accuracy": (wf.get("directional") or {}).get("accuracy"),
                "historical_n": (wf.get("directional") or {}).get("n"),
                "auc_directional": wf.get("auc_directional"),
                "path": wf.get("path"),
            })
        _ = pseudo
        out.sort(key=lambda e: -(e.get("historical_accuracy") or 0.0))
        return out

    def _blend(self, model_probs: dict[str, float],
               active: list[dict[str, Any]]) -> dict[str, float]:
        """Media geometrica fra modello ed edge, rinormalizzata.

        La geometrica e non l'aritmetica perche' e' la combinazione che
        **penalizza il disaccordo**: se il modello dice 70% LONG e l'edge dice
        20%, l'aritmetica esce 45% e sembra una posizione tiepida; la
        geometrica esce piu' bassa e piu' incerta, che e' la descrizione onesta
        di due fonti che si contraddicono.
        """
        combos = [model_probs] + [e["probabilities"] for e in active]
        out: dict[str, float] = {}
        for cls in (LONG, SHORT, FLAT):
            product = 1.0
            for probs in combos:
                product *= max(probs.get(cls, 1e-6), 1e-6)
            out[cls] = product ** (1.0 / len(combos))
        total = sum(out.values())
        return ({k: v / total for k, v in out.items()} if total > 0
                else dict(model_probs))

    # ------------------------------------------------- analoghi e obiettivi
    def _analogues(self, frame: builder.Frame, regime: Regime, direction: str,
                   horizon_min: int) -> dict[str, Any]:
        """Che cosa e' successo storicamente in condizioni simili.

        L'intervallo di prezzo NON si inventa da una formula: si legge dalla
        distribuzione dei movimenti realizzati nelle finestre storiche con lo
        stesso regime. Se quelle finestre sono meno di trenta, non si mostra un
        intervallo: si dichiara che non c'e' base per stimarlo. Un intervallo
        inventato e' peggio di nessun intervallo, perche' sembra una misura.
        """
        now = timeutil.now_ms()
        key = f"{bucket(regime.name)}|{direction}|{horizon_min}"
        if key in self._analogues_cache and now - self._analogues_ts < 900_000:
            return self._analogues_cache[key]

        # Si guarda un archivio ampio, ma solo passato: tutte le finestre usate
        # sono chiuse e i loro esiti gia' realizzati.
        hist = builder.build_frame(self.store, config.SYMBOL,
                                   end_ms=now - horizon_min * 60_000,
                                   with_live=False)
        if len(hist) < 500:
            result = {"status": "STORIA INSUFFICIENTE", "n": 0}
            self._analogues_cache[key] = result
            return result

        ds = builder.build_dataset(hist, horizon_min=horizon_min, step=5)
        target_bucket = bucket(regime.name)
        moves: list[float] = []
        mfes: list[float] = []
        maes: list[float] = []
        times: list[float] = []
        # La volatilita' tipica delle finestre analoghe: e' il metro con cui si
        # riporta la distribuzione storica alla scala di adesso.
        sigmas: list[float] = []
        try:
            rv_col = ds.column("rv_60_bps")
        except ValueError:
            rv_col = [None] * len(ds)

        for reg, outcome, rv in zip(ds.regimes, ds.outcomes, rv_col):
            if outcome is None or not outcome.complete:
                continue
            if target_bucket != "SCONOSCIUTO" and reg != target_bucket:
                continue
            if rv is not None and math.isfinite(rv):
                s = horizon_sigma_bps(rv, horizon_min, config.BAR_MINUTES)
                if s:
                    sigmas.append(s)
            signed = outcome.signed_mfe(direction)
            if outcome.return_bps is None or signed is None:
                continue
            sign = 1.0 if direction == LONG else -1.0
            moves.append(outcome.return_bps * sign)
            mfes.append(signed)
            adverse = outcome.signed_mae(direction)
            if adverse is not None:
                maes.append(adverse)
            t = (outcome.time_to_mfe_min if direction == LONG
                 else outcome.time_to_mae_min)
            if t is not None:
                times.append(t)

        if len(moves) < 30:
            result = {"status": "ANALOGHI INSUFFICIENTI", "n": len(moves),
                      "regime": target_bucket,
                      "note": ("Meno di trenta finestre storiche in questo "
                               "regime: non c'e' base per un intervallo di "
                               "prezzo, e inventarlo sarebbe peggio.")}
        else:
            result = {
                "status": "OK",
                "n": len(moves),
                "regime": target_bucket,
                "median_move_bps": round(median(moves) or 0.0, 2),
                "p25_move_bps": round(quantile(moves, 0.25) or 0.0, 2),
                "p75_move_bps": round(quantile(moves, 0.75) or 0.0, 2),
                "p90_move_bps": round(quantile(moves, 0.90) or 0.0, 2),
                "median_mfe_bps": round(median(mfes) or 0.0, 2),
                "median_mae_bps": (round(median(maes) or 0.0, 2)
                                   if maes else None),
                "median_time_to_mfe_min": (round(median(times) or 0.0, 1)
                                           if times else None),
                "positive_share": round(
                    sum(1 for m in moves if m > 0) / len(moves), 3),
                "reference_sigma_bps": (round(median(sigmas) or 0.0, 2)
                                        if sigmas else None),
            }
        self._analogues_cache[key] = result
        self._analogues_ts = now
        return result

    def _targets(self, price: float, direction: str, analogues: dict[str, Any],
                 frame: builder.Frame, horizon_min: int,
                 verdict: str) -> dict[str, Any]:
        """L'intervallo di prezzo atteso, in scala con la volatilita' di adesso.

        Gli analoghi danno la forma della distribuzione; la volatilita'
        corrente da' la scala. Usare gli analoghi grezzi significherebbe
        applicare l'ampiezza media di sei mesi a un'ora che potrebbe essere il
        doppio o la meta' piu' agitata.
        """
        if verdict == WAIT or analogues.get("status") != "OK":
            return {"low": None, "high": None, "expected_move_pct": None}

        i = len(frame) - 1
        current_sigma = horizon_sigma_bps(frame.s("rv_60", i), horizon_min,
                                          config.BAR_MINUTES)
        reference = analogues.get("reference_sigma_bps")
        scale = 1.0
        if current_sigma and reference:
            scale = clamp(current_sigma / reference, 0.4, 2.5)

        lo_bps = (analogues.get("p25_move_bps") or 0.0) * scale
        hi_bps = (analogues.get("p75_move_bps") or 0.0) * scale
        mid_bps = (analogues.get("median_move_bps") or 0.0) * scale

        sign = 1.0 if direction == LONG else -1.0
        p_lo = price * (1.0 + sign * lo_bps / 10_000.0)
        p_hi = price * (1.0 + sign * hi_bps / 10_000.0)
        low, high = min(p_lo, p_hi), max(p_lo, p_hi)
        return {
            "low": round(low, 2),
            "high": round(high, 2),
            "expected_move_pct": round(sign * mid_bps / 100.0, 4),
            "scale": round(scale, 3),
        }

    def _invalidation(self, price: float, direction: str,
                      frame: builder.Frame, verdict: str) -> float | None:
        """Il prezzo a cui la tesi e' sbagliata.

        Non e' uno stop loss — questo programma non gestisce posizioni. E' il
        livello oltre il quale la lettura che ha prodotto la previsione non
        regge piu': per un LONG, sotto il minimo recente non si sta piu'
        salendo, si sta scendendo, e la previsione va considerata smentita.
        """
        if verdict == WAIT:
            return None
        i = len(frame) - 1
        atr = frame.s("atr_14", i)
        low = frame.s("low_60", i)
        high = frame.s("high_60", i)
        if direction == LONG:
            candidates = [v for v in (low, price - 1.5 * atr if atr else None)
                          if v is not None and v < price]
            return round(max(candidates), 2) if candidates else None
        candidates = [v for v in (high, price + 1.5 * atr if atr else None)
                      if v is not None and v > price]
        return round(min(candidates), 2) if candidates else None

    # ------------------------------------------------------------- i motivi
    def _reasons(self, contributions: list[dict[str, Any]],
                 active_edges: list[dict[str, Any]], regime: Regime,
                 row: dict[str, float | None],
                 analogues: dict[str, Any]) -> list[dict[str, Any]]:
        """Perche' il sistema dice quello che dice, in ordine di peso.

        Sono pesi, non cause. In un modello lineare il contributo di una
        variabile e' esattamente il suo peso per il suo valore standardizzato,
        e chiamarlo "motivo" e' onesto solo se si ricorda che vuol dire questo.
        """
        out: list[dict[str, Any]] = []
        for edge in active_edges[:2]:
            out.append({
                "kind": "edge",
                "text": f"Edge validato attivo: {edge['label']}",
                "detail": (f"storicamente corretto nel "
                           f"{(edge.get('historical_accuracy') or 0):.0%} dei "
                           f"casi su {edge.get('historical_n')} osservazioni "
                           "indipendenti"),
                "direction": edge["direction"],
                "weight": edge.get("historical_accuracy"),
            })
        for c in contributions:
            value = row.get(c["feature"])
            out.append({
                "kind": "feature",
                "text": f"{_pretty(c['feature'])}: {_fmt(value)}",
                "detail": (f"spinge verso {c['direction']} "
                           f"({c['value_z']:+.2f} deviazioni dalla norma)"),
                "direction": c["direction"],
                "weight": abs(c["push"]),
            })
        out.append({
            "kind": "regime",
            "text": f"Regime: {regime.name}",
            "detail": (f"tendenza {regime.trend}, volatilita' "
                       f"{regime.volatility}" +
                       (", volatilita' compressa" if regime.compressed else "")),
            "direction": FLAT,
            "weight": regime.confidence,
        })
        if analogues.get("status") == "OK":
            out.append({
                "kind": "analoghi",
                "text": (f"{analogues['n']} finestre storiche simili "
                         f"({analogues['regime']})"),
                "detail": (f"movimento mediano {analogues['median_move_bps']:+.1f} "
                           f"bps, favorevole nel {analogues['positive_share']:.0%} "
                           "dei casi"),
                "direction": FLAT,
                "weight": 0.5,
            })
        return out

    # ------------------------------------------------------------- pannelli
    def _panels(self, frame: builder.Frame, row: dict[str, float | None],
                snapshot: Any, regime: Regime) -> dict[str, Any]:
        """I riquadri della dashboard, gia' pronti da mostrare."""
        i = len(frame) - 1
        payload: dict[str, Any] = {}
        if snapshot is not None and snapshot["payload"]:
            try:
                payload = json.loads(snapshot["payload"])
            except (ValueError, TypeError):
                payload = {}

        def g(name: str) -> float | None:
            v = row.get(name)
            return None if v is None or not math.isfinite(v) else round(v, 4)

        news_rows = self.store.news(
            since_ms=timeutil.now_ms() - config.NEWS_LOOKBACK_HOURS * timeutil.HOUR_MS,
            limit=8)

        return {
            "price_action": {
                "close": frame.closes[i] if len(frame) else None,
                "ret_5m_bps": g("ret_5m"), "ret_15m_bps": g("ret_15m"),
                "ret_30m_bps": g("ret_30m"), "ret_60m_bps": g("ret_60m"),
                "rsi_14": g("rsi_14"), "macd_hist_bps": g("macd_hist_bps"),
                "atr_bps": g("atr_bps"), "bb_z": g("bb_z"),
                "bb_width_pct": g("bb_width_pct"),
                "price_vs_vwap_bps": g("price_vs_vwap_bps"),
                "vwap": frame.s("vwap_60", i),
                "range_position": g("range_position"),
                "high_60": frame.s("high_60", i),
                "low_60": frame.s("low_60", i),
            },
            "volume": {
                "burst": g("vol_burst"), "ratio_60": g("vol_ratio_60"),
                "percentile": g("vol_pctile"),
                "turnover_percentile": g("turnover_pctile"),
                "slope_15": g("vol_slope_15"),
                "volume_24h": snapshot["volume_24h"] if snapshot else None,
                "turnover_24h": snapshot["turnover_24h"] if snapshot else None,
            },
            "order_flow": {
                "available": g("taker_imb_5m") is not None,
                "taker_imbalance_5m": g("taker_imb_5m"),
                "taker_imbalance_15m": g("taker_imb_15m"),
                "cvd_slope_15": g("cvd_slope_15"),
                "cvd_z_60": g("cvd_z_60"),
                "cvd_price_divergence": g("cvd_price_div"),
                "book_imbalance": (snapshot["book_imbalance"]
                                   if snapshot else None),
                "book_imbalance_top": (snapshot["book_imbalance_top"]
                                       if snapshot else None),
                "spread_bps": snapshot["spread_bps"] if snapshot else None,
                "note": ("Il flusso ordini esiste solo da quando il collector "
                         "raccoglie: Bybit non ne pubblica lo storico."),
            },
            "open_interest": {
                "value": frame.s("open_interest", i),
                "usd": (snapshot["open_interest_value"] if snapshot else None),
                "change_15m_pct": g("oi_chg_15m_pct"),
                "change_60m_pct": g("oi_chg_60m_pct"),
                "acceleration": g("oi_accel"),
                "percentile": g("oi_pctile"),
                "price_agreement": g("oi_price_agree"),
                "reading": _oi_reading(g("oi_chg_15m_pct"), g("ret_15m")),
            },
            "derivatives": {
                "funding_rate": g("funding_rate"),
                "funding_bps": g("funding_bps"),
                "funding_z": g("funding_z"),
                "minutes_to_funding": g("mins_to_funding"),
                "basis_bps": snapshot["basis_bps"] if snapshot else None,
                "mark_price": snapshot["mark_price"] if snapshot else None,
                "index_price": snapshot["index_price"] if snapshot else None,
                "long_short_ratio": g("ls_ratio"),
                "long_short_z": g("ls_z"),
                "liquidations": payload.get("liquidations") or {
                    "available": False,
                    "reason": ("Bybit v5 pubblica le liquidazioni solo via "
                               "WebSocket, senza storico REST."),
                },
            },
            "market_context": {
                "eth_ret_15m_bps": g("eth_ret_15m"),
                "sol_ret_15m_bps": g("sol_ret_15m"),
                "eth_lead_5m_bps": g("eth_lead_5m"),
                "sol_lead_5m_bps": g("sol_lead_5m"),
                "eth_correlation_60": g("eth_corr_60"),
                "breadth": payload.get("breadth") or {"available": False},
                "session": timeutil.session_of(timeutil.now_ms()),
                "regime": regime.to_dict(),
            },
            "news": {
                "impact_1h": g("news_impact_1h"),
                "direction_1h": g("news_direction_1h"),
                "items": [
                    {"ts": r["ts"], "at": timeutil.iso(r["ts"]),
                     "source": r["source"], "title": r["title"],
                     "link": r["link"], "impact": r["impact"],
                     "direction": r["direction"]}
                    for r in news_rows
                ],
            },
        }

    # -------------------------------------------------- osservazioni in ombra
    def _record_edge_observations(self, forecast: Forecast,
                                  active: list[dict[str, Any]]) -> None:
        """Registra ogni attivazione di edge, anche di quelli non validati.

        E' il meccanismo che permette a un edge di maturare in ombra senza che
        influenzi nulla: l'attivazione si scrive adesso, l'esito si attacca fra
        trenta minuti quando il tempo sara' passato davvero.
        """
        rows: list[dict[str, Any]] = []
        names = sorted(forecast.features.keys())
        values = [forecast.features.get(n) for n in names]
        index = {n: j for j, n in enumerate(names)}

        for db_row in self.store.edges([lifecycle.SHADOW, lifecycle.VALIDATED,
                                        lifecycle.DECAYING]):
            record = lifecycle.EdgeRecord.from_row(db_row)
            fitted_def = (record.metrics or {}).get("fitted") or {}
            thresholds = fitted_def.get("thresholds")
            if not thresholds:
                continue
            try:
                rule = edges_mod.EdgeRule.from_dict(record.definition)
            except (KeyError, TypeError):
                continue
            fitted = edges_mod.FittedEdge(
                rule=rule, thresholds=list(thresholds),
                p_long=fitted_def.get("p_long", 1 / 3),
                p_short=fitted_def.get("p_short", 1 / 3),
                p_flat=fitted_def.get("p_flat", 1 / 3),
                train_activations=0)
            if not fitted.activates(values, index):
                continue
            rows.append({
                "edge_id": record.edge_id, "ts": forecast.ts,
                "direction": fitted.direction,
                "probability": fitted.probabilities().get(fitted.direction),
                "regime": forecast.regime.get("name"),
                "label": None, "return_bps": None, "mfe_bps": None,
                "mae_bps": None, "time_to_mfe_min": None, "correct": None,
                "is_live": 1,
            })
        _ = active
        if rows:
            self.store.add_edge_observations(rows)


def _oi_reading(oi_change: float | None, ret: float | None) -> str:
    """La lettura classica dell'interazione open interest / prezzo."""
    if oi_change is None or ret is None:
        return "non disponibile"
    if oi_change > 0 and ret > 0:
        return "posizioni long nuove: salita sostenuta da capitale in entrata"
    if oi_change > 0 and ret < 0:
        return "posizioni short nuove: discesa sostenuta da capitale in entrata"
    if oi_change < 0 and ret > 0:
        return "chiusura di short: salita da ricoperture, meno sostenibile"
    if oi_change < 0 and ret < 0:
        return "chiusura di long: discesa da liquidazioni di posizioni, sfogo"
    return "nessuna interazione netta"


_LABELS = {
    "ret_5m": "Rendimento 5m", "ret_15m": "Rendimento 15m",
    "ret_30m": "Rendimento 30m", "ret_60m": "Rendimento 60m",
    "rsi_14": "RSI(14)", "rsi_14_dev": "RSI(14) scostamento da 50",
    "macd_hist_bps": "Istogramma MACD", "atr_bps": "ATR",
    "bb_z": "Posizione nelle Bollinger", "bb_width_pct": "Ampiezza Bollinger",
    "price_vs_vwap_bps": "Distanza dalla VWAP",
    "vwap_dist_sigma": "Distanza VWAP in sigma",
    "vol_burst": "Scoppio di volume", "vol_pctile": "Percentile del volume",
    "taker_imb_5m": "Squilibrio taker 5m",
    "taker_imb_15m": "Squilibrio taker 15m",
    "cvd_slope_15": "Pendenza del CVD", "cvd_z_60": "CVD normalizzato",
    "book_imbalance": "Squilibrio del libro",
    "oi_chg_15m_pct": "Variazione open interest 15m",
    "oi_accel": "Accelerazione open interest",
    "funding_bps": "Funding", "funding_z": "Funding normalizzato",
    "basis_bps": "Base mark/index",
    "eth_ret_15m": "ETH 15m", "sol_ret_15m": "SOL 15m",
    "eth_lead_5m": "ETH in anticipo su BTC",
    "compression": "Compressione di volatilita'",
    "range_position": "Posizione nel range",
    "streak": "Serie consecutiva",
}


def _pretty(name: str) -> str:
    return _LABELS.get(name, name.replace("_", " "))


def _fmt(value: float | None) -> str:
    if value is None or not math.isfinite(value):
        return "n/d"
    if abs(value) >= 1000:
        return f"{value:,.0f}"
    if abs(value) >= 10:
        return f"{value:.1f}"
    return f"{value:.3f}"


# --------------------------------------------------------------------------
# Giudizio delle previsioni passate
# --------------------------------------------------------------------------
def score_pending(store: Store | None = None,
                  now_ms: int | None = None) -> dict[str, Any]:
    """Attacca l'esito alle previsioni scadute, e alle osservazioni degli edge.

    E' la funzione che rende il sistema onesto nel tempo: ogni previsione
    emessa e' stata scritta prima, e qui viene giudicata dopo, con lo stesso
    metro delle etichette storiche. Nessuna previsione sparisce perche' e'
    andata male.
    """
    store = store or Store()
    now = now_ms or timeutil.now_ms()
    pending = store.pending_forecasts(now)
    scored = 0

    for row in pending:
        horizon = row["horizon_min"]
        start = row["ts"]
        end = start + horizon * 60_000
        bars = store.bars(row["symbol"], start_ms=start + 60_000, end_ms=end)
        if len(bars) < max(3, horizon // 3):
            continue
        entry = row["price"]
        if not entry:
            continue

        from ..features.indicators import Bar
        from ..features.labels import outcome_from_bars

        future = [Bar(b["ts"], b["open"], b["high"], b["low"], b["close"],
                      b["volume"] or 0.0, b["turnover"] or 0.0) for b in bars]
        # La banda si ricalcola con la stessa regola dello storico: usare una
        # soglia diversa a posteriori renderebbe le due misure incomparabili.
        sigma = horizon_sigma_bps(
            _recent_vol(store, row["symbol"], start), horizon,
            config.BAR_MINUTES)
        outcome = outcome_from_bars(future, entry, horizon, band_bps(sigma))

        correct: int | None = None
        if row["verdict"] in (LONG, SHORT):
            if outcome.label in (LONG, SHORT):
                correct = 1 if outcome.label == row["verdict"] else 0

        store.score_forecast(
            row["forecast_id"],
            outcome_ts=end, outcome_price=outcome.exit,
            outcome_return_bps=outcome.return_bps,
            outcome_label=outcome.label,
            outcome_mfe_bps=outcome.mfe_bps,
            outcome_mae_bps=outcome.mae_bps,
            outcome_correct=correct,
            time_to_mfe_min=outcome.time_to_mfe_min)
        scored += 1

    edge_scored = _score_edge_observations(store, now)
    return {"forecasts_scored": scored, "edge_observations_scored": edge_scored,
            "pending": len(pending)}


def _recent_vol(store: Store, symbol: str, ts: int) -> float | None:
    rows = store.bars(symbol, end_ms=ts, limit=61, newest_first=True)
    closes = [r["close"] for r in reversed(rows)]
    if len(closes) < 20:
        return None
    from ..features.indicators import realized_vol_bps
    return realized_vol_bps(closes, min(60, len(closes) - 1))


def _score_edge_observations(store: Store, now: int) -> int:
    """Stesso trattamento per le attivazioni degli edge in ombra."""
    horizon = config.PRIMARY_HORIZON_MIN
    cutoff = now - horizon * 60_000
    rows = store.conn.execute(
        "SELECT edge_id, ts, direction FROM edge_observations "
        "WHERE label IS NULL AND ts <= ? ORDER BY ts ASC LIMIT 2000",
        (cutoff,)).fetchall()
    if not rows:
        return 0

    from ..features.indicators import Bar
    from ..features.labels import outcome_from_bars

    updates: list[dict[str, Any]] = []
    for row in rows:
        start = row["ts"]
        end = start + horizon * 60_000
        bars = store.bars(config.SYMBOL, start_ms=start + 60_000, end_ms=end)
        if len(bars) < max(3, horizon // 3):
            continue
        entry_rows = store.bars(config.SYMBOL, end_ms=start, limit=1,
                                newest_first=True)
        if not entry_rows:
            continue
        entry = entry_rows[0]["close"]
        future = [Bar(b["ts"], b["open"], b["high"], b["low"], b["close"],
                      b["volume"] or 0.0, b["turnover"] or 0.0) for b in bars]
        sigma = horizon_sigma_bps(_recent_vol(store, config.SYMBOL, start),
                                  horizon, config.BAR_MINUTES)
        outcome = outcome_from_bars(future, entry, horizon, band_bps(sigma))
        direction = row["direction"]
        correct = (1 if outcome.label == direction else 0) \
            if outcome.label in (LONG, SHORT) else None
        updates.append({
            "edge_id": row["edge_id"], "ts": start, "direction": direction,
            "probability": None, "regime": None, "label": outcome.label,
            "return_bps": outcome.return_bps,
            "mfe_bps": outcome.signed_mfe(direction),
            "mae_bps": outcome.signed_mae(direction),
            "time_to_mfe_min": (outcome.time_to_mfe_min if direction == LONG
                                else outcome.time_to_mae_min),
            "correct": correct, "is_live": 1,
        })

    if not updates:
        return 0
    with store.tx() as conn:
        for u in updates:
            conn.execute(
                "UPDATE edge_observations SET label=?, return_bps=?, mfe_bps=?,"
                " mae_bps=?, time_to_mfe_min=?, correct=? "
                "WHERE edge_id=? AND ts=?",
                (u["label"], u["return_bps"], u["mfe_bps"], u["mae_bps"],
                 u["time_to_mfe_min"], u["correct"], u["edge_id"], u["ts"]))
    return len(updates)
