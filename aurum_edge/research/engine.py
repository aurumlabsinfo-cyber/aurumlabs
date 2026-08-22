"""Il motore di ricerca: studia in continuazione, promuove quasi mai.

Un giro completo, nell'ordine, e nessun passo si puo' saltare:

1. **Ricostruisce il dataset** dallo storico e attacca le etichette a 30 minuti.
2. **Cerca la fuga di informazione** prima di guardare qualunque risultato. Se
   una feature e' correlata all'etichetta oltre ogni ragionevolezza, il giro si
   ferma li': un vantaggio trovato su dati contaminati non e' un vantaggio
   piccolo, e' un artefatto.
3. **Taglia l'holdout fresco**, con la purga. Da questo momento quella coda non
   viene toccata da nessuno.
4. **Addestra il modello** in walk-forward con purga ed embargo, e lo confronta
   con il campione in carica.
5. **Prova ogni edge candidato** con le soglie ricavate dentro ogni fold.
6. **Corregge per test multipli**: con duecento candidati provati sugli stessi
   dati, i migliori sono i migliori anche se non c'e' niente da trovare.
7. **Conferma i sopravvissuti sull'holdout**, una volta sola.
8. **Aggiorna il ciclo di vita** e scrive il referto.

Il risultato normale e' NESSUN EDGE VALIDATO. Non e' un fallimento del motore:
e' cio' che dicono i dati la maggior parte del tempo, e dirlo e' l'unico modo
di rendere credibile la volta in cui dira' il contrario.
"""

from __future__ import annotations

import json
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable

from .. import config
from ..data.store import Store
from ..features import builder
from ..features.dataset import Dataset
from ..model import logistic
from ..util import timeutil
from . import edges as edges_mod
from . import lifecycle, metrics, validation

Log = Callable[[str], None]


def _noop(_: str) -> None:
    pass


@dataclass
class ResearchReport:
    run_id: str
    started_ts: int
    ended_ts: int | None = None
    status: str = "IN_CORSO"
    dataset: dict[str, Any] = field(default_factory=dict)
    leakage: dict[str, Any] = field(default_factory=dict)
    power: dict[str, Any] = field(default_factory=dict)
    model: dict[str, Any] = field(default_factory=dict)
    edges: dict[str, Any] = field(default_factory=dict)
    catalogue: dict[str, Any] = field(default_factory=dict)
    conclusion: str = ""
    errors: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "started": timeutil.iso(self.started_ts),
            "ended": timeutil.iso(self.ended_ts),
            "duration_s": (round((self.ended_ts - self.started_ts) / 1000.0, 1)
                           if self.ended_ts else None),
            "status": self.status,
            "dataset": self.dataset,
            "leakage": self.leakage,
            "power": self.power,
            "model": self.model,
            "edges": self.edges,
            "catalogue": self.catalogue,
            "conclusion": self.conclusion,
            "errors": self.errors,
        }


class ResearchEngine:
    """Un giro di ricerca. Si puo' lanciare a mano o in ciclo."""

    def __init__(self, store: Store | None = None, log: Log = _noop) -> None:
        self.store = store or Store()
        self.log = log

    # ------------------------------------------------------------- un giro
    def run(self, *, horizon_min: int | None = None,
            max_candidates: int | None = None,
            model_epochs: int = 70) -> ResearchReport:
        horizon_min = horizon_min or config.PRIMARY_HORIZON_MIN
        report = ResearchReport(run_id=uuid.uuid4().hex[:12],
                                started_ts=timeutil.now_ms())
        began = time.monotonic()

        # --------------------------------------------------------- 1. dati
        self.log("[ricerca] costruzione del dataset")
        frame = builder.build_frame(self.store, config.SYMBOL)
        if not len(frame):
            report.status = "NESSUN DATO"
            report.conclusion = (
                "L'archivio non contiene barre. Serve un backfill "
                "(`python3 -m aurum_edge backfill`) prima di poter studiare "
                "qualunque cosa.")
            return self._finish(report, began)

        ds = builder.build_dataset(frame, horizon_min=horizon_min)
        ds = ds.labelled()
        report.dataset = ds.summary()
        report.dataset["span"] = {
            "from": timeutil.iso(ds.ts[0]) if ds.ts else None,
            "to": timeutil.iso(ds.ts[-1]) if ds.ts else None,
            "days": (round((ds.ts[-1] - ds.ts[0]) / 86_400_000, 2)
                     if len(ds.ts) > 1 else 0.0),
        }
        report.catalogue = edges_mod.describe_catalogue(ds.names)

        if len(ds) < config.MIN_ROWS_FOR_VALIDATION:
            report.status = "DATI INSUFFICIENTI"
            report.conclusion = (
                f"{len(ds)} righe utilizzabili contro le "
                f"{config.MIN_ROWS_FOR_VALIDATION} minime. Con meno storia di "
                "cosi' ogni risultato sarebbe indistinguibile dal rumore, e "
                "mostrarlo sarebbe peggio che non mostrarlo.")
            return self._finish(report, began)

        # ------------------------------------------------------- 2. leakage
        report.leakage = validation.leakage_report(ds)
        if report.leakage.get("status") == "SOSPETTO" and \
                report.leakage.get("suspects"):
            report.status = "SOSPETTA FUGA DI INFORMAZIONE"
            names = ", ".join(s["feature"] for s in report.leakage["suspects"])
            report.conclusion = (
                f"Il controllo ha trovato feature troppo correlate con "
                f"l'etichetta ({names}). Il giro si ferma qui di proposito: "
                "qualunque risultato ottenuto ora sarebbe un artefatto, e un "
                "artefatto convincente e' peggio di nessun risultato.")
            return self._finish(report, began)

        # ------------------------------------------- 3. holdout e potenza
        selection, holdout = validation.holdout_split(ds)
        horizon_ms = horizon_min * 60_000
        indep_sel = len(validation.independent_indices(selection.ts, horizon_ms))
        indep_hold = len(validation.independent_indices(holdout.ts, horizon_ms))
        base = (ds.summary().get("classes") or {}).get("base_rate") or 0.5
        report.power = {
            **validation.sample_power(indep_sel, base),
            "selection_rows": len(selection),
            "holdout_rows": len(holdout),
            "holdout_independent": indep_hold,
        }
        self.log(f"[ricerca] {len(ds)} righe, {indep_sel} indipendenti in "
                 f"selezione, {indep_hold} nell'holdout")

        splits = validation.make_folds(selection)
        if not splits:
            report.status = "DATI INSUFFICIENTI"
            report.conclusion = (
                "Dopo la purga non restano fold di test utilizzabili. Serve "
                "piu' storia: ridurre la purga farebbe sparire il problema e "
                "comparire i falsi vantaggi.")
            return self._finish(report, began)

        # --------------------------------------------------- 4. il modello
        try:
            report.model = self._study_model(selection, holdout, splits,
                                             model_epochs)
        except Exception as exc:                                # noqa: BLE001
            report.errors.append(f"modello: {type(exc).__name__}: {exc}")
            report.model = {"status": "ERRORE", "detail": str(exc)}

        # ----------------------------------------------------- 5-7. gli edge
        try:
            report.edges = self._study_edges(selection, holdout, splits,
                                             max_candidates)
        except Exception as exc:                                # noqa: BLE001
            report.errors.append(f"edge: {type(exc).__name__}: {exc}")
            report.edges = {"status": "ERRORE", "detail": str(exc)}

        report.status = "COMPLETO"
        report.conclusion = self._conclude(report)
        return self._finish(report, began)

    # ------------------------------------------------------------- modello
    def _study_model(self, selection: Dataset, holdout: Dataset,
                     splits: list[validation.FoldSplit],
                     epochs: int) -> dict[str, Any]:
        self.log("[ricerca] walk-forward del modello")

        def fit(train: Dataset) -> logistic.LogisticModel:
            return logistic.fit(train, epochs=epochs, max_rows=4000)

        def predict(model: logistic.LogisticModel, test: Dataset
                    ) -> list[dict[str, float]]:
            return model.predict(test)

        walk = validation.walk_forward(selection, fit, predict, splits=splits)
        verdict = validation.judge(walk)

        out: dict[str, Any] = {
            "walk_forward": walk,
            "verdict": verdict.to_dict(),
        }

        if walk.get("status") != "COMPLETO":
            out["status"] = walk.get("status")
            return out

        # Il modello finale si addestra su tutta la selezione, si tara la
        # temperatura su una coda della selezione stessa che l'addestramento
        # non ha visto, e si giudica una volta sola sull'holdout.
        inner_train, inner_cal = validation.holdout_split(selection, 0.15)
        final = logistic.fit(inner_train, epochs=epochs, max_rows=4000)
        final.temperature = logistic.calibrate_temperature(final, inner_cal)

        holdout_result = self._score_on(final, holdout)
        out["holdout"] = holdout_result
        out["training"] = final.training
        out["temperature"] = final.temperature

        holdout_ok = self._holdout_ok(holdout_result)
        out["status"] = "PROMOSSO" if (verdict.passed and holdout_ok) else "NON PROMOSSO"
        out["holdout_confirms"] = holdout_ok

        self._register_model(final, out, promoted=(out["status"] == "PROMOSSO"),
                             rows=len(selection))
        return out

    def _score_on(self, model: logistic.LogisticModel,
                  ds: Dataset) -> dict[str, Any]:
        if len(ds) == 0:
            return {"status": "HOLDOUT VUOTO"}
        probs = model.predict(ds)
        preds = [max(p, key=lambda k: p[k]) for p in probs]
        idx = validation.independent_indices(ds.ts, ds.horizon_min * 60_000)
        card = metrics.score(
            [probs[i] for i in idx], [preds[i] for i in idx],
            [ds.labels[i] or "" for i in idx],
            outcomes=[ds.outcomes[i] for i in idx] if ds.outcomes else None,
            n_independent=len(idx))
        result = card.to_dict()
        result["by_regime"] = self._by_regime(ds, probs, preds, idx)
        result["status"] = "OK"
        return result

    def _by_regime(self, ds: Dataset, probs: list[dict[str, float]],
                   preds: list[str], idx: list[int]) -> dict[str, Any]:
        """Le stesse misure, separate per regime.

        Una media fra due regimi in cui il pattern ha segno opposto e' zero, e
        uno zero cosi' e' il modo piu' efficace di buttare via un vantaggio
        vero. Sotto le trenta osservazioni il gruppo si dichiara insufficiente
        invece di riportare un numero.
        """
        if not ds.regimes:
            return {}
        groups: dict[str, list[int]] = {}
        for i in idx:
            groups.setdefault(ds.regimes[i] or "SCONOSCIUTO", []).append(i)
        out: dict[str, Any] = {}
        for name, indices in groups.items():
            if len(indices) < 30:
                out[name] = {"n": len(indices),
                             "status": "CAMPIONE INSUFFICIENTE"}
                continue
            card = metrics.score(
                [probs[i] for i in indices], [preds[i] for i in indices],
                [ds.labels[i] or "" for i in indices],
                n_independent=len(indices))
            d = card.to_dict()
            out[name] = {
                "n": len(indices),
                "balanced_accuracy": d.get("balanced_accuracy"),
                "directional": d.get("directional"),
                "base_rate": d.get("directional_base_rate"),
                "auc_directional": d.get("auc_directional"),
            }
        return out

    def _holdout_ok(self, result: dict[str, Any]) -> bool:
        """L'holdout conferma? Soglie piu' morbide, perche' il campione e' piccolo.

        Non e' un'indulgenza: l'holdout e' un quarto dei dati e va giudicato
        con la potenza che ha. Si chiede che non contraddica — non che ripeta
        il risultato con la stessa forza, cosa che con quel campione non
        potrebbe fare nemmeno un vantaggio reale.
        """
        if result.get("status") != "OK":
            return False
        d = result.get("directional") or {}
        acc, base = d.get("accuracy"), result.get("directional_base_rate")
        auc_dir = result.get("auc_directional")
        if acc is None or base is None:
            return False
        return bool(acc >= base and (auc_dir or 0.0) >= 0.52)

    def _register_model(self, model: logistic.LogisticModel,
                        result: dict[str, Any], *, promoted: bool,
                        rows: int) -> None:
        """Champion e challenger, confrontati sulla capacita' predittiva.

        Nessun euro compare in questo confronto, e non e' una dimenticanza: il
        prodotto e' una previsione, e il criterio deve essere la qualita' della
        previsione. Un modello che guadagna di piu' perche' e' capitato in un
        mese favorevole non e' un modello migliore.
        """
        now = timeutil.now_ms()
        model_id = f"logit-{now}-{uuid.uuid4().hex[:6]}"
        holdout = result.get("holdout") or {}
        summary = {
            "walk_forward": {
                "balanced_accuracy": (result.get("walk_forward", {})
                                      .get("overall", {})
                                      .get("balanced_accuracy")),
                "auc_directional": (result.get("walk_forward", {})
                                    .get("overall", {})
                                    .get("auc_directional")),
                "independent": result.get("walk_forward", {})
                                     .get("independent_samples"),
                "consistency": result.get("walk_forward", {})
                                     .get("fold_consistency"),
            },
            "holdout": {
                "balanced_accuracy": holdout.get("balanced_accuracy"),
                "auc_directional": holdout.get("auc_directional"),
                "directional": holdout.get("directional"),
                "calibration": (holdout.get("calibration") or {}).get("ece"),
                "by_regime": holdout.get("by_regime"),
            },
            "verdict": result.get("verdict"),
        }

        champion = self.store.model_by_role("CHAMPION")
        role = "CHALLENGER"
        if promoted:
            if champion is None:
                role = "CHAMPION"
            else:
                try:
                    old = json.loads(champion["metrics"] or "{}")
                except (ValueError, TypeError):
                    old = {}
                old_score = _skill(old)
                new_score = _skill(summary)
                if new_score > old_score + 0.01:
                    role = "CHAMPION"
                    self.store.set_model_role(champion["model_id"], "RETIRED")
                    self.log(f"[ricerca] nuovo campione: {new_score:.4f} contro "
                             f"{old_score:.4f}")

        self.store.save_model({
            "model_id": model_id, "created_ts": now, "role": role,
            "kind": "logistic-multinomiale", "horizon_min": config.PRIMARY_HORIZON_MIN,
            "params": model.to_json(),
            "metrics": json.dumps(summary, default=str),
            "trained_from": None, "trained_to": now, "rows": rows,
            "notes": ("promosso dopo walk-forward e holdout" if promoted
                      else "non ha superato i cancelli: resta sfidante"),
        })
        result["model_id"] = model_id
        result["role"] = role

    # ---------------------------------------------------------------- edge
    def _study_edges(self, selection: Dataset, holdout: Dataset,
                     splits: list[validation.FoldSplit],
                     max_candidates: int | None) -> dict[str, Any]:
        rules = edges_mod.candidate_rules(selection.names)
        if max_candidates:
            rules = rules[:max_candidates]
        self.log(f"[ricerca] {len(rules)} edge candidati")

        verdicts: dict[str, validation.Verdict] = {}
        walks: dict[str, dict[str, Any]] = {}
        rule_by_id: dict[str, edges_mod.EdgeRule] = {}
        skipped = 0

        for rule in rules:
            rule_by_id[rule.edge_id] = rule

            def fit(train: Dataset, _r: edges_mod.EdgeRule = rule):
                return edges_mod.fit_edge(_r, train)

            def predict(fitted: Any, test: Dataset) -> list[dict[str, float]]:
                return edges_mod.predict_edge(fitted, test)

            def mask(fitted: Any, test: Dataset) -> list[bool]:
                if fitted is None:
                    return [False] * len(test.rows)
                return edges_mod.activation_mask(fitted, test)

            # `mask_fn` e' cio' che rende sensata la misura di un edge: si
            # giudica dove parla, contro la base rate di tutto il periodo.
            walk = validation.walk_forward(selection, fit, predict,
                                           splits=splits, mask_fn=mask)
            if walk.get("status") != "COMPLETO":
                skipped += 1
                continue
            # Le soglie restano quelle del modello dove hanno senso. I tre
            # cancelli che una regola condizionale non puo' superare per
            # costruzione vengono spenti da `judge` stesso, non allentati.
            verdicts[rule.edge_id] = validation.judge(walk)
            walks[rule.edge_id] = walk

        fdr = validation.apply_fdr(verdicts)
        survivors = fdr.get("survivors", [])
        self.log(f"[ricerca] {len(verdicts)} valutati, "
                 f"{fdr.get('candidates', 0)} oltre le soglie, "
                 f"{len(survivors)} dopo la correzione")

        confirmed: list[dict[str, Any]] = []
        for edge_id in survivors:
            rule = rule_by_id[edge_id]
            fitted = edges_mod.fit_edge(rule, selection)
            if fitted is None:
                continue
            holdout_result = self._edge_on_holdout(fitted, holdout)
            record = self._record_edge(rule, fitted, walks[edge_id],
                                       verdicts[edge_id], holdout_result)
            confirmed.append(record)

        # Anche i bocciati vanno registrati: l'archivio dei rifiuti e' cio' che
        # impedisce di riprovare ogni mezz'ora lo stesso pattern morto.
        rejected_sample = []
        for edge_id, verdict in list(verdicts.items())[:400]:
            if verdict.passed or edge_id in survivors:
                continue
            rule = rule_by_id[edge_id]
            rejected_sample.append({
                "edge_id": edge_id, "label": rule.label,
                "family": rule.family,
                "reasons": verdict.reasons[:2],
            })

        promoted = [c for c in confirmed if c["state"] == lifecycle.SHADOW
                    or c["state"] == lifecycle.VALIDATED]

        return {
            "status": "COMPLETO",
            "candidates": len(rules),
            "evaluated": len(verdicts),
            "skipped_insufficient": skipped,
            "passed_thresholds": fdr.get("candidates", 0),
            "survived_fdr": len(survivors),
            "confirmed": confirmed,
            "promoted": len(promoted),
            "fdr": {k: v for k, v in fdr.items() if k != "adjusted"},
            "rejected_examples": rejected_sample[:12],
        }

    def _edge_on_holdout(self, fitted: edges_mod.FittedEdge,
                         holdout: Dataset) -> dict[str, Any]:
        """L'holdout fresco. Si guarda una volta, e la risposta e' vincolante."""
        if len(holdout) == 0:
            return {"status": "HOLDOUT VUOTO"}
        mask = edges_mod.activation_mask(fitted, holdout)
        idx = [i for i in validation.independent_indices(
            holdout.ts, holdout.horizon_min * 60_000) if mask[i]]
        if len(idx) < 20:
            return {"status": "ATTIVAZIONI INSUFFICIENTI",
                    "activations": len(idx),
                    "note": ("Nell'holdout l'edge si e' attivato troppo poco "
                             "per essere confermato o smentito.")}
        direction = fitted.direction
        labels = [holdout.labels[i] for i in idx]
        resolved = [l for l in labels if l in (metrics.LONG, metrics.SHORT)]
        hits = sum(1 for l in resolved if l == direction)
        from ..util.numeric import wilson_interval
        lo, hi = wilson_interval(hits, len(resolved)) if resolved else (0.0, 1.0)
        base = (max(resolved.count(metrics.LONG), resolved.count(metrics.SHORT))
                / len(resolved)) if resolved else None
        return {
            "status": "OK",
            "activations": len(idx),
            "resolved": len(resolved),
            "accuracy": round(hits / len(resolved), 4) if resolved else None,
            "ci95": [round(lo, 4), round(hi, 4)],
            "base_rate": round(base, 4) if base is not None else None,
            "confirms": bool(resolved and base is not None
                             and hits / len(resolved) > base),
        }

    def _record_edge(self, rule: edges_mod.EdgeRule,
                     fitted: edges_mod.FittedEdge, walk: dict[str, Any],
                     verdict: validation.Verdict,
                     holdout: dict[str, Any]) -> dict[str, Any]:
        """Scrive l'edge nell'archivio e lo fa avanzare di uno stato al massimo.

        Uno stato per volta, mai due. Un edge che passa validazione e holdout
        arriva IN OMBRA, non VALIDATO: manca il pezzo che nessun dato storico
        puo' dare, cioe' aver funzionato su previsioni fatte prima di
        conoscerne l'esito.
        """
        now = timeutil.now_ms()
        existing = self.store.edge(rule.edge_id)
        overall = walk.get("overall") or {}
        summary = {
            "walk_forward": {
                "independent": walk.get("independent_samples"),
                "balanced_accuracy": overall.get("balanced_accuracy"),
                "directional": overall.get("directional"),
                "base_rate": overall.get("directional_base_rate"),
                "auc_directional": overall.get("auc_directional"),
                "brier": overall.get("brier"),
                "calibration": (overall.get("calibration") or {}).get("ece"),
                "consistency": walk.get("fold_consistency"),
                "path": overall.get("path"),
            },
            "holdout": holdout,
            "verdict": verdict.to_dict(),
            "fitted": fitted.to_dict(),
            "measured_at": timeutil.iso(now),
        }

        if existing is None:
            record = lifecycle.EdgeRecord(
                edge_id=rule.edge_id, family=rule.family, label=rule.label,
                definition=rule.to_dict(), state=lifecycle.DISCOVERED,
                direction=fitted.direction, created_ts=now, updated_ts=now,
                state_ts=now)
            record.transition(lifecycle.VALIDATING,
                              "supera le soglie e la correzione per test multipli",
                              now)
        else:
            record = lifecycle.EdgeRecord.from_row(existing)
            # La direzione la decidono i dati piu' recenti, non quelli del
            # primo giro: se cambia, e' un'informazione, non un dettaglio.
            record.direction = fitted.direction

        record.metrics = summary

        if holdout.get("status") == "OK" and holdout.get("confirms"):
            if record.state in (lifecycle.DISCOVERED, lifecycle.VALIDATING):
                record.transition(
                    lifecycle.SHADOW,
                    f"holdout fresco conferma: {holdout.get('accuracy')} contro "
                    f"una base rate di {holdout.get('base_rate')} su "
                    f"{holdout.get('resolved')} casi risolti", now)
        elif holdout.get("status") == "OK" and not holdout.get("confirms"):
            record.transition(
                lifecycle.REJECTED,
                f"l'holdout fresco smentisce: {holdout.get('accuracy')} contro "
                f"una base rate di {holdout.get('base_rate')}", now)
        else:
            record.transition(
                lifecycle.VALIDATING,
                f"holdout non concludente: {holdout.get('status')}", now)

        self.store.upsert_edge(record.to_row())
        return record.to_dict()

    # -------------------------------------------------------- ombra e decadenza
    def update_shadow_states(self) -> dict[str, Any]:
        """Fa avanzare gli edge in ombra usando le osservazioni raccolte dal vivo.

        Va chiamato spesso e costa poco: legge solo l'archivio delle
        osservazioni. E' il pezzo del ciclo che non si puo' comprimere, perche'
        aspetta il tempo reale.
        """
        moved: list[dict[str, Any]] = []
        for row in self.store.edges([lifecycle.SHADOW, lifecycle.VALIDATED,
                                     lifecycle.DECAYING]):
            record = lifecycle.EdgeRecord.from_row(row)
            obs = [dict(o) for o in self.store.edge_observations(record.edge_id)]
            live = [o for o in obs if o["is_live"]]

            if record.state == lifecycle.SHADOW:
                verdict = lifecycle.shadow_verdict(live)
                record.metrics["shadow"] = verdict
                if verdict.get("ready"):
                    if verdict.get("beats_base"):
                        record.transition(lifecycle.VALIDATED,
                                          verdict["reason"])
                    else:
                        record.transition(lifecycle.REJECTED,
                                          "in ombra non ha battuto la base "
                                          "rate: " + verdict["reason"])
                    moved.append(record.to_dict())
            else:
                reference = ((record.metrics.get("walk_forward") or {})
                             .get("directional") or {}).get("accuracy")
                verdict = lifecycle.decay_verdict(live, reference)
                record.metrics["decay"] = verdict
                if verdict.get("decaying") and record.state == lifecycle.VALIDATED:
                    record.transition(lifecycle.DECAYING, verdict["reason"])
                    moved.append(record.to_dict())
                elif (record.state == lifecycle.DECAYING
                      and not verdict.get("decaying")
                      and verdict.get("n", 0) >= config.DECAY_WINDOW_SAMPLES):
                    record.transition(lifecycle.VALIDATED,
                                      "il calo e' rientrato: " + verdict["reason"])
                    moved.append(record.to_dict())
            self.store.upsert_edge(record.to_row())
        return {"checked": True, "moved": moved}

    # ------------------------------------------------------------ conclusione
    def _conclude(self, report: ResearchReport) -> str:
        edges = report.edges or {}
        model = report.model or {}
        validated = self.store.edges([lifecycle.VALIDATED])
        shadow = self.store.edges([lifecycle.SHADOW])

        parts: list[str] = []
        if validated:
            parts.append(f"{len(validated)} edge validati e utilizzabili.")
        else:
            parts.append("NESSUN EDGE VALIDATO.")
        if shadow:
            parts.append(f"{len(shadow)} in osservazione in ombra: hanno "
                         "superato lo storico, ora devono funzionare in avanti.")
        parts.append(
            f"Provati {edges.get('candidates', 0)} pattern, "
            f"{edges.get('passed_thresholds', 0)} hanno superato le soglie, "
            f"{edges.get('survived_fdr', 0)} sono sopravvissuti alla "
            "correzione per test multipli.")

        if model.get("status") == "PROMOSSO":
            parts.append("Il modello supera walk-forward e holdout.")
        elif model.get("status") == "NON PROMOSSO":
            reasons = (model.get("verdict") or {}).get("reasons") or []
            parts.append("Il modello NON supera i cancelli" +
                         (f": {reasons[0]}." if reasons else "."))

        power = report.power or {}
        if power.get("detectable_lift") is not None and not validated:
            parts.append(power.get("note", ""))
        return " ".join(p for p in parts if p)

    def _finish(self, report: ResearchReport, began: float) -> ResearchReport:
        report.ended_ts = timeutil.now_ms()
        edges = report.edges or {}
        self.store.add_research_run({
            "run_id": report.run_id,
            "started_ts": report.started_ts,
            "ended_ts": report.ended_ts,
            "rows": (report.dataset or {}).get("rows"),
            "span_from": None, "span_to": None,
            "candidates": edges.get("candidates"),
            "promoted": edges.get("promoted"),
            "rejected": (edges.get("evaluated", 0) -
                         edges.get("survived_fdr", 0)),
            "status": report.status,
            "report": json.dumps(report.to_dict(), default=str),
        })
        self.log(f"[ricerca] giro {report.run_id} concluso in "
                 f"{time.monotonic() - began:.1f}s: {report.status}")
        return report


def _skill(summary: dict[str, Any]) -> float:
    """Un punteggio unico per confrontare campione e sfidante.

    Pesa l'holdout piu' del walk-forward, perche' e' l'unico pezzo che la
    selezione non ha visto. Mescola AUC direzionale e accuratezza bilanciata:
    la prima misura l'ordinamento, la seconda la decisione, e un modello utile
    deve fare bene entrambe.
    """
    wf = summary.get("walk_forward") or {}
    ho = summary.get("holdout") or {}

    def part(block: dict[str, Any]) -> float:
        auc = block.get("auc_directional")
        bal = block.get("balanced_accuracy")
        vals = [v for v in (auc, bal) if isinstance(v, (int, float))]
        return sum(vals) / len(vals) if vals else 0.0

    return 0.35 * part(wf) + 0.65 * part(ho)


def run_forever(store: Store | None = None, log: Log = print) -> None:
    engine = ResearchEngine(store=store, log=log)
    log(f"[ricerca] ciclo attivo, un giro ogni "
        f"{config.RESEARCH_INTERVAL_SECONDS}s")
    try:
        while True:
            engine.run()
            engine.update_shadow_states()
            time.sleep(config.RESEARCH_INTERVAL_SECONDS)
    except KeyboardInterrupt:
        log("[ricerca] arresto")
