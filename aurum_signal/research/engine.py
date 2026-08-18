"""Il motore di ricerca: impara da tutto, promuove quasi niente.

Studia cinque fonti, e le ultime tre sono quelle che di solito nessuno guarda:

* le operazioni **vinte** e **perse**, per estrarne la firma;
* le decisioni **bloccate** (ombra), per sapere se un filtro protegge o costa;
* i **NO_TRADE**, perche' anche non operare e' una scelta che si puo' sbagliare.

Il percorso per entrare in produzione e' obbligatorio e non ha scorciatoie:

    SCOPERTA -> BACKTEST -> WALK-FORWARD CON PURGA -> HOLDOUT FRESCO
             -> VALIDAZIONE -> OMBRA LIVE -> PROMOZIONE

Ogni passaggio elimina candidati. E' il suo scopo: su dati finanziari, un
processo che promuove spesso e' un processo rotto.
"""

from __future__ import annotations

import json
import math
import time
import uuid
from dataclasses import dataclass, field
from typing import Any

from ..core.signals import DRAW, LOSS, WIN
from ..ml.calibration import (calibration_verdict, reliability_curve,
                              wilson_interval)
from .validation import (Dataset, WalkForwardValidator, benjamini_hochberg,
                         binomial_p_value, holdout_split, leakage_check)

CHAMPION, CHALLENGER, RETIRED = "CHAMPION", "CHALLENGER", "RETIRED"
WINNING, LOSING = "WINNING", "LOSING"


def _bucket(value: float | None, edges: tuple[float, ...]) -> str:
    """Discretizza un valore continuo in una fascia leggibile."""
    if value is None:
        return "n/d"
    for i, e in enumerate(edges):
        if value < e:
            return f"<{e:g}" if i == 0 else f"{edges[i-1]:g}..{e:g}"
    return f">={edges[-1]:g}"


def setup_signature(features: dict[str, Any], regime: str) -> dict[str, str]:
    """La firma di una configurazione di mercato.

    Poche dimensioni, discretizzate grossolanamente: e' una scelta contro il
    sovradattamento. Una firma troppo fine identifica un solo momento della
    storia e il suo "tasso di vittoria" e' l'esito di quel momento, non una
    regolarita'.
    """
    return {
        "regime": regime or "n/d",
        "flow": _bucket(features.get("tick_imbalance_5s"), (-0.3, 0.0, 0.3)),
        "momentum": _bucket(features.get("momentum"), (-1.0, 0.0, 1.0)),
        "vol": _bucket(features.get("move_over_noise"), (1.5, 3.0, 6.0)),
        "session": ("overlap" if features.get("session_overlap")
                    else "london" if features.get("session_london")
                    else "newyork" if features.get("session_newyork")
                    else "asia"),
    }


def signature_id(sig: dict[str, str]) -> str:
    return "|".join(f"{k}={v}" for k, v in sorted(sig.items()))


@dataclass
class Setup:
    setup_id: str
    kind: str
    conditions: dict[str, str]
    samples: int = 0
    wins: int = 0
    losses: int = 0
    draws: int = 0

    @property
    def decided(self) -> int:
        return self.wins + self.losses

    @property
    def win_rate(self) -> float | None:
        return (self.wins / self.decided) if self.decided else None

    def expectancy(self, payout: float) -> float | None:
        wr = self.win_rate
        return None if wr is None else wr * payout - (1 - wr)

    def to_row(self, payout: float, ts: int) -> dict[str, Any]:
        lo, hi = wilson_interval(self.wins, self.decided) if self.decided else (0, 1)
        return {
            "setup_id": self.setup_id, "kind": self.kind, "ts": ts,
            "regime": self.conditions.get("regime"),
            "conditions": json.dumps(self.conditions),
            "samples": self.samples, "wins": self.wins, "losses": self.losses,
            "draws": self.draws,
            "win_rate": self.win_rate, "ci_low": lo, "ci_high": hi,
            "expectancy": self.expectancy(payout),
            "note": None,
        }


class SetupLibrary:
    """Le configurazioni che hanno vinto e quelle che hanno perso.

    Costruita dai dati, non scritta a mano. Un setup entra in libreria solo con
    un campione minimo e con il limite INFERIORE dell'intervallo dalla parte
    giusta: la stima puntuale su venti operazioni non dice niente.
    """

    def __init__(self, cfg) -> None:
        self.cfg = cfg
        self.setups: dict[str, Setup] = {}

    def observe(self, features: dict, regime: str, result: str) -> None:
        sig = setup_signature(features, regime)
        sid = signature_id(sig)
        s = self.setups.get(sid)
        if s is None:
            s = Setup(setup_id=sid, kind=WINNING, conditions=sig)
            self.setups[sid] = s
        s.samples += 1
        if result == WIN:
            s.wins += 1
        elif result == LOSS:
            s.losses += 1
        else:
            s.draws += 1

    def classify(self) -> tuple[list[Setup], list[Setup]]:
        """Separa vincenti e perdenti, con il campione e l'intervallo giusti."""
        breakeven = self.cfg.breakeven_win_rate
        winning: list[Setup] = []
        losing: list[Setup] = []
        for s in self.setups.values():
            if s.decided < self.cfg.setup_min_samples:
                continue
            lo, hi = wilson_interval(s.wins, s.decided)
            if lo > breakeven:
                s.kind = WINNING
                winning.append(s)
            elif hi < breakeven:
                s.kind = LOSING
                losing.append(s)
        winning.sort(key=lambda x: -(x.win_rate or 0))
        losing.sort(key=lambda x: (x.win_rate or 1))
        return winning, losing

    def similar(self, features: dict, regime: str,
                limit: int = 3) -> list[dict[str, Any]]:
        """I casi passati piu' simili a quello presente.

        La somiglianza da sola non basta e il risultato lo dichiara: un setup
        molto simile con quaranta campioni vale meno di uno un po' meno simile
        con quattrocento.
        """
        target = setup_signature(features, regime)
        scored: list[tuple[float, Setup]] = []
        for s in self.setups.values():
            if s.decided < max(20, self.cfg.setup_min_samples // 3):
                continue
            matches = sum(1 for k, v in target.items() if s.conditions.get(k) == v)
            similarity = matches / len(target)
            if similarity >= 0.6:
                scored.append((similarity, s))
        scored.sort(key=lambda x: (-x[0], -(x[1].decided)))
        out = []
        for sim, s in scored[:limit]:
            lo, hi = wilson_interval(s.wins, s.decided)
            out.append({
                "setup_id": s.setup_id, "similarity": round(sim, 3),
                "samples": s.decided, "win_rate": round(s.win_rate or 0, 4),
                "ci95": [round(lo, 4), round(hi, 4)],
                "conditions": s.conditions,
                "reliable": bool(s.decided >= self.cfg.setup_min_samples
                                 and (lo > self.cfg.breakeven_win_rate
                                      or hi < self.cfg.breakeven_win_rate)),
            })
        return out


class ShadowAnalyser:
    """I filtri proteggono o costano?

    Per ogni motivo di blocco confronta il tasso di vittoria che AVREBBERO
    avuto le decisioni scartate con quello delle emesse. Un filtro che scarta
    operazioni vincenti non protegge: toglie. Senza questa misura, allentare o
    stringere una soglia e' tirare a indovinare.
    """

    def __init__(self, cfg, db) -> None:
        self.cfg = cfg
        self.db = db

    def report(self) -> dict[str, Any]:
        rows = self.db.query(
            "SELECT blocking_reason, result FROM shadow_decisions "
            "WHERE result IN ('WIN','LOSS')")
        emitted = self.db.query(
            "SELECT result FROM signals WHERE result IN ('WIN','LOSS')")
        if not rows:
            return {"status": "NESSUN DATO",
                    "note": "nessuna decisione bloccata ha ancora un esito"}
        breakeven = self.cfg.breakeven_win_rate
        em_wins = sum(1 for r in emitted if r["result"] == WIN)
        em_n = len(emitted)
        by_reason: dict[str, list[int]] = {}
        for r in rows:
            b = by_reason.setdefault(r["blocking_reason"] or "?", [0, 0])
            b[1] += 1
            if r["result"] == WIN:
                b[0] += 1
        out = []
        for reason, (wins, n) in sorted(by_reason.items(), key=lambda kv: -kv[1][1]):
            lo, hi = wilson_interval(wins, n)
            wr = wins / n
            out.append({
                "blocker": reason, "blocked": n, "shadow_win_rate": round(wr, 4),
                "ci95": [round(lo, 4), round(hi, 4)],
                "expectancy": round(wr * self.cfg.payout - (1 - wr), 4),
                "costly": bool(lo > breakeven),
                "verdict": ("COSTA: le decisioni scartate battevano il pareggio"
                            if lo > breakeven else
                            "protegge: le scartate perdevano" if hi < breakeven
                            else "indifferente entro l'incertezza"),
            })
        return {
            "status": "OK",
            "emitted_win_rate": round(em_wins / em_n, 4) if em_n else None,
            "emitted_samples": em_n,
            "breakeven": round(breakeven, 4),
            "blockers": out,
            "note": ("Un filtro va giudicato su cosa ha scartato, non su quanto "
                     "spesso scatta."),
        }


class ResearchEngine:
    """Genera candidati, li valida, ne promuove pochissimi.

    Gira su un thread separato dal loop di decisione: un addestramento che
    impiega qualche secondo non deve mai ritardare un segnale con scadenza a
    sessanta secondi.
    """

    def __init__(self, cfg, db) -> None:
        self.cfg = cfg
        self.db = db
        self.validator = WalkForwardValidator(cfg)
        self.library = SetupLibrary(cfg)
        self.shadow = ShadowAnalyser(cfg, db)
        self.champion_id: str | None = None
        #: Il modello effettivamente promosso, pronto per il motore decisionale.
        #: Finche' e' None il motore decide con le sole euristiche — ed e' lo
        #: stato normale: la maggior parte dei giri di ricerca non promuove
        #: niente, ed e' cosi' che deve andare.
        self.champion_model: Any = None
        self.champion_metrics: dict[str, Any] = {}
        self.runs = 0
        self.promotions = 0
        self.rollbacks = 0
        self.last_report: dict[str, Any] | None = None
        self.history: list[dict[str, Any]] = []

    # ------------------------------------------------------------- candidati
    def generate_candidates(self, ds: Dataset) -> list[dict[str, Any]]:
        """Sottoinsiemi di feature come ipotesi da testare.

        Volutamente pochi e leggibili: ogni candidato in piu' e' un'occasione
        in piu' di trovare un vantaggio che non esiste, e la correzione per
        test multipli fa pagare quel prezzo a tutti gli altri.
        """
        groups = {
            "momentum": ("return_5s_bps", "return_15s_bps", "momentum",
                         "acceleration"),
            "microstructure": ("tick_imbalance_1s", "tick_imbalance_5s",
                               "tick_imbalance_15s", "quote_velocity_10s"),
            "volatility": ("realized_vol_5s_bps", "vol_ratio", "move_over_noise",
                           "vol_percentile"),
            "statistical": ("zscore_60s", "autocorr_1", "entropy_60s",
                            "trend_strength"),
            "session": ("session_overlap", "session_london", "session_asia",
                        "seconds_to_next_minute"),
        }
        available = set(ds.names)
        out: list[dict[str, Any]] = []
        for name, feats in groups.items():
            cols = [f for f in feats if f in available]
            if len(cols) >= 2:
                out.append({"name": name, "features": cols})
        # Combinazioni a due gruppi: la piu' semplice forma di interazione.
        names = [c["name"] for c in out]
        for i in range(len(names)):
            for j in range(i + 1, len(names)):
                a = next(c for c in out if c["name"] == names[i])
                b = next(c for c in out if c["name"] == names[j])
                out.append({"name": f"{a['name']}+{b['name']}",
                            "features": a["features"] + b["features"]})
        return out[:24]

    # -------------------------------------------------------------- il giro
    def run_once(self, ds: Dataset) -> dict[str, Any]:
        """Un ciclo completo: scoperta, validazione, holdout, verdetto."""
        started = time.time()
        self.runs += 1
        report: dict[str, Any] = {
            "run": self.runs, "ts": int(started * 1000),
            "rows": len(ds), "status": "COMPLETO",
        }

        if len(ds) < self.cfg.research_min_samples:
            report.update({"status": "DATI INSUFFICIENTI",
                           "needed": self.cfg.research_min_samples,
                           "note": "cercare un vantaggio qui misurerebbe rumore"})
            self.last_report = report
            return report

        # 0. Prima di tutto: c'e' fuga di informazione dal futuro?
        leak = leakage_check(ds, self.cfg.horizon_seconds * 1000)
        report["leakage"] = leak
        if leak.get("status") == "SOSPETTO" and leak.get("suspects"):
            report["status"] = "BLOCCATO: SOSPETTA FUGA DAL FUTURO"
            report["note"] = ("Una o piu' feature sono quasi identiche "
                              "all'etichetta: qualunque risultato sarebbe finto.")
            self.last_report = report
            return report

        # 1. Holdout fresco, messo da parte PRIMA di guardare qualunque cosa.
        discovery, holdout = holdout_split(ds, 0.25)
        report["split"] = {"discovery": len(discovery), "holdout": len(holdout)}

        # 2. Candidati, ognuno validato in walk-forward con purga.
        from ..ml.model import PureLogistic, fit_estimator, predict_proba
        candidates = self.generate_candidates(discovery)
        results: list[dict[str, Any]] = []
        for cand in candidates:
            idxs = [discovery.names.index(f) for f in cand["features"]]
            sub = Dataset([[r[i] for i in idxs] for r in discovery.rows],
                          discovery.y, discovery.ts, cand["features"],
                          discovery.horizon_s)

            def fit(d, _i=idxs):
                return fit_estimator(PureLogistic(epochs=14), d.rows, d.y)

            def pred(m, d):
                return predict_proba(m, d.rows)

            val = self.validator.run(sub, fit, pred, folds=3)
            if val.get("status") != "COMPLETO":
                continue
            results.append({
                "name": cand["name"], "features": cand["features"],
                "accuracy": val.get("accuracy_independent"),
                "samples": val.get("independent_samples"),
                "p_value": val.get("p_value_vs_breakeven", 1.0),
                "ci95": val.get("ci95"),
                "consistency": val.get("fold_consistency"),
            })
        report["candidates_tested"] = len(results)

        if not results:
            report.update({"status": "NESSUN CANDIDATO VALIDABILE",
                           "note": "dopo la purga non restano righe sufficienti"})
            self.last_report = report
            return report

        # 3. Correzione per test multipli.
        survives, adjusted = benjamini_hochberg(
            [r["p_value"] for r in results], self.cfg.research_fdr_alpha)
        for r, s, a in zip(results, survives, adjusted):
            r["p_adjusted"] = round(a, 6)
            r["survives_fdr"] = bool(s)
        fdr_survivors = [r for r in results if r["survives_fdr"]]
        report["fdr_survivors"] = len(fdr_survivors)

        # 4. Holdout fresco: l'ultima parola.
        promoted: list[dict[str, Any]] = []
        for r in fdr_survivors:
            idxs = [holdout.names.index(f) for f in r["features"]
                    if f in holdout.names]
            if len(idxs) != len(r["features"]):
                continue
            train_idx = [discovery.names.index(f) for f in r["features"]]
            train_sub = Dataset([[x[i] for i in train_idx] for x in discovery.rows],
                                discovery.y, discovery.ts, r["features"],
                                discovery.horizon_s)
            hold_sub = Dataset([[x[i] for i in idxs] for x in holdout.rows],
                               holdout.y, holdout.ts, r["features"],
                               holdout.horizon_s)
            model = fit_estimator(PureLogistic(epochs=14), train_sub.rows, train_sub.y)
            probs = predict_proba(model, hold_sub.rows)
            from .validation import independent_indices
            idx = independent_indices(hold_sub.ts, self.cfg.horizon_seconds * 1000)
            if len(idx) < 20:
                # Non e' un dettaglio da nascondere: dice quanti DATI servono.
                # A 60 secondi di orizzonte, venti osservazioni indipendenti
                # nell'holdout richiedono circa un'ora e mezza di mercato in
                # totale. Sotto quella soglia nessun candidato puo' essere
                # promosso, e il motivo va scritto invece di far sparire il
                # candidato in silenzio.
                need_minutes = int(20 * self.cfg.horizon_seconds / 60 / 0.25)
                r["holdout"] = {
                    "status": "CAMPIONE INSUFFICIENTE",
                    "independent": len(idx), "needed": 20,
                    "note": (f"servono ~{need_minutes} minuti di dati in totale "
                             f"perche' l'holdout al 25% contenga 20 osservazioni "
                             f"distanziate di {self.cfg.horizon_seconds}s"),
                }
                continue
            wins = sum(1 for i in idx if (probs[i] > 0.5) == (hold_sub.y[i] == 1))
            n = len(idx)
            lo, hi = wilson_interval(wins, n)
            p = binomial_p_value(wins, n, self.cfg.breakeven_win_rate)
            r["holdout"] = {
                "samples": n, "win_rate": round(wins / n, 4),
                "ci95": [round(lo, 4), round(hi, 4)],
                "p_value": round(p, 6),
                "passes": bool(lo > self.cfg.breakeven_win_rate and p < 0.05),
            }
            if r["holdout"]["passes"]:
                promoted.append(r)

        report["holdout_survivors"] = len(promoted)
        report["candidates"] = sorted(
            results, key=lambda x: x.get("p_adjusted", 1.0))[:10]
        report["promoted"] = promoted

        # 5. Il vincitore diventa un modello utilizzabile, altrimenti tutto
        #    questo percorso non cambia niente: senza questo passo il sistema
        #    studia, conclude, e continua a decidere come prima.
        if promoted:
            best = min(promoted, key=lambda r: r["holdout"]["p_value"])
            report["champion"] = self._promote(best, discovery, holdout, ds)
        report["duration_s"] = round(time.time() - started, 2)
        short = [r for r in fdr_survivors
                 if (r.get("holdout") or {}).get("status") == "CAMPIONE INSUFFICIENTE"]
        if promoted:
            report["verdict"] = (f"{len(promoted)} candidati hanno superato "
                                 f"l'intero percorso (su {len(results)} testati).")
        elif short:
            report["verdict"] = (
                f"{len(fdr_survivors)} candidati hanno superato la correzione per "
                f"test multipli, ma l'holdout non ha abbastanza osservazioni "
                f"indipendenti per giudicarli. "
                + (short[0]["holdout"].get("note") or ""))
        else:
            report["verdict"] = (
                f"Nessun candidato ha superato il percorso su {len(results)} "
                "testati. E' il risultato piu' frequente e non e' un guasto: "
                "significa che in questi dati non c'e' un vantaggio dimostrabile.")

        if self.db is not None:
            self.db.upsert("research_experiments", {
                "experiment_id": uuid.uuid4().hex[:16],
                "ts": report["ts"], "kind": "walk_forward",
                "hypothesis": json.dumps([c["name"] for c in candidates]),
                "samples": len(ds),
                "result": json.dumps(report, default=str)[:200_000],
                "p_value": min((r["p_value"] for r in results), default=1.0),
                "p_value_adjusted": min((r.get("p_adjusted", 1.0) for r in results),
                                        default=1.0),
                "survived_fdr": len(fdr_survivors),
                "survived_holdout": len(promoted),
                "verdict": report["verdict"][:500],
            })
        self.last_report = report
        self.history.append({k: report[k] for k in
                             ("run", "ts", "status", "candidates_tested",
                              "fdr_survivors", "holdout_survivors")
                             if k in report})
        self.history = self.history[-50:]
        return report

    # ------------------------------------------------------------ promozione
    def _promote(self, candidate: dict[str, Any], discovery: Dataset,
                 holdout: Dataset, full: Dataset) -> dict[str, Any]:
        """Trasforma un candidato che ha superato tutto in un modello utile.

        Due dettagli decidono se questo passo e' onesto o finto.

        **La calibrazione si stima FUORI CAMPIONE.** Il modello addestrato
        sulla scoperta predice l'holdout, e Platt si stima su quelle
        previsioni. Calibrare sulle stesse righe dell'addestramento produce
        una curva perfetta che non descrive niente.

        **Il modello finale si riaddestra su tutto.** Il candidato e' stato
        giudicato usando solo la scoperta; una volta superato il giudizio,
        buttare via un quarto dei dati sarebbe uno spreco. Il giudizio resta
        quello dato prima, e resta allegato al modello.
        """
        from ..ml.calibration import brier_score, fit_platt, log_loss
        from ..ml.model import Model, fit_estimator, predict_proba

        feats = candidate["features"]
        try:
            d_idx = [discovery.names.index(f) for f in feats]
            h_idx = [holdout.names.index(f) for f in feats]
            f_idx = [full.names.index(f) for f in feats]
        except ValueError:
            return {"status": "FALLITA", "reason": "feature non allineate"}

        from ..ml.model import PureLogistic
        # a) modello di giudizio: solo scoperta -> previsioni sull'holdout.
        judge = fit_estimator(PureLogistic(epochs=14),
                              [[r[i] for i in d_idx] for r in discovery.rows],
                              discovery.y)
        hold_rows = [[r[i] for i in h_idx] for r in holdout.rows]
        hold_probs = predict_proba(judge, hold_rows)
        calibrator = fit_platt(hold_probs, holdout.y)

        before = {"brier": brier_score(hold_probs, holdout.y),
                  "log_loss": log_loss(hold_probs, holdout.y)}
        cal_probs = [calibrator.transform(p) for p in hold_probs]
        after = {"brier": brier_score(cal_probs, holdout.y),
                 "log_loss": log_loss(cal_probs, holdout.y)}
        # Una calibrazione che PEGGIORA il Brier non si applica: sarebbe
        # rumore stimato su un campione piccolo travestito da correzione.
        keep_cal = after["brier"] <= before["brier"] + 1e-6

        # b) modello finale: tutti i dati, stesso algoritmo, stesse feature.
        final = fit_estimator(PureLogistic(epochs=14),
                              [[r[i] for i in f_idx] for r in full.rows], full.y)

        model_id = f"m_{uuid.uuid4().hex[:10]}"
        model = Model(
            model_id=model_id, algorithm="logistic_pure", feature_names=feats,
            horizon_s=self.cfg.horizon_seconds, estimator=final,
            calibrator=calibrator if keep_cal else None, calibrated=keep_cal,
            n_train=len(full), created_ts=int(time.time() * 1000),
            metrics={"holdout": candidate["holdout"],
                     "brier_before": round(before["brier"], 6),
                     "brier_after": round(after["brier"], 6),
                     "calibration_kept": keep_cal},
            validation={"candidate": candidate["name"],
                        "accuracy_independent": candidate.get("accuracy"),
                        "p_adjusted": candidate.get("p_adjusted"),
                        "walk_forward_samples": candidate.get("samples"),
                        "fold_consistency": candidate.get("consistency")})

        previous = self.champion_id
        self.champion_id = model_id
        self.champion_model = model
        self.champion_metrics = dict(model.metrics)
        self.promotions += 1

        if self.db is not None:
            row = model.to_row()
            row["is_champion"] = 1
            self.db.upsert("model_versions", row)
            self.db.add("model_metrics", {
                "model_id": model_id, "ts": model.created_ts,
                "scope": "holdout_fresco",
                "samples": candidate["holdout"]["samples"],
                "accuracy": candidate["holdout"]["win_rate"],
                "win_rate": candidate["holdout"]["win_rate"],
                "brier": round(after["brier"] if keep_cal else before["brier"], 6),
                "log_loss": round(after["log_loss"] if keep_cal
                                  else before["log_loss"], 6)})
            # La curva di affidabilita', una riga per fascia: e' la forma in
            # cui la calibrazione si puo' davvero leggere ("quando dice 62%,
            # quante volte ha vinto?"), non un solo numero riassuntivo.
            for band in reliability_curve(cal_probs if keep_cal else hold_probs,
                                          holdout.y):
                self.db.add("calibration", {
                    "model_id": model_id, "ts": model.created_ts,
                    "bucket": band["bucket"], "samples": band["samples"],
                    "stated": band["stated"], "realised": band["realised"],
                    "ci_low": band["ci_low"], "ci_high": band["ci_high"]})
            self.db.event("research", "INFO",
                          f"modello {model_id} promosso a campione",
                          {"previous": previous, "candidate": candidate["name"]})

        return {
            "status": "PROMOSSO", "model_id": model_id,
            "previous": previous, "candidate": candidate["name"],
            "features": feats, "n_train": len(full),
            "calibration_applied": keep_cal,
            "brier": round(after["brier"] if keep_cal else before["brier"], 6),
            "holdout": candidate["holdout"],
            "note": ("Il modello entra nella decisione con peso 0.5 se "
                     "calibrato, 0.25 altrimenti: non sostituisce gli agenti, "
                     "si aggiunge a loro."),
        }

    def demote(self, reason: str) -> dict[str, Any]:
        """Ritiro del campione: si torna alle sole euristiche.

        Serve quando il modello promosso comincia a sbagliare in produzione.
        Tornare indietro deve essere semplice e immediato quanto promuovere,
        altrimenti nella pratica non si torna indietro mai.
        """
        old = self.champion_id
        self.champion_id = None
        self.champion_model = None
        self.champion_metrics = {}
        self.rollbacks += 1
        if self.db is not None and old:
            self.db.upsert("model_versions", {"model_id": old, "is_champion": 0})
            self.db.event("research", "WARNING",
                          f"campione {old} ritirato", {"reason": reason})
        return {"status": "RITIRATO", "model_id": old, "reason": reason}

    # ------------------------------------------------------------- study mode
    def study(self, wallet, signals, decision_engine, booster) -> dict[str, Any]:
        """Il referto dopo il fallimento di un ciclo.

        Non serve a "cambiare qualcosa": serve a capire cosa e' andato storto.
        Cambiare parametri perche' il capitale e' finito, senza sapere perche',
        e' il modo piu' rapido di sostituire una strategia mediocre con una
        peggiore che sembra nuova.
        """
        cycle = wallet.current
        breakeven = self.cfg.breakeven_win_rate
        rows = self.db.query(
            "SELECT * FROM signals WHERE cycle_id=? AND result IS NOT NULL",
            (cycle.cycle_id,))
        decided = [r for r in rows if r["result"] in (WIN, LOSS)]
        wins = sum(1 for r in decided if r["result"] == WIN)
        n = len(decided)
        lo, hi = wilson_interval(wins, n) if n else (0.0, 1.0)

        by_regime: dict[str, list[int]] = {}
        by_conf: dict[str, list[int]] = {}
        for r in decided:
            g = by_regime.setdefault(r["regime"] or "?", [0, 0])
            g[1] += 1
            g[0] += 1 if r["result"] == WIN else 0
            b = by_conf.setdefault(_bucket(r["confidence"], (0.58, 0.62, 0.68)), [0, 0])
            b[1] += 1
            b[0] += 1 if r["result"] == WIN else 0

        winning, losing = self.library.classify()
        report = {
            "cycle_id": cycle.cycle_id,
            "trades": cycle.trades, "wins": cycle.wins, "losses": cycle.losses,
            "draws": cycle.draws,
            "win_rate": round(wins / n, 4) if n else None,
            "ci95": [round(lo, 4), round(hi, 4)],
            "breakeven": round(breakeven, 4),
            "p_value_vs_breakeven": (round(binomial_p_value(wins, n, breakeven), 5)
                                     if n else None),
            "by_regime": {k: {"samples": v[1], "win_rate": round(v[0] / v[1], 4)}
                          for k, v in sorted(by_regime.items()) if v[1] >= 3},
            "by_confidence": {k: {"samples": v[1], "win_rate": round(v[0] / v[1], 4)}
                              for k, v in sorted(by_conf.items()) if v[1] >= 3},
            "calibration": decision_engine.calibration_state(),
            "reliability": decision_engine.reliability.snapshot(),
            "shadow": self.shadow.report(),
            "booster": booster.report(breakeven) if booster else None,
            "winning_setups": [s.to_row(self.cfg.payout, int(time.time() * 1000))
                               for s in winning[:5]],
            "losing_setups": [s.to_row(self.cfg.payout, int(time.time() * 1000))
                              for s in losing[:5]],
        }
        # La diagnosi in una frase, che e' quello che si legge davvero.
        if n < 30:
            report["diagnosis"] = (
                f"Solo {n} operazioni decise: il ciclo e' finito troppo presto "
                "per attribuire la perdita a qualcosa di piu' del caso.")
        elif hi < breakeven:
            report["diagnosis"] = (
                f"Tasso di vittoria {wins/n:.1%} con l'intero intervallo sotto "
                f"il pareggio {breakeven:.1%}: la strategia perdeva davvero, "
                "non e' stata sfortuna.")
        elif lo > breakeven:
            report["diagnosis"] = (
                "Il tasso di vittoria batteva il pareggio: il ciclo e' finito "
                "per varianza o per dimensione della puntata, non per l'edge.")
        else:
            report["diagnosis"] = (
                f"Tasso {wins/n:.1%}, intervallo [{lo:.1%}, {hi:.1%}] a cavallo "
                f"del pareggio {breakeven:.1%}: i dati non distinguono questa "
                "strategia dal caso.")
        if self.db is not None:
            self.db.upsert("wallet_cycles", {
                **cycle.to_row(),
                "study": json.dumps(report, default=str)[:200_000]})
        return report

    def status(self) -> dict[str, Any]:
        winning, losing = self.library.classify()
        return {
            "enabled": self.cfg.research_enabled,
            "runs": self.runs,
            "promotions": self.promotions,
            "rollbacks": self.rollbacks,
            "champion": self.champion_id,
            "setups_tracked": len(self.library.setups),
            "winning_setups": len(winning),
            "losing_setups": len(losing),
            "last_report": self.last_report,
            "history": self.history[-10:],
            "policy": ("scoperta -> walk-forward con purga -> correzione per "
                       "test multipli -> holdout fresco -> ombra -> promozione"),
        }
