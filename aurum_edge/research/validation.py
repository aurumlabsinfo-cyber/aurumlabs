"""Validazione temporale. Il freno, e il file piu' importante del progetto.

Un sistema che genera decine di candidati e li prova tutti sugli stessi dati
**trovera' sempre qualcosa**. Non e' un rischio: e' una certezza matematica.
Con quaranta test indipendenti e soglia 0.05, in media due passano per puro
caso, e sono proprio quelli che sembrano piu' convincenti, perche' sono gli
estremi della distribuzione del rumore.

Cinque difese, tutte necessarie, nessuna sufficiente da sola:

1. **Purga.** Fra la fine dell'addestramento e l'inizio del test si scarta un
   intervallo pari almeno all'orizzonte dell'etichetta. Senza, l'ultima riga di
   addestramento ha un'etichetta che vive dentro il periodo di test.
2. **Embargo.** Ancora un intervallo dopo la purga, perche' la memoria del
   mercato non si azzera esattamente all'orizzonte.
3. **Campioni indipendenti.** Righe al minuto con etichette a trenta minuti si
   sovrappongono per il 97%. Contarle tutte come indipendenti restringe gli
   intervalli di confidenza di un fattore cinque e trasforma il rumore in
   significativita'.
4. **Benjamini-Hochberg.** Controlla la quota di falsi positivi fra i
   sopravvissuti a tutti i test fatti, non a uno alla volta.
5. **Holdout fresco.** Una coda finale che la selezione non vede mai. Si guarda
   una volta sola, alla fine. Se un vantaggio non sopravvive qui, non esiste.

Il mestiere di questo file non e' trovare vantaggi: e' rendere caro trovarli.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Any, Callable, Sequence

from .. import config
from ..features.dataset import Dataset
from ..util.numeric import benjamini_hochberg, binomial_tail, wilson_interval
from . import metrics

# Un predittore: dato un dataset di addestramento restituisce un oggetto; dato
# quell'oggetto e un dataset di test restituisce una probabilita' per classe.
FitFn = Callable[[Dataset], Any]
PredictFn = Callable[[Any, Dataset], list[dict[str, float]]]


def independent_indices(timestamps: Sequence[int], horizon_ms: int) -> list[int]:
    """Indici distanziati di almeno un orizzonte.

    Il prezzo e' brutale: da 43.200 righe al mese a 1.440. Ed e' proprio per
    questo che va pagato — quelle 1.440 sono osservazioni, le altre 41.760 sono
    la stessa informazione riscritta.
    """
    out: list[int] = []
    last = -(10 ** 18)
    for i, ts in enumerate(timestamps):
        if ts - last >= horizon_ms:
            out.append(i)
            last = ts
    return out


def leakage_report(ds: Dataset) -> dict[str, Any]:
    """Cerca il futuro dentro le feature. Il controllo e' rozzo e coglie molto.

    Una colonna quasi perfettamente correlata con l'etichetta non e' una
    feature potente: e' l'etichetta travestita. Un'accuratezza del 95% su
    questo problema non e' un successo, e' una diagnosi.
    """
    n = len(ds)
    if n < 50:
        return {"status": "CAMPIONE INSUFFICIENTE", "rows": n}

    y = [1.0 if lab == metrics.LONG else -1.0 if lab == metrics.SHORT else 0.0
         for lab in ds.labels]
    mean_y = sum(y) / n
    var_y = sum((v - mean_y) ** 2 for v in y)
    suspects: list[dict[str, Any]] = []

    if var_y > 0:
        for j, name in enumerate(ds.names):
            col = [r[j] for r in ds.rows]
            valid = [(c, yy) for c, yy in zip(col, y)
                     if c is not None and math.isfinite(c)]
            if len(valid) < n * 0.5:
                continue
            mx = sum(c for c, _ in valid) / len(valid)
            my = sum(yy for _, yy in valid) / len(valid)
            num = sum((c - mx) * (yy - my) for c, yy in valid)
            dx = math.sqrt(sum((c - mx) ** 2 for c, _ in valid))
            dy = math.sqrt(sum((yy - my) ** 2 for _, yy in valid))
            if dx <= 0 or dy <= 0:
                continue
            corr = num / (dx * dy)
            if abs(corr) > 0.80:
                suspects.append({"feature": name, "correlation": round(corr, 4)})

    monotonic = all(ds.ts[i] <= ds.ts[i + 1] for i in range(len(ds.ts) - 1))
    duplicated = len(ds.ts) - len(set(ds.ts))
    status = "OK" if (not suspects and monotonic and duplicated == 0) else "SOSPETTO"
    return {
        "status": status,
        "rows": n,
        "suspects": suspects,
        "timestamps_monotonic": monotonic,
        "duplicate_timestamps": duplicated,
        "note": ("Una correlazione oltre 0.80 con l'etichetta e' quasi sempre "
                 "una feature che guarda il futuro. Timestamp non crescenti "
                 "rendono finto ogni taglio cronologico."),
    }


@dataclass
class Fold:
    index: int
    train_rows: int
    test_rows: int
    test_independent: int
    train_to_ms: int
    test_from_ms: int
    purged_rows: int
    scorecard: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "fold": self.index,
            "train_rows": self.train_rows,
            "test_rows": self.test_rows,
            "test_independent": self.test_independent,
            "purged_rows": self.purged_rows,
            "train_to": self.train_to_ms,
            "test_from": self.test_from_ms,
            "balanced_accuracy": self.scorecard.get("balanced_accuracy"),
            "directional_accuracy": (self.scorecard.get("directional") or {}
                                     ).get("accuracy"),
            "directional_n": (self.scorecard.get("directional") or {}).get("n"),
            "base_rate": self.scorecard.get("base_rate"),
            "auc_directional": self.scorecard.get("auc_directional"),
            "auc_long": self.scorecard.get("auc_long"),
            "auc_short": self.scorecard.get("auc_short"),
            "brier": self.scorecard.get("brier"),
        }


def holdout_split(ds: Dataset, fraction: float | None = None,
                  purge_ms: int | None = None) -> tuple[Dataset, Dataset]:
    """Taglia la coda fresca, con la purga anche qui.

    Dimenticare la purga fra selezione e holdout e' l'errore che rende
    l'holdout una seconda porzione di training travestita da giudice.
    """
    fraction = config.HOLDOUT_FRACTION if fraction is None else fraction
    purge_ms = config.CONFIG.purge_ms if purge_ms is None else purge_ms
    n = len(ds)
    if n == 0:
        return ds, ds
    cut = int(n * (1.0 - fraction))
    if cut <= 0 or cut >= n:
        return ds, ds.slice(n, n)
    boundary_ts = ds.ts[cut]
    train_end = cut
    while train_end > 0 and ds.ts[train_end - 1] > boundary_ts - purge_ms:
        train_end -= 1
    return ds.slice(0, train_end), ds.slice(cut, n)


@dataclass
class FoldSplit:
    """Un taglio addestramento/test gia' materializzato.

    I fold si costruiscono **una volta sola** e si riusano per tutti i
    candidati. Ricostruirli per ognuno costerebbe, con duecento candidati e sei
    fold, milleduecento copie dello stesso archivio: il tempo di calcolo
    diventerebbe la ragione per cui si prova un candidato in meno, ed e' la
    ragione sbagliata.
    """

    index: int
    train: Dataset
    test: Dataset
    train_to_ms: int
    purged_rows: int
    independent: list[int]


def make_folds(ds: Dataset, *, folds: int | None = None,
               purge_ms: int | None = None,
               min_test_rows: int = 60) -> list[FoldSplit]:
    """Fold espansivi con purga ed embargo.

    I fold sono **espansivi** e non scorrevoli: ogni fold addestra su tutto il
    passato disponibile fino a quel punto e prova sul futuro immediato. E' cio'
    che il sistema fara' davvero in produzione, e una validazione che non
    somiglia alla produzione misura un'altra cosa.
    """
    folds = folds or config.WALK_FORWARD_FOLDS
    purge_ms = config.CONFIG.purge_ms if purge_ms is None else purge_ms
    horizon_ms = ds.horizon_min * 60_000
    n = len(ds)
    out: list[FoldSplit] = []
    block = n // (folds + 1)
    if block < min_test_rows:
        return out

    for k in range(folds):
        train_end = block * (k + 1)
        if train_end < 200:
            continue
        # La purga: si scartano le righe di test la cui finestra di etichetta
        # si sovrappone all'ultima riga di addestramento.
        cutoff_ts = ds.ts[train_end - 1] + purge_ms
        test_start = train_end
        while test_start < n and ds.ts[test_start] < cutoff_ts:
            test_start += 1
        purged = test_start - train_end
        test_end = min(n, test_start + block)
        if test_end - test_start < min_test_rows:
            continue
        test = ds.slice(test_start, test_end)
        out.append(FoldSplit(
            index=k, train=ds.slice(0, train_end), test=test,
            train_to_ms=ds.ts[train_end - 1], purged_rows=purged,
            independent=independent_indices(test.ts, horizon_ms)))
    return out


def walk_forward(ds: Dataset, fit: FitFn, predict: PredictFn, *,
                 folds: int | None = None,
                 purge_ms: int | None = None,
                 min_test_rows: int = 60,
                 splits: Sequence[FoldSplit] | None = None,
                 mask_fn: Callable[[Any, Dataset], list[bool]] | None = None
                 ) -> dict[str, Any]:
    """Il cuore della validazione. Passare `splits` riusa fold gia' costruiti.

    `mask_fn` serve ai predittori che si astengono. Un edge parla solo quando
    le sue condizioni valgono: giudicarlo anche sulle righe in cui tace
    diluisce il suo risultato dentro migliaia di non-risposte e non misura
    nulla. Con la maschera, l'accuratezza si calcola dove parla, mentre il
    riferimento resta la base rate dell'intero periodo di test — cosi' la
    domanda e' "i momenti che sceglie sono piu' prevedibili della media?"
    invece della domanda tautologica "dentro i momenti che sceglie, la classe
    piu' frequente e' la classe piu' frequente?".
    """
    purge_ms = config.CONFIG.purge_ms if purge_ms is None else purge_ms
    n = len(ds)

    if n < config.MIN_ROWS_FOR_VALIDATION:
        return {
            "status": "DATI INSUFFICIENTI",
            "rows": n, "needed": config.MIN_ROWS_FOR_VALIDATION,
            "note": ("Un walk-forward su poche righe non misura un vantaggio: "
                     "misura il rumore, e lo fa sembrare stabile."),
        }

    if splits is None:
        splits = make_folds(ds, folds=folds, purge_ms=purge_ms,
                            min_test_rows=min_test_rows)
    if not splits:
        return {"status": "DATI INSUFFICIENTI", "rows": n,
                "note": ("Con i fold richiesti ogni blocco di test resterebbe "
                         "sotto il minimo utilizzabile.")}

    results: list[Fold] = []
    all_probs: list[dict[str, float]] = []
    all_pred: list[str] = []
    all_actual: list[str] = []
    all_outcomes: list[Any] = []
    all_ts: list[int] = []
    all_baseline: list[str] = []

    for split in splits:
        k = split.index
        train, test = split.train, split.test
        purged = split.purged_rows
        try:
            model = fit(train)
            probs = predict(model, test)
        except Exception as exc:                                # noqa: BLE001
            return {"status": "ERRORE",
                    "detail": f"{type(exc).__name__}: {exc}", "fold": k}

        preds = [_argmax(p) for p in probs]
        idx = split.independent
        baseline = [test.labels[i] or "" for i in idx]
        if mask_fn is not None:
            mask = mask_fn(model, test)
            spoken = [i for i in idx if i < len(mask) and mask[i]]
            if len(spoken) < 5:
                # Un edge che nel test non si attiva quasi mai non ha prodotto
                # un risultato debole: non ha prodotto un risultato.
                continue
            idx = spoken
        card = metrics.score([probs[i] for i in idx],
                             [preds[i] for i in idx],
                             [test.labels[i] or "" for i in idx],
                             outcomes=([test.outcomes[i] for i in idx]
                                       if test.outcomes else None),
                             n_independent=len(idx),
                             baseline_labels=(baseline if mask_fn is not None
                                              else None))

        results.append(Fold(
            index=k, train_rows=len(train), test_rows=len(test),
            test_independent=len(idx),
            train_to_ms=split.train_to_ms, test_from_ms=test.ts[0],
            purged_rows=purged, scorecard=card.to_dict()))

        for i in idx:
            all_probs.append(probs[i])
            all_pred.append(preds[i])
            all_actual.append(test.labels[i] or "")
            all_ts.append(test.ts[i])
            if test.outcomes:
                all_outcomes.append(test.outcomes[i])
        all_baseline.extend(baseline)

    if not results:
        return {"status": "NESSUN FOLD VALIDO",
                "note": ("Dopo la purga non restano righe di test sufficienti. "
                         "Serve piu' storia, non meno purga.")}

    overall = metrics.score(all_probs, all_pred, all_actual,
                            outcomes=all_outcomes or None,
                            n_independent=len(all_actual),
                            baseline_labels=(all_baseline if mask_fn is not None
                                             else None))

    # Coerenza fra i fold: quanti superano la propria base rate. Un vantaggio
    # che vive in un fold su sei non e' instabile, e' assente.
    consistent = 0
    counted = 0
    for f in results:
        acc = (f.scorecard.get("directional") or {}).get("accuracy")
        base = f.scorecard.get("directional_base_rate")
        if acc is None or base is None:
            continue
        counted += 1
        if acc > base:
            consistent += 1
    consistency = consistent / counted if counted else None

    dir_stats = overall.directional
    dir_n = dir_stats.get("n") or 0
    dir_acc = dir_stats.get("accuracy")
    dir_base = overall.notes.get("directional_base_rate")
    p_value = None
    if dir_n and dir_acc is not None and dir_base is not None:
        p_value = binomial_tail(int(round(dir_acc * dir_n)), dir_n, dir_base)

    return {
        "status": "COMPLETO",
        "folds": [f.to_dict() for f in results],
        "rows": n,
        "independent_samples": len(all_actual),
        "abstaining": mask_fn is not None,
        "baseline_samples": len(all_baseline) if mask_fn is not None else None,
        "purge_ms": purge_ms,
        "horizon_min": ds.horizon_min,
        "overall": overall.to_dict(),
        "fold_consistency": (round(consistency, 3)
                             if consistency is not None else None),
        "p_value_vs_base": round(p_value, 6) if p_value is not None else None,
        "note": ("Il confronto e' contro la classe piu' frequente del campione, "
                 "non contro il 50%. I campioni sono distanziati di un "
                 "orizzonte: contare le finestre sovrapposte darebbe intervalli "
                 "molto piu' stretti del vero."),
    }


def _argmax(probs: dict[str, float]) -> str:
    if not probs:
        return metrics.FLAT
    return max(probs, key=lambda k: probs[k])


@dataclass
class Verdict:
    """L'esito di un candidato dopo tutte le difese."""

    passed: bool
    reasons: list[str]
    checks: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {"passed": self.passed, "reasons": self.reasons,
                "checks": self.checks}


def judge(walk: dict[str, Any], *, min_independent: int | None = None,
          min_balanced: float | None = None, min_auc: float | None = None,
          min_lift: float | None = None, min_consistency: float | None = None,
          max_ece: float | None = None,
          abstaining: bool | None = None) -> Verdict:
    """Applica le soglie. Ogni "no" viene spiegato, perche' e' il no che informa.

    `abstaining` distingue un modello da un edge, e la distinzione non e' una
    concessione: tre di questi cancelli sono **matematicamente impossibili** da
    superare per una regola condizionale, e lasciarli attivi non sarebbe
    severita' ma un cancello murato.

    * *accuratezza bilanciata* — un edge, quando si attiva, dichiara sempre la
      stessa classe. Un predittore costante prende esattamente 1/3, sempre, per
      costruzione. Misurarlo non dice niente sull'edge.
    * *AUC* — con probabilita' costanti sulle righe attivate non c'e' nessun
      ordinamento da valutare: l'AUC vale 0.5 per definizione.
    * *chiamate nei due versi* — un edge afferma una direzione sola. Chiedergli
      di dirne due e' chiedergli di non essere un edge.

    Al loro posto restano i cancelli che per un edge hanno senso, e sono
    esattamente quelli che contano: campione indipendente sufficiente, vantaggio
    sulla base rate **dell'intero periodo** (non del proprio sottoinsieme),
    limite inferiore dell'intervallo sopra quella base, coerenza fra i fold,
    calibrazione.
    """
    min_independent = (config.MIN_INDEPENDENT_SAMPLES if min_independent is None
                       else min_independent)
    min_balanced = (config.EDGE_MIN_BALANCED_ACC if min_balanced is None
                    else min_balanced)
    min_auc = config.EDGE_MIN_AUC if min_auc is None else min_auc
    min_lift = config.EDGE_MIN_LIFT_OVER_BASE if min_lift is None else min_lift
    min_consistency = (config.EDGE_MIN_FOLD_CONSISTENCY if min_consistency is None
                       else min_consistency)
    max_ece = config.EDGE_MAX_ECE if max_ece is None else max_ece
    if abstaining is None:
        abstaining = bool(walk.get("abstaining"))

    checks: dict[str, Any] = {}
    checks["abstaining"] = abstaining
    reasons: list[str] = []

    if walk.get("status") != "COMPLETO":
        return Verdict(False, [f"validazione non completata: {walk.get('status')}"
                               f" — {walk.get('note') or walk.get('detail') or ''}"],
                       {"status": walk.get("status")})

    overall = walk.get("overall") or {}
    dir_stats = overall.get("directional") or {}
    n_ind = dir_stats.get("n") or 0
    checks["independent_directional_samples"] = n_ind
    if n_ind < min_independent:
        reasons.append(
            f"solo {n_ind} previsioni direzionali indipendenti, ne servono "
            f"{min_independent}: sotto questa soglia l'intervallo di confidenza "
            "e' piu' largo del vantaggio che si vorrebbe misurare")

    bal = overall.get("balanced_accuracy")
    checks["balanced_accuracy"] = bal
    if not abstaining and (bal is None or bal < min_balanced):
        reasons.append(
            f"accuratezza bilanciata {bal} sotto {min_balanced}: il modello non "
            "distingue le classi meglio di quanto le conti")

    acc = dir_stats.get("accuracy")
    base = overall.get("directional_base_rate")
    base_calls = overall.get("directional_base_rate_on_calls")
    ci = dir_stats.get("ci95") or [None, None]
    checks["directional_accuracy"] = acc
    checks["directional_base_rate"] = base
    checks["directional_base_rate_on_calls"] = base_calls
    checks["flat_rate"] = dir_stats.get("flat_rate")
    checks["ci95"] = ci
    if acc is not None and base is not None:
        checks["lift"] = round(acc - base, 4)
        if acc - base < min_lift:
            reasons.append(
                f"vantaggio {acc - base:+.4f} sulla base rate {base}: sotto "
                f"{min_lift} non e' distinguibile dal caso")
        if ci[0] is not None and ci[0] <= base:
            reasons.append(
                f"il limite inferiore dell'intervallo ({ci[0]}) non supera la "
                f"base rate ({base}): la stima puntuale non basta")
    else:
        reasons.append("accuratezza direzionale non calcolabile")

    # Il caso degenere: dire sempre la stessa cosa dentro un campione
    # sbilanciato. Rispetto al riferimento incondizionato sembra bravura; sul
    # proprio stesso sottoinsieme non guadagna niente.
    if not abstaining and acc is not None and base_calls is not None:
        checks["lift_on_calls"] = round(acc - base_calls, 4)
        if acc - base_calls < min_lift / 2.0:
            reasons.append(
                f"sulle stesse chiamate, dire sempre una sola direzione "
                f"prenderebbe {base_calls} contro {acc}: il vantaggio viene "
                "dallo sbilanciamento del campione, non dalla previsione")

    longs = dir_stats.get("long_calls") or 0
    shorts = dir_stats.get("short_calls") or 0
    total_calls = longs + shorts
    checks["call_balance"] = (round(min(longs, shorts) / total_calls, 3)
                              if total_calls else None)
    if not abstaining and total_calls and min(longs, shorts) / total_calls < 0.10:
        reasons.append(
            f"le chiamate sono quasi tutte in un verso ({longs} LONG contro "
            f"{shorts} SHORT): un predittore che non cambia mai idea non e' "
            "distinguibile da una scommessa sulla direzione del periodo")

    # Il cancello e' l'AUC DIREZIONALE, non quella uno-contro-tutti. La
    # seconda si puo' superare sapendo prevedere solo la volatilita': se sta
    # per arrivare un movimento, P(LONG) e P(SHORT) salgono insieme e i casi
    # FLAT restano in fondo alla graduatoria, il che basta a portarla sopra
    # 0.5 senza sapere niente della direzione. Misurato su cammini casuali,
    # dove per costruzione non c'e' direzione da prevedere, l'uno-contro-tutti
    # si assesta stabilmente intorno a 0.53.
    auc_dir = overall.get("auc_directional")
    checks["auc_directional"] = auc_dir
    checks["auc_one_vs_rest"] = [overall.get("auc_long"), overall.get("auc_short")]
    if not abstaining and (auc_dir is None or auc_dir < min_auc):
        reasons.append(
            f"AUC direzionale {auc_dir} sotto {min_auc}: sui casi in cui il "
            "mercato si e' mosso, il punteggio non ordina il verso meglio di "
            "una moneta")

    cons = walk.get("fold_consistency")
    checks["fold_consistency"] = cons
    if cons is None or cons < min_consistency:
        reasons.append(
            f"coerenza fra i fold {cons} sotto {min_consistency}: il vantaggio "
            "vive in pochi periodi e sparisce negli altri")

    cal = overall.get("calibration") or {}
    ece = cal.get("ece")
    checks["ece"] = ece
    if ece is not None and ece > max_ece:
        reasons.append(
            f"errore di calibrazione {ece} oltre {max_ece}: le probabilita' "
            "dichiarate non corrispondono alle frequenze realizzate")

    p = walk.get("p_value_vs_base")
    checks["p_value_vs_base"] = p

    return Verdict(passed=not reasons, reasons=reasons, checks=checks)


def apply_fdr(verdicts: dict[str, Verdict], alpha: float | None = None
              ) -> dict[str, Any]:
    """Benjamini-Hochberg sui candidati che hanno superato le soglie.

    Sopravvivere alle soglie e' necessario ma non basta: se si sono provati
    quaranta pattern, alcuni dei sopravvissuti sono sopravvissuti per caso.
    Questa correzione stima quanti, e li elimina.
    """
    alpha = config.FDR_ALPHA if alpha is None else alpha
    ids = [k for k, v in verdicts.items() if v.passed]
    if not ids:
        return {"tested": len(verdicts), "candidates": 0, "survivors": [],
                "alpha": alpha,
                "note": "nessun candidato ha superato le soglie di base"}

    p_values = [verdicts[k].checks.get("p_value_vs_base") or 1.0 for k in ids]
    survives, adjusted = benjamini_hochberg(p_values, alpha)
    survivors = [k for k, ok in zip(ids, survives) if ok]
    for k, adj, ok in zip(ids, adjusted, survives):
        verdicts[k].checks["p_adjusted"] = round(adj, 6)
        if not ok:
            verdicts[k].passed = False
            verdicts[k].reasons.append(
                f"eliminato dalla correzione per test multipli: p corretto "
                f"{adj:.4f} oltre {alpha}. Con {len(ids)} candidati provati "
                "sugli stessi dati, un risultato come questo si ottiene per caso")
    return {
        "tested": len(verdicts), "candidates": len(ids),
        "survivors": survivors, "alpha": alpha,
        "adjusted": {k: round(a, 6) for k, a in zip(ids, adjusted)},
    }


def sample_power(n_independent: int, base_rate: float,
                 target_lift: float = 0.05) -> dict[str, Any]:
    """Quanto vantaggio si riesce a vedere con il campione che si ha.

    Il numero piu' onesto che una dashboard di ricerca possa mostrare: dice se
    "nessun edge trovato" significa "non c'e'" oppure "non si vedrebbe
    comunque".
    """
    if n_independent <= 0:
        return {"n": 0, "detectable_lift": None,
                "note": "nessuna osservazione indipendente"}
    p = min(max(base_rate, 0.01), 0.99)
    se = math.sqrt(p * (1 - p) / n_independent)
    detectable = 1.96 * se
    lo, hi = wilson_interval(int(round(p * n_independent)), n_independent)
    return {
        "n": n_independent,
        "base_rate": round(p, 4),
        "standard_error": round(se, 5),
        "detectable_lift": round(detectable, 4),
        "ci_width": round(hi - lo, 4),
        "enough_for_target": bool(detectable <= target_lift),
        "note": (
            f"Con {n_independent} osservazioni indipendenti si distingue dal "
            f"caso solo un vantaggio di almeno {detectable:.1%}. Un vantaggio "
            f"reale del {target_lift:.0%} " +
            ("sarebbe visibile." if detectable <= target_lift else
             "resterebbe invisibile: servono piu' dati, non piu' modelli.")),
    }
