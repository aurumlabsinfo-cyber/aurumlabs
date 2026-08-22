"""Le misure. Ognuna risponde a una domanda che le altre non toccano.

* **accuratezza direzionale**: quante volte la direzione era giusta. Da sola e'
  la piu' ingannevole di tutte, perche' sale semplicemente prevedendo sempre la
  classe piu' frequente.
* **accuratezza bilanciata**: la media delle accuratezze per classe. Un modello
  che dice sempre FLAT ottiene 0.33 qui, contro lo 0.62 dell'accuratezza
  normale. E' la differenza fra sapere e contare.
* **AUC**: la probabilita' che a un caso positivo sia assegnato un punteggio
  piu' alto che a uno negativo. Non dipende dalla soglia, quindi non si puo'
  gonfiare scegliendo bene il taglio.
* **Brier**: errore quadratico sulla probabilita'. Punisce la sicurezza
  sbagliata, che e' il danno vero in questo progetto.
* **calibrazione (ECE)**: quando si dice 70%, succede il 70% delle volte? Se no,
  quel numero non e' una probabilita', e' un'etichetta.
* **MFE e MAE**: il percorso. Una previsione che finisce +40 bps dopo essere
  passata a -60 e' formalmente corretta e praticamente inutilizzabile.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Sequence

from ..util.numeric import clamp, median, quantile, wilson_interval

LONG, SHORT, FLAT = "LONG", "SHORT", "FLAT"
CLASSES = (SHORT, FLAT, LONG)


# --------------------------------------------------------------------------
# Classificazione
# --------------------------------------------------------------------------
def confusion(predicted: Sequence[str], actual: Sequence[str]
              ) -> dict[str, dict[str, int]]:
    matrix = {p: {a: 0 for a in CLASSES} for p in CLASSES}
    for p, a in zip(predicted, actual):
        if p in matrix and a in matrix[p]:
            matrix[p][a] += 1
    return matrix


def accuracy(predicted: Sequence[str], actual: Sequence[str]) -> float | None:
    pairs = [(p, a) for p, a in zip(predicted, actual) if p and a]
    if not pairs:
        return None
    return sum(1 for p, a in pairs if p == a) / len(pairs)


def balanced_accuracy(predicted: Sequence[str], actual: Sequence[str]
                      ) -> float | None:
    """Media delle sensibilita' per classe, sulle sole classi presenti."""
    per_class: dict[str, list[int]] = {c: [0, 0] for c in CLASSES}
    for p, a in zip(predicted, actual):
        if a not in per_class:
            continue
        per_class[a][1] += 1
        if p == a:
            per_class[a][0] += 1
    recalls = [hit / tot for hit, tot in per_class.values() if tot > 0]
    return sum(recalls) / len(recalls) if recalls else None


def directional_accuracy(predicted: Sequence[str], actual: Sequence[str]
                         ) -> dict[str, Any]:
    """L'accuratezza sulle sole previsioni direzionali.

    Serve una definizione precisa, perche' la versione ingenua non e'
    confrontabile con niente. Se si contano come errori anche i casi in cui il
    mercato e' rimasto FLAT, il numero che ne esce **non puo'** raggiungere la
    quota di LONG o SHORT del campione: si sta dividendo per un insieme e
    confrontando con la frequenza in un altro. E' un confronto perso in
    partenza, e fa apparire pessimo anche un predittore corretto.

    Quindi si separano due domande diverse:

    * `accuracy` — **quando il mercato si e' mosso**, la direzione era quella
      giusta? Denominatore: previsioni direzionali con esito direzionale. E'
      questo il numero che si confronta con la base rate.
    * `flat_rate` — quanto spesso il sistema si e' esposto e non e' successo
      niente. Non e' un errore di direzione, e' un errore di tempismo, e va
      misurato a parte perche' si corregge in un altro modo.
    """
    calls = [(p, a) for p, a in zip(predicted, actual)
             if p in (LONG, SHORT) and a in CLASSES]
    if not calls:
        return {"n": 0, "n_calls": 0, "accuracy": None, "ci95": None,
                "flat_rate": None, "hit_including_flat": None}

    resolved = [(p, a) for p, a in calls if a in (LONG, SHORT)]
    hits = sum(1 for p, a in resolved if p == a)
    flats = len(calls) - len(resolved)
    # Il numero indulgente: conta come non-smentita anche il nulla di fatto.
    non_contrary = sum(1 for p, a in calls if a != _opposite(p))
    lo, hi = wilson_interval(hits, len(resolved)) if resolved else (None, None)
    return {
        # `n` e' il campione su cui si misura la direzione: quello risolto.
        "n": len(resolved),
        "n_calls": len(calls),
        "accuracy": round(hits / len(resolved), 4) if resolved else None,
        "ci95": ([round(lo, 4), round(hi, 4)]
                 if lo is not None and hi is not None else None),
        "flat_rate": round(flats / len(calls), 4),
        "hit_including_flat": round(non_contrary / len(calls), 4),
        "long_calls": sum(1 for p, _ in calls if p == LONG),
        "short_calls": sum(1 for p, _ in calls if p == SHORT),
    }


def _opposite(direction: str) -> str:
    return SHORT if direction == LONG else LONG if direction == SHORT else FLAT


# --------------------------------------------------------------------------
# Probabilita'
# --------------------------------------------------------------------------
def auc(scores: Sequence[float], positives: Sequence[int]) -> float | None:
    """AUC con la statistica dei ranghi di Mann-Whitney, pareggi a meta'."""
    pairs = [(s, y) for s, y in zip(scores, positives)
             if s is not None and math.isfinite(s) and y in (0, 1)]
    n_pos = sum(1 for _, y in pairs if y == 1)
    n_neg = len(pairs) - n_pos
    if n_pos == 0 or n_neg == 0:
        return None
    ordered = sorted(pairs, key=lambda x: x[0])
    ranks: list[float] = [0.0] * len(ordered)
    i = 0
    while i < len(ordered):
        j = i
        while j + 1 < len(ordered) and ordered[j + 1][0] == ordered[i][0]:
            j += 1
        avg_rank = (i + j) / 2.0 + 1.0
        for k in range(i, j + 1):
            ranks[k] = avg_rank
        i = j + 1
    rank_sum = sum(r for r, (_, y) in zip(ranks, ordered) if y == 1)
    return (rank_sum - n_pos * (n_pos + 1) / 2.0) / (n_pos * n_neg)


def brier(probs: Sequence[float], outcomes: Sequence[int]) -> float | None:
    pairs = [(p, o) for p, o in zip(probs, outcomes)
             if p is not None and math.isfinite(p) and o in (0, 1)]
    if not pairs:
        return None
    return sum((p - o) ** 2 for p, o in pairs) / len(pairs)


def brier_multiclass(prob_rows: Sequence[dict[str, float]],
                     actual: Sequence[str]) -> float | None:
    """Brier a tre classi. Zero e' perfetto, 2 e' il peggio possibile."""
    pairs = [(p, a) for p, a in zip(prob_rows, actual) if p and a in CLASSES]
    if not pairs:
        return None
    total = 0.0
    for probs, a in pairs:
        for c in CLASSES:
            target = 1.0 if c == a else 0.0
            total += (probs.get(c, 0.0) - target) ** 2
    return total / len(pairs)


def log_loss(probs: Sequence[float], outcomes: Sequence[int]) -> float | None:
    pairs = [(p, o) for p, o in zip(probs, outcomes)
             if p is not None and math.isfinite(p) and o in (0, 1)]
    if not pairs:
        return None
    total = 0.0
    for p, o in pairs:
        q = clamp(p, 1e-9, 1 - 1e-9)
        total += -(o * math.log(q) + (1 - o) * math.log(1 - q))
    return total / len(pairs)


def reliability(probs: Sequence[float], outcomes: Sequence[int],
                bins: int = 10) -> dict[str, Any]:
    """La curva di affidabilita' e l'errore di calibrazione atteso.

    Ogni fascia porta con se' il proprio intervallo di confidenza: una fascia
    con otto casi non dice niente, e va mostrata come tale invece di essere
    mediata dentro un numero unico che sembra preciso.
    """
    pairs = [(p, o) for p, o in zip(probs, outcomes)
             if p is not None and math.isfinite(p) and o in (0, 1)]
    if len(pairs) < 20:
        return {"status": "CAMPIONE INSUFFICIENTE", "n": len(pairs),
                "bins": [], "ece": None, "mce": None}

    buckets: list[list[tuple[float, int]]] = [[] for _ in range(bins)]
    for p, o in pairs:
        idx = min(bins - 1, int(clamp(p, 0.0, 0.9999) * bins))
        buckets[idx].append((p, o))

    rows: list[dict[str, Any]] = []
    ece = 0.0
    mce = 0.0
    total = len(pairs)
    for idx, bucket in enumerate(buckets):
        if not bucket:
            continue
        declared = sum(p for p, _ in bucket) / len(bucket)
        hits = sum(o for _, o in bucket)
        realised = hits / len(bucket)
        lo, hi = wilson_interval(hits, len(bucket))
        gap = abs(declared - realised)
        ece += (len(bucket) / total) * gap
        mce = max(mce, gap)
        rows.append({
            "bin": f"{idx / bins:.0%}-{(idx + 1) / bins:.0%}",
            "n": len(bucket),
            "declared": round(declared, 4),
            "realised": round(realised, 4),
            "ci95": [round(lo, 4), round(hi, 4)],
            "gap": round(declared - realised, 4),
            # Una fascia "significativa" e' una in cui il dichiarato cade fuori
            # dall'intervallo del realizzato: li' la probabilita' e' smentita.
            "contradicted": bool(declared < lo or declared > hi),
        })

    verdict = ("BUONA" if ece <= 0.05 else
               "ACCETTABILE" if ece <= 0.10 else "SCADENTE")
    return {
        "status": "OK", "n": total, "bins": rows,
        "ece": round(ece, 4), "mce": round(mce, 4), "verdict": verdict,
    }


# --------------------------------------------------------------------------
# Percorso
# --------------------------------------------------------------------------
def path_stats(mfe: Sequence[float | None], mae: Sequence[float | None],
               returns: Sequence[float | None],
               time_to_mfe: Sequence[float | None] | None = None
               ) -> dict[str, Any]:
    """Le statistiche del percorso, in punti base."""
    def clean(xs: Sequence[float | None]) -> list[float]:
        return [float(x) for x in xs
                if x is not None and math.isfinite(float(x))]

    m, a, r = clean(mfe), clean(mae), clean(returns)
    t = clean(time_to_mfe or [])
    if not r:
        return {"n": 0}
    return {
        "n": len(r),
        "mean_move_bps": round(sum(r) / len(r), 2),
        "median_move_bps": round(median(r) or 0.0, 2),
        "mean_mfe_bps": round(sum(m) / len(m), 2) if m else None,
        "median_mfe_bps": round(median(m) or 0.0, 2) if m else None,
        "mean_mae_bps": round(sum(a) / len(a), 2) if a else None,
        "median_mae_bps": round(median(a) or 0.0, 2) if a else None,
        "p25_move_bps": round(quantile(r, 0.25) or 0.0, 2),
        "p75_move_bps": round(quantile(r, 0.75) or 0.0, 2),
        "p10_move_bps": round(quantile(r, 0.10) or 0.0, 2),
        "p90_move_bps": round(quantile(r, 0.90) or 0.0, 2),
        # Quanto del massimo favorevole resta alla fine. Sotto 0.3 il movimento
        # c'e' stato ma e' rientrato: una previsione giusta e inservibile.
        "capture_ratio": (round((sum(r) / len(r)) / (sum(m) / len(m)), 3)
                          if m and sum(m) != 0 else None),
        "median_time_to_mfe_min": round(median(t) or 0.0, 1) if t else None,
        "p75_time_to_mfe_min": round(quantile(t, 0.75) or 0.0, 1) if t else None,
    }


@dataclass
class Scorecard:
    """Il referto completo di un predittore su un campione."""

    n: int
    n_independent: int
    base_rate: float | None
    majority_class: str | None
    accuracy: float | None
    balanced_accuracy: float | None
    directional: dict[str, Any]
    auc_directional: float | None
    auc_long: float | None
    auc_short: float | None
    brier: float | None
    calibration: dict[str, Any]
    path: dict[str, Any]
    confusion: dict[str, dict[str, int]]
    notes: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "n": self.n, "n_independent": self.n_independent,
            "base_rate": self.base_rate, "majority_class": self.majority_class,
            "accuracy": self.accuracy,
            "balanced_accuracy": self.balanced_accuracy,
            "directional": self.directional,
            "auc_directional": self.auc_directional,
            "auc_long": self.auc_long, "auc_short": self.auc_short,
            "brier": self.brier, "calibration": self.calibration,
            "path": self.path, "confusion": self.confusion,
            **self.notes,
        }

    @property
    def lift(self) -> float | None:
        """Quanto l'accuratezza direzionale supera il caso, dentro il campione.

        Il riferimento non e' 50%: e' la quota della classe direzionale piu'
        frequente fra i casi effettivamente previsti. Su un campione con il 55%
        di SHORT, indovinare il 56% delle volte non e' un vantaggio.
        """
        acc = self.directional.get("accuracy")
        base = self.notes.get("directional_base_rate")
        if acc is None or base is None:
            return None
        return round(acc - base, 4)


def score(prob_rows: Sequence[dict[str, float]], predicted: Sequence[str],
          actual: Sequence[str], outcomes: Sequence[Any] | None = None,
          n_independent: int | None = None,
          notes: dict[str, Any] | None = None,
          baseline_labels: Sequence[str] | None = None) -> Scorecard:
    """Costruisce il referto completo. Un solo posto in cui si misura.

    `baseline_labels` serve ai predittori che **si astengono**. Un edge parla
    solo quando le sue condizioni valgono, e giudicarlo sulle righe in cui tace
    non misura niente. Ma se si restringe il campione alle sole righe in cui
    parla, anche la base rate si restringe con esso, e il confronto diventa
    tautologico: un edge che dice sempre LONG prende esattamente la quota di
    LONG del proprio sottoinsieme, cioe' un vantaggio di zero per costruzione,
    qualunque cosa faccia.

    Passando qui le etichette dell'**intero** periodo di test, l'accuratezza si
    misura dove l'edge parla e il riferimento resta quello generale. La domanda
    diventa quella giusta: i momenti che questo edge sceglie sono piu'
    prevedibili della media?
    """
    valid = [i for i, a in enumerate(actual) if a in CLASSES]
    actual_v = [actual[i] for i in valid]
    pred_v = [predicted[i] for i in valid]
    probs_v = [prob_rows[i] for i in valid] if prob_rows else []

    counts = {c: actual_v.count(c) for c in CLASSES}
    total = len(actual_v)
    majority = max(counts, key=lambda k: counts[k]) if total else None
    base = counts[majority] / total if total and majority else None

    # Due riferimenti, perche' misurano due abilita' diverse e confonderli e'
    # il modo piu' facile di dichiarare un vantaggio che non c'e'.
    #
    # `dir_base` (incondizionato): sull'intero campione, quanto prenderebbe chi
    # dice sempre la stessa direzione. E' il confronto giusto per un sistema
    # che sceglie ANCHE quando parlare: la scelta del momento e' meta' del
    # mestiere, e va accreditata.
    #
    # `dir_base_calls` (condizionato alle chiamate): sullo stesso sottoinsieme
    # su cui il sistema si e' esposto, quanto prenderebbe chi dice sempre la
    # stessa direzione. Serve a smascherare il caso degenere — un modello che
    # dice solo LONG dentro un campione rialzista sembra bravo rispetto al
    # riferimento incondizionato, ma qui non guadagna niente.
    baseline = (baseline_labels if baseline_labels is not None else actual_v)
    dir_actual = [a for a in baseline if a in (LONG, SHORT)]
    dir_base = (max(dir_actual.count(LONG), dir_actual.count(SHORT)) /
                len(dir_actual)) if dir_actual else None
    resolved_calls = [a for p, a in zip(pred_v, actual_v)
                      if p in (LONG, SHORT) and a in (LONG, SHORT)]
    dir_base_calls = (max(resolved_calls.count(LONG), resolved_calls.count(SHORT))
                      / len(resolved_calls)) if resolved_calls else None

    # AUC "uno contro tutti". Attenzione a come si legge: siccome il resto
    # include FLAT, questa misura premia anche chi sa solo prevedere la
    # VOLATILITA'. Sapere che sta per arrivare un movimento alza insieme
    # P(LONG) e P(SHORT), e questo da solo porta l'AUC sopra 0.5 senza alcuna
    # capacita' direzionale. Verificato su cammini casuali con volatilita' a
    # grappoli: l'AUC uno-contro-tutti si assesta stabilmente intorno a 0.53
    # dove la direzione e' per costruzione imprevedibile. Resta come
    # diagnostica, non come cancello.
    auc_l = auc([p.get(LONG, 0.0) for p in probs_v],
                [1 if a == LONG else 0 for a in actual_v]) if probs_v else None
    auc_s = auc([p.get(SHORT, 0.0) for p in probs_v],
                [1 if a == SHORT else 0 for a in actual_v]) if probs_v else None

    # AUC DIREZIONALE: il numero su cui si decide. Si guardano solo i casi in
    # cui il mercato si e' effettivamente mosso, e si ordina per la quota
    # relativa fra le due probabilita' direzionali. Cosi' il canale della
    # volatilita' e' chiuso: resta solo la domanda "da che parte", che e'
    # quella a cui il prodotto deve rispondere.
    auc_dir = None
    if probs_v:
        pairs_dir = [(p, a) for p, a in zip(probs_v, actual_v)
                     if a in (LONG, SHORT)]
        if pairs_dir:
            ratios = []
            for p, _ in pairs_dir:
                pl, ps = p.get(LONG, 0.0), p.get(SHORT, 0.0)
                total = pl + ps
                ratios.append(pl / total if total > 1e-12 else 0.5)
            auc_dir = auc(ratios, [1 if a == LONG else 0 for _, a in pairs_dir])

    # La calibrazione si misura sulla probabilita' della direzione dichiarata,
    # perche' e' quella che l'utente legge sulla dashboard.
    cal_probs: list[float] = []
    cal_out: list[int] = []
    for p, pred, act in zip(probs_v, pred_v, actual_v):
        if pred not in (LONG, SHORT):
            continue
        cal_probs.append(p.get(pred, 0.0))
        cal_out.append(1 if act == pred else 0)

    path: dict[str, Any] = {"n": 0}
    if outcomes:
        outs = [outcomes[i] for i in valid]
        preds = pred_v
        mfe, mae, rets, tmfe = [], [], [], []
        for o, pred in zip(outs, preds):
            if o is None or pred not in (LONG, SHORT):
                continue
            sign = 1.0 if pred == LONG else -1.0
            rets.append(None if o.return_bps is None else o.return_bps * sign)
            mfe.append(o.signed_mfe(pred))
            mae.append(o.signed_mae(pred))
            tmfe.append(o.time_to_mfe_min if pred == LONG else o.time_to_mae_min)
        path = path_stats(mfe, mae, rets, tmfe)

    return Scorecard(
        n=total,
        n_independent=n_independent if n_independent is not None else total,
        base_rate=round(base, 4) if base is not None else None,
        majority_class=majority,
        accuracy=(round(accuracy(pred_v, actual_v) or 0.0, 4)
                  if total else None),
        balanced_accuracy=(round(balanced_accuracy(pred_v, actual_v) or 0.0, 4)
                           if total else None),
        directional=directional_accuracy(pred_v, actual_v),
        auc_directional=round(auc_dir, 4) if auc_dir is not None else None,
        auc_long=round(auc_l, 4) if auc_l is not None else None,
        auc_short=round(auc_s, 4) if auc_s is not None else None,
        brier=(round(brier_multiclass(probs_v, actual_v) or 0.0, 5)
               if probs_v else None),
        calibration=reliability(cal_probs, cal_out),
        path=path,
        confusion=confusion(pred_v, actual_v),
        notes={
            "class_counts": counts,
            "directional_base_rate": (round(dir_base, 4)
                                      if dir_base is not None else None),
            "directional_base_rate_on_calls": (
                round(dir_base_calls, 4) if dir_base_calls is not None else None),
            **(notes or {}),
        })
