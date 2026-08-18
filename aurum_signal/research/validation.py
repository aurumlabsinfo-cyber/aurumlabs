"""Validazione statistica: il freno che impedisce di crederci troppo presto.

Questo e' il file piu' importante del progetto, e vale la pena dire perche'.

Il sistema genera decine di candidati — feature, soglie, pattern, regole del
booster. Testandoli tutti sugli stessi dati, **qualcosa sembrera' sempre
significativo**: con 40 test indipendenti e soglia 0.05, in media due passano
per puro caso. Senza una correzione, il "laboratorio di ricerca" diventa una
macchina per produrre illusioni convincenti.

Quattro difese, tutte necessarie e nessuna sufficiente da sola:

1. **Purga ed embargo.** Fra addestramento e test si butta via un intervallo
   pari almeno all'orizzonte: senza, il test contiene il futuro
   dell'addestramento e l'accuratezza sale per costruzione.
2. **Campioni indipendenti.** Finestre da 60 secondi campionate ogni secondo si
   sovrappongono per il 98%: contarle come 3.600 osservazioni indipendenti
   all'ora e' il modo piu' comune di ottenere intervalli di confidenza dieci
   volte piu' stretti del vero.
3. **Benjamini-Hochberg.** Controlla la quota di falsi positivi fra i
   sopravvissuti, invece di lasciare che i test multipli scorrano liberi.
4. **Holdout fresco.** Una porzione finale di dati che nessun candidato ha mai
   visto durante la selezione. Se il vantaggio non sopravvive qui, non esiste.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Any, Callable, Sequence

from ..ml.calibration import brier_score, wilson_interval


def binomial_p_value(successes: int, n: int, p0: float = 0.5) -> float:
    """Probabilita' di ottenere almeno tanti successi se p0 fosse vera.

    `p0` NON e' 0.5 quando c'e' un payout: con 0.80 il riferimento e' 55,56%.
    Testare contro la moneta invece che contro il pareggio economico e' un
    errore che fa sembrare vincente una strategia che perde.
    """
    if n <= 0:
        return 1.0
    successes = max(0, min(n, successes))
    # Coda superiore della binomiale, calcolata in modo stabile.
    total = 0.0
    log_p0 = math.log(p0) if p0 > 0 else -1e9
    log_q0 = math.log(1 - p0) if p0 < 1 else -1e9
    for k in range(successes, n + 1):
        log_c = (math.lgamma(n + 1) - math.lgamma(k + 1) - math.lgamma(n - k + 1))
        total += math.exp(log_c + k * log_p0 + (n - k) * log_q0)
    return min(1.0, max(0.0, total))


def benjamini_hochberg(p_values: Sequence[float],
                       alpha: float = 0.05) -> tuple[list[bool], list[float]]:
    """Controlla la quota di falsi positivi fra i risultati dichiarati.

    Ritorna (sopravvive, p corretto) nell'ordine originale.
    """
    n = len(p_values)
    if n == 0:
        return [], []
    order = sorted(range(n), key=lambda i: p_values[i])
    adjusted = [1.0] * n
    survives = [False] * n
    prev = 1.0
    # Si scorre dal p piu' grande al piu' piccolo per rendere il p corretto
    # monotono, come richiede la procedura.
    for rank in range(n, 0, -1):
        i = order[rank - 1]
        adj = min(prev, p_values[i] * n / rank)
        adjusted[i] = adj
        prev = adj
    for i in range(n):
        survives[i] = adjusted[i] <= alpha
    return survives, adjusted


def independent_indices(timestamps: Sequence[int], horizon_ms: int) -> list[int]:
    """Indici distanziati di almeno un orizzonte.

    Trasforma un campione sovrapposto in uno utilizzabile per un test. Il
    prezzo da pagare e' brutale — da 3.600 righe all'ora a 60 — ed e' proprio
    per questo che va pagato: quei 60 sono osservazioni vere, gli altri 3.540
    sono la stessa informazione ripetuta.
    """
    out: list[int] = []
    last = -10 ** 18
    for i, ts in enumerate(timestamps):
        if ts - last >= horizon_ms:
            out.append(i)
            last = ts
    return out


@dataclass
class Fold:
    index: int
    train_rows: int
    test_rows: int
    test_independent: int
    purge_gap_ms: int
    accuracy: float | None = None
    accuracy_independent: float | None = None
    baseline: float | None = None
    brier: float | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "fold": self.index, "train_rows": self.train_rows,
            "test_rows": self.test_rows,
            "test_independent": self.test_independent,
            "purge_gap_ms": self.purge_gap_ms,
            "accuracy": self.accuracy,
            "accuracy_independent": self.accuracy_independent,
            "baseline": self.baseline, "brier": self.brier,
        }


@dataclass
class Dataset:
    """Righe causali con l'etichetta a +orizzonte."""

    rows: list[list[float]]
    y: list[int]
    ts: list[int]
    names: list[str]
    horizon_s: int
    entry: list[float] = field(default_factory=list)
    exit: list[float] = field(default_factory=list)
    notes: dict[str, Any] = field(default_factory=dict)

    def __len__(self) -> int:
        return len(self.rows)

    def slice(self, lo: int, hi: int) -> "Dataset":
        return Dataset(self.rows[lo:hi], self.y[lo:hi], self.ts[lo:hi],
                       list(self.names), self.horizon_s,
                       self.entry[lo:hi] if self.entry else [],
                       self.exit[lo:hi] if self.exit else [], dict(self.notes))


class WalkForwardValidator:
    """Fold espansivi, con purga ed embargo fra addestramento e test."""

    def __init__(self, cfg) -> None:
        self.cfg = cfg

    @property
    def purge_gap_ms(self) -> int:
        """Orizzonte piu' embargo: e' il minimo che rende il test onesto."""
        return int((self.cfg.horizon_seconds + self.cfg.ml_embargo_seconds) * 1000)

    def run(self, ds: Dataset, fit_fn: Callable, predict_fn: Callable,
            folds: int | None = None) -> dict[str, Any]:
        folds = folds or self.cfg.ml_walk_forward_folds
        n = len(ds)
        if n < 200:
            return {"status": "DATI INSUFFICIENTI", "rows": n,
                    "needed": 200,
                    "note": "un walk-forward su poche righe misura il rumore"}

        gap = self.purge_gap_ms
        block = n // (folds + 1)
        results: list[Fold] = []
        all_probs: list[float] = []
        all_true: list[int] = []

        for k in range(folds):
            train_end = block * (k + 1)
            if train_end < 100:
                continue
            # La purga: si scartano le righe entro `gap` dalla fine del train.
            cutoff_ts = ds.ts[train_end - 1] + gap
            test_start = train_end
            while test_start < n and ds.ts[test_start] < cutoff_ts:
                test_start += 1
            test_end = min(n, test_start + block)
            if test_end - test_start < 30:
                continue

            train = ds.slice(0, train_end)
            test = ds.slice(test_start, test_end)
            try:
                model = fit_fn(train)
                probs = predict_fn(model, test)
            except Exception as exc:            # noqa: BLE001
                return {"status": "ERRORE", "detail": f"{type(exc).__name__}: {exc}"}

            correct = [(p > 0.5) == (t == 1) for p, t in zip(probs, test.y)]
            idx = independent_indices(test.ts, self.cfg.horizon_seconds * 1000)
            indep = [correct[i] for i in idx]
            base = max(sum(test.y) / len(test.y), 1 - sum(test.y) / len(test.y))
            results.append(Fold(
                index=k, train_rows=len(train), test_rows=len(test),
                test_independent=len(idx), purge_gap_ms=gap,
                accuracy=round(sum(correct) / len(correct), 4),
                accuracy_independent=(round(sum(indep) / len(indep), 4)
                                      if indep else None),
                baseline=round(base, 4),
                brier=(round(brier_score(probs, test.y), 5)
                       if probs else None)))
            all_probs.extend(probs[i] for i in idx)
            all_true.extend(test.y[i] for i in idx)

        if not results:
            return {"status": "NESSUN FOLD VALIDO",
                    "note": "dopo la purga non restano righe di test sufficienti"}

        wins = sum(1 for p, t in zip(all_probs, all_true) if (p > 0.5) == (t == 1))
        n_ind = len(all_true)
        lo, hi = wilson_interval(wins, n_ind) if n_ind else (0.0, 1.0)
        breakeven = self.cfg.breakeven_win_rate
        p_value = binomial_p_value(wins, n_ind, breakeven) if n_ind else 1.0
        accs = [f.accuracy_independent for f in results
                if f.accuracy_independent is not None]
        consistency = (sum(1 for a in accs if a > breakeven) / len(accs)
                       if accs else 0.0)

        return {
            "status": "COMPLETO",
            "folds": [f.to_dict() for f in results],
            "independent_samples": n_ind,
            "accuracy_independent": round(wins / n_ind, 4) if n_ind else None,
            "ci95": [round(lo, 4), round(hi, 4)],
            "breakeven": round(breakeven, 4),
            "p_value_vs_breakeven": round(p_value, 6),
            "beats_breakeven": bool(lo > breakeven),
            "fold_consistency": round(consistency, 3),
            "brier": (round(brier_score(all_probs, all_true), 5)
                      if all_probs else None),
            "purge_gap_ms": gap,
            "note": ("Il confronto e' contro il PAREGGIO del payout, non contro "
                     "il lancio di una moneta: e' quella la soglia che conta. "
                     "I campioni sono distanziati di un orizzonte: le finestre "
                     "sovrapposte darebbero intervalli molto piu' stretti del vero."),
        }


def leakage_check(ds: Dataset, horizon_ms: int) -> dict[str, Any]:
    """Cerca la fuga di informazione dal futuro.

    Il controllo e' semplice ma coglie l'errore piu' comune: se una colonna e'
    quasi perfettamente correlata con l'etichetta, quasi certamente non e' una
    feature — e' l'etichetta travestita. Un'accuratezza del 99% su questo
    problema non e' un successo: e' una diagnosi.
    """
    if len(ds) < 50:
        return {"status": "DATI INSUFFICIENTI"}
    suspects: list[dict[str, Any]] = []
    n = len(ds)
    mean_y = sum(ds.y) / n
    for j, name in enumerate(ds.names):
        col = [r[j] for r in ds.rows]
        finite = [v for v in col if math.isfinite(v)]
        if len(finite) < n * 0.5:
            continue
        m = sum(finite) / len(finite)
        num = sum((col[i] - m) * (ds.y[i] - mean_y)
                  for i in range(n) if math.isfinite(col[i]))
        den_x = math.sqrt(sum((v - m) ** 2 for v in finite))
        den_y = math.sqrt(sum((v - mean_y) ** 2 for v in ds.y))
        if den_x <= 0 or den_y <= 0:
            continue
        corr = num / (den_x * den_y)
        if abs(corr) > 0.85:
            suspects.append({"feature": name, "correlation": round(corr, 4)})
    # I timestamp devono essere crescenti: un dataset fuori ordine renderebbe
    # ogni split cronologico una finzione.
    monotonic = all(ds.ts[i] <= ds.ts[i + 1] for i in range(len(ds.ts) - 1))
    return {
        "status": "OK" if not suspects and monotonic else "SOSPETTO",
        "suspects": suspects,
        "timestamps_monotonic": monotonic,
        "rows": n,
        "note": ("Una correlazione sopra 0.85 con l'etichetta e' quasi sempre "
                 "una feature che guarda il futuro, non un vantaggio."),
    }


def holdout_split(ds: Dataset, fraction: float = 0.25) -> tuple[Dataset, Dataset]:
    """Taglia una coda FRESCA che la selezione non deve mai vedere."""
    cut = int(len(ds) * (1.0 - fraction))
    return ds.slice(0, cut), ds.slice(cut, len(ds))
