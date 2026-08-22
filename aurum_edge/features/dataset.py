"""Il dataset: righe causali, etichette a orizzonte, e la loro contabilita'.

Un dataset finanziario non e' una matrice. E' una matrice **piu' i timestamp**,
perche' senza timestamp non si puo' purgare, non si puo' mettere un embargo e
non si puo' contare quante osservazioni indipendenti ci sono davvero. Quasi
tutte le illusioni nascono da dataset che hanno perso il tempo per strada.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Any, Sequence

from .labels import Outcome, class_distribution


@dataclass
class Dataset:
    names: list[str]
    rows: list[list[float | None]]
    ts: list[int]
    labels: list[str | None]
    prices: list[float]
    outcomes: list[Outcome | None] = field(default_factory=list)
    regimes: list[str] = field(default_factory=list)
    horizon_min: int = 30
    dropped: dict[str, float] = field(default_factory=dict)
    notes: dict[str, Any] = field(default_factory=dict)

    def __len__(self) -> int:
        return len(self.rows)

    # ------------------------------------------------------------- porzioni
    def slice(self, lo: int, hi: int) -> "Dataset":
        return Dataset(
            names=list(self.names),
            rows=self.rows[lo:hi], ts=self.ts[lo:hi],
            labels=self.labels[lo:hi], prices=self.prices[lo:hi],
            outcomes=self.outcomes[lo:hi] if self.outcomes else [],
            regimes=self.regimes[lo:hi] if self.regimes else [],
            horizon_min=self.horizon_min, dropped=dict(self.dropped),
            notes=dict(self.notes))

    def select(self, indices: Sequence[int]) -> "Dataset":
        return Dataset(
            names=list(self.names),
            rows=[self.rows[i] for i in indices],
            ts=[self.ts[i] for i in indices],
            labels=[self.labels[i] for i in indices],
            prices=[self.prices[i] for i in indices],
            outcomes=[self.outcomes[i] for i in indices] if self.outcomes else [],
            regimes=[self.regimes[i] for i in indices] if self.regimes else [],
            horizon_min=self.horizon_min, dropped=dict(self.dropped),
            notes=dict(self.notes))

    def column(self, name: str) -> list[float | None]:
        j = self.names.index(name)
        return [r[j] for r in self.rows]

    # ------------------------------------------------------------ pulizia
    def coverage(self) -> dict[str, float]:
        """Quota di valori presenti per colonna. Serve a scartare, non a decorare."""
        n = len(self.rows)
        if n == 0:
            return {name: 0.0 for name in self.names}
        out: dict[str, float] = {}
        for j, name in enumerate(self.names):
            present = sum(1 for r in self.rows
                          if r[j] is not None and math.isfinite(r[j]))
            out[name] = present / n
        return out

    def drop_sparse(self, min_coverage: float = 0.80) -> "Dataset":
        """Toglie le colonne troppo vuote, e ricorda quali e perche'.

        Sullo storico ricostruito, le colonne di flusso ordini e di libro sono
        vuote quasi ovunque: non esiste un archivio pubblico da cui prenderle.
        Tenerle e riempirle di zeri insegnerebbe al modello che "flusso zero" e'
        lo stato normale del passato e "flusso vero" e' un'anomalia del
        presente — cioe' esattamente il contrario di quello che sono.
        """
        cov = self.coverage()
        keep = [j for j, name in enumerate(self.names)
                if cov[name] >= min_coverage]
        dropped = {name: round(cov[name], 4) for name in self.names
                   if cov[name] < min_coverage}
        return Dataset(
            names=[self.names[j] for j in keep],
            rows=[[r[j] for j in keep] for r in self.rows],
            ts=list(self.ts), labels=list(self.labels), prices=list(self.prices),
            outcomes=list(self.outcomes), regimes=list(self.regimes),
            horizon_min=self.horizon_min,
            dropped={**self.dropped, **dropped}, notes=dict(self.notes))

    def labelled(self) -> "Dataset":
        """Solo le righe con un'etichetta completa."""
        idx = [i for i, lab in enumerate(self.labels)
               if lab is not None
               and (not self.outcomes or self.outcomes[i] is None
                    or self.outcomes[i].complete)]
        return self.select(idx)

    def imputed(self, medians: dict[str, float] | None = None
                ) -> tuple[list[list[float]], dict[str, float]]:
        """Matrice densa: i buchi residui prendono la mediana della colonna.

        Le mediane vanno calcolate SOLO sulle righe di addestramento e poi
        passate qui per il test: calcolarle su tutto il dataset e' una fuga di
        informazione piccola ma reale, perche' la mediana del test contiene il
        futuro.
        """
        if medians is None:
            medians = {}
            for j, name in enumerate(self.names):
                vals = sorted(r[j] for r in self.rows
                              if r[j] is not None and math.isfinite(r[j]))
                medians[name] = vals[len(vals) // 2] if vals else 0.0
        dense: list[list[float]] = []
        for row in self.rows:
            dense.append([
                row[j] if (row[j] is not None and math.isfinite(row[j]))
                else medians.get(name, 0.0)
                for j, name in enumerate(self.names)])
        return dense, medians

    # ------------------------------------------------------------- diagnosi
    def summary(self) -> dict[str, Any]:
        dist = class_distribution(self.labels)
        return {
            "rows": len(self.rows),
            "features": len(self.names),
            "horizon_min": self.horizon_min,
            "from_ms": self.ts[0] if self.ts else None,
            "to_ms": self.ts[-1] if self.ts else None,
            "classes": dist,
            "dropped_features": self.dropped,
            **self.notes,
        }
