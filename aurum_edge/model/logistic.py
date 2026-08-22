"""Regressione logistica multinomiale, pura Python, tre classi.

Perche' un modello lineare e non un bosco di alberi. Non per purismo: per il
campione. Sei mesi di storia a un minuto sembrano 250.000 righe, ma con
etichette a trenta minuti sono circa **8.000 osservazioni indipendenti**. Con
ottomila osservazioni e cinquanta variabili rumorose, un modello ad alta
capacita' non impara il mercato: impara il campione. Un modello lineare
regolarizzato ha meno modi di illudersi, e i suoi coefficienti si possono
leggere e discutere — cosa che, in un progetto il cui scopo e' capire, vale
piu' di qualche punto di accuratezza in addestramento.

Tre scelte importanti:

* **standardizzazione con statistiche del solo addestramento.** Media e
  deviazione si calcolano sul train e si applicano al test. Calcolarle su tutto
  significa far entrare la distribuzione futura nel presente.
* **pesi di classe bilanciati.** Senza, con FLAT al 45% il modello impara che
  dire FLAT e' quasi sempre giusto, ed e' vero — ed e' inutile.
* **selezione delle variabili dentro il fold.** Scegliere le venti feature piu'
  correlate sull'intero dataset e poi validare e' una fuga classica: la
  selezione ha gia' visto il test.
"""

from __future__ import annotations

import json
import math
from dataclasses import dataclass, field
from typing import Any, Sequence

from ..features.dataset import Dataset
from ..util.numeric import softmax

SHORT, FLAT, LONG = "SHORT", "FLAT", "LONG"
CLASSES = (SHORT, FLAT, LONG)


@dataclass
class Standardizer:
    """Media e deviazione per colonna, stimate una volta sola sul train."""

    names: list[str]
    means: list[float]
    stds: list[float]

    @classmethod
    def fit(cls, rows: Sequence[Sequence[float]], names: Sequence[str]
            ) -> "Standardizer":
        n = len(rows)
        k = len(names)
        means = [0.0] * k
        stds = [1.0] * k
        if n == 0:
            return cls(list(names), means, stds)
        for j in range(k):
            col = [float(r[j]) for r in rows]
            m = sum(col) / n
            var = sum((v - m) ** 2 for v in col) / max(1, n - 1)
            means[j] = m
            # Una colonna costante ha deviazione zero: dividerla darebbe
            # infiniti. Resta a 1 e il modello la vedra' sempre a zero, che e'
            # esattamente quanto informa.
            stds[j] = math.sqrt(var) if var > 1e-18 else 1.0
        return cls(list(names), means, stds)

    def apply(self, row: Sequence[float]) -> list[float]:
        return [(float(v) - m) / s
                for v, m, s in zip(row, self.means, self.stds)]

    def to_dict(self) -> dict[str, Any]:
        return {"names": self.names, "means": self.means, "stds": self.stds}

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "Standardizer":
        return cls(list(d["names"]), list(d["means"]), list(d["stds"]))


def select_features(ds: Dataset, dense: Sequence[Sequence[float]],
                    top_k: int = 24) -> list[int]:
    """Screening univariato: le colonne piu' legate all'etichetta direzionale.

    Si usa la correlazione con il segno dell'etichetta (+1 LONG, -1 SHORT, 0
    FLAT). Serve a togliere di mezzo le colonne che portano solo rumore, non a
    scoprire nulla: la scoperta e' compito della validazione, e questo passo
    avviene sempre e solo dentro il fold di addestramento.
    """
    n = len(dense)
    if n == 0 or not ds.names:
        return []
    y = [1.0 if lab == LONG else -1.0 if lab == SHORT else 0.0
         for lab in ds.labels]
    my = sum(y) / n
    dy = math.sqrt(sum((v - my) ** 2 for v in y))
    if dy <= 0:
        return list(range(min(top_k, len(ds.names))))

    scores: list[tuple[float, int]] = []
    for j in range(len(ds.names)):
        col = [row[j] for row in dense]
        mx = sum(col) / n
        dx = math.sqrt(sum((v - mx) ** 2 for v in col))
        if dx <= 0:
            continue
        num = sum((col[i] - mx) * (y[i] - my) for i in range(n))
        scores.append((abs(num / (dx * dy)), j))
    scores.sort(key=lambda x: -x[0])
    return sorted(j for _, j in scores[:top_k])


@dataclass
class LogisticModel:
    """Pesi, standardizzatore, colonne scelte. Tutto quello che serve per predire."""

    feature_names: list[str]
    selected: list[int]
    standardizer: Standardizer
    weights: list[list[float]]        # [classe][feature]
    bias: list[float]                 # [classe]
    classes: tuple[str, ...] = CLASSES
    medians: dict[str, float] = field(default_factory=dict)
    temperature: float = 1.0
    training: dict[str, Any] = field(default_factory=dict)

    # ------------------------------------------------------------- predizione
    def raw_scores(self, dense_row: Sequence[float]) -> list[float]:
        x = self.standardizer.apply([dense_row[j] for j in self.selected])
        out = []
        for c in range(len(self.classes)):
            w = self.weights[c]
            out.append(self.bias[c] + sum(wi * xi for wi, xi in zip(w, x)))
        return out

    def predict_row(self, dense_row: Sequence[float]) -> dict[str, float]:
        """Probabilita' per classe, con la temperatura applicata.

        La temperatura maggiore di 1 appiattisce le probabilita'. Non e' un
        trucco estetico: un modello lineare addestrato con pesi di classe tende
        a essere troppo sicuro, e su questo problema la sicurezza sbagliata
        costa piu' dell'errore.
        """
        scores = self.raw_scores(dense_row)
        t = max(self.temperature, 1e-6)
        probs = softmax([s / t for s in scores])
        return dict(zip(self.classes, probs))

    def predict(self, ds: Dataset) -> list[dict[str, float]]:
        dense, _ = ds.imputed(self.medians or None)
        # Le colonne del dataset devono corrispondere a quelle viste in
        # addestramento: se il dataset ne ha altre, si riallineano per nome.
        if ds.names != self.feature_names:
            index = {name: j for j, name in enumerate(ds.names)}
            remap: list[int | None] = [index.get(name)
                                       for name in self.feature_names]
            dense = [[(row[j] if j is not None else
                       self.medians.get(self.feature_names[k], 0.0))
                      for k, j in enumerate(remap)] for row in dense]
        return [self.predict_row(row) for row in dense]

    # ------------------------------------------------------------ persistenza
    def to_dict(self) -> dict[str, Any]:
        return {
            "feature_names": self.feature_names,
            "selected": self.selected,
            "standardizer": self.standardizer.to_dict(),
            "weights": self.weights,
            "bias": self.bias,
            "classes": list(self.classes),
            "medians": self.medians,
            "temperature": self.temperature,
            "training": self.training,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), separators=(",", ":"))

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "LogisticModel":
        return cls(
            feature_names=list(d["feature_names"]),
            selected=list(d["selected"]),
            standardizer=Standardizer.from_dict(d["standardizer"]),
            weights=[list(w) for w in d["weights"]],
            bias=list(d["bias"]),
            classes=tuple(d.get("classes") or CLASSES),
            medians=dict(d.get("medians") or {}),
            temperature=float(d.get("temperature") or 1.0),
            training=dict(d.get("training") or {}))

    @classmethod
    def from_json(cls, text: str) -> "LogisticModel":
        return cls.from_dict(json.loads(text))

    # ------------------------------------------------------ compatibilita'
    @property
    def selected_names(self) -> list[str]:
        """I nomi delle variabili che hanno davvero un peso."""
        return [self.feature_names[j] for j in self.selected
                if 0 <= j < len(self.feature_names)]

    def missing_features(self, available: Sequence[str]) -> list[str]:
        """Le variabili con un peso che il costruttore attuale non produce piu'.

        Serve contro un guasto silenzioso e sgradevole: se il costruttore di
        feature cambia — una colonna rinominata, una tolta perche' duplicata —
        un modello addestrato prima continua a funzionare senza errori, perche'
        i valori mancanti vengono riempiti con la mediana. Le probabilita'
        cambiano, nessuna eccezione viene sollevata, e il numero sulla
        dashboard e' semplicemente un po' meno vero di prima.

        Un modello che ha perso una delle sue variabili non e' un modello
        leggermente degradato: e' un modello diverso, che nessuno ha validato.
        """
        present = set(available)
        return [name for name in self.selected_names if name not in present]

    # ---------------------------------------------------------- spiegazione
    def contributions(self, dense_row: Sequence[float]) -> list[dict[str, Any]]:
        """Quanto ogni variabile spinge verso LONG contro SHORT, in questo istante.

        E' la differenza dei contributi alle due classi direzionali: e' quello
        che si puo' onestamente chiamare "il motivo" di una previsione in un
        modello lineare. Non e' una causa, e' un peso.
        """
        x = self.standardizer.apply([dense_row[j] for j in self.selected])
        i_long = self.classes.index(LONG)
        i_short = self.classes.index(SHORT)
        out: list[dict[str, Any]] = []
        for k, j in enumerate(self.selected):
            delta = (self.weights[i_long][k] - self.weights[i_short][k]) * x[k]
            out.append({
                "feature": self.feature_names[j],
                "value_z": round(x[k], 3),
                "push": round(delta, 4),
                "direction": LONG if delta > 0 else SHORT if delta < 0 else FLAT,
            })
        out.sort(key=lambda d: -abs(d["push"]))
        return out


def fit(ds: Dataset, *, epochs: int = 90, l2: float = 1.0,
        learning_rate: float = 0.35, top_k: int = 24,
        max_rows: int = 4000, class_weight: str = "balanced",
        seed: int = 3) -> LogisticModel:
    """Addestra con AdaGrad. Deterministico a parita' di dati e di seme.

    AdaGrad e non SGD semplice perche' le feature hanno scale di
    informativita' molto diverse anche dopo la standardizzazione: un passo
    unico o e' troppo grande per le rumorose o troppo piccolo per le utili.
    """
    import random

    work = ds.labelled()
    if len(work) == 0:
        raise ValueError("nessuna riga etichettata su cui addestrare")

    dense_all, medians = work.imputed()

    # Sottocampionamento: righe consecutive al minuto con etichette a trenta
    # minuti sono quasi la stessa riga. Tenerle tutte moltiplica il tempo di
    # calcolo senza aggiungere informazione.
    stride = max(1, len(dense_all) // max_rows)
    idx = list(range(0, len(dense_all), stride))
    dense = [dense_all[i] for i in idx]
    sub = work.select(idx)

    selected = select_features(sub, dense, top_k)
    if not selected:
        selected = list(range(min(top_k, len(work.names))))

    picked = [[row[j] for j in selected] for row in dense]
    names = [work.names[j] for j in selected]
    std = Standardizer.fit(picked, names)
    X = [std.apply(row) for row in picked]
    y = [CLASSES.index(lab) if lab in CLASSES else 1 for lab in sub.labels]

    n = len(X)
    k = len(selected)
    n_classes = len(CLASSES)

    counts = [max(1, y.count(c)) for c in range(n_classes)]
    if class_weight == "balanced":
        weights_c = [n / (n_classes * counts[c]) for c in range(n_classes)]
    else:
        weights_c = [1.0] * n_classes

    W = [[0.0] * k for _ in range(n_classes)]
    b = [0.0] * n_classes
    gW = [[1e-8] * k for _ in range(n_classes)]
    gb = [1e-8] * n_classes

    rng = random.Random(seed)
    order = list(range(n))
    last_loss = float("inf")
    history: list[float] = []

    for epoch in range(epochs):
        rng.shuffle(order)
        loss = 0.0
        for i in order:
            xi = X[i]
            yi = y[i]
            cw = weights_c[yi]
            scores = [b[c] + sum(W[c][j] * xi[j] for j in range(k))
                      for c in range(n_classes)]
            probs = softmax(scores)
            loss -= cw * math.log(max(probs[yi], 1e-12))
            for c in range(n_classes):
                err = cw * (probs[c] - (1.0 if c == yi else 0.0))
                Wc, gWc = W[c], gW[c]
                for j in range(k):
                    g = err * xi[j] + l2 * Wc[j] / n
                    gWc[j] += g * g
                    Wc[j] -= learning_rate * g / math.sqrt(gWc[j])
                gb[c] += err * err
                b[c] -= learning_rate * err / math.sqrt(gb[c])
        loss /= n
        history.append(round(loss, 6))
        # Arresto anticipato: quando la perdita smette di scendere in modo
        # apprezzabile, le epoche successive adattano solo il rumore.
        if last_loss - loss < 1e-5 and epoch >= 20:
            break
        last_loss = loss

    model = LogisticModel(
        feature_names=list(work.names), selected=selected, standardizer=std,
        weights=W, bias=b, medians=medians,
        training={
            "rows_available": len(work),
            "rows_used": n,
            "stride": stride,
            "epochs_run": len(history),
            "final_loss": history[-1] if history else None,
            "l2": l2, "top_k": top_k,
            "class_counts": {CLASSES[c]: counts[c] for c in range(n_classes)},
            "class_weights": {CLASSES[c]: round(weights_c[c], 3)
                              for c in range(n_classes)},
        })
    return model


def calibrate_temperature(model: LogisticModel, ds: Dataset, *,
                          grid: Sequence[float] = (0.6, 0.8, 1.0, 1.2, 1.5,
                                                   1.8, 2.2, 2.8, 3.5, 4.5)
                          ) -> float:
    """Sceglie la temperatura che minimizza la log-loss su dati mai visti.

    Va chiamata su una porzione **separata** dall'addestramento. Tararla sugli
    stessi dati produce una calibrazione perfetta in laboratorio e nessuna
    calibrazione in produzione.
    """
    work = ds.labelled()
    if len(work) < 100:
        return model.temperature

    dense, _ = work.imputed(model.medians or None)
    base_scores = [model.raw_scores(row) for row in dense]
    targets = [CLASSES.index(lab) if lab in CLASSES else 1
               for lab in work.labels]

    best_t, best_loss = model.temperature, float("inf")
    for t in grid:
        total = 0.0
        for scores, yi in zip(base_scores, targets):
            probs = softmax([s / t for s in scores])
            total -= math.log(max(probs[yi], 1e-12))
        loss = total / len(targets)
        if loss < best_loss:
            best_loss, best_t = loss, t
    model.training["calibration_loss"] = round(best_loss, 5)
    model.training["calibration_rows"] = len(work)
    return best_t
