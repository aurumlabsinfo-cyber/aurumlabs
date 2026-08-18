"""Calibrazione: rendere la confidenza un numero verificabile.

Il requisito e' preciso: se il sistema mostra 70%, si deve poter controllare
quanto spesso quel gruppo ha davvero vinto. Senza calibrazione, "70%" e' solo
l'uscita di una formula — un numero che sembra una probabilita' senza esserlo.

Due metodi, entrambi senza dipendenze:

* **Platt**: una logistica su una variabile. Poche righe bastano, ma assume
  che la distorsione abbia una forma sigmoidale.
* **Isotonica**: monotona a scalini, non assume una forma. Serve piu' campione
  ed e' quella giusta quando ce n'e' abbastanza.

E poi la misura che conta piu' delle due: la **curva di affidabilita'**, cioe'
il confronto fra dichiarato e realizzato per fascia, con l'intervallo di
confidenza accanto. E' l'unico modo di dire "questa confidenza vale qualcosa"
senza chiedere di crederci.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Sequence


def wilson_interval(successes: int, n: int, z: float = 1.96) -> tuple[float, float]:
    """Intervallo di Wilson: regge anche con pochi campioni e p vicino a 0 o 1."""
    if n <= 0:
        return (0.0, 1.0)
    p = successes / n
    d = 1.0 + z * z / n
    centre = (p + z * z / (2 * n)) / d
    half = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / d
    return (max(0.0, centre - half), min(1.0, centre + half))


def brier_score(probs: Sequence[float], outcomes: Sequence[int]) -> float | None:
    """Errore quadratico medio della probabilita'. Piu' basso e' meglio.

    A differenza dell'accuratezza, punisce la sicurezza sbagliata: dire 90% e
    perdere costa molto piu' che dire 55% e perdere.
    """
    if not probs or len(probs) != len(outcomes):
        return None
    return sum((p - o) ** 2 for p, o in zip(probs, outcomes)) / len(probs)


def log_loss(probs: Sequence[float], outcomes: Sequence[int]) -> float | None:
    if not probs or len(probs) != len(outcomes):
        return None
    total = 0.0
    for p, o in zip(probs, outcomes):
        q = min(max(p, 1e-9), 1 - 1e-9)
        total += -(o * math.log(q) + (1 - o) * math.log(1 - q))
    return total / len(probs)


@dataclass
class Calibrator:
    """Trasforma una probabilita' grezza in una calibrata."""

    method: str = "platt"
    a: float = 1.0
    b: float = 0.0
    #: Per l'isotonica: punti (x, y) crescenti.
    knots: list[tuple[float, float]] | None = None
    samples: int = 0

    def transform(self, p: float) -> float:
        p = min(max(p, 1e-6), 1 - 1e-6)
        if self.method == "isotonic" and self.knots:
            return self._interpolate(p)
        logit = math.log(p / (1 - p))
        z = self.a * logit + self.b
        return 1.0 / (1.0 + math.exp(-max(-35.0, min(35.0, z))))

    def _interpolate(self, p: float) -> float:
        ks = self.knots or []
        if not ks:
            return p
        if p <= ks[0][0]:
            return ks[0][1]
        if p >= ks[-1][0]:
            return ks[-1][1]
        for i in range(1, len(ks)):
            x0, y0 = ks[i - 1]
            x1, y1 = ks[i]
            if p <= x1:
                if x1 == x0:
                    return y1
                return y0 + (y1 - y0) * (p - x0) / (x1 - x0)
        return ks[-1][1]

    def to_dict(self) -> dict[str, Any]:
        return {"method": self.method, "a": self.a, "b": self.b,
                "knots": self.knots, "samples": self.samples}

    @classmethod
    def from_dict(cls, d: dict) -> "Calibrator":
        return cls(method=d.get("method", "platt"), a=float(d.get("a", 1.0)),
                   b=float(d.get("b", 0.0)), knots=d.get("knots"),
                   samples=int(d.get("samples", 0)))


def fit_platt(probs: Sequence[float], outcomes: Sequence[int],
              epochs: int = 200, lr: float = 0.05) -> Calibrator:
    """Una logistica sui logit del modello: due parametri, poche righe."""
    logits = [math.log(min(max(p, 1e-6), 1 - 1e-6) /
                       (1 - min(max(p, 1e-6), 1 - 1e-6))) for p in probs]
    a, b = 1.0, 0.0
    n = len(logits)
    if n < 20:
        return Calibrator("platt", 1.0, 0.0, samples=n)
    for _ in range(epochs):
        ga = gb = 0.0
        for z, o in zip(logits, outcomes):
            p = 1.0 / (1.0 + math.exp(-max(-35.0, min(35.0, a * z + b))))
            err = p - o
            ga += err * z
            gb += err
        a -= lr * ga / n
        b -= lr * gb / n
    return Calibrator("platt", a, b, samples=n)


def fit_isotonic(probs: Sequence[float], outcomes: Sequence[int],
                 min_samples: int = 300) -> Calibrator | None:
    """Regressione isotonica con l'algoritmo pool-adjacent-violators.

    Non assume nessuna forma della distorsione: si limita a imporre che a
    probabilita' dichiarate piu' alte corrispondano frequenze realizzate non
    inferiori. E' l'unica ipotesi che si puo' davvero difendere.
    """
    if len(probs) < min_samples:
        return None
    pairs = sorted(zip(probs, outcomes))
    xs = [p for p, _ in pairs]
    ys = [float(o) for _, o in pairs]
    weights = [1.0] * len(ys)
    # PAVA: finche' due blocchi adiacenti violano la monotonia, li fonde.
    i = 0
    while i < len(ys) - 1:
        if ys[i] <= ys[i + 1]:
            i += 1
            continue
        total_w = weights[i] + weights[i + 1]
        merged = (ys[i] * weights[i] + ys[i + 1] * weights[i + 1]) / total_w
        ys[i] = merged
        weights[i] = total_w
        xs[i] = (xs[i] * weights[i] + xs[i + 1]) / (weights[i] + 1)
        del ys[i + 1], weights[i + 1], xs[i + 1]
        if i > 0:
            i -= 1
    # Riduce a un numero gestibile di nodi.
    step = max(1, len(xs) // 40)
    knots = [(xs[i], ys[i]) for i in range(0, len(xs), step)]
    if knots[-1][0] < xs[-1]:
        knots.append((xs[-1], ys[-1]))
    return Calibrator("isotonic", knots=knots, samples=len(probs))


def reliability_curve(probs: Sequence[float], outcomes: Sequence[int],
                      buckets: int = 6) -> list[dict[str, Any]]:
    """Dichiarato contro realizzato, fascia per fascia, con l'intervallo.

    Se la frequenza realizzata sta sistematicamente sotto quella dichiarata, il
    sistema e' sovrasicuro e i suoi numeri non sono probabilita'. Questa
    tabella e' il modo di accorgersene senza doverci credere sulla parola.
    """
    if not probs:
        return []
    lo, hi = 0.5, 1.0                 # si guarda la CONFIDENZA, non la direzione
    edges = [lo + i * (hi - lo) / buckets for i in range(buckets + 1)]
    out: list[dict[str, Any]] = []
    for a, b in zip(edges, edges[1:]):
        sel = [(p, o) for p, o in zip(probs, outcomes)
               if a <= max(p, 1 - p) < b + 1e-9]
        if not sel:
            continue
        n = len(sel)
        # "Vinto" significa: la direzione indicata dalla probabilita' era giusta.
        wins = sum(1 for p, o in sel if (p > 0.5) == (o == 1))
        ci_lo, ci_hi = wilson_interval(wins, n)
        out.append({
            "bucket": f"{a:.2f}-{b:.2f}",
            "samples": n,
            "stated": round(sum(max(p, 1 - p) for p, _ in sel) / n, 4),
            "realised": round(wins / n, 4),
            "ci_low": round(ci_lo, 4), "ci_high": round(ci_hi, 4),
        })
    return out


def calibration_verdict(curve: list[dict[str, Any]],
                        breakeven: float) -> dict[str, Any]:
    """Un giudizio in una riga su cosa vale la confidenza mostrata."""
    if not curve:
        return {"status": "NESSUN DATO",
                "note": "servono esiti per dire se la confidenza significhi qualcosa"}
    total = sum(b["samples"] for b in curve)
    gap = sum((b["realised"] - b["stated"]) * b["samples"] for b in curve) / total
    above = [b for b in curve if b["ci_low"] > breakeven]
    return {
        "status": "OK",
        "samples": total,
        "mean_gap": round(gap, 4),
        "overconfident": gap < -0.03,
        "buckets_above_breakeven": len(above),
        "verdict": (
            "La confidenza dichiarata e' sistematicamente piu' alta della "
            "frequenza realizzata: questi numeri non sono probabilita'."
            if gap < -0.03 else
            "Nessuna fascia supera il pareggio con il limite inferiore "
            "dell'intervallo: non c'e' ancora prova di un vantaggio."
            if not above else
            f"{len(above)} fasce battono il pareggio anche al limite "
            "inferiore dell'intervallo."),
    }
