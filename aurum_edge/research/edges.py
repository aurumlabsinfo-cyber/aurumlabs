"""Il catalogo degli edge: pattern candidati, e come si provano.

Un edge, qui, e' una regola **condizionale**: non dice sempre qualcosa, dice
qualcosa solo quando certe condizioni valgono. E' questa la differenza fra un
edge e un modello — il modello e' obbligato a rispondere su ogni riga, l'edge
sceglie il proprio terreno. La sua misura, di conseguenza, si fa solo sulle
righe in cui si e' attivato, ed e' li' che va confrontato con il caso.

La soglia e' la parte delicata. Se si sceglie la soglia guardando tutti i dati
e poi si misura sugli stessi dati, si sta misurando la soglia, non l'edge:
qualunque serie ha un taglio che la divide bene, e trovarlo non richiede che
esista un fenomeno. Qui la soglia si ricava **sempre e solo dal quantile del
periodo di addestramento**, dentro ogni fold, e si applica al periodo di test
senza ritoccarla.

Le famiglie coprono le richieste del progetto. Due sono dichiarate non
disponibili invece di essere simulate:

* **liquidazioni**: Bybit v5 le pubblica solo via WebSocket, senza storico. Un
  edge di inversione da liquidazione non e' studiabile all'indietro, e
  dedurre le liquidazioni dalle candele e' un'invenzione;
* **flusso ordini e libro** sullo storico ricostruito: esistono solo da quando
  il collector gira. Le famiglie ci sono e si attivano appena i dati arrivano.
"""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass, field
from typing import Any, Iterable, Sequence

from ..features.dataset import Dataset
from ..util.numeric import quantile
from .metrics import FLAT, LONG, SHORT

# --------------------------------------------------------------------------
# Famiglie: quali feature appartengono a quale storia
# --------------------------------------------------------------------------
FAMILIES: dict[str, dict[str, Any]] = {
    "volume_breakout": {
        "label": "Rottura di volume",
        "features": ["vol_burst", "vol_pctile", "vol_ratio_60",
                     "turnover_pctile", "vol_slope_15"],
        "note": ("Il volume anomalo segnala partecipazione, non direzione. Da "
                 "solo non dovrebbe bastare, e infatti quasi mai basta: e' un "
                 "candidato naturale per le combinazioni."),
    },
    "order_flow": {
        "label": "Continuazione di flusso",
        "features": ["taker_imb_5m", "taker_imb_15m", "book_imbalance",
                     "book_imbalance_top"],
        "note": ("Chi attraversa lo spread ha fretta. Lo squilibrio dei taker "
                 "e' la misura piu' diretta di quella fretta."),
        "live_only": True,
    },
    "cvd": {
        "label": "CVD e assorbimento",
        "features": ["cvd_slope_15", "cvd_z_60", "cvd_price_div"],
        "note": ("La divergenza fra CVD e prezzo indica assorbimento: qualcuno "
                 "sta prendendo tutto senza far muovere il prezzo."),
        "live_only": True,
    },
    "open_interest": {
        "label": "Open interest",
        "features": ["oi_chg_15m_pct", "oi_chg_60m_pct", "oi_accel",
                     "oi_pctile", "oi_price_agree", "oi_price_impulse"],
        "note": ("Open interest che sale con il prezzo sono posizioni nuove; "
                 "che scende con il prezzo sono chiusure. Sono due mercati "
                 "diversi con lo stesso grafico."),
    },
    "vwap": {
        "label": "VWAP: continuazione o rientro",
        "features": ["price_vs_vwap_bps", "vwap_dist_sigma"],
        "note": ("Lontano dalla VWAP il prezzo o accelera o rientra, e quale "
                 "delle due dipende dal regime: per questo l'edge va misurato "
                 "anche per regime, non solo in media."),
    },
    "volatility": {
        "label": "Compressione ed espansione",
        "features": ["compression", "bb_width_pctile", "rv_pctile",
                     "atr_pctile"],
        "note": ("La compressione predice l'ampiezza del movimento successivo, "
                 "quasi mai il suo verso. Un edge di sola compressione che "
                 "sembra direzionale e' un sospetto, non una scoperta."),
    },
    "funding": {
        "label": "Estremi di funding e base",
        "features": ["funding_bps", "funding_z", "basis_bps",
                     "mins_to_funding"],
        "note": ("Un funding molto positivo dice che i long pagano: e' un "
                 "affollamento, e gli affollamenti si sgonfiano."),
    },
    "sentiment": {
        "label": "Rapporto conti long/short",
        "features": ["ls_ratio", "ls_z"],
        "note": ("Conta i conti, non il capitale. Va letto come sentimento del "
                 "dettaglio, che storicamente e' piu' utile al contrario."),
    },
    "cross_market": {
        "label": "Guida e ritardo fra mercati",
        "features": ["eth_ret_15m", "sol_ret_15m", "eth_lead_5m",
                     "sol_lead_5m", "eth_corr_60", "breadth_ratio"],
        "note": ("Quando un'altra moneta si muove per prima, a volte BTC "
                 "segue. 'A volte' e' il motivo per cui va misurato."),
    },
    "momentum": {
        "label": "Struttura di prezzo",
        "features": ["ret_5m", "ret_15m", "ret_30m", "ret_60m", "rsi_14_dev",
                     "macd_hist_bps", "streak", "range_position",
                     "price_vs_ema_slow_bps", "ema_fast_mid_bps",
                     "ema_mid_slow_bps", "bb_z"],
        "note": "La famiglia piu' affollata, e quindi la piu' esposta al caso.",
    },
    "news": {
        "label": "Reazione alle notizie",
        "features": ["news_impact_1h", "news_direction_1h"],
        "note": ("Misura la reazione, non la notizia. Il sistema non capisce "
                 "il contenuto: vede quanto e in che verso il flusso di "
                 "notizie e' cambiato."),
        "live_only": True,
    },
    "liquidation": {
        "label": "Inversione da liquidazioni",
        "features": [],
        "note": ("NON DISPONIBILE. Bybit v5 pubblica le liquidazioni solo via "
                 "WebSocket e non ne conserva lo storico: questa famiglia non "
                 "e' studiabile all'indietro. Resta dichiarata perche' la sua "
                 "assenza e' un'informazione, e perche' dedurre le "
                 "liquidazioni dalle candele sarebbe inventarle."),
        "unavailable": True,
    },
}

FEATURE_FAMILY: dict[str, str] = {
    feat: family
    for family, spec in FAMILIES.items()
    for feat in spec["features"]
}


# --------------------------------------------------------------------------
# Regole
# --------------------------------------------------------------------------
@dataclass(frozen=True)
class Condition:
    """Una condizione su una feature, con la soglia espressa in quantile.

    La soglia si porta dietro il **quantile**, non il valore. Il valore assoluto
    di `vol_burst` non e' confrontabile fra un mese calmo e uno agitato; il
    novantesimo percentile lo e'. E costringe a ricalcolare il taglio su ogni
    periodo di addestramento, che e' esattamente cio' che si vuole.
    """

    feature: str
    side: str            # "high" | "low"
    q: float

    def describe(self, threshold: float | None = None) -> str:
        op = ">=" if self.side == "high" else "<="
        val = f"{threshold:.4g}" if threshold is not None else f"q{self.q:.2f}"
        return f"{self.feature} {op} {val}"

    def to_dict(self) -> dict[str, Any]:
        return {"feature": self.feature, "side": self.side, "q": self.q}


@dataclass(frozen=True)
class EdgeRule:
    """Un edge: una o due condizioni. La direzione NON e' parte della regola.

    Questa e' una correzione a una versione precedente che generava ogni
    condizione due volte, una per LONG e una per SHORT. Era sbagliata in due
    modi. Il primo: le due varianti producevano previsioni identiche, perche'
    la probabilita' dichiarata viene dalla frequenza osservata nel training e
    non dalla direzione scritta nella regola — quindi il campo `direction` era
    decorativo, e si vedeva, perche' le due varianti ottenevano la stessa
    accuratezza fino alla terza cifra. Il secondo, piu' grave: raddoppiava il
    numero di candidati senza aggiungere informazione, e siccome la correzione
    per test multipli conta i candidati provati, rendeva la soglia
    ingiustificatamente severa per tutti gli altri.

    Ora la direzione la decidono i dati: `fit_edge` guarda quale classe
    direzionale prevale fra le attivazioni del periodo di addestramento. E'
    anche piu' onesto — non si presuppone che il volume alto significhi
    rialzo, lo si misura.
    """

    conditions: tuple[Condition, ...]
    family: str

    @property
    def edge_id(self) -> str:
        payload = json.dumps({
            "c": [c.to_dict() for c in self.conditions],
            "f": self.family,
        }, sort_keys=True)
        return hashlib.sha1(payload.encode()).hexdigest()[:16]

    @property
    def label(self) -> str:
        # Una combinazione porta il nome composto delle due famiglie
        # ("vwap+volume_breakout"), che non e' una chiave di FAMILIES: va
        # scomposto, altrimenti l'etichetta esplode proprio sui candidati piu'
        # interessanti, cioe' quelli che uniscono due storie diverse.
        parts = " E ".join(c.describe() for c in self.conditions)
        names = " + ".join(
            FAMILIES[f]["label"] if f in FAMILIES else f
            for f in self.family.split("+"))
        return f"{names}: {parts}"

    def to_dict(self) -> dict[str, Any]:
        return {
            "conditions": [c.to_dict() for c in self.conditions],
            "family": self.family,
            "label": self.label,
        }

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "EdgeRule":
        return cls(
            conditions=tuple(Condition(**c) for c in d["conditions"]),
            family=d["family"])


@dataclass
class FittedEdge:
    """Un edge con le soglie fissate su un periodo di addestramento.

    Porta anche la probabilita' empirica osservata **nel training**: e' quella
    che verra' dichiarata sul test, e quindi e' la probabilita' che la
    calibrazione mettera' alla prova.
    """

    rule: EdgeRule
    thresholds: list[float]
    p_long: float
    p_short: float
    p_flat: float
    train_activations: int

    def activates(self, row: Sequence[float | None],
                  index: dict[str, int]) -> bool:
        for cond, thr in zip(self.rule.conditions, self.thresholds):
            j = index.get(cond.feature)
            if j is None:
                return False
            v = row[j]
            if v is None or not math.isfinite(v):
                return False
            if cond.side == "high" and v < thr:
                return False
            if cond.side == "low" and v > thr:
                return False
        return True

    def probabilities(self) -> dict[str, float]:
        return {LONG: self.p_long, SHORT: self.p_short, FLAT: self.p_flat}

    @property
    def direction(self) -> str:
        """La direzione che i dati di addestramento indicano, non una premessa."""
        return LONG if self.p_long >= self.p_short else SHORT

    def to_dict(self) -> dict[str, Any]:
        return {
            **self.rule.to_dict(),
            "direction": self.direction,
            "thresholds": [round(t, 6) for t in self.thresholds],
            "p_long": round(self.p_long, 4),
            "p_short": round(self.p_short, 4),
            "p_flat": round(self.p_flat, 4),
            "train_activations": self.train_activations,
            "description": " E ".join(
                c.describe(t) for c, t in zip(self.rule.conditions,
                                              self.thresholds)),
        }


# --------------------------------------------------------------------------
# Generazione dei candidati
# --------------------------------------------------------------------------
SINGLE_QUANTILES = (0.90, 0.80, 0.20, 0.10)
COMBO_QUANTILES = (0.80, 0.20)


def candidate_rules(available: Sequence[str], *,
                    include_combos: bool = True,
                    max_combos: int = 120) -> list[EdgeRule]:
    """Genera i candidati sulle sole feature effettivamente presenti.

    Il numero di candidati e' anche il costo statistico: piu' se ne provano,
    piu' severa deve essere la correzione per test multipli. Non e' gratis
    generarne di piu', ed e' il motivo per cui le combinazioni sono limitate a
    coppie di famiglie diverse — due condizioni della stessa famiglia sono
    quasi sempre la stessa condizione detta due volte.
    """
    present = [f for f in available if f in FEATURE_FAMILY]
    rules: list[EdgeRule] = []

    for feat in present:
        family = FEATURE_FAMILY[feat]
        for q in SINGLE_QUANTILES:
            side = "high" if q >= 0.5 else "low"
            rules.append(EdgeRule((Condition(feat, side, q),), family))

    if not include_combos:
        return rules

    combos: list[EdgeRule] = []
    for i, feat_a in enumerate(present):
        fam_a = FEATURE_FAMILY[feat_a]
        for feat_b in present[i + 1:]:
            fam_b = FEATURE_FAMILY[feat_b]
            if fam_a == fam_b:
                continue
            for qa in COMBO_QUANTILES:
                for qb in COMBO_QUANTILES:
                    side_a = "high" if qa >= 0.5 else "low"
                    side_b = "high" if qb >= 0.5 else "low"
                    combos.append(EdgeRule(
                        (Condition(feat_a, side_a, qa),
                         Condition(feat_b, side_b, qb)),
                        f"{fam_a}+{fam_b}"))
    # Un ordine deterministico, poi il taglio: l'insieme dei candidati non deve
    # dipendere dall'ordine in cui il dizionario e' stato costruito.
    combos.sort(key=lambda r: r.edge_id)
    rules.extend(combos[:max_combos])
    return rules


def family_of(rule: EdgeRule) -> str:
    return rule.family.split("+")[0]


# --------------------------------------------------------------------------
# Adattamento e applicazione
# --------------------------------------------------------------------------
def fit_edge(rule: EdgeRule, train: Dataset,
             min_activations: int = 30) -> FittedEdge | None:
    """Fissa le soglie sui quantili del training e misura la frequenza li'.

    Restituisce `None` se l'edge non si attiva abbastanza: una regola che nel
    periodo di addestramento e' scattata otto volte non ha una probabilita'
    empirica, ha un aneddoto.
    """
    index = {name: j for j, name in enumerate(train.names)}
    thresholds: list[float] = []
    for cond in rule.conditions:
        j = index.get(cond.feature)
        if j is None:
            return None
        col = [r[j] for r in train.rows]
        thr = quantile(col, cond.q)
        if thr is None:
            return None
        thresholds.append(thr)

    fitted = FittedEdge(rule, thresholds, 0.0, 0.0, 0.0, 0)
    counts = {LONG: 0, SHORT: 0, FLAT: 0}
    total = 0
    for row, label in zip(train.rows, train.labels):
        if not fitted.activates(row, index):
            continue
        total += 1
        if label in counts:
            counts[label] += 1
    if total < min_activations:
        return None

    labelled = sum(counts.values())
    if labelled == 0:
        return None
    # Smoothing di Laplace: con quaranta attivazioni e zero SHORT, la stima
    # grezza direbbe "impossibile". Con l'aggiunta di un caso per classe dice
    # "raro", che e' cio' che i dati sostengono davvero.
    denom = labelled + 3.0
    fitted.p_long = (counts[LONG] + 1.0) / denom
    fitted.p_short = (counts[SHORT] + 1.0) / denom
    fitted.p_flat = (counts[FLAT] + 1.0) / denom
    fitted.train_activations = total
    return fitted


def predict_edge(fitted: FittedEdge | None, test: Dataset
                 ) -> list[dict[str, float]]:
    """Probabilita' per riga. Dove l'edge non si attiva, si dichiara ignoranza.

    "Ignoranza" e' la distribuzione delle classi osservata: e' il modo corretto
    di dire "qui non ho niente da aggiungere", e fa si' che le righe non
    attivate non contino ne' a favore ne' contro.
    """
    if fitted is None:
        return [{LONG: 1 / 3, SHORT: 1 / 3, FLAT: 1 / 3} for _ in test.rows]

    index = {name: j for j, name in enumerate(test.names)}
    neutral = {LONG: 1 / 3, SHORT: 1 / 3, FLAT: 1 / 3}
    probs = fitted.probabilities()
    out: list[dict[str, float]] = []
    for row in test.rows:
        out.append(dict(probs) if fitted.activates(row, index) else dict(neutral))
    return out


def activation_mask(fitted: FittedEdge, ds: Dataset) -> list[bool]:
    index = {name: j for j, name in enumerate(ds.names)}
    return [fitted.activates(row, index) for row in ds.rows]


def unavailable_families(available: Sequence[str]) -> list[dict[str, Any]]:
    """Le famiglie che non si possono studiare, e perche'. Va detto, non nascosto."""
    present = set(available)
    out: list[dict[str, Any]] = []
    for name, spec in FAMILIES.items():
        if spec.get("unavailable"):
            out.append({"family": name, "label": spec["label"],
                        "reason": spec["note"]})
            continue
        if spec["features"] and not (set(spec["features"]) & present):
            out.append({
                "family": name, "label": spec["label"],
                "reason": (
                    "Nessuna delle sue feature e' presente nel dataset con "
                    "copertura sufficiente" +
                    (": questi dati esistono solo da quando il collector "
                     "raccoglie in avanti." if spec.get("live_only") else ".")),
            })
    return out


def describe_catalogue(available: Sequence[str]) -> dict[str, Any]:
    rules = candidate_rules(available)
    by_family: dict[str, int] = {}
    for r in rules:
        by_family[r.family] = by_family.get(r.family, 0) + 1
    return {
        "candidates": len(rules),
        "by_family": dict(sorted(by_family.items(), key=lambda kv: -kv[1])),
        "unavailable": unavailable_families(available),
        "note": ("Ogni candidato in piu' alza la soglia che i sopravvissuti "
                 "devono superare: la correzione per test multipli conta "
                 "quanti se ne sono provati, non quanti ne sono passati."),
    }
