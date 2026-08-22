"""Il ciclo di vita di un edge. Entrare costa, restare costa ancora.

    SCOPERTO -> IN VALIDAZIONE -> IN OMBRA -> VALIDATO -> IN DECADIMENTO
                     |                |            |
                     +--> RIFIUTATO <-+------------+

Ogni stato ha una domanda sola, e finche' non ha risposta non si passa oltre:

* **SCOPERTO** — si attiva abbastanza spesso da poter essere misurato?
* **IN VALIDAZIONE** — supera walk-forward con purga, holdout fresco e
  correzione per test multipli?
* **IN OMBRA** — continua a funzionare su previsioni fatte *in avanti*, che
  nessun campione storico puo' aver influenzato? E' l'unico stato che non si
  puo' accelerare: richiede tempo reale, e serve proprio a quello.
* **VALIDATO** — puo' contribuire alla previsione mostrata.
* **IN DECADIMENTO** — funzionava e ha smesso. Torna in ombra o esce.
* **RIFIUTATO** — con il motivo scritto accanto, perche' i motivi del rifiuto
  sono la parte piu' istruttiva dell'archivio.

Il passaggio in ombra non e' una formalita': e' l'unico test che non puo'
essere contaminato dalla selezione, perche' avviene su dati che non esistevano
quando l'edge e' stato scelto.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any

from .. import config
from ..util import timeutil

DISCOVERED = "SCOPERTO"
VALIDATING = "IN_VALIDAZIONE"
SHADOW = "IN_OMBRA"
VALIDATED = "VALIDATO"
DECAYING = "IN_DECADIMENTO"
REJECTED = "RIFIUTATO"

ORDER = (DISCOVERED, VALIDATING, SHADOW, VALIDATED, DECAYING, REJECTED)

# Quante osservazioni in ombra servono prima di promuovere. Non e' un numero
# magico: e' il campione minimo perche' l'intervallo di Wilson su una quota
# vicina a 0.55 sia piu' stretto del vantaggio che si vuole dimostrare.
SHADOW_MIN_OBSERVATIONS = 60
SHADOW_MIN_HOURS = 24


@dataclass
class EdgeRecord:
    """Un edge nell'archivio, con la sua storia di stati."""

    edge_id: str
    family: str
    label: str
    definition: dict[str, Any]
    state: str
    direction: str
    created_ts: int
    updated_ts: int
    state_ts: int
    metrics: dict[str, Any] = field(default_factory=dict)
    reject_reason: str | None = None
    history: list[dict[str, Any]] = field(default_factory=list)

    # ------------------------------------------------------------- passaggi
    def transition(self, new_state: str, reason: str, now: int | None = None
                   ) -> None:
        now = now or timeutil.now_ms()
        if new_state == self.state:
            self.updated_ts = now
            return
        self.history.append({
            "from": self.state, "to": new_state, "ts": now,
            "at": timeutil.iso(now), "reason": reason,
        })
        self.state = new_state
        self.state_ts = now
        self.updated_ts = now
        if new_state == REJECTED:
            self.reject_reason = reason

    @property
    def age_hours(self) -> float:
        return (timeutil.now_ms() - self.created_ts) / 3_600_000.0

    @property
    def state_age_hours(self) -> float:
        return (timeutil.now_ms() - self.state_ts) / 3_600_000.0

    def to_row(self) -> dict[str, Any]:
        return {
            "edge_id": self.edge_id, "family": self.family,
            "label": self.label,
            "definition": json.dumps(self.definition, separators=(",", ":")),
            "state": self.state, "direction": self.direction,
            "created_ts": self.created_ts, "updated_ts": self.updated_ts,
            "state_ts": self.state_ts,
            "metrics": json.dumps(self.metrics, separators=(",", ":"),
                                  default=str),
            "reject_reason": self.reject_reason,
            "history": json.dumps(self.history, separators=(",", ":")),
        }

    @classmethod
    def from_row(cls, row: Any) -> "EdgeRecord":
        def load(text: Any, default: Any) -> Any:
            if not text:
                return default
            try:
                return json.loads(text)
            except (ValueError, TypeError):
                return default

        return cls(
            edge_id=row["edge_id"], family=row["family"], label=row["label"],
            definition=load(row["definition"], {}), state=row["state"],
            direction=row["direction"] or "", created_ts=row["created_ts"],
            updated_ts=row["updated_ts"], state_ts=row["state_ts"],
            metrics=load(row["metrics"], {}),
            reject_reason=row["reject_reason"],
            history=load(row["history"], []))

    def to_dict(self) -> dict[str, Any]:
        return {
            "edge_id": self.edge_id, "family": self.family,
            "label": self.label, "state": self.state,
            "direction": self.direction,
            "created": timeutil.iso(self.created_ts),
            "state_since": timeutil.iso(self.state_ts),
            "state_age_hours": round(self.state_age_hours, 1),
            "definition": self.definition,
            "metrics": self.metrics,
            "reject_reason": self.reject_reason,
            "transitions": len(self.history),
        }


def shadow_verdict(observations: list[Any], *,
                   min_obs: int = SHADOW_MIN_OBSERVATIONS,
                   min_hours: float = SHADOW_MIN_HOURS) -> dict[str, Any]:
    """Giudica un edge sulle osservazioni raccolte in avanti.

    Due condizioni insieme, e nessuna delle due basta: abbastanza osservazioni
    **e** abbastanza tempo trascorso. Sessanta osservazioni raccolte in due ore
    vengono tutte dalla stessa condizione di mercato; sessanta ore con quattro
    osservazioni non dicono niente.
    """
    from ..util.numeric import wilson_interval

    resolved = [o for o in observations
                if o["label"] in ("LONG", "SHORT") and o["correct"] is not None]
    n = len(resolved)
    if not observations:
        return {"ready": False, "n": 0,
                "reason": "nessuna osservazione in ombra registrata"}

    span_h = ((max(o["ts"] for o in observations) -
               min(o["ts"] for o in observations)) / 3_600_000.0)

    if n < min_obs or span_h < min_hours:
        return {
            "ready": False, "n": n, "span_hours": round(span_h, 1),
            "needed_observations": min_obs, "needed_hours": min_hours,
            "reason": (
                f"{n} osservazioni risolte su {min_obs} in {span_h:.1f} ore su "
                f"{min_hours}. L'ombra non si puo' accelerare: e' l'unico test "
                "su dati che non esistevano quando l'edge e' stato scelto."),
        }

    hits = sum(1 for o in resolved if o["correct"])
    rate = hits / n
    lo, hi = wilson_interval(hits, n)
    longs = sum(1 for o in resolved if o["label"] == "LONG")
    base = max(longs, n - longs) / n
    return {
        "ready": True, "n": n, "span_hours": round(span_h, 1),
        "accuracy": round(rate, 4), "ci95": [round(lo, 4), round(hi, 4)],
        "base_rate": round(base, 4),
        "beats_base": bool(lo > base),
        "reason": (
            f"in ombra: {hits}/{n} corrette ({rate:.1%}), intervallo "
            f"[{lo:.1%}, {hi:.1%}] contro una base rate del {base:.1%}"),
    }


def decay_verdict(observations: list[Any], reference_accuracy: float | None,
                  *, window: int | None = None,
                  drop: float | None = None) -> dict[str, Any]:
    """Un edge validato sta smettendo di funzionare?

    Il confronto e' con **se' stesso**, non con la base rate: un edge che
    passava al 62% e ora sta al 53% e' ancora sopra il caso, ma il fenomeno che
    lo generava si sta esaurendo, e continuare a mostrarlo con la vecchia
    probabilita' significa dichiarare un numero che non vale piu'.
    """
    window = config.DECAY_WINDOW_SAMPLES if window is None else window
    drop = config.DECAY_DROP if drop is None else drop

    resolved = [o for o in observations
                if o["label"] in ("LONG", "SHORT") and o["correct"] is not None]
    if reference_accuracy is None or len(resolved) < window:
        return {"decaying": False, "n": len(resolved), "window": window,
                "reason": "campione recente insufficiente per dichiarare un calo"}

    recent = resolved[-window:]
    hits = sum(1 for o in recent if o["correct"])
    rate = hits / len(recent)
    gap = reference_accuracy - rate
    return {
        "decaying": bool(gap >= drop),
        "n": len(recent),
        "recent_accuracy": round(rate, 4),
        "reference_accuracy": round(reference_accuracy, 4),
        "drop": round(gap, 4),
        "threshold": drop,
        "reason": (
            f"nelle ultime {len(recent)} osservazioni l'accuratezza e' "
            f"{rate:.1%} contro il {reference_accuracy:.1%} di riferimento "
            f"(calo {gap:+.1%})"),
    }
