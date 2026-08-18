"""Configurazione centrale. Nessun numero magico sparso nel codice.

Ogni valore ha un default esplicito, puo' essere sovrascritto da `.env` o da
variabile d'ambiente, e i valori impossibili vengono rifiutati all'avvio
invece di produrre comportamenti assurdi dieci minuti dopo.
"""

from __future__ import annotations

import os
from dataclasses import asdict, dataclass, field, fields
from pathlib import Path
from typing import Any

VERSION = "1.0.0"
APP_NAME = "AURUM SIGNAL ENGINE M60"

#: Modalita' della sorgente dati. Deve essere sempre visibile nell'interfaccia:
#: non si mostrano mai dati simulati come se fossero di mercato.
MODE_LIVE = "LIVE"
MODE_REPLAY = "REPLAY"
MODE_SIMULATION = "SIMULATION"


def _load_dotenv(path: str = ".env") -> None:
    """Legge un `.env` senza dipendenze esterne.

    Le variabili gia' presenti nell'ambiente vincono: un valore passato al
    processo e' piu' esplicito di uno scritto in un file mesi fa.
    """
    p = Path(path)
    if not p.exists():
        return
    for raw in p.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def _get(name: str, default: Any) -> Any:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    if isinstance(default, bool):
        return raw.strip().lower() in ("1", "true", "yes", "on", "si")
    if isinstance(default, int) and not isinstance(default, bool):
        try:
            return int(float(raw))
        except ValueError:
            return default
    if isinstance(default, float):
        try:
            return float(raw)
        except ValueError:
            return default
    return raw


@dataclass
class Config:
    """Tutta la configurazione del sistema, in un posto solo."""

    # ------------------------------------------------------------ strumento
    #: Una sola coppia. Il progetto e' costruito attorno a EUR/USD e non
    #: pretende di essere generico: le soglie, il tick e le sessioni sono sue.
    symbol: str = "EUR/USD"
    #: Un pip di EUR/USD e' 0.0001; il tick delle quotazioni e' 0.00001.
    tick_size: float = 0.00001
    price_decimals: int = 5

    # ------------------------------------------------------------- orizzonte
    horizon_seconds: int = 60
    #: Quanto prima dell'entrata deve arrivare il segnale. E' un requisito
    #: operativo (serve tempo per cliccare) ma ha un COSTO statistico: con 30
    #: secondi di anticipo si sta predicendo t+90, non t+60. Il sistema misura
    #: quel costo invece di ignorarlo.
    signal_lead_seconds: int = 30
    #: Ultimi secondi in cui il segnale non cambia piu': serve a poter operare
    #: senza che la direzione cambi mentre stai cliccando.
    signal_freeze_seconds: int = 5
    #: Ogni quanto il motore valuta una nuova opportunita'.
    decision_interval_ms: int = 250
    #: Le entrate sono allineate al minuto: e' come si opera davvero.
    align_entries_to_minute: bool = True

    # -------------------------------------------------------------- economia
    payout: float = 0.80
    virtual_capital: float = 500.0
    virtual_stake: float = 25.0
    #: Margine minimo di probabilita' sopra il pareggio perche' valga la pena.
    min_edge: float = 0.04
    #: Confidenza minima. Deve restare legata al payout: sotto il pareggio non
    #: e' un segnale debole, e' un segnale perdente.
    min_confidence: float = 0.58
    #: Massimo di segnali contemporanei.
    max_concurrent_signals: int = 1

    # ----------------------------------------------------------- market data
    #: Adapter in ordine di preferenza. Il primo che si connette vince, gli
    #: altri restano come riserva.
    market_adapters: str = "twelvedata,finnhub,exchangerate"
    twelvedata_api_key: str = ""
    finnhub_api_key: str = ""
    #: Ogni quanto interrogare gli adapter REST (quelli WebSocket non pollano).
    quote_poll_seconds: float = 1.0
    #: Oltre questa eta' una quotazione non descrive piu' il presente.
    max_quote_age_ms: int = 5_000
    #: Oltre questa latenza il feed e' inutilizzabile per un orizzonte da 60s.
    max_feed_latency_ms: int = 2_000

    # ------------------------------------------------------------- news
    news_enabled: bool = True
    calendar_sources: str = "faireconomy,tradingeconomics"
    calendar_poll_seconds: int = 900
    #: Finestra di divieto attorno a un dato macro ad alto impatto.
    news_block_before_minutes: float = 15.0
    news_block_after_minutes: float = 10.0
    news_medium_before_minutes: float = 4.0
    news_medium_after_minutes: float = 3.0

    # ------------------------------------------------------------------- ML
    ml_enabled: bool = True
    #: Righe minime prima di provare ad addestrare. Sotto questa soglia un
    #: modello non e' prudente: e' rumore con dei coefficienti.
    ml_min_samples: int = 3_000
    ml_retrain_seconds: int = 1_800
    #: Distanza fra addestramento e test. DEVE essere almeno l'orizzonte,
    #: altrimenti il test vede il futuro dell'addestramento.
    ml_embargo_seconds: int = 120
    ml_walk_forward_folds: int = 4
    #: Righe minime nella coda di calibrazione perche' calibrare abbia senso.
    ml_min_calibration_rows: int = 400

    # -------------------------------------------------------------- research
    research_enabled: bool = True
    research_interval_seconds: int = 1_800
    #: Correzione per test multipli. Con decine di candidati testati sugli
    #: stessi dati, senza questa qualcosa sembra sempre significativo.
    research_fdr_alpha: float = 0.05
    research_min_samples: int = 200
    #: Il campione minimo perche' un setup entri in libreria.
    setup_min_samples: int = 60

    # --------------------------------------------------------------- booster
    booster_enabled: bool = True
    #: "shadow" = osserva e registra senza toccare il segnale live.
    #: "live" = puo' modificare il segnale, e ci arriva solo dopo validazione.
    booster_mode: str = "shadow"
    booster_min_samples: int = 500

    # ---------------------------------------------------------- notification
    telegram_enabled: bool = False
    telegram_bot_token: str = ""
    telegram_chat_id: str = ""
    audio_enabled: bool = True

    # -------------------------------------------------------------- storage
    database_path: str = "aurum_m60.db"
    persist_ticks: bool = True
    persist_features: bool = True
    #: Ogni quanto scaricare su disco le righe accodate.
    db_flush_ms: int = 500

    # ------------------------------------------------------------ interfaccia
    http_host: str = "127.0.0.1"
    http_port: int = 8100
    log_level: str = "INFO"

    # -------------------------------------------------------------- runtime
    #: Non e' configurabile dall'esterno: lo imposta l'adapter che vince.
    mode: str = field(default=MODE_LIVE)

    def __post_init__(self) -> None:
        """La validazione vale anche costruendo Config() a mano.

        Prima girava solo dentro `load()`: un test o uno script che creava la
        configurazione direttamente poteva ottenere un payout negativo e
        scoprirlo molte ore dopo, sotto forma di numeri assurdi.
        """
        self.validate()

    # ---------------------------------------------------------------- derivati
    @property
    def horizon_ms(self) -> int:
        return self.horizon_seconds * 1000

    @property
    def lead_ms(self) -> int:
        return self.signal_lead_seconds * 1000

    @property
    def freeze_ms(self) -> int:
        return self.signal_freeze_seconds * 1000

    @property
    def breakeven_win_rate(self) -> float:
        """Con payout 0.80 servono 55,56% di vittorie per non perdere.

        E' il numero piu' importante del sistema: una confidenza sotto questo
        valore non e' "debole", e' perdente.
        """
        return 1.0 / (1.0 + self.payout)

    @property
    def effective_min_probability(self) -> float:
        """La soglia vera: il pareggio piu' il margine richiesto."""
        return max(self.min_confidence, self.breakeven_win_rate + self.min_edge)

    @property
    def adapter_names(self) -> list[str]:
        return [x.strip().lower() for x in self.market_adapters.split(",") if x.strip()]

    # ----------------------------------------------------------- costruzione
    @classmethod
    def load(cls, env_file: str = ".env", **overrides: Any) -> "Config":
        _load_dotenv(env_file)
        values: dict[str, Any] = {}
        for f in fields(cls):
            if f.name == "mode":
                continue
            default = getattr(cls, f.name, None)
            if isinstance(default, property):
                continue
            values[f.name] = _get(f.name.upper(), f.default)
        values.update(overrides)
        return cls(**values)      # __post_init__ valida

    def validate(self) -> None:
        """Rifiuta all'avvio cio' che non ha senso.

        Un payout negativo o un anticipo piu' lungo dell'orizzonte non sono
        configurazioni "aggressive": sono errori che renderebbero insensato
        ogni numero prodotto dopo.
        """
        problems: list[str] = []
        if self.horizon_seconds <= 0:
            problems.append("horizon_seconds deve essere > 0")
        if not 0.0 < self.payout <= 5.0:
            problems.append("payout deve stare fra 0 e 5 (0.80 = 80%)")
        if self.signal_lead_seconds < 0:
            problems.append("signal_lead_seconds non puo' essere negativo")
        if self.signal_freeze_seconds > self.signal_lead_seconds:
            problems.append(
                "signal_freeze_seconds non puo' superare signal_lead_seconds: "
                "il segnale si congelerebbe prima di esistere")
        if self.virtual_stake <= 0 or self.virtual_capital <= 0:
            problems.append("capitale e puntata devono essere > 0")
        if self.virtual_stake > self.virtual_capital:
            problems.append(
                "virtual_stake supera virtual_capital: il ciclo fallirebbe "
                "prima della prima operazione")
        if self.ml_embargo_seconds < self.horizon_seconds:
            problems.append(
                f"ml_embargo_seconds ({self.ml_embargo_seconds}) e' sotto "
                f"l'orizzonte ({self.horizon_seconds}): il test vedrebbe il "
                "futuro dell'addestramento")
        if self.booster_mode not in ("shadow", "live", "off"):
            problems.append("booster_mode deve essere shadow, live oppure off")
        if self.tick_size <= 0:
            problems.append("tick_size deve essere > 0")
        if problems:
            raise ValueError("Configurazione non valida:\n  - " +
                             "\n  - ".join(problems))

    def to_dict(self) -> dict[str, Any]:
        out = asdict(self)
        out.update({
            "version": VERSION,
            "app": APP_NAME,
            "breakeven_win_rate": round(self.breakeven_win_rate, 6),
            "effective_min_probability": round(self.effective_min_probability, 6),
        })
        # Le chiavi non escono mai da qui.
        for secret in ("twelvedata_api_key", "finnhub_api_key",
                       "telegram_bot_token", "telegram_chat_id"):
            out[secret] = "***" if out.get(secret) else ""
        return out
