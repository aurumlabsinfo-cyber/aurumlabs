"""Configurazione. Un solo posto dove cambiare i numeri che contano.

Le costanti qui dentro non sono preferenze estetiche: quasi tutte spostano il
confine fra "ho trovato un vantaggio" e "mi sto illudendo". Ognuna e'
commentata con il motivo per cui vale quel valore, perche' fra sei mesi il
motivo sara' l'unica cosa che permette di cambiarlo con cognizione.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field, asdict
from typing import Any


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.environ[name])
    except (KeyError, ValueError):
        return default


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.environ[name])
    except (KeyError, ValueError):
        return default


def _env_str(name: str, default: str) -> str:
    return os.environ.get(name, default) or default


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on", "si"}


# --------------------------------------------------------------------------
# Orizzonti
# --------------------------------------------------------------------------
# L'orizzonte primario del prodotto: 30 minuti. Gli altri esistono perche' la
# ricostruzione storica li registra tutti, e perche' la durata prevista di un
# movimento si stima confrontando quanto e' arrivato a 5, 10, 15... minuti.
HORIZONS_MIN: tuple[int, ...] = (5, 10, 15, 20, 30, 45, 60)
PRIMARY_HORIZON_MIN = 30

# --------------------------------------------------------------------------
# Etichette
# --------------------------------------------------------------------------
# Una previsione direzionale ha senso solo se "direzione" e' definita. Un
# movimento di 3 punti base su BTC non e' una direzione: e' rumore, e sotto i
# costi di qualunque esecuzione. La banda FLAT e' quindi dinamica: si adatta
# alla volatilita' corrente, con un pavimento fisso.
#
#   banda = max(LABEL_BAND_MIN_BPS, LABEL_BAND_SIGMA_K * sigma_orizzonte)
#
# Sopra banda -> LONG, sotto -banda -> SHORT, in mezzo -> FLAT.
LABEL_BAND_MIN_BPS = _env_float("AURUM_LABEL_BAND_MIN_BPS", 10.0)
LABEL_BAND_SIGMA_K = _env_float("AURUM_LABEL_BAND_SIGMA_K", 0.5)

# --------------------------------------------------------------------------
# Validazione temporale
# --------------------------------------------------------------------------
# La purga: fra la fine dell'addestramento e l'inizio del test si butta via un
# intervallo almeno pari all'orizzonte dell'etichetta. Senza, l'ultima riga di
# addestramento ha un'etichetta che vive dentro il periodo di test, e il test
# contiene il futuro del training. E' il singolo errore che piu' spesso produce
# risultati "eccellenti" che non si ripetono mai fuori.
PURGE_MINUTES = PRIMARY_HORIZON_MIN
# L'embargo: dopo la purga si scarta ancora un po', perche' la memoria del
# mercato non finisce esattamente all'orizzonte.
EMBARGO_MINUTES = _env_int("AURUM_EMBARGO_MIN", 30)

WALK_FORWARD_FOLDS = _env_int("AURUM_WF_FOLDS", 6)
# La coda finale che nessuna selezione deve mai vedere. Si guarda una volta.
HOLDOUT_FRACTION = _env_float("AURUM_HOLDOUT_FRACTION", 0.25)

# Righe minime perche' un walk-forward misuri qualcosa invece del rumore.
MIN_ROWS_FOR_VALIDATION = _env_int("AURUM_MIN_ROWS", 2000)
# Osservazioni INDIPENDENTI minime perche' un edge possa essere promosso.
# Indipendenti significa distanziate di almeno un orizzonte: 30 righe al minuto
# sullo stesso movimento di 30 minuti sono una osservazione, non trenta.
MIN_INDEPENDENT_SAMPLES = _env_int("AURUM_MIN_INDEP", 120)

# Soglia per i test multipli. Con 40 candidati e alpha 0.05, due passano per
# caso: Benjamini-Hochberg controlla la quota di falsi positivi fra i
# sopravvissuti invece di lasciarli scorrere liberi.
FDR_ALPHA = _env_float("AURUM_FDR_ALPHA", 0.10)

# --------------------------------------------------------------------------
# Soglie di promozione di un edge
# --------------------------------------------------------------------------
# Il riferimento NON e' 50%. Per una previsione a tre classi il riferimento e'
# la classe piu' frequente nel periodo di test (la "base rate"): battere una
# moneta e' facile quando il mercato e' piatto il 60% del tempo.
#
# Sull'accuratezza bilanciata il caso vale 1/3, non 1/2: una soglia a 0.42
# chiede un miglioramento relativo di circa un quarto rispetto al caso, che su
# tre classi con etichette rumorose e' gia' molto. Metterla a 0.55 renderebbe
# la promozione irraggiungibile anche per un vantaggio reale, e un cancello che
# non si apre mai non e' prudenza: e' un cancello rotto.
EDGE_MIN_BALANCED_ACC = _env_float("AURUM_EDGE_MIN_BAL_ACC", 0.42)
EDGE_MIN_AUC = _env_float("AURUM_EDGE_MIN_AUC", 0.55)
# Il limite inferiore dell'intervallo di confidenza deve stare sopra la base
# rate: non basta che la stima puntuale la superi.
EDGE_MIN_LIFT_OVER_BASE = _env_float("AURUM_EDGE_MIN_LIFT", 0.02)
# Quanti fold su quanti devono confermare. Un edge che vince in un fold su sei
# e perde negli altri cinque non e' instabile: e' assente.
EDGE_MIN_FOLD_CONSISTENCY = _env_float("AURUM_EDGE_MIN_CONSISTENCY", 0.60)
# Errore di calibrazione atteso massimo. Una probabilita' dichiarata al 70%
# che si realizza al 45% non e' "poco precisa": e' falsa.
EDGE_MAX_ECE = _env_float("AURUM_EDGE_MAX_ECE", 0.10)

# Quando un edge validato smette di funzionare. Si misura sulla finestra
# recente contro la sua stessa storia, non contro la base rate.
DECAY_WINDOW_SAMPLES = _env_int("AURUM_DECAY_WINDOW", 60)
DECAY_DROP = _env_float("AURUM_DECAY_DROP", 0.08)

# --------------------------------------------------------------------------
# Cancello della decisione
# --------------------------------------------------------------------------
# Sotto questa probabilita' non si dice niente. E' il cuore del "meglio WAIT".
MIN_DIRECTIONAL_PROB = _env_float("AURUM_MIN_PROB", 0.58)
# Il margine sulla direzione opposta: 0.50 contro 0.49 non e' una direzione.
MIN_PROB_MARGIN = _env_float("AURUM_MIN_MARGIN", 0.10)
# Qualita' minima del segnale (freschezza dati, copertura feature, regime noto).
MIN_SIGNAL_QUALITY = _env_float("AURUM_MIN_QUALITY", 0.55)

# --------------------------------------------------------------------------
# Mercato e raccolta dati
# --------------------------------------------------------------------------
SYMBOL = _env_str("AURUM_SYMBOL", "BTCUSDT")
CONTEXT_SYMBOLS: tuple[str, ...] = ("ETHUSDT", "SOLUSDT")
# Il paniere per la market breadth: quanto il mercato si muove insieme.
BREADTH_SYMBOLS: tuple[str, ...] = (
    "BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "BNBUSDT",
    "DOGEUSDT", "ADAUSDT", "AVAXUSDT", "LINKUSDT", "LTCUSDT",
)
CATEGORY = "linear"          # perpetual USDT su Bybit
BYBIT_REST = _env_str("AURUM_BYBIT_REST", "https://api.bybit.com")

# Il passo della griglia storica e della raccolta live. Un minuto e' il
# compromesso giusto per un orizzonte di 30: abbastanza fine da vedere
# l'accelerazione, abbastanza grosso da non annegare nel rumore di tick.
BAR_MINUTES = 1
# Ogni quanto il collector interroga gli endpoint live.
POLL_SECONDS = _env_int("AURUM_POLL_SECONDS", 20)
# Dati piu' vecchi di questo non sono "live": la previsione si degrada e alla
# fine si rifiuta di parlare.
STALE_SECONDS = _env_int("AURUM_STALE_SECONDS", 180)

HTTP_TIMEOUT = _env_float("AURUM_HTTP_TIMEOUT", 20.0)
HTTP_RETRIES = _env_int("AURUM_HTTP_RETRIES", 3)
# Bybit consente molto di piu' sui market data pubblici; questo passo tiene il
# backfill lontano dai limiti anche su connessioni veloci.
HTTP_MIN_INTERVAL = _env_float("AURUM_HTTP_MIN_INTERVAL", 0.12)

# --------------------------------------------------------------------------
# Ricerca
# --------------------------------------------------------------------------
RESEARCH_INTERVAL_SECONDS = _env_int("AURUM_RESEARCH_INTERVAL", 1800)
# Giorni di storia richiesti prima che la ricerca abbia senso. 30 giorni a
# barre di un minuto sono ~43.000 righe, ma solo ~1.400 osservazioni
# indipendenti a 30 minuti: e' il numero che conta, ed e' gia' poco.
MIN_HISTORY_DAYS = _env_int("AURUM_MIN_HISTORY_DAYS", 30)
BACKFILL_DAYS = _env_int("AURUM_BACKFILL_DAYS", 180)

# --------------------------------------------------------------------------
# News
# --------------------------------------------------------------------------
NEWS_FEEDS: tuple[tuple[str, str], ...] = (
    ("CoinDesk", "https://www.coindesk.com/arc/outboundfeeds/rss/"),
    ("Cointelegraph", "https://cointelegraph.com/rss"),
    ("The Block", "https://www.theblock.co/rss.xml"),
    ("Bitcoin Magazine", "https://bitcoinmagazine.com/feed"),
    ("Decrypt", "https://decrypt.co/feed"),
)
NEWS_REFRESH_SECONDS = _env_int("AURUM_NEWS_REFRESH", 600)
NEWS_LOOKBACK_HOURS = _env_int("AURUM_NEWS_LOOKBACK_H", 12)

# --------------------------------------------------------------------------
# Percorsi e server
# --------------------------------------------------------------------------
DB_PATH = _env_str("AURUM_DB", "aurum_edge.db")
HTTP_HOST = _env_str("AURUM_HOST", "127.0.0.1")
HTTP_PORT = _env_int("AURUM_PORT", 8090)

# Interruttore di sicurezza concettuale. Non esiste codice di esecuzione in
# questo pacchetto; questa costante esiste per essere verificata da un test che
# fallisce se qualcuno un giorno ne aggiungesse.
EXECUTION_ENABLED = False


@dataclass(frozen=True)
class Config:
    """Fotografia immutabile dei parametri, per salvarla accanto ai risultati.

    Un risultato di validazione senza i parametri con cui e' stato ottenuto non
    e' riproducibile, e quindi non e' un risultato.
    """

    symbol: str = SYMBOL
    category: str = CATEGORY
    bar_minutes: int = BAR_MINUTES
    horizons_min: tuple[int, ...] = HORIZONS_MIN
    primary_horizon_min: int = PRIMARY_HORIZON_MIN
    label_band_min_bps: float = LABEL_BAND_MIN_BPS
    label_band_sigma_k: float = LABEL_BAND_SIGMA_K
    purge_minutes: int = PURGE_MINUTES
    embargo_minutes: int = EMBARGO_MINUTES
    walk_forward_folds: int = WALK_FORWARD_FOLDS
    holdout_fraction: float = HOLDOUT_FRACTION
    min_rows: int = MIN_ROWS_FOR_VALIDATION
    min_independent: int = MIN_INDEPENDENT_SAMPLES
    fdr_alpha: float = FDR_ALPHA
    min_balanced_acc: float = EDGE_MIN_BALANCED_ACC
    min_auc: float = EDGE_MIN_AUC
    min_lift: float = EDGE_MIN_LIFT_OVER_BASE
    min_fold_consistency: float = EDGE_MIN_FOLD_CONSISTENCY
    max_ece: float = EDGE_MAX_ECE
    min_prob: float = MIN_DIRECTIONAL_PROB
    min_margin: float = MIN_PROB_MARGIN
    min_quality: float = MIN_SIGNAL_QUALITY
    db_path: str = DB_PATH

    @property
    def purge_ms(self) -> int:
        """Purga + embargo, in millisecondi. Il minimo che rende onesto un test."""
        return int((self.purge_minutes + self.embargo_minutes) * 60_000)

    @property
    def horizon_ms(self) -> int:
        return int(self.primary_horizon_min * 60_000)

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["horizons_min"] = list(self.horizons_min)
        return d


CONFIG = Config()
