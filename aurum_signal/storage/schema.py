"""Schema SQLite.

Regola che guida tutto lo schema: da un `signal_id` si deve poter ricostruire
l'intero stato che ha prodotto quel segnale — mercato, feature, agenti,
modello, regime, blocchi, latenza, esito e impatto sul portafoglio. Un segnale
che non si puo' ricostruire non si puo' nemmeno studiare, e studiare e' lo
scopo del progetto.
"""

from __future__ import annotations

SCHEMA_VERSION = 1

SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;

-- ------------------------------------------------------------------ mercato
CREATE TABLE IF NOT EXISTS market_ticks (
    ts INTEGER NOT NULL,
    received_ts INTEGER NOT NULL,
    source TEXT NOT NULL,
    mode TEXT NOT NULL,              -- LIVE | REPLAY | SIMULATION, mai confusi
    bid REAL, ask REAL, mid REAL, last REAL,
    spread REAL, spread_bps REAL,
    latency_ms REAL
);
CREATE INDEX IF NOT EXISTS ix_ticks_ts ON market_ticks(ts);

CREATE TABLE IF NOT EXISTS candles (
    bucket_s INTEGER NOT NULL,
    ts INTEGER NOT NULL,
    open REAL, high REAL, low REAL, close REAL,
    tick_count INTEGER,
    bid_close REAL, ask_close REAL, spread_avg REAL,
    mode TEXT,
    PRIMARY KEY (bucket_s, ts)
);

CREATE TABLE IF NOT EXISTS features (
    ts INTEGER NOT NULL,
    mode TEXT,
    mid REAL, spread_bps REAL, regime TEXT, quality REAL,
    payload TEXT NOT NULL              -- JSON: il vettore causale completo
);
CREATE INDEX IF NOT EXISTS ix_features_ts ON features(ts);

-- ------------------------------------------------------------------ decisioni
CREATE TABLE IF NOT EXISTS decisions (
    decision_id TEXT PRIMARY KEY,
    ts INTEGER NOT NULL,
    mode TEXT,
    planned_entry_ts INTEGER,
    expiry_ts INTEGER,
    lead_ms INTEGER,
    direction TEXT,                    -- CALL | PUT | NO_TRADE
    probability REAL,
    confidence REAL,
    edge REAL,
    regime TEXT,
    quality REAL,
    news_risk TEXT,
    emitted INTEGER,                   -- 1 se e' diventato un segnale
    primary_blocker TEXT,
    blockers TEXT,                     -- JSON: tutti i motivi, in ordine
    reasons TEXT,                      -- JSON: spiegazione leggibile
    agent_payload TEXT,                -- JSON: opinione di ogni agente
    ml_probability REAL,
    ml_model_id TEXT,
    booster_score REAL,
    booster_action TEXT,
    latency_payload TEXT,              -- JSON: ogni stadio della pipeline
    feature_snapshot TEXT              -- JSON: le feature al momento
);
CREATE INDEX IF NOT EXISTS ix_decisions_ts ON decisions(ts);
CREATE INDEX IF NOT EXISTS ix_decisions_blocker ON decisions(primary_blocker);

-- ------------------------------------------------------------------- segnali
CREATE TABLE IF NOT EXISTS signals (
    signal_id TEXT PRIMARY KEY,
    decision_id TEXT,
    cycle_id INTEGER,
    created_ts INTEGER NOT NULL,
    entry_ts INTEGER NOT NULL,
    expiry_ts INTEGER NOT NULL,
    lead_ms INTEGER,
    direction TEXT NOT NULL,
    state TEXT NOT NULL,               -- WATCH|PRE_SIGNAL|CONFIRMED|...
    probability REAL, confidence REAL, edge REAL,
    regime TEXT, news_risk TEXT,
    entry_price REAL, expiry_price REAL,
    price_basis TEXT,                  -- quale prezzo decide l'esito
    result TEXT,                       -- WIN | LOSS | DRAW | UNKNOWN
    stake REAL, payout REAL, pnl REAL, balance_after REAL,
    strategy_version TEXT, model_version TEXT, booster_version TEXT,
    cancelled_reason TEXT,
    mode TEXT
);
CREATE INDEX IF NOT EXISTS ix_signals_entry ON signals(entry_ts);
CREATE INDEX IF NOT EXISTS ix_signals_cycle ON signals(cycle_id);

-- L'evoluzione del segnale fra T-lead e T: e' cio' che distingue un segnale
-- che si rafforza da uno che sta morendo.
CREATE TABLE IF NOT EXISTS signal_updates (
    signal_id TEXT NOT NULL,
    ts INTEGER NOT NULL,
    seconds_to_entry REAL,
    state TEXT,
    direction TEXT,
    probability REAL, confidence REAL, edge REAL,
    order_flow REAL, ml_probability REAL, booster_score REAL,
    note TEXT,
    PRIMARY KEY (signal_id, ts)
);

-- Fotografie a T-30, T-25, ... T. Servono alla ricerca sugli anticipatori.
CREATE TABLE IF NOT EXISTS preview_snapshots (
    signal_id TEXT NOT NULL,
    offset_s INTEGER NOT NULL,         -- -30, -25, ... 0
    ts INTEGER NOT NULL,
    payload TEXT NOT NULL,
    PRIMARY KEY (signal_id, offset_s)
);

-- ---------------------------------------------------------------- ombra
-- Ogni opportunita' BLOCCATA viene comunque valutata a posteriori: e' l'unico
-- modo di sapere se un filtro protegge o costa.
CREATE TABLE IF NOT EXISTS shadow_decisions (
    decision_id TEXT PRIMARY KEY,
    ts INTEGER NOT NULL,
    entry_ts INTEGER, expiry_ts INTEGER,
    direction TEXT, probability REAL, confidence REAL, edge REAL,
    blocking_reason TEXT,
    entry_price REAL, expiry_price REAL,
    result TEXT,
    regime TEXT, mode TEXT
);
CREATE INDEX IF NOT EXISTS ix_shadow_reason ON shadow_decisions(blocking_reason);

-- ------------------------------------------------------------- portafoglio
CREATE TABLE IF NOT EXISTS wallet_cycles (
    cycle_id INTEGER PRIMARY KEY,
    started_ts INTEGER NOT NULL,
    ended_ts INTEGER,
    starting_balance REAL, ending_balance REAL,
    trades INTEGER, wins INTEGER, losses INTEGER, draws INTEGER,
    win_rate REAL, roi REAL, max_drawdown REAL,
    strategy_version TEXT, model_version TEXT, booster_version TEXT,
    reason_closed TEXT,
    study TEXT                         -- JSON: il referto dello Study Mode
);

CREATE TABLE IF NOT EXISTS wallet_ledger (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts INTEGER NOT NULL,
    cycle_id INTEGER,
    kind TEXT NOT NULL,                -- OPEN | TRADE | CLOSE
    signal_id TEXT,
    result TEXT,
    stake REAL, amount REAL, balance_after REAL,
    note TEXT
);
CREATE INDEX IF NOT EXISTS ix_ledger_ts ON wallet_ledger(ts);

-- --------------------------------------------------------------- modelli
CREATE TABLE IF NOT EXISTS model_versions (
    model_id TEXT PRIMARY KEY,
    ts INTEGER NOT NULL,
    algorithm TEXT,
    horizon_s INTEGER,
    n_train INTEGER,
    feature_names TEXT,
    metrics TEXT,
    calibration TEXT,
    validation TEXT,
    artifact TEXT,
    is_champion INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS model_metrics (
    model_id TEXT NOT NULL,
    ts INTEGER NOT NULL,
    scope TEXT,                        -- overall | regime:TREND | hour:14 ...
    samples INTEGER,
    accuracy REAL, win_rate REAL, brier REAL, log_loss REAL,
    PRIMARY KEY (model_id, ts, scope)
);

CREATE TABLE IF NOT EXISTS calibration (
    model_id TEXT NOT NULL,
    ts INTEGER NOT NULL,
    bucket TEXT NOT NULL,
    samples INTEGER,
    stated REAL, realised REAL, ci_low REAL, ci_high REAL,
    PRIMARY KEY (model_id, ts, bucket)
);

-- --------------------------------------------------------------- ricerca
CREATE TABLE IF NOT EXISTS strategy_versions (
    strategy_id TEXT PRIMARY KEY,
    ts INTEGER NOT NULL,
    parent_id TEXT,
    params TEXT,
    status TEXT,                       -- CHALLENGER | CHAMPION | RETIRED
    promoted_ts INTEGER, retired_ts INTEGER,
    reason TEXT
);

CREATE TABLE IF NOT EXISTS strategy_metrics (
    strategy_id TEXT NOT NULL,
    ts INTEGER NOT NULL,
    scope TEXT,
    samples INTEGER,
    win_rate REAL, expectancy REAL, profit_factor REAL, max_drawdown REAL,
    ci_low REAL, ci_high REAL, p_value REAL,
    PRIMARY KEY (strategy_id, ts, scope)
);

CREATE TABLE IF NOT EXISTS research_experiments (
    experiment_id TEXT PRIMARY KEY,
    ts INTEGER NOT NULL,
    kind TEXT,
    hypothesis TEXT,
    samples INTEGER,
    result TEXT,                       -- JSON
    p_value REAL, p_value_adjusted REAL,
    survived_fdr INTEGER, survived_holdout INTEGER,
    verdict TEXT
);

CREATE TABLE IF NOT EXISTS regime_history (
    ts INTEGER PRIMARY KEY,
    regime TEXT, confidence REAL, volatility_bps REAL
);

CREATE TABLE IF NOT EXISTS news_events (
    event_id TEXT PRIMARY KEY,
    ts INTEGER NOT NULL,
    currency TEXT, title TEXT, impact TEXT,
    source TEXT, forecast TEXT, previous TEXT
);

CREATE TABLE IF NOT EXISTS system_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts INTEGER NOT NULL,
    component TEXT, level TEXT, message TEXT, detail TEXT
);
CREATE INDEX IF NOT EXISTS ix_sysev_ts ON system_events(ts);

CREATE TABLE IF NOT EXISTS latency_metrics (
    ts INTEGER NOT NULL,
    stage TEXT NOT NULL,
    ms REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_latency ON latency_metrics(ts, stage);

-- ------------------------------------------------------ microstruttura
CREATE TABLE IF NOT EXISTS order_flow_snapshots (
    ts INTEGER PRIMARY KEY,
    score REAL,
    components TEXT,                   -- JSON: contributo per finestra
    available INTEGER,                 -- 0 = il provider non da' abbastanza dati
    note TEXT
);

CREATE TABLE IF NOT EXISTS stream_patterns (
    pattern_id TEXT NOT NULL,
    ts INTEGER NOT NULL,
    regime TEXT,
    features_before TEXT,
    order_flow REAL,
    direction TEXT,
    future_return_10s REAL, future_return_20s REAL,
    future_return_30s REAL, future_return_60s REAL,
    success INTEGER,
    PRIMARY KEY (pattern_id, ts)
);

-- --------------------------------------------------------------- booster
CREATE TABLE IF NOT EXISTS booster_versions (
    booster_id TEXT PRIMARY KEY,
    ts INTEGER NOT NULL,
    mode TEXT,                         -- shadow | live | retired
    params TEXT, metrics TEXT, validated INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS booster_decisions (
    decision_id TEXT PRIMARY KEY,
    signal_id TEXT,
    ts INTEGER NOT NULL,
    booster_id TEXT,
    base_probability REAL,
    booster_score REAL, booster_probability REAL,
    action TEXT,                       -- BOOST | NEUTRAL | DEBOOST | VETO
    reasons TEXT,
    applied INTEGER,                   -- 0 in shadow: NON tocca il live
    base_result TEXT, boosted_simulated_result TEXT,
    latency_ms REAL
);

CREATE TABLE IF NOT EXISTS booster_metrics (
    booster_id TEXT NOT NULL,
    ts INTEGER NOT NULL,
    samples INTEGER,
    base_win_rate REAL, boosted_win_rate REAL,
    base_expectancy REAL, boosted_expectancy REAL,
    brier_base REAL, brier_boosted REAL,
    verdict TEXT,
    PRIMARY KEY (booster_id, ts)
);

-- ------------------------------------------------------------- librerie
CREATE TABLE IF NOT EXISTS setups (
    setup_id TEXT PRIMARY KEY,
    kind TEXT NOT NULL,                -- WINNING | LOSING
    ts INTEGER NOT NULL,
    regime TEXT,
    conditions TEXT,                   -- JSON: la firma del setup
    samples INTEGER,
    wins INTEGER, losses INTEGER, draws INTEGER,
    win_rate REAL, ci_low REAL, ci_high REAL, expectancy REAL,
    note TEXT
);

CREATE TABLE IF NOT EXISTS similarity_results (
    decision_id TEXT NOT NULL,
    setup_id TEXT NOT NULL,
    ts INTEGER NOT NULL,
    similarity REAL, historical_win_rate REAL, samples INTEGER,
    PRIMARY KEY (decision_id, setup_id)
);

CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT
);
"""

#: Tabelle scritte a blocchi dal writer, con l'ordine delle colonne.
BATCH_COLUMNS: dict[str, tuple[str, ...]] = {
    "market_ticks": ("ts", "received_ts", "source", "mode", "bid", "ask", "mid",
                     "last", "spread", "spread_bps", "latency_ms"),
    "features": ("ts", "mode", "mid", "spread_bps", "regime", "quality", "payload"),
    "latency_metrics": ("ts", "stage", "ms"),
    "regime_history": ("ts", "regime", "confidence", "volatility_bps"),
    "order_flow_snapshots": ("ts", "score", "components", "available", "note"),
    "system_events": ("ts", "component", "level", "message", "detail"),
    "signal_updates": ("signal_id", "ts", "seconds_to_entry", "state", "direction",
                       "probability", "confidence", "edge", "order_flow",
                       "ml_probability", "booster_score", "note"),
    "preview_snapshots": ("signal_id", "offset_s", "ts", "payload"),
    "wallet_ledger": ("ts", "cycle_id", "kind", "signal_id", "result", "stake",
                      "amount", "balance_after", "note"),
}

#: Tabelle con chiave primaria aggiornabile (upsert).
UPSERT_KEYS: dict[str, str] = {
    "decisions": "decision_id",
    "signals": "signal_id",
    "shadow_decisions": "decision_id",
    "booster_decisions": "decision_id",
    "candles": "bucket_s,ts",
    "setups": "setup_id",
    "wallet_cycles": "cycle_id",
    "model_versions": "model_id",
    "strategy_versions": "strategy_id",
    "news_events": "event_id",
    "meta": "key",
}
