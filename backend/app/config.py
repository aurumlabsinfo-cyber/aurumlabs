"""Application settings.

Every value is sourced from environment variables (see `.env.example`).
No credential is ever hard-coded here, and the public market-data feeds used by
this application do not require any credential at all.
"""

from __future__ import annotations

from functools import lru_cache
from typing import Literal

from pydantic import field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=(".env", "../.env"),
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    # ------------------------------------------------------------------ app
    app_name: str = "BTC 5-Second Quant Engine"
    env: Literal["dev", "test", "prod"] = "dev"
    log_level: str = "INFO"
    api_host: str = "0.0.0.0"
    api_port: int = 8000

    # ------------------------------------------------------------- database
    database_url: str = "postgresql+asyncpg://postgres:postgres@localhost:5432/btcquant"
    db_pool_size: int = 10
    db_max_overflow: int = 10
    db_echo: bool = False
    # Persistence is batched to keep the hot path non-blocking.
    db_flush_interval_ms: int = 500
    db_batch_max_rows: int = 2000
    persist_market_ticks: bool = True
    persist_trades: bool = True
    persist_features: bool = True
    persist_book_updates: bool = False  # very high volume: off by default
    book_snapshot_interval_s: int = 30
    # How often a snapshot of paper-trading performance is written to
    # `performance_metrics`, building a history of how the edge evolves.
    performance_metrics_interval_s: int = 300

    # ------------------------------------------------------------- exchange
    # Comma separated. The first entry is the primary (authoritative) source.
    exchanges: str = "binance_spot"
    symbol: str = "BTCUSDT"
    binance_ws_base: str = "wss://stream.binance.com:9443"
    binance_rest_base: str = "https://api.binance.com"
    binance_futures_ws_base: str = "wss://fstream.binance.com"
    binance_futures_rest_base: str = "https://fapi.binance.com"
    coinbase_ws_base: str = "wss://ws-feed.exchange.coinbase.com"
    coinbase_rest_base: str = "https://api.exchange.coinbase.com"
    orderbook_depth_limit: int = 1000
    orderbook_stream_speed_ms: Literal[100, 1000] = 100
    ws_reconnect_base_delay_s: float = 1.0
    ws_reconnect_max_delay_s: float = 30.0
    ws_stale_timeout_s: float = 10.0
    rest_timeout_s: float = 10.0
    http_proxy_url: str | None = None

    # The synthetic adapter produces MODEL-GENERATED data. It exists so the
    # pipeline can be exercised offline (CI, tests, air-gapped hosts). Every row
    # it produces is flagged `is_synthetic = true` end to end and the UI shows a
    # permanent warning banner. It must never be used to evaluate an edge.
    allow_synthetic_source: bool = False

    # ---------------------------------------------------------- feature eng
    feature_interval_ms: int = 100
    tick_buffer_seconds: int = 300
    trade_buffer_seconds: int = 300
    volatility_window_s: float = 30.0
    book_wall_multiple: float = 3.0
    large_trade_quantile: float = 0.95

    # ---------------------------------------------------------- data gating
    # Calibrated against real BTC data (Crypto.com BTC_USDT, 2026-08-14): the
    # quoted spread there is one tick, about 0.0016 bps. A 3 bps gate was ~1900x
    # looser than the market and could never fire; 1 bps still only trips on a
    # genuine dislocation.
    max_spread_bps: float = 1.0
    max_latency_ms: float = 750.0
    max_feed_gap_ms: float = 2000.0
    min_data_quality: float = 0.75
    min_warmup_seconds: float = 60.0
    #: NO TRADE when this share of recent horizon-length windows had no net
    #: price change at all. Measured at 0.32 on real BTC at a 5s horizon, so a
    #: binary bet there is largely a bet on a tie.
    max_zero_move_fraction: float = 0.35
    #: The expected move must be worth at least this many ticks. The quoted
    #: spread is not the real barrier at this horizon - price standing still is.
    min_expected_move_ticks: float = 2.0
    #: Top-of-book sizes below this notional are treated as dust: on real data
    #: the touch is frequently a ~$100 order against ~$10,000 on the other side,
    #: which pins L1 imbalance near +/-1 and makes it meaningless.
    min_l1_notional: float = 500.0

    # -------------------------------------------------------------- signals
    signal_enabled: bool = True
    signal_horizon_s: float = 5.0
    signal_min_confidence: float = 0.60
    signal_min_edge: float = 0.04  # |P(up) - 0.5| required
    signal_cooldown_ms: int = 3000
    signal_max_concurrent: int = 1
    signal_wait_timeout_s: float = 30.0
    # Trigger distance = k * sigma over the horizon, clamped by spread.
    trigger_sigma_k: float = 0.35
    trigger_min_ticks: float = 1.0
    trigger_max_bps: float = 8.0
    trigger_price_source: Literal["mid", "last", "micro"] = "mid"
    tick_size: float = 0.01

    # ------------------------------------------------- learning from every window
    #: Record the engine's lean on EVERY evaluated window, not only the ones
    #: that became signals. Training only on emitted signals learns the
    #: engine's own filter rather than the market.
    shadow_decisions_enabled: bool = True
    #: One shadow row at most per this interval. Features are computed every
    #: 100ms; storing all of them is ~860k rows/day for no extra information at
    #: a minute-scale horizon.
    shadow_decision_interval_ms: int = 1000

    #: Re-run the validated pipeline on a schedule and activate the result -
    #: but only when its walk-forward edge classifies as PROVEN or PROMISING.
    #: A run that concludes NO ROBUST EDGE leaves the live engine untouched.
    auto_retrain_enabled: bool = False
    retrain_interval_s: float = 3600.0
    retrain_initial_delay_s: float = 900.0
    retrain_splits: int = 5

    # ------------------------------------------------------- paper trading
    # Binary-option payout. `None` => UNKNOWN => no monetary P&L is reported.
    binary_payout: float | None = None
    paper_stake: float = 1.0

    # ------------------------------------------------------------------ ml
    model_dir: str = "./models"
    active_model_id: str | None = None
    ml_min_samples: int = 5000
    ml_embargo_s: float = 30.0

    # ------------------------------------------------------------ security
    cors_origins: str = "http://localhost:3000,http://127.0.0.1:3000"
    admin_api_key: str | None = None
    rate_limit_requests: int = 120
    rate_limit_window_s: int = 60
    ws_max_connections: int = 200

    @field_validator("symbol")
    @classmethod
    def _upper_symbol(cls, v: str) -> str:
        return v.upper().strip()

    @model_validator(mode="after")
    def _scale_windows_to_horizon(self) -> "Settings":
        """Grow the time windows when a long horizon is configured.

        The defaults are sized for the 5-second horizon this engine started
        with. Point it at 15 minutes and they become incoherent: a 300-second
        tick buffer cannot even hold one horizon, so `sigma_over(900s)` would be
        extrapolated from data that does not exist and the long-window features
        would all be unavailable.

        Anything set explicitly in the environment is left exactly as given -
        this only fills in values the operator did not choose.
        """
        h = self.signal_horizon_s
        explicit = self.model_fields_set

        # Four horizons of history: enough for the horizon-length return, its
        # volatility estimate, and a lookback long enough to be a sample.
        needed_buffer = max(300, int(h * 4))
        if "tick_buffer_seconds" not in explicit:
            self.tick_buffer_seconds = max(self.tick_buffer_seconds, needed_buffer)
        if "trade_buffer_seconds" not in explicit:
            self.trade_buffer_seconds = max(self.trade_buffer_seconds, needed_buffer)

        # Sigma over the horizon is estimated from this lookback. Shorter than
        # the horizon itself and it is an extrapolation, not a measurement.
        if "volatility_window_s" not in explicit:
            self.volatility_window_s = max(self.volatility_window_s, h * 2)

        # A 3-second cooldown between 15-minute trades is meaningless, and the
        # trigger cannot be allowed to wait longer than the trade itself.
        if "signal_cooldown_ms" not in explicit:
            self.signal_cooldown_ms = max(self.signal_cooldown_ms, int(h * 1000 * 0.2))
        if "signal_wait_timeout_s" not in explicit:
            self.signal_wait_timeout_s = max(self.signal_wait_timeout_s, h * 0.5)

        # Walk-forward embargo must exceed the label's own memory, or the test
        # fold sees the training fold's future.
        if "ml_embargo_s" not in explicit:
            self.ml_embargo_s = max(self.ml_embargo_s, h * 2)

        return self

    @property
    def horizon_ms(self) -> int:
        return int(self.signal_horizon_s * 1000)

    @property
    def exchange_list(self) -> list[str]:
        return [e.strip() for e in self.exchanges.split(",") if e.strip()]

    @property
    def cors_origin_list(self) -> list[str]:
        return [o.strip() for o in self.cors_origins.split(",") if o.strip()]

    @property
    def sync_database_url(self) -> str:
        """psycopg/plain URL, used by offline tooling (backtest CLI)."""
        return self.database_url.replace("+asyncpg", "")


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
