"""Configuration for AURUM EDGE 1.0.

Every knob lives here, is read once at start-up and is written into the ``runs``
table so that any trade can be replayed against the exact configuration that
produced it.  Values come from the environment (optionally seeded from a
``.env`` file next to the repository root); there is no hidden default scattered
through the code base.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Any


# --------------------------------------------------------------------------
# environment helpers
# --------------------------------------------------------------------------

def load_dotenv(path: str | os.PathLike[str]) -> None:
    """Seed ``os.environ`` from a .env file without overriding real env vars."""
    p = Path(path)
    if not p.is_file():
        return
    for raw in p.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        os.environ.setdefault(key, value)


def _env_str(name: str, default: str) -> str:
    value = os.environ.get(name)
    return default if value is None or value == "" else value


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    try:
        return float(raw)
    except ValueError as exc:  # pragma: no cover - configuration typo
        raise ValueError(f"{name} must be a number, got {raw!r}") from exc


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except ValueError as exc:  # pragma: no cover - configuration typo
        raise ValueError(f"{name} must be an integer, got {raw!r}") from exc


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


# --------------------------------------------------------------------------
# configuration blocks
# --------------------------------------------------------------------------

@dataclass(frozen=True)
class BybitConfig:
    """Endpoints and credentials.  Bybit V5 is the only venue this system knows."""

    testnet: bool = False
    api_key: str = ""
    api_secret: str = ""
    recv_window_ms: int = 5000
    category: str = "linear"
    quote_coin: str = "USDT"
    account_type: str = "UNIFIED"

    @property
    def rest_base(self) -> str:
        return "https://api-testnet.bybit.com" if self.testnet else "https://api.bybit.com"

    @property
    def ws_public(self) -> str:
        host = "stream-testnet.bybit.com" if self.testnet else "stream.bybit.com"
        return f"wss://{host}/v5/public/{self.category}"

    @property
    def ws_private(self) -> str:
        host = "stream-testnet.bybit.com" if self.testnet else "stream.bybit.com"
        return f"wss://{host}/v5/private"

    @property
    def has_credentials(self) -> bool:
        return bool(self.api_key and self.api_secret)


@dataclass(frozen=True)
class ScanConfig:
    """Block 1 - how the market is read."""

    # universe selection
    min_turnover_24h_usd: float = 25_000_000.0
    max_spread_bps: float = 8.0
    max_universe: int = 220
    universe_refresh_s: float = 300.0

    # feeds
    depth_topic_universe: str = "orderbook.1"     # cheap top-of-book for everyone
    depth_topic_focus: str = "orderbook.50"       # full depth for tradable focus set
    focus_size: int = 30
    focus_refresh_s: float = 15.0
    topics_per_connection: int = 180
    subscribe_batch: int = 10
    ping_interval_s: float = 20.0
    ping_timeout_s: float = 12.0
    reconnect_base_delay_s: float = 1.0
    reconnect_max_delay_s: float = 30.0

    # freshness / quality
    stale_book_ms: float = 2_500.0
    stale_trade_ms: float = 60_000.0
    stale_feed_ms: float = 5_000.0                # no message at all on a connection
    max_clock_skew_ms: float = 3_000.0
    max_latency_ms: float = 1_500.0
    book_depth_levels: int = 10                   # levels used for imbalance
    scan_interval_s: float = 0.25
    history_window_s: float = 120.0


@dataclass(frozen=True)
class DecideConfig:
    """Block 2 - capital, risk and the economics of a trade."""

    target_net_eur: float = 2.0
    min_expectancy_eur: float = 0.25
    min_quality: float = 0.55
    min_probability: float = 0.56

    margin_min_eur: float = 40.0
    margin_max_eur: float = 60.0
    leverage_default: float = 10.0
    leverage_min: float = 3.0
    leverage_max: float = 10.0

    reserve_fraction: float = 0.35                # share of equity never committed
    max_concurrent_positions: int = 6
    max_positions_per_symbol: int = 1
    max_total_exposure_fraction: float = 4.0      # notional / equity
    max_loss_per_trade_eur: float = 2.0
    daily_max_loss_eur: float = 40.0

    max_hold_s: float = 120.0
    min_hold_s: float = 1.5
    stop_vol_multiple: float = 2.2
    trail_activate_fraction: float = 1.0          # of target, before trailing starts
    trail_give_back_fraction: float = 0.35        # of best profit
    edge_exit_probability: float = 0.44           # p below this -> edge is gone
    cooldown_after_exit_s: float = 20.0
    cooldown_after_loss_s: float = 60.0

    # cost model (fractions of notional, per side)
    taker_fee_rate: float = 0.00055
    maker_fee_rate: float = 0.0002
    slippage_safety_bps: float = 0.6
    cost_safety_multiple: float = 1.35            # required move / break-even move

    decision_log_top_k: int = 8
    log_all_no_trade: bool = False


@dataclass(frozen=True)
class ExecuteConfig:
    """Block 3 - order life cycle."""

    order_timeout_s: float = 8.0
    fill_timeout_s: float = 10.0
    reconcile_on_start: bool = True
    reconcile_interval_s: float = 60.0
    post_reconnect_block_s: float = 2.0
    paper_latency_ms: float = 120.0
    paper_maker_probability: float = 0.0          # paper trades are taker trades
    kill_switch_file: str = ""


@dataclass(frozen=True)
class LearnConfig:
    """Champion / challenger pipeline."""

    enabled: bool = True
    interval_s: float = 900.0
    min_trades_for_training: int = 120
    holdout_fraction: float = 0.25
    walk_forward_folds: int = 4
    purge_seconds: float = 300.0
    shadow_min_decisions: int = 200
    promote_min_net_improvement: float = 0.05     # +5% net profit out of sample
    promote_max_drawdown_ratio: float = 1.10      # challenger dd <= 110% champion dd
    l2: float = 1.0
    learning_rate: float = 0.08
    epochs: int = 220


@dataclass(frozen=True)
class ApiConfig:
    host: str = "0.0.0.0"
    port: int = 8100
    push_interval_s: float = 0.5
    cors_origin: str = "*"
    token: str = ""                               # optional shared secret


@dataclass(frozen=True)
class Config:
    mode: str = "paper"                           # paper | live
    db_path: str = ""
    log_level: str = "INFO"
    log_json: bool = False
    paper_start_equity_eur: float = 1000.0
    eur_per_usdt: float = 1.0                     # conversion used for reporting
    bybit: BybitConfig = field(default_factory=BybitConfig)
    scan: ScanConfig = field(default_factory=ScanConfig)
    decide: DecideConfig = field(default_factory=DecideConfig)
    execute: ExecuteConfig = field(default_factory=ExecuteConfig)
    learn: LearnConfig = field(default_factory=LearnConfig)
    api: ApiConfig = field(default_factory=ApiConfig)

    @property
    def is_live(self) -> bool:
        return self.mode == "live"

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        # never persist secrets
        data["bybit"]["api_secret"] = "***" if self.bybit.api_secret else ""
        data["bybit"]["api_key"] = (
            self.bybit.api_key[:4] + "***" if self.bybit.api_key else ""
        )
        return data


def default_db_path() -> str:
    """Absolute path of the one canonical database."""
    raw = os.environ.get("AURUM_DB_PATH")
    if raw:
        return str(Path(raw).expanduser().resolve())
    root = Path(os.environ.get("AURUM_HOME", Path.home() / ".aurum-edge")).expanduser()
    return str((root / "aurum_edge.sqlite3").resolve())


def load_config() -> Config:
    """Build the configuration from the environment."""
    mode = _env_str("AURUM_MODE", "paper").strip().lower()
    if mode not in {"paper", "live"}:
        raise ValueError(f"AURUM_MODE must be 'paper' or 'live', got {mode!r}")

    bybit = BybitConfig(
        testnet=_env_bool("BYBIT_TESTNET", False),
        api_key=_env_str("BYBIT_API_KEY", ""),
        api_secret=_env_str("BYBIT_API_SECRET", ""),
        recv_window_ms=_env_int("BYBIT_RECV_WINDOW_MS", 5000),
    )
    scan = ScanConfig(
        min_turnover_24h_usd=_env_float("AURUM_MIN_TURNOVER_24H", 25_000_000.0),
        max_spread_bps=_env_float("AURUM_MAX_SPREAD_BPS", 8.0),
        max_universe=_env_int("AURUM_MAX_UNIVERSE", 220),
        focus_size=_env_int("AURUM_FOCUS_SIZE", 30),
        scan_interval_s=_env_float("AURUM_SCAN_INTERVAL_S", 0.25),
        stale_book_ms=_env_float("AURUM_STALE_BOOK_MS", 2_500.0),
        max_latency_ms=_env_float("AURUM_MAX_LATENCY_MS", 1_500.0),
    )
    decide = DecideConfig(
        target_net_eur=_env_float("AURUM_TARGET_NET_EUR", 2.0),
        margin_min_eur=_env_float("AURUM_MARGIN_MIN_EUR", 40.0),
        margin_max_eur=_env_float("AURUM_MARGIN_MAX_EUR", 60.0),
        leverage_default=_env_float("AURUM_LEVERAGE", 10.0),
        reserve_fraction=_env_float("AURUM_RESERVE_FRACTION", 0.35),
        max_concurrent_positions=_env_int("AURUM_MAX_POSITIONS", 6),
        max_loss_per_trade_eur=_env_float("AURUM_MAX_LOSS_PER_TRADE_EUR", 2.0),
        daily_max_loss_eur=_env_float("AURUM_DAILY_MAX_LOSS_EUR", 40.0),
        taker_fee_rate=_env_float("AURUM_TAKER_FEE", 0.00055),
        maker_fee_rate=_env_float("AURUM_MAKER_FEE", 0.0002),
        log_all_no_trade=_env_bool("AURUM_LOG_ALL_NO_TRADE", False),
    )
    execute = ExecuteConfig(
        kill_switch_file=_env_str("AURUM_KILL_SWITCH_FILE", ""),
    )
    learn = LearnConfig(
        enabled=_env_bool("AURUM_LEARN_ENABLED", True),
        interval_s=_env_float("AURUM_LEARN_INTERVAL_S", 900.0),
        min_trades_for_training=_env_int("AURUM_LEARN_MIN_TRADES", 120),
    )
    api = ApiConfig(
        host=_env_str("AURUM_API_HOST", "0.0.0.0"),
        port=_env_int("AURUM_API_PORT", 8100),
        token=_env_str("AURUM_API_TOKEN", ""),
    )
    return Config(
        mode=mode,
        db_path=default_db_path(),
        log_level=_env_str("AURUM_LOG_LEVEL", "INFO"),
        log_json=_env_bool("AURUM_LOG_JSON", False),
        paper_start_equity_eur=_env_float("AURUM_PAPER_EQUITY_EUR", 1000.0),
        eur_per_usdt=_env_float("AURUM_EUR_PER_USDT", 1.0),
        bybit=bybit,
        scan=scan,
        decide=decide,
        execute=execute,
        learn=learn,
        api=api,
    )
