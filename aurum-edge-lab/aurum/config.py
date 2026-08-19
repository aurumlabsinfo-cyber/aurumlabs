"""Configuration: load, validate, bound.

Two rules drive this module.

1.  Nothing that decides whether a trade may happen is a magic number buried in
    a module.  It lives here, it is validated, and it is reported by ``/config``.
2.  Values the frontend is allowed to change are bounded *server side*.  The
    ``SETTABLE`` table below is the whole list of what a UI may move and how far;
    anything else is structural and requires a restart with a new config file.

Environment overrides accept either separator::

    AURUM_SET__market__replay_speed=8      # works with every shell
    env 'AURUM_SET__market.replay_speed=8' # dotted form needs `env`

The dotted form reads better in documentation but a shell will not accept it as
an inline ``VAR=x cmd`` assignment, because identifiers cannot contain dots.
"""

from __future__ import annotations

import os
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

ENV_PREFIX = "AURUM_SET__"

# Friendly aliases so the common knobs do not need the dotted syntax.
ENV_ALIASES: dict[str, str] = {
    "AURUM_ENV": "app.env",
    "AURUM_LOG_LEVEL": "app.log_level",
    "AURUM_LOG_JSON": "app.log_json",
    "AURUM_DATA_DIR": "app.data_dir",
    "AURUM_API_HOST": "api.host",
    "AURUM_API_PORT": "api.port",
    "AURUM_FEED": "market.feed",
    "AURUM_VENUE": "market.venue",
    "AURUM_REST_BASE": "market.rest_base",
    "AURUM_WS_BASE": "market.ws_base",
    "AURUM_DB_URL": "storage.url",
    "AURUM_RESEARCH_ENABLED": "research.enabled",
    "AURUM_EXPLORATION": "exploration.enabled",
    "AURUM_EXPLORATION_TRADES_PER_DAY": "exploration.trades_per_day",
}


class _Model(BaseModel):
    model_config = ConfigDict(extra="forbid", validate_assignment=True)


class SymbolSpec(_Model):
    symbol: str
    tier: str = "DYNAMIC"
    role: str = ""
    use: str = ""

    @field_validator("symbol")
    @classmethod
    def _upper(cls, v: str) -> str:
        return v.strip().upper()

    @field_validator("tier")
    @classmethod
    def _tier(cls, v: str) -> str:
        v = v.strip().upper()
        if v not in {"CORE", "MAJOR", "DYNAMIC"}:
            raise ValueError(f"tier must be CORE, MAJOR or DYNAMIC, got {v!r}")
        return v


class AppConfig(_Model):
    name: str = "AURUM EDGE LAB"
    version: str = "1.0.0"
    env: str = "development"
    data_dir: str = "./data"
    log_level: str = "INFO"
    log_json: bool = False

    @field_validator("env")
    @classmethod
    def _env(cls, v: str) -> str:
        v = v.strip().lower()
        if v not in {"development", "production"}:
            raise ValueError("app.env must be 'development' or 'production'")
        return v

    @field_validator("log_level")
    @classmethod
    def _level(cls, v: str) -> str:
        v = v.strip().upper()
        if v not in {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}:
            raise ValueError(f"unknown log level {v!r}")
        return v


class ApiConfig(_Model):
    host: str = "127.0.0.1"
    port: int = Field(default=8002, ge=1, le=65535)
    cors_origins: list[str] = Field(default_factory=lambda: ["http://localhost:3000"])
    ws_broadcast_interval_ms: int = Field(default=500, ge=100, le=10_000)


class ReconnectConfig(_Model):
    initial_delay_s: float = Field(default=1.0, gt=0)
    max_delay_s: float = Field(default=60.0, gt=0)
    factor: float = Field(default=2.0, gt=1.0)
    jitter: float = Field(default=0.25, ge=0, le=1)


class MarketConfig(_Model):
    venue: str = "bybit_linear"
    feed: str = "live"
    # Endpoints are left empty on purpose. An empty field is filled from the
    # selected venue's defaults (aurum/venues.py); a field set explicitly wins.
    # Hard-coding one venue's URLs as the default is how you end up pointing a
    # Bybit run at Binance's host and getting a 404 you have to go looking for.
    rest_base: str = ""
    ws_base: str = ""
    ws_path: str = ""
    rest_depth_path: str = ""
    rest_exchange_info_path: str = ""
    rest_time_path: str = ""
    #: Product category, where the venue requires one (Bybit does, Binance does not).
    category: str = ""
    symbols: list[SymbolSpec] = Field(default_factory=list)
    depth_levels: int = Field(default=20, ge=5, le=1000)
    depth_stream_speed: str = "100ms"
    snapshot_limit: int = Field(default=500, ge=100, le=1000)
    streams: list[str] = Field(default_factory=lambda: ["depth", "aggTrade", "bookTicker", "markPrice"])
    queue_size: int = Field(default=20_000, ge=1000)
    heartbeat_timeout_s: float = Field(default=20.0, gt=0)
    stale_after_ms: int = Field(default=3000, ge=250)
    resync_cooldown_s: float = Field(default=5.0, ge=0)
    max_resyncs_per_hour: int = Field(default=30, ge=1)
    reconnect: ReconnectConfig = Field(default_factory=ReconnectConfig)
    time_sync_interval_s: float = Field(default=300.0, gt=0)
    # Replay feed only; ignored when feed == "live".
    replay_path: str = ""
    replay_speed: float = Field(default=0.0, ge=0)  # 0 = as fast as possible

    @field_validator("feed")
    @classmethod
    def _feed(cls, v: str) -> str:
        v = v.strip().lower()
        if v not in {"live", "replay"}:
            raise ValueError("market.feed must be 'live' or 'replay'")
        return v

    @field_validator("depth_stream_speed")
    @classmethod
    def _speed(cls, v: str) -> str:
        if v not in {"100ms", "250ms", "500ms"}:
            raise ValueError("market.depth_stream_speed must be 100ms, 250ms or 500ms")
        return v

    @model_validator(mode="after")
    def _check_symbols(self) -> MarketConfig:
        if not self.symbols:
            raise ValueError("market.symbols must not be empty")
        seen = {s.symbol for s in self.symbols}
        if len(seen) != len(self.symbols):
            raise ValueError("market.symbols contains duplicates")
        return self

    @model_validator(mode="after")
    def _resolve_venue(self) -> MarketConfig:
        """Fill every empty endpoint from the selected venue's defaults.

        Uses ``object.__setattr__`` rather than plain assignment: this model has
        ``validate_assignment`` on, and assigning inside an ``after`` validator
        would re-enter validation and recurse.
        """
        from .venues import endpoints_for  # local import keeps the module cycle-free

        spec = endpoints_for(self.venue)
        for field in (
            "rest_base", "ws_base", "ws_path", "rest_depth_path",
            "rest_exchange_info_path", "rest_time_path", "category",
        ):
            if not getattr(self, field):
                object.__setattr__(self, field, getattr(spec, field))
        if self.snapshot_limit > spec.max_rest_depth:
            object.__setattr__(self, "snapshot_limit", spec.max_rest_depth)
        return self

    @field_validator("venue")
    @classmethod
    def _venue(cls, v: str) -> str:
        from .venues import VENUES

        v = v.strip().lower()
        if v not in VENUES:
            raise ValueError(f"unknown venue {v!r}; supported: {', '.join(sorted(VENUES))}")
        return v

    @property
    def symbol_names(self) -> list[str]:
        return [s.symbol for s in self.symbols]


class FeaturesConfig(_Model):
    cadence_ms: int = Field(default=250, ge=50, le=5000)
    return_horizons_ms: list[int] = Field(default_factory=lambda: [250, 500, 1000, 2000, 5000, 10000, 30000, 60000])
    volatility_windows_ms: list[int] = Field(default_factory=lambda: [1000, 5000, 10000, 30000, 60000])
    ofi_windows_ms: list[int] = Field(default_factory=lambda: [500, 1000, 5000, 10000])
    depth_levels: list[int] = Field(default_factory=lambda: [1, 5, 10])
    buffer_seconds: int = Field(default=900, ge=60)
    persist_every_n: int = Field(default=4, ge=1)

    # Deduplicated and ordered at parse time: downstream code indexes these
    # lists by position and names features after their values, so a config that
    # lists 5000 twice would otherwise produce two identical feature columns.
    @field_validator("return_horizons_ms", "volatility_windows_ms", "ofi_windows_ms", "depth_levels")
    @classmethod
    def _sorted_unique(cls, v: list[int]) -> list[int]:
        if not v:
            raise ValueError("must not be empty")
        if any(x <= 0 for x in v):
            raise ValueError("values must be positive")
        return sorted(set(v))


class RegimeConfig(_Model):
    vol_lookback_s: int = Field(default=300, ge=30)
    trend_lookback_s: int = Field(default=120, ge=10)
    low_vol_percentile: float = Field(default=33.0, ge=1, le=99)
    high_vol_percentile: float = Field(default=67.0, ge=1, le=99)

    @model_validator(mode="after")
    def _order(self) -> RegimeConfig:
        if self.low_vol_percentile >= self.high_vol_percentile:
            raise ValueError("regime.low_vol_percentile must be below high_vol_percentile")
        return self


class CrossMarketConfig(_Model):
    lead_lag_ms: list[int] = Field(
        default_factory=lambda: [100, 250, 500, 750, 1000, 2000, 3000, 5000, 10000, 30000, 60000]
    )
    window_s: int = Field(default=300, ge=30)
    min_samples: int = Field(default=240, ge=30)
    refresh_s: float = Field(default=15.0, gt=0)


class QualityConfig(_Model):
    min_score_to_trade: float = Field(default=0.60, ge=0, le=1)
    max_spread_bps: float = Field(default=20.0, gt=0)
    max_latency_ms: float = Field(default=1500.0, gt=0)
    min_book_levels: int = Field(default=5, ge=1)
    min_events_per_min: int = Field(default=30, ge=0)
    max_sequence_gaps_per_min: int = Field(default=3, ge=0)
    snapshot_max_age_s: float = Field(default=900.0, gt=0)


class FxConfig(_Model):
    usdt_per_eur: float = Field(default=1.08, gt=0)
    source: str = "static"


class WalletConfig(_Model):
    starting_balance_eur: float = Field(default=100.00, gt=0)
    currency: str = "EUR"


class RiskConfig(_Model):
    risk_per_trade_pct: float = Field(default=1.0, ge=0.10, le=2.00)
    max_concurrent_positions: int = Field(default=3, ge=1, le=10)
    max_exposure_pct: float = Field(default=150.0, gt=0, le=1000)
    max_symbol_exposure_pct: float = Field(default=60.0, gt=0, le=1000)
    daily_loss_limit_pct: float = Field(default=10.0, gt=0, le=100)
    max_drawdown_pct: float = Field(default=25.0, gt=0, le=100)
    wallet_failure_equity_pct: float = Field(default=40.0, gt=0, le=100)
    cooldown_s: float = Field(default=45.0, ge=0)
    duplicate_window_s: float = Field(default=30.0, ge=0)
    min_notional_eur: float = Field(default=5.0, gt=0)
    max_leverage: float = Field(default=3.0, ge=1.0, le=10.0)
    max_holding_s: float = Field(default=900.0, gt=0)


class CostsConfig(_Model):
    version: str = "cost-v1"
    taker_fee_bps: float = Field(default=4.5, ge=0)
    maker_fee_bps: float = Field(default=2.0, ge=0)
    extra_slippage_bps: float = Field(default=0.5, ge=0)
    latency_ms: float = Field(default=120.0, ge=0)
    latency_penalty_bps_per_100ms: float = Field(default=0.15, ge=0)
    impact_model: str = "book_walk"

    @field_validator("impact_model")
    @classmethod
    def _impact(cls, v: str) -> str:
        if v not in {"book_walk", "fixed"}:
            raise ValueError("costs.impact_model must be 'book_walk' or 'fixed'")
        return v


class ResearchConfig(_Model):
    enabled: bool = True
    cycle_interval_s: float = Field(default=45.0, gt=0)
    min_warmup_s: float = Field(default=120.0, ge=0)
    max_hypotheses_per_agent_per_cycle: int = Field(default=6, ge=1)
    max_open_hypotheses: int = Field(default=400, ge=10)
    min_samples_to_evaluate: int = Field(default=120, ge=10)
    memory_similarity_threshold: float = Field(default=0.90, ge=0, le=1)
    retest_cooldown_h: float = Field(default=6.0, ge=0)


class ValidationConfig(_Model):
    train_frac: float = Field(default=0.40, gt=0, lt=1)
    validation_frac: float = Field(default=0.20, gt=0, lt=1)
    holdout_frac: float = Field(default=0.20, gt=0, lt=1)
    walkforward_folds: int = Field(default=4, ge=2, le=20)
    embargo_multiple: float = Field(default=3.0, ge=0)
    min_fold_samples: int = Field(default=25, ge=5)
    fdr_alpha: float = Field(default=0.10, gt=0, lt=1)
    min_net_edge_bps: float = Field(default=0.5)
    min_profit_factor: float = Field(default=1.05, ge=1.0)
    max_drawdown_bps: float = Field(default=400.0, gt=0)
    shadow_min_signals: int = Field(default=25, ge=1)
    shadow_min_minutes: float = Field(default=20.0, ge=0)
    champion_replace_margin: float = Field(default=0.15, ge=0)
    degrade_edge_ratio: float = Field(default=0.40, ge=0, le=1)
    degrade_min_signals: int = Field(default=20, ge=1)

    @model_validator(mode="after")
    def _fracs(self) -> ValidationConfig:
        total = self.train_frac + self.validation_frac + self.holdout_frac
        if total >= 1.0:
            raise ValueError(
                "validation.train_frac + validation_frac + holdout_frac must leave room "
                f"for walk-forward folds (got {total:.2f})"
            )
        return self


class ExplorationConfig(_Model):
    """Trade on a schedule, with no validated edge, to exercise the live path.

    This is deliberately *not* a strategy and the code never pretends it is.
    Its purpose is the thing research cannot give you: an execution path that
    has actually executed, fills measured against the cost model on real books,
    wallet arithmetic that has moved, and a post-mortem with something in it.

    Everything it produces is tagged ``exploration`` — in the signal, in the
    database, and in every API response — so it can never be counted as
    evidence of an edge, and so the validated-performance figures stay clean.

    The expected outcome is a loss of roughly the round trip per trade. That is
    not a defect: it is the measurement. If forced trading were profitable, the
    research gates would have found the edge and promoted a champion.
    """

    enabled: bool = False
    #: Target entries per day. The scheduler paces to 86400/this, and the risk
    #: caps (concurrent positions, cooldown, exposure) still bind — so this is
    #: a ceiling, not a promise.
    trades_per_day: int = Field(default=250, ge=1, le=5000)
    #: How long an exploration position is held before it is closed on horizon.
    #: Short holds are what make the target rate reachable inside a handful of
    #: concurrent slots: 250/day needs one entry every ~345 s.
    hold_s: float = Field(default=120.0, gt=0)
    #: Sized far smaller than a real signal by default. 250 round trips a day
    #: at the normal 1% risk would end the cycle on costs alone before the day
    #: was out, and a cycle that fails for that reason measures nothing.
    risk_per_trade_pct: float = Field(default=0.10, ge=0.01, le=2.00)
    #: Empty means every configured symbol, taken round-robin.
    symbols: list[str] = Field(default_factory=list)
    #: Seed for the direction draw, so a run can be reproduced exactly.
    seed: int = 7

    @property
    def interval_s(self) -> float:
        return 86_400.0 / float(self.trades_per_day)


class RetentionConfig(_Model):
    market_events_hours: float = Field(default=6.0, gt=0)
    features_hours: float = Field(default=24.0, gt=0)
    orderbook_snapshots_hours: float = Field(default=6.0, gt=0)
    aggregate_before_delete: bool = True


class StorageConfig(_Model):
    driver: str = "sqlite"
    url: str = ""
    batch_size: int = Field(default=500, ge=1)
    flush_interval_ms: int = Field(default=500, ge=10)
    queue_size: int = Field(default=50_000, ge=100)
    retention: RetentionConfig = Field(default_factory=RetentionConfig)
    vacuum_interval_h: float = Field(default=24.0, gt=0)
    #: Raw events are the source of truth for reconstruction and research, so
    #: they are stored by default; retention bounds the cost.  Turn this off on
    #: a small disk and the feature snapshots remain, but event-level replay of
    #: a past incident stops being possible.
    persist_market_events: bool = True
    persist_orderbook_every_n: int = Field(default=5, ge=1)

    @field_validator("driver")
    @classmethod
    def _driver(cls, v: str) -> str:
        v = v.strip().lower()
        if v not in {"sqlite", "postgres"}:
            raise ValueError("storage.driver must be 'sqlite' or 'postgres'")
        return v


class DiagnosticsConfig(_Model):
    rejection_window_s: float = Field(default=3600.0, gt=0)
    export_dir: str = "./data/exports"


class Config(_Model):
    app: AppConfig = Field(default_factory=AppConfig)
    api: ApiConfig = Field(default_factory=ApiConfig)
    market: MarketConfig
    features: FeaturesConfig = Field(default_factory=FeaturesConfig)
    regime: RegimeConfig = Field(default_factory=RegimeConfig)
    cross_market: CrossMarketConfig = Field(default_factory=CrossMarketConfig)
    quality: QualityConfig = Field(default_factory=QualityConfig)
    fx: FxConfig = Field(default_factory=FxConfig)
    wallet: WalletConfig = Field(default_factory=WalletConfig)
    risk: RiskConfig = Field(default_factory=RiskConfig)
    costs: CostsConfig = Field(default_factory=CostsConfig)
    research: ResearchConfig = Field(default_factory=ResearchConfig)
    validation: ValidationConfig = Field(default_factory=ValidationConfig)
    exploration: ExplorationConfig = Field(default_factory=ExplorationConfig)
    storage: StorageConfig = Field(default_factory=StorageConfig)
    diagnostics: DiagnosticsConfig = Field(default_factory=DiagnosticsConfig)

    # ---------------------------------------------------------------- derived

    @model_validator(mode="after")
    def _production_rules(self) -> Config:
        if self.app.env == "production" and self.market.feed != "live":
            raise ValueError(
                "market.feed='replay' is refused when app.env='production': the production "
                "runtime has no synthetic market-data path."
            )
        return self

    @property
    def data_dir(self) -> Path:
        return Path(self.app.data_dir).expanduser().resolve()

    @property
    def db_url(self) -> str:
        if self.storage.url:
            return self.storage.url
        return f"sqlite:///{self.data_dir / 'aurum.db'}"

    @property
    def export_dir(self) -> Path:
        return Path(self.diagnostics.export_dir).expanduser().resolve()

    def public_dict(self) -> dict[str, Any]:
        """Config as served by ``/config`` — no secrets live in this file, but the
        database URL can carry credentials under PostgreSQL, so it is redacted."""
        data = self.model_dump(mode="json")
        url = self.db_url
        if "@" in url:
            scheme, _, rest = url.partition("://")
            url = f"{scheme}://***@{rest.rpartition('@')[2]}"
        data["storage"]["resolved_url"] = url
        data["storage"]["url"] = "***" if self.storage.url and "@" in self.storage.url else self.storage.url
        return data


# --------------------------------------------------------------------------
# Runtime-settable values.  The frontend Settings page may change these and
# nothing else; each carries a hard server-side bound.
# --------------------------------------------------------------------------


@dataclass(frozen=True)
class Settable:
    path: str
    kind: type
    minimum: float | None = None
    maximum: float | None = None
    note: str = ""


SETTABLE: tuple[Settable, ...] = (
    Settable("risk.risk_per_trade_pct", float, 0.10, 2.00, "Percent of equity risked per paper trade"),
    Settable("risk.max_concurrent_positions", int, 1, 10, "Simultaneous open paper positions"),
    Settable("risk.max_exposure_pct", float, 1.0, 300.0, "Total notional exposure cap, % of equity"),
    Settable("risk.max_symbol_exposure_pct", float, 1.0, 300.0, "Per-symbol notional exposure cap, % of equity"),
    Settable("risk.daily_loss_limit_pct", float, 1.0, 100.0, "Daily loss circuit breaker"),
    Settable("risk.max_drawdown_pct", float, 1.0, 100.0, "Cycle drawdown circuit breaker"),
    Settable("risk.cooldown_s", float, 0.0, 3600.0, "Per-symbol cooldown after a signal"),
    Settable("quality.min_score_to_trade", float, 0.0, 1.0, "Data-quality score required to enter"),
    Settable("quality.max_spread_bps", float, 0.1, 500.0, "Maximum spread allowed at entry"),
    Settable("research.enabled", bool, None, None, "Run research cycles"),
    Settable("research.cycle_interval_s", float, 5.0, 3600.0, "Seconds between research cycles"),
)

SETTABLE_BY_PATH: dict[str, Settable] = {s.path: s for s in SETTABLE}


class ConfigError(ValueError):
    """Raised for an invalid configuration file or an out-of-bounds setting."""


def _coerce(raw: str) -> Any:
    """Turn an environment string into the YAML scalar it denotes."""
    try:
        return yaml.safe_load(raw)
    except yaml.YAMLError:
        return raw


def _set_path(tree: dict[str, Any], dotted: str, value: Any) -> None:
    parts = dotted.split(".")
    node = tree
    for part in parts[:-1]:
        nxt = node.get(part)
        if not isinstance(nxt, dict):
            nxt = {}
            node[part] = nxt
        node = nxt
    node[parts[-1]] = value


def _get_path(obj: Any, dotted: str) -> Any:
    node = obj
    for part in dotted.split("."):
        node = getattr(node, part)
    return node


def env_overrides(environ: dict[str, str] | None = None) -> dict[str, Any]:
    """Collect ``AURUM_SET__a.b=c`` and alias overrides into a nested dict."""
    environ = dict(os.environ if environ is None else environ)
    tree: dict[str, Any] = {}
    for alias, path in ENV_ALIASES.items():
        if alias in environ:
            _set_path(tree, path, _coerce(environ[alias]))
    for key, value in environ.items():
        if key.startswith(ENV_PREFIX):
            path = key[len(ENV_PREFIX) :]
            if path:
                # Both separators are accepted. A shell cannot set a variable
                # whose name contains a dot with the inline `VAR=x cmd` form —
                # bash rejects it as an invalid identifier — so the dotted style
                # only works through `env`. The double underscore always works.
                _set_path(tree, path.replace("__", "."), _coerce(value))
    return tree


def deep_merge(base: dict[str, Any], overlay: dict[str, Any]) -> dict[str, Any]:
    out = dict(base)
    for key, value in overlay.items():
        if isinstance(value, dict) and isinstance(out.get(key), dict):
            out[key] = deep_merge(out[key], value)
        else:
            out[key] = value
    return out


def default_config_path() -> Path:
    return Path(__file__).resolve().parent.parent / "config.yaml"


def load_config(
    path: str | Path | None = None,
    *,
    overrides: dict[str, Any] | None = None,
    use_env: bool = True,
) -> Config:
    """Read ``config.yaml``, layer environment overrides, validate."""
    cfg_path = Path(path) if path else default_config_path()
    if not cfg_path.exists():
        raise ConfigError(f"config file not found: {cfg_path}")
    with cfg_path.open("r", encoding="utf-8-sig") as fh:
        raw = yaml.safe_load(fh) or {}
    if not isinstance(raw, dict):
        raise ConfigError(f"{cfg_path} must contain a YAML mapping")
    if use_env:
        raw = deep_merge(raw, env_overrides())
    if overrides:
        raw = deep_merge(raw, overrides)
    try:
        return Config.model_validate(raw)
    except Exception as exc:  # pydantic ValidationError, ValueError
        raise ConfigError(f"invalid configuration in {cfg_path}: {exc}") from exc


def apply_setting(config: Config, path: str, value: Any) -> Any:
    """Apply one runtime setting, enforcing the server-side bound.

    Returns the coerced value that was stored.  Raises :class:`ConfigError` for
    an unknown path or a value outside its hard bounds — the frontend cannot
    widen a limit by asking nicely.
    """
    spec = SETTABLE_BY_PATH.get(path)
    if spec is None:
        raise ConfigError(f"'{path}' is not a runtime-settable value")
    if spec.kind is bool:
        if isinstance(value, str):
            coerced: Any = value.strip().lower() in {"1", "true", "yes", "on"}
        else:
            coerced = bool(value)
    else:
        try:
            coerced = spec.kind(value)
        except (TypeError, ValueError) as exc:
            raise ConfigError(f"'{path}' expects {spec.kind.__name__}, got {value!r}") from exc
        if spec.minimum is not None and coerced < spec.minimum:
            raise ConfigError(f"'{path}' must be >= {spec.minimum} (got {coerced})")
        if spec.maximum is not None and coerced > spec.maximum:
            raise ConfigError(f"'{path}' must be <= {spec.maximum} (got {coerced})")

    section_path, _, leaf = path.rpartition(".")
    section = _get_path(config, section_path) if section_path else config
    setattr(section, leaf, coerced)  # validate_assignment re-runs the field checks
    return coerced


def settable_report(config: Config) -> list[dict[str, Any]]:
    """What the Settings page may change, with current values and bounds."""
    report = []
    for spec in SETTABLE:
        report.append(
            {
                "path": spec.path,
                "value": _get_path(config, spec.path),
                "type": spec.kind.__name__,
                "min": spec.minimum,
                "max": spec.maximum,
                "note": spec.note,
            }
        )
    return report


def describe_symbols(config: Config) -> Iterable[dict[str, str]]:
    for spec in config.market.symbols:
        yield {"symbol": spec.symbol, "tier": spec.tier, "role": spec.role, "use": spec.use}
