from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from aurum.config import (
    Config,
    ConfigError,
    apply_setting,
    env_overrides,
    load_config,
    settable_report,
)

ROOT = Path(__file__).resolve().parent.parent


def test_default_config_loads_ten_symbols(config: Config) -> None:
    assert len(config.market.symbols) == 10
    assert config.market.symbol_names[:2] == ["BTCUSDT", "ETHUSDT"]
    tiers = {s.tier for s in config.market.symbols}
    assert tiers == {"CORE", "MAJOR", "DYNAMIC"}


def test_wallet_starts_at_one_hundred_euro(config: Config) -> None:
    assert config.wallet.starting_balance_eur == 100.00
    assert config.wallet.currency == "EUR"


def test_env_override_accepts_both_separators() -> None:
    tree = env_overrides({"AURUM_SET__api.port": "9100", "AURUM_FEED": "replay"})
    assert tree["api"]["port"] == 9100
    assert tree["market"]["feed"] == "replay"

    # A shell cannot set a name containing a dot with `VAR=x cmd`, so the
    # double-underscore form has to work identically.
    underscored = env_overrides({"AURUM_SET__market__replay_speed": "8"})
    assert underscored["market"]["replay_speed"] == 8


def test_production_refuses_replay_feed(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match="no synthetic market-data path"):
        load_config(
            ROOT / "config.yaml",
            use_env=False,
            overrides={"app": {"env": "production", "data_dir": str(tmp_path)}, "market": {"feed": "replay"}},
        )


def test_unknown_key_is_rejected(tmp_path: Path) -> None:
    bad = tmp_path / "bad.yaml"
    base = yaml.safe_load((ROOT / "config.yaml").read_text(encoding="utf-8-sig"))
    base["risk"]["make_me_rich"] = True
    bad.write_text(yaml.safe_dump(base), encoding="utf-8")
    with pytest.raises(ConfigError):
        load_config(bad, use_env=False)


def test_settings_are_bounded_server_side(config: Config) -> None:
    assert apply_setting(config, "risk.risk_per_trade_pct", 1.5) == 1.5
    assert config.risk.risk_per_trade_pct == 1.5

    with pytest.raises(ConfigError, match="must be <= 2.0"):
        apply_setting(config, "risk.risk_per_trade_pct", 25.0)
    # The rejected value must not have been applied.
    assert config.risk.risk_per_trade_pct == 1.5

    with pytest.raises(ConfigError, match="not a runtime-settable value"):
        apply_setting(config, "wallet.starting_balance_eur", 1_000_000)
    assert config.wallet.starting_balance_eur == 100.00


def test_settable_report_exposes_bounds(config: Config) -> None:
    report = {row["path"]: row for row in settable_report(config)}
    assert report["risk.risk_per_trade_pct"]["max"] == 2.00
    assert report["quality.min_score_to_trade"]["min"] == 0.0


def test_validation_fractions_must_leave_room_for_folds(tmp_path: Path) -> None:
    with pytest.raises(ConfigError):
        load_config(
            ROOT / "config.yaml",
            use_env=False,
            overrides={
                "app": {"data_dir": str(tmp_path)},
                "validation": {"train_frac": 0.5, "validation_frac": 0.3, "holdout_frac": 0.3},
            },
        )
