from __future__ import annotations

import os

import pytest

# Tests must never touch a live venue or the production database.
os.environ.setdefault("ENV", "test")
os.environ.setdefault("EXCHANGES", "synthetic")
os.environ.setdefault("ALLOW_SYNTHETIC_SOURCE", "true")
os.environ.setdefault("MIN_WARMUP_SECONDS", "0")
os.environ.setdefault("LOG_LEVEL", "WARNING")
os.environ.setdefault(
    "DATABASE_URL",
    "postgresql+asyncpg://postgres:postgres@localhost:5432/btcquant_test",
)

from app.config import Settings  # noqa: E402


@pytest.fixture
def settings() -> Settings:
    return Settings(
        env="test",
        exchanges="synthetic",
        allow_synthetic_source=True,
        min_warmup_seconds=0.0,
        signal_min_confidence=0.55,
        signal_cooldown_ms=0,
        feature_interval_ms=50,
    )


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


def pytest_configure(config):
    config.addinivalue_line("markers", "integration: needs a live PostgreSQL")
