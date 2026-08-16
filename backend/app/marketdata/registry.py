"""Adapter factory. Add a venue here once its adapter exists."""

from __future__ import annotations

from typing import Callable

from app.config import Settings
from app.marketdata.base import ExchangeAdapter
from app.marketdata.binance import BinanceFuturesAdapter, BinanceSpotAdapter
from app.marketdata.coinbase import CoinbaseAdapter
from app.marketdata.synthetic import SyntheticAdapter

Factory = Callable[[Settings], ExchangeAdapter]


def _binance_spot(s: Settings) -> ExchangeAdapter:
    return BinanceSpotAdapter(
        symbol=s.symbol,
        ws_base=s.binance_ws_base,
        rest_base=s.binance_rest_base,
        depth_speed_ms=s.orderbook_stream_speed_ms,
        rest_timeout_s=s.rest_timeout_s,
        stale_timeout_s=s.ws_stale_timeout_s,
    )


def _binance_futures(s: Settings) -> ExchangeAdapter:
    return BinanceFuturesAdapter(
        symbol=s.symbol,
        ws_base=s.binance_futures_ws_base,
        rest_base=s.binance_futures_rest_base,
        rest_timeout_s=s.rest_timeout_s,
    )


def _coinbase(s: Settings) -> ExchangeAdapter:
    return CoinbaseAdapter(
        symbol=s.symbol,
        ws_base=s.coinbase_ws_base,
        rest_base=s.coinbase_rest_base,
        rest_timeout_s=s.rest_timeout_s,
    )


def _synthetic(s: Settings) -> ExchangeAdapter:
    if not s.allow_synthetic_source:
        raise RuntimeError(
            "The synthetic source is disabled. It emits MODEL-GENERATED data, "
            "not market data. Set ALLOW_SYNTHETIC_SOURCE=true only for offline "
            "development or tests."
        )
    return SyntheticAdapter(symbol=s.symbol, tick_size=s.tick_size)


REGISTRY: dict[str, Factory] = {
    "binance_spot": _binance_spot,
    "binance_futures": _binance_futures,
    "coinbase": _coinbase,
    "synthetic": _synthetic,
}


def build_adapter(name: str, settings: Settings) -> ExchangeAdapter:
    try:
        factory = REGISTRY[name]
    except KeyError as exc:
        raise ValueError(
            f"unknown exchange '{name}'. Available: {sorted(REGISTRY)}"
        ) from exc
    return factory(settings)


def available_exchanges() -> list[str]:
    return sorted(REGISTRY)
