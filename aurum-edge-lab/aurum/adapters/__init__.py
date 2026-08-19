"""Venue adapters.

``build_feed`` is the only place that decides which adapter runs, and it refuses
anything but a live feed under ``app.env=production``.
"""

from __future__ import annotations

from ..config import Config
from .base import DepthSnapshot, DepthUpdate, FeedStats, MarketFeed
from .binance_futures import BinanceFuturesFeed
from .replay import ReplayFeed


def build_feed(config: Config) -> MarketFeed:
    symbols = config.market.symbol_names
    if config.market.feed == "replay":
        if config.app.env == "production":  # belt and braces; config validation also refuses this
            raise RuntimeError("replay feed is not permitted in production")
        if not config.market.replay_path:
            raise ValueError("market.feed='replay' requires market.replay_path")
        return ReplayFeed(symbols, config.market.replay_path, speed=config.market.replay_speed)

    if config.market.venue != "binance_usdm":
        raise ValueError(f"no adapter for venue {config.market.venue!r}")

    return BinanceFuturesFeed(
        symbols,
        rest_base=config.market.rest_base,
        ws_base=config.market.ws_base,
        ws_path=config.market.ws_path,
        depth_path=config.market.rest_depth_path,
        exchange_info_path=config.market.rest_exchange_info_path,
        time_path=config.market.rest_time_path,
        depth_speed=config.market.depth_stream_speed,
        streams=config.market.streams,
        heartbeat_timeout_s=config.market.heartbeat_timeout_s,
        reconnect_initial_s=config.market.reconnect.initial_delay_s,
        reconnect_max_s=config.market.reconnect.max_delay_s,
        reconnect_factor=config.market.reconnect.factor,
        reconnect_jitter=config.market.reconnect.jitter,
    )


__all__ = [
    "BinanceFuturesFeed",
    "DepthSnapshot",
    "DepthUpdate",
    "FeedStats",
    "MarketFeed",
    "ReplayFeed",
    "build_feed",
]
