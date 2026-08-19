"""Adapter tests.

No network is touched.  The wire formats below are the shapes the Binance USD-M
public streams document; testing ``_normalise`` against them is what catches a
field being read from the wrong key, which is the failure mode that produces
plausible-looking numbers rather than an error.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from aurum.adapters import build_feed
from aurum.adapters.binance_futures import BinanceFuturesFeed
from aurum.adapters.replay import ReplayFeed
from aurum.config import Config, ConfigError, load_config
from aurum.domain import EventKind, Side

ROOT = Path(__file__).resolve().parent.parent


def feed() -> BinanceFuturesFeed:
    return BinanceFuturesFeed(
        ["BTCUSDT", "ETHUSDT"],
        rest_base="https://fapi.binance.com",
        ws_base="wss://fstream.binance.com",
    )


def test_stream_names_cover_every_symbol_and_stream() -> None:
    names = feed().stream_names()
    assert "btcusdt@depth@100ms" in names
    assert "btcusdt@aggTrade" in names
    assert "btcusdt@bookTicker" in names
    assert "btcusdt@markPrice@1s" in names
    assert len(names) == 8  # 2 symbols x 4 streams


def test_endpoints_come_from_config_not_constants() -> None:
    custom = BinanceFuturesFeed(
        ["BTCUSDT"], rest_base="https://testnet.binancefuture.com", ws_base="wss://stream.binancefuture.com"
    )
    url = custom._shard_url(custom._shards()[0])
    assert url.startswith("wss://stream.binancefuture.com/stream?streams=")
    assert custom.rest_base == "https://testnet.binancefuture.com"


def test_depth_update_keeps_the_three_sequence_fields() -> None:
    event = feed()._normalise(
        {
            "e": "depthUpdate", "E": 1_700_000_000_100, "T": 1_700_000_000_050, "s": "BTCUSDT",
            "U": 157, "u": 160, "pu": 149,
            "b": [["60000.10", "1.5"]], "a": [["60001.20", "0.0"]],
        },
        recv_ms=1_700_000_000_120,
    )
    assert event is not None and event.kind is EventKind.DEPTH
    assert event.ts_ms == 1_700_000_000_050  # transaction time, not event time
    assert event.latency_ms == 70
    assert event.payload["U"] == 157 and event.payload["u"] == 160 and event.payload["pu"] == 149
    assert event.payload["b"] == [(60000.10, 1.5)]
    assert event.payload["a"] == [(60001.20, 0.0)]  # a zero is a deletion, kept as such


def test_aggtrade_maker_flag_is_inverted_into_the_aggressor() -> None:
    # m=True means the buyer was the maker, so the aggressor was the SELLER.
    seller = feed()._normalise(
        {"e": "aggTrade", "E": 2, "T": 1, "s": "BTCUSDT", "a": 5, "p": "60000", "q": "0.5", "m": True},
        recv_ms=3,
    )
    buyer = feed()._normalise(
        {"e": "aggTrade", "E": 2, "T": 1, "s": "BTCUSDT", "a": 6, "p": "60000", "q": "0.5", "m": False},
        recv_ms=3,
    )
    assert seller is not None and seller.payload["aggressor"] == Side.SELL.value
    assert buyer is not None and buyer.payload["aggressor"] == Side.BUY.value


def test_book_ticker_and_mark_price_normalise() -> None:
    ticker = feed()._normalise(
        {"e": "bookTicker", "u": 400, "E": 5, "T": 4, "s": "ETHUSDT",
         "b": "2500.1", "B": "3", "a": "2500.5", "A": "4"},
        recv_ms=6,
    )
    assert ticker is not None and ticker.kind is EventKind.BOOK_TICKER
    assert ticker.payload == {"bid": 2500.1, "bid_qty": 3.0, "ask": 2500.5, "ask_qty": 4.0, "update_id": 400}

    mark = feed()._normalise(
        {"e": "markPriceUpdate", "E": 10, "s": "BTCUSDT", "p": "60010.5", "i": "60000.0",
         "P": "60005.0", "r": "0.0001", "T": 99},
        recv_ms=11,
    )
    assert mark is not None and mark.kind is EventKind.MARK_PRICE
    assert mark.payload["mark_price"] == 60010.5
    assert mark.payload["funding_rate"] == 0.0001


def test_unknown_event_types_are_ignored_not_guessed() -> None:
    assert feed()._normalise({"e": "forceOrder", "s": "BTCUSDT", "o": {}}, recv_ms=1) is None
    assert feed()._normalise({"e": "depthUpdate"}, recv_ms=1) is None  # no symbol


def test_combined_and_raw_frames_both_produce_events() -> None:
    adapter = feed()
    seen = []
    adapter.on_event(seen.append)
    inner = {"e": "aggTrade", "E": 2, "T": 1, "s": "BTCUSDT", "a": 5, "p": "1", "q": "1", "m": False}
    adapter._handle_frame(json.dumps({"stream": "btcusdt@aggTrade", "data": inner}))
    adapter._handle_frame(json.dumps(inner))
    assert len(seen) == 2


def test_unparseable_frame_is_counted_not_raised() -> None:
    adapter = feed()
    adapter._handle_frame("{not json")
    assert adapter.stats.errors == 1


def test_replay_feed_marks_every_event_as_not_live(tmp_path: Path) -> None:
    path = tmp_path / "replay.jsonl"
    path.write_text(
        "\n".join(
            [
                json.dumps({"aurum_replay": 1, "venue": "test"}),
                json.dumps({"symbol": "BTCUSDT", "kind": "snapshot", "ts_ms": 0,
                            "payload": {"lastUpdateId": 1, "bids": [[10.0, 1.0]], "asks": [[11.0, 1.0]]}}),
                json.dumps({"symbol": "BTCUSDT", "kind": "trade", "ts_ms": 5,
                            "payload": {"price": 10.5, "qty": 1.0, "aggressor": "BUY"}}),
            ]
        ),
        encoding="utf-8",
    )
    replay = ReplayFeed(["BTCUSDT"], path)
    assert replay.load() == 1
    assert replay.kind == "replay"
    event = replay._to_event({"symbol": "BTCUSDT", "kind": "trade", "ts_ms": 5, "payload": {}})
    assert event is not None and event.source == "replay"


def test_production_cannot_build_a_replay_feed(tmp_path: Path) -> None:
    with pytest.raises(ConfigError):
        load_config(
            ROOT / "config.yaml",
            use_env=False,
            overrides={
                "app": {"env": "production", "data_dir": str(tmp_path)},
                "market": {"feed": "replay", "replay_path": "x.jsonl"},
            },
        )


def test_build_feed_selects_the_configured_adapter(config: Config, tmp_path: Path) -> None:
    assert isinstance(build_feed(config), BinanceFuturesFeed)
    config.market.feed = "replay"
    with pytest.raises(ValueError, match="replay_path"):
        build_feed(config)
    config.market.replay_path = str(tmp_path / "x.jsonl")
    assert isinstance(build_feed(config), ReplayFeed)
