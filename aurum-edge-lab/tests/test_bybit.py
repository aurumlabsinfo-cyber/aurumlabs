"""Bybit V5 adapter.

No network is touched. The wire shapes below are the ones Bybit's public V5
streams document, and testing ``_normalise`` against them is what catches a
field read from the wrong key — the failure that produces plausible numbers
rather than an error.

The four tests that matter most cover the four ways Bybit differs from Binance:
the snapshot arrives on the socket, the sequence is a single counter, the
heartbeat is ours to send, and the taker side is stated rather than inverted.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from aurum.adapters import build_feed
from aurum.adapters.binance_futures import BinanceFuturesFeed
from aurum.adapters.bybit_linear import PING_INTERVAL_S, BybitLinearFeed
from aurum.config import Config, ConfigError, load_config
from aurum.domain import EventKind, Side
from aurum.venues import VENUES, ws_depth_for

ROOT = Path(__file__).resolve().parent.parent


def feed(**kwargs) -> BybitLinearFeed:
    defaults = {
        "rest_base": "https://api.bybit.com",
        "ws_base": "wss://stream.bybit.com",
        "ws_path": "/v5/public/linear",
        "category": "linear",
        "ws_depth": 50,
    }
    defaults.update(kwargs)
    return BybitLinearFeed(["BTCUSDT", "ETHUSDT"], **defaults)


# ------------------------------------------------------------------- venues


def test_every_venue_has_a_complete_endpoint_set() -> None:
    for name, spec in VENUES.items():
        assert spec.rest_base.startswith("https://"), name
        assert spec.ws_base.startswith("wss://"), name
        assert spec.rest_depth_path and spec.rest_time_path, name
        assert spec.docs, f"{name} has no documentation link to check against"


def test_switching_venue_cannot_leave_the_other_venues_urls_behind(tmp_path: Path) -> None:
    """The configuration mistake this system most needs to prevent."""
    bybit = load_config(
        ROOT / "config.yaml", use_env=False,
        overrides={"app": {"data_dir": str(tmp_path)}, "market": {"venue": "bybit_linear"}},
    )
    assert bybit.market.rest_base == "https://api.bybit.com"
    assert bybit.market.ws_path == "/v5/public/linear"
    assert bybit.market.category == "linear"

    binance = load_config(
        ROOT / "config.yaml", use_env=False,
        overrides={"app": {"data_dir": str(tmp_path)}, "market": {"venue": "binance_usdm"}},
    )
    assert binance.market.rest_base == "https://fapi.binance.com"
    assert binance.market.ws_path == "/stream"
    assert binance.market.category == ""


def test_an_explicit_endpoint_still_wins(tmp_path: Path) -> None:
    config = load_config(
        ROOT / "config.yaml", use_env=False,
        overrides={
            "app": {"data_dir": str(tmp_path)},
            "market": {"venue": "bybit_linear", "rest_base": "https://my-proxy.internal"},
        },
    )
    assert config.market.rest_base == "https://my-proxy.internal"
    assert config.market.ws_base == "wss://stream.bybit.com"  # the rest still resolved


def test_an_unknown_venue_is_refused(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match="unknown venue"):
        load_config(
            ROOT / "config.yaml", use_env=False,
            overrides={"app": {"data_dir": str(tmp_path)}, "market": {"venue": "ftx"}},
        )


def test_snapshot_limit_is_clamped_to_what_the_venue_allows(tmp_path: Path) -> None:
    config = load_config(
        ROOT / "config.yaml", use_env=False,
        overrides={
            "app": {"data_dir": str(tmp_path)},
            "market": {"venue": "bybit_linear", "snapshot_limit": 1000},
        },
    )
    assert config.market.snapshot_limit == 500, "Bybit rejects a REST depth above 500"


def test_ws_depth_picks_the_smallest_book_that_covers_the_features() -> None:
    # Deeper is not better on Bybit: depth 50 pushes every 20 ms, depth 200
    # every 100 ms, so taking 200 "to be safe" makes the book five times staler.
    assert ws_depth_for("bybit_linear", 20) == 50
    assert ws_depth_for("bybit_linear", 1) == 1
    assert ws_depth_for("bybit_linear", 60) == 200
    assert ws_depth_for("bybit_linear", 9999) == 500
    # Binance does not make you choose one.
    assert ws_depth_for("binance_usdm", 20) == 20


def test_build_feed_selects_the_adapter_from_the_venue(config: Config) -> None:
    config.market.venue = "bybit_linear"
    assert isinstance(build_feed(config), BybitLinearFeed)
    config.market.venue = "binance_usdm"
    assert isinstance(build_feed(config), BinanceFuturesFeed)


# -------------------------------------------------------------------- topics


def test_topics_cover_every_symbol_without_duplicating_tickers() -> None:
    topics = feed().topics()
    assert "orderbook.50.BTCUSDT" in topics
    assert "publicTrade.BTCUSDT" in topics
    # markPrice and bookTicker both map onto Bybit's single tickers topic, and
    # subscribing twice would double every ticker event.
    assert topics.count("tickers.BTCUSDT") == 1
    assert len(topics) == 6  # 2 symbols x (orderbook + publicTrade + tickers)
    assert feed().ws_url() == "wss://stream.bybit.com/v5/public/linear"


# ----------------------------------------------------------------- orderbook


def test_a_streamed_snapshot_becomes_a_snapshot_event() -> None:
    """Bybit seeds the book on the socket; there is no REST pull to splice."""
    events = feed()._normalise(
        {
            "topic": "orderbook.50.BTCUSDT",
            "type": "snapshot",
            "ts": 1_700_000_000_200,
            "cts": 1_700_000_000_100,
            "data": {"s": "BTCUSDT", "b": [["60000.10", "1.5"]], "a": [["60001.20", "2.0"]],
                     "u": 480, "seq": 99},
        },
        recv_ms=1_700_000_000_250,
    )
    assert len(events) == 1
    event = events[0]
    assert event.kind is EventKind.SNAPSHOT
    assert event.ts_ms == 1_700_000_000_100, "cts is closer to the match than ts"
    assert event.payload["lastUpdateId"] == 480
    assert event.payload["bids"] == [(60000.10, 1.5)]


def test_a_delta_maps_the_single_counter_onto_the_sequence_fields() -> None:
    """Bybit's ``u`` increments by one; the book validates it like Binance's pu."""
    events = feed()._normalise(
        {
            "topic": "orderbook.50.BTCUSDT",
            "type": "delta",
            "cts": 1_700_000_000_300,
            "data": {"s": "BTCUSDT", "b": [["60000.10", "0"]], "a": [], "u": 481},
        },
        recv_ms=1_700_000_000_310,
    )
    assert len(events) == 1
    payload = events[0].payload
    assert events[0].kind is EventKind.DEPTH
    assert (payload["U"], payload["u"], payload["pu"]) == (481, 481, 480)
    assert payload["b"] == [(60000.10, 0.0)], "a zero size is a deletion, kept as such"


def test_update_id_one_is_treated_as_a_restart_snapshot() -> None:
    events = feed()._normalise(
        {
            "topic": "orderbook.50.BTCUSDT",
            "type": "delta",  # Bybit may still label it delta
            "cts": 1,
            "data": {"s": "BTCUSDT", "b": [["1", "1"]], "a": [["2", "1"]], "u": 1},
        },
        recv_ms=2,
    )
    assert events[0].kind is EventKind.SNAPSHOT, "u=1 means the topic restarted"


def test_a_bybit_delta_chain_is_accepted_by_the_order_book() -> None:
    """End to end: the mapping has to satisfy the book's own validation."""
    from aurum.adapters.base import DepthSnapshot
    from aurum.market.orderbook import OrderBook, update_from_event

    adapter = feed()
    book = OrderBook("BTCUSDT")

    snap = adapter._normalise(
        {"topic": "orderbook.50.BTCUSDT", "type": "snapshot", "cts": 1000,
         "data": {"s": "BTCUSDT", "b": [["100", "2"], ["99", "3"]], "a": [["101", "1"]], "u": 10}},
        recv_ms=1005,
    )[0]
    assert book.apply_snapshot(
        DepthSnapshot("BTCUSDT", int(snap.payload["lastUpdateId"]), snap.ts_ms, snap.recv_ms,
                      list(snap.payload["bids"]), list(snap.payload["asks"]))
    )

    for update_id, bid in ((11, "2.5"), (12, "2.7"), (13, "2.9")):
        event = adapter._normalise(
            {"topic": "orderbook.50.BTCUSDT", "type": "delta", "cts": 1000 + update_id,
             "data": {"s": "BTCUSDT", "b": [["100", bid]], "a": [], "u": update_id}},
            recv_ms=1010 + update_id,
        )[0]
        assert book.apply_update(update_from_event("BTCUSDT", event.ts_ms, event.recv_ms, event.payload))

    assert book.ready and book.last_update_id == 13
    assert book.top(2).bids[0].qty == 2.9

    # A skipped update must break the chain, exactly as a Binance pu gap does.
    gap = adapter._normalise(
        {"topic": "orderbook.50.BTCUSDT", "type": "delta", "cts": 2000,
         "data": {"s": "BTCUSDT", "b": [["100", "5"]], "a": [], "u": 20}},
        recv_ms=2005,
    )[0]
    assert not book.apply_update(update_from_event("BTCUSDT", gap.ts_ms, gap.recv_ms, gap.payload))
    assert book.stats.sequence_gaps == 1


# -------------------------------------------------------------------- trades


def test_the_taker_side_is_read_directly_not_inverted() -> None:
    """Bybit states the aggressor; Binance states the maker. Confusing the two
    inverts every order-flow feature while leaving them all plausible."""
    events = feed()._normalise(
        {
            "topic": "publicTrade.BTCUSDT",
            "ts": 1_700_000_000_000,
            "data": [
                {"T": 1_700_000_000_000, "s": "BTCUSDT", "S": "Buy", "v": "0.5", "p": "60000",
                 "i": "abc-123"},
                {"T": 1_700_000_000_001, "s": "BTCUSDT", "S": "Sell", "v": "0.25", "p": "59999",
                 "i": "abc-124"},
            ],
        },
        recv_ms=1_700_000_000_010,
    )
    assert len(events) == 2
    assert events[0].payload["aggressor"] == Side.BUY.value
    assert events[0].payload["qty"] == 0.5
    assert events[1].payload["aggressor"] == Side.SELL.value

    # The same economic event on Binance arrives as m=False for a buy.
    binance = BinanceFuturesFeed(
        ["BTCUSDT"], rest_base="https://fapi.binance.com", ws_base="wss://fstream.binance.com"
    )
    equivalent = binance._normalise(
        {"e": "aggTrade", "E": 2, "T": 1, "s": "BTCUSDT", "a": 5, "p": "60000", "q": "0.5",
         "m": False},
        recv_ms=3,
    )
    assert equivalent is not None
    assert equivalent.payload["aggressor"] == events[0].payload["aggressor"], (
        "the two venues must agree on which side was the aggressor"
    )


def test_a_non_numeric_trade_id_is_hashed_not_dropped() -> None:
    events = feed()._normalise(
        {"topic": "publicTrade.BTCUSDT", "ts": 1,
         "data": [{"T": 1, "s": "BTCUSDT", "S": "Buy", "v": "1", "p": "1",
                   "i": "3f2a-uuid-style"}]},
        recv_ms=2,
    )
    assert events[0].payload["trade_id"] > 0


# ------------------------------------------------------------------- tickers


def test_the_ticker_delta_is_merged_not_read_as_complete() -> None:
    """For linear contracts a tickers push carries only what changed.

    Reading one as complete blanks mark price and funding every time the best
    bid alone moves, which silently zeroes the derivative features.
    """
    adapter = feed()
    first = adapter._normalise(
        {
            "topic": "tickers.BTCUSDT",
            "type": "snapshot",
            "cts": 1000,
            "data": {"symbol": "BTCUSDT", "markPrice": "60010.5", "indexPrice": "60000.0",
                     "fundingRate": "0.0001", "nextFundingTime": "1700000000000",
                     "bid1Price": "60000.0", "bid1Size": "3", "ask1Price": "60001.0",
                     "ask1Size": "4"},
        },
        recv_ms=1010,
    )
    kinds = {e.kind for e in first}
    assert kinds == {EventKind.MARK_PRICE, EventKind.BOOK_TICKER}
    mark = next(e for e in first if e.kind is EventKind.MARK_PRICE)
    assert mark.payload["mark_price"] == 60010.5
    assert mark.payload["funding_rate"] == 0.0001

    # A delta carrying only the bid must not erase the mark price.
    second = adapter._normalise(
        {"topic": "tickers.BTCUSDT", "type": "delta", "cts": 1100,
         "data": {"symbol": "BTCUSDT", "bid1Price": "60000.5"}},
        recv_ms=1110,
    )
    mark2 = next(e for e in second if e.kind is EventKind.MARK_PRICE)
    assert mark2.payload["mark_price"] == 60010.5, "a partial push wiped the mark price"
    ticker2 = next(e for e in second if e.kind is EventKind.BOOK_TICKER)
    assert ticker2.payload["bid"] == 60000.5
    assert ticker2.payload["ask"] == 60001.0, "the unchanged side was lost"


# ------------------------------------------------------------------- control


def test_a_rejected_subscription_is_recorded_not_ignored() -> None:
    adapter = feed()
    adapter._handle_frame(json.dumps(
        {"success": False, "ret_msg": "Invalid symbol :[NOTACOINUSDT]", "op": "subscribe"}
    ))
    assert adapter.subscribe_errors, "a rejected subscription was swallowed"
    assert "Invalid symbol" in adapter.subscribe_errors[0]
    assert adapter.stats.errors == 1


def test_control_frames_produce_no_market_events() -> None:
    adapter = feed()
    seen: list = []
    adapter.on_event(seen.append)
    adapter._handle_frame(json.dumps({"success": True, "op": "subscribe", "conn_id": "x"}))
    adapter._handle_frame(json.dumps({"op": "pong", "ret_msg": "pong"}))
    adapter._handle_frame("{not json")
    assert seen == []
    assert adapter.stats.errors == 1  # only the unparseable frame counts


def test_the_client_sends_its_own_heartbeat() -> None:
    """Bybit drops a silent connection; the library's protocol pings do not count."""
    assert PING_INTERVAL_S < 20.0, "Bybit expects a ping roughly every 20 s"


def test_rest_errors_inside_a_200_response_are_raised() -> None:
    """Bybit reports application errors with HTTP 200 and a non-zero retCode."""
    adapter = feed()
    assert adapter._unwrap({"retCode": 0, "result": {"u": 1}}) == {"u": 1}
    with pytest.raises(RuntimeError, match="retCode=10001"):
        adapter._unwrap({"retCode": 10001, "retMsg": "params error", "result": {}})


def test_the_adapter_holds_no_credentials() -> None:
    import inspect

    from aurum.adapters import bybit_linear

    source = inspect.getsource(bybit_linear)
    for forbidden in ("api_key", "X-BAPI-SIGN", "recv_window", "/v5/order", "hmac"):
        assert forbidden not in source, f"the Bybit adapter references {forbidden!r}"
