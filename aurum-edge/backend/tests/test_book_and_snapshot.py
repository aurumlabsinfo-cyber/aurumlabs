"""Book sequencing and the feature maths inside a MarketSnapshot."""

from __future__ import annotations

import pytest

from aurum_edge.config import Config
from aurum_edge.scan.book import BookState, OrderBook
from aurum_edge.scan.market_core import DEPTH_BANDS_BPS, Instrument, SymbolState


def book_message(update_id: int, bid: float, ask: float, bid_size: float = 10.0,
                 ask_size: float = 10.0, snapshot: bool = False, ts: float = 1000.0,
                 levels: int = 5) -> dict:
    return {
        "topic": "orderbook.50.TESTUSDT",
        "type": "snapshot" if snapshot else "delta",
        "ts": ts,
        "cts": ts,
        "data": {
            "s": "TESTUSDT",
            "b": [[f"{bid - i * 0.01:.4f}", f"{bid_size}"] for i in range(levels)],
            "a": [[f"{ask + i * 0.01:.4f}", f"{ask_size}"] for i in range(levels)],
            "u": update_id,
            "seq": update_id * 2,
        },
    }


# --------------------------------------------------------------------- book

def test_delta_before_snapshot_is_refused() -> None:
    book = OrderBook("TESTUSDT")
    assert book.apply(book_message(5, 100.0, 100.1), local_ms=1000.0) is False
    assert book.state is BookState.RESYNC
    assert "delta before snapshot" in book.resync_reason


def test_sequence_gap_forces_resync_and_empties_the_book() -> None:
    book = OrderBook("TESTUSDT")
    assert book.apply(book_message(1, 100.0, 100.1, snapshot=True), 1000.0) is True
    assert book.apply(book_message(2, 100.01, 100.11), 1001.0) is True
    assert book.state is BookState.OK

    # u jumps from 2 to 4: the local book is now a guess
    assert book.apply(book_message(4, 100.02, 100.12), 1002.0) is False
    assert book.state is BookState.RESYNC
    assert book.gaps == 1
    assert book.best_bid_price is None

    # further deltas stay refused until a snapshot repairs it
    assert book.apply(book_message(5, 100.03, 100.13), 1003.0) is False
    assert book.apply(book_message(6, 100.03, 100.13, snapshot=True), 1004.0) is True
    assert book.state is BookState.OK


def test_crossed_book_is_rejected() -> None:
    book = OrderBook("TESTUSDT")
    book.apply(book_message(1, 100.0, 100.1, snapshot=True), 1000.0)
    assert book.apply(book_message(2, 101.0, 100.5, snapshot=True), 1001.0) is False
    assert book.state is BookState.CROSSED
    assert book.crossed_events == 1


def test_zero_size_removes_a_level() -> None:
    book = OrderBook("TESTUSDT")
    book.apply(book_message(1, 100.0, 100.1, snapshot=True, levels=3), 1000.0)
    assert len(book.bids) == 3
    book.apply(
        {
            "topic": "orderbook.50.TESTUSDT", "type": "delta", "ts": 1001.0, "cts": 1001.0,
            "data": {"s": "TESTUSDT", "b": [["100.0000", "0"]], "a": [], "u": 2, "seq": 4},
        },
        1001.0,
    )
    assert 100.0 not in book.bids
    assert book.best_bid_price == pytest.approx(99.99)


def test_microprice_leans_towards_the_thin_side() -> None:
    book = OrderBook("TESTUSDT")
    book.apply(book_message(1, 100.0, 100.1, bid_size=90.0, ask_size=10.0, snapshot=True), 1000.0)
    # heavy bid, thin ask -> price is pulled up towards the ask
    assert book.microprice() > book.mid
    assert book.imbalance(5) > 0


def test_walk_and_slippage_use_real_depth() -> None:
    book = OrderBook("TESTUSDT")
    book.apply(book_message(1, 100.0, 100.1, bid_size=1.0, ask_size=1.0, snapshot=True,
                            levels=5), 1000.0)
    avg, filled, full = book.walk("Buy", 2.5)
    assert filled == pytest.approx(2.5)
    assert full is True
    assert 100.1 <= avg <= 100.2
    partial = book.walk("Buy", 99.0)
    assert partial[2] is False
    assert book.slippage_bps("Buy", 2.5) > 0


def test_depth_curve_is_monotonic() -> None:
    book = OrderBook("TESTUSDT")
    book.apply(book_message(1, 100.0, 100.1, snapshot=True, levels=20), 1000.0)
    bid_curve, ask_curve = book.depth_curve(DEPTH_BANDS_BPS)
    assert bid_curve == sorted(bid_curve)
    assert ask_curve == sorted(ask_curve)
    assert ask_curve[-1] > 0


# ----------------------------------------------------------------- snapshot

def make_state() -> SymbolState:
    instrument = Instrument(
        symbol="TESTUSDT", tick_size=0.01, qty_step=0.001, min_qty=0.001,
        min_notional_usd=5.0, max_leverage=25.0, status="Trading",
        contract_type="LinearPerpetual",
    )
    state = SymbolState(symbol="TESTUSDT", instrument=instrument, history_window_s=120.0)
    state.focus = True
    state.reset_book("orderbook.50")
    return state


def test_snapshot_carries_one_synchronised_view(cfg: Config) -> None:
    state = make_state()
    ts = 1_000_000.0
    state.on_book(book_message(1, 100.0, 100.1, snapshot=True, ts=ts), ts)
    for i in range(1, 70):
        moment = ts + i * 1000.0
        price = 100.0 * (1 + i * 0.0001)
        # a walking price replaces the whole book, so send snapshots: delta
        # sequencing itself is covered by the dedicated book tests above
        state.on_book(book_message(i + 1, price, price + 0.01, ts=moment, snapshot=True), moment)
        state.on_trades(
            {"topic": "publicTrade.TESTUSDT", "ts": moment,
             "data": [{"T": moment, "s": "TESTUSDT", "S": "Buy", "v": "1", "p": str(price)}]},
            moment,
        )
    state.on_ticker(
        {"topic": "tickers.TESTUSDT", "ts": ts + 69_100,
         "data": {"symbol": "TESTUSDT", "openInterest": "1000", "turnover24h": "5e8",
                  "lastPrice": "100.07", "fundingRate": "0.0001"}},
        ts + 69_100,
    )

    # built just after the last update, so every horizon has data behind it
    snap = state.build(ts + 69_100.0, seq=1, cfg=cfg, source="fake")
    assert snap is not None
    assert snap.symbol == "TESTUSDT"
    assert snap.bid < snap.ask
    assert snap.spread_bps > 0
    assert snap.mid == pytest.approx((snap.bid + snap.ask) / 2)
    assert snap.ret_1s_bps > 0 and snap.ret_60s_bps > 0
    assert snap.ret_60s_bps > snap.ret_1s_bps          # longer horizon, bigger move
    assert snap.volatility_bps > 0
    assert snap.trades_60s > 0 and snap.volume_60s_usd > 0
    assert snap.aggression_60s == pytest.approx(1.0)   # every trade was a buy
    assert snap.open_interest == 1000.0
    assert snap.book_state == "OK"
    assert snap.source == "fake"
    assert snap.ts_exchange_ms > 0 and snap.ts_local_ms > 0
    assert snap.depth_curve_ask_usd[-1] > 0
    # every feature the model needs is present and finite
    features = snap.features("LONG")
    assert len(features) == len(snap.feature_vector("LONG"))
    assert all(isinstance(v, float) for v in features.values())


def test_features_flip_sign_with_the_side(cfg: Config) -> None:
    state = make_state()
    ts = 1_000_000.0
    state.on_book(book_message(1, 100.0, 100.1, snapshot=True, ts=ts), ts)
    for i in range(1, 30):
        moment = ts + i * 1000.0
        price = 100.0 * (1 + i * 0.0002)
        state.on_book(book_message(i + 1, price, price + 0.01, ts=moment, snapshot=True), moment)
    snap = state.build(ts + 30_000.0, seq=1, cfg=cfg, source="fake")
    assert snap is not None
    long_features = snap.features("LONG")
    short_features = snap.features("SHORT")
    assert long_features["mom_1s"] == pytest.approx(-short_features["mom_1s"])
    assert long_features["ofi_5s"] == pytest.approx(-short_features["ofi_5s"])
    assert long_features["vol_ratio"] == pytest.approx(short_features["vol_ratio"])


def test_ofi_is_positive_when_bids_grow_and_asks_shrink(cfg: Config) -> None:
    state = make_state()
    ts = 1_000_000.0
    state.on_book(book_message(1, 100.0, 100.1, bid_size=10, ask_size=10, snapshot=True, ts=ts), ts)
    for i in range(1, 8):
        moment = ts + i * 100.0
        state.on_book(
            book_message(i + 1, 100.0, 100.1, bid_size=10 + i * 5, ask_size=max(10 - i, 1),
                         ts=moment),
            moment,
        )
    snap = state.build(ts + 800.0, seq=1, cfg=cfg, source="fake")
    assert snap is not None
    assert snap.ofi_1s > 0
    assert snap.imbalance_top > 0


def test_stale_book_makes_the_snapshot_untradable(cfg: Config) -> None:
    state = make_state()
    ts = 1_000_000.0
    state.on_book(book_message(1, 100.0, 100.1, snapshot=True, ts=ts), ts)
    for i in range(1, 40):
        moment = ts + i * 500.0
        state.on_book(book_message(i + 1, 100.0, 100.1, ts=moment), moment)

    fresh = state.build(ts + 20_000.0, seq=1, cfg=cfg, source="fake")
    stale = state.build(ts + 60_000.0, seq=2, cfg=cfg, source="fake")
    assert stale is not None and fresh is not None
    assert stale.book_age_ms > cfg.scan.stale_book_ms
    assert stale.quality == "BAD"
    assert stale.tradable is False
    assert any("stale" in reason for reason in stale.quality_reasons)


def test_slippage_estimate_grows_with_size(cfg: Config) -> None:
    state = make_state()
    ts = 1_000_000.0
    state.on_book(book_message(1, 100.0, 100.1, bid_size=1, ask_size=1, snapshot=True,
                               ts=ts, levels=20), ts)
    for i in range(1, 20):
        moment = ts + i * 1000.0
        state.on_book(book_message(i + 1, 100.0, 100.1, bid_size=1, ask_size=1,
                                   ts=moment, levels=20), moment)
    snap = state.build(ts + 20_000.0, seq=1, cfg=cfg, source="fake")
    assert snap is not None
    small = snap.slippage_bps_for("LONG", 100.0)
    large = snap.slippage_bps_for("LONG", 50_000.0)
    assert large > small >= 0


def test_features_are_bounded_even_on_a_thin_window(cfg: Config) -> None:
    """One warm-up outlier must not saturate the model at p=1.000."""
    from aurum_edge.decide.model import champion_v1
    from aurum_edge.scan.snapshot import FEATURE_CLIP

    state = make_state()
    ts = 1_000_000.0
    state.on_book(book_message(1, 100.0, 100.1, snapshot=True, ts=ts), ts)
    # a violent move with almost no history behind it
    for i in range(1, 4):
        moment = ts + i * 200.0
        price = 100.0 * (1 + i * 0.02)
        state.on_book(book_message(i + 1, price, price + 0.01, ts=moment, snapshot=True), moment)
    snap = state.build(ts + 800.0, seq=1, cfg=cfg, source="fake")
    assert snap is not None

    features = snap.features("LONG")
    assert all(abs(v) <= FEATURE_CLIP for v in features.values()), features
    probability = champion_v1().probability(features)
    assert probability < 1.0, "a bounded feature set cannot produce certainty"
    assert snap.tradable is False, "and a thin window is not tradable anyway"


def test_volume_acceleration_uses_the_history_it_actually_has(cfg: Config) -> None:
    state = make_state()
    ts = 1_000_000.0
    state.on_book(book_message(1, 100.0, 100.1, snapshot=True, ts=ts), ts)
    # a steady tape: acceleration must read ~1.0, not the 60/5 window ratio
    for i in range(1, 40):
        moment = ts + i * 500.0
        state.on_book(book_message(i + 1, 100.0, 100.1, ts=moment, snapshot=True), moment)
        state.on_trades(
            {"topic": "publicTrade.TESTUSDT", "ts": moment,
             "data": [{"T": moment, "s": "TESTUSDT", "S": "Buy", "v": "1", "p": "100.0"}]},
            moment,
        )
    snap = state.build(ts + 19_600.0, seq=1, cfg=cfg, source="fake")
    assert snap is not None
    assert 0.7 < snap.volume_acceleration < 1.4, snap.volume_acceleration
