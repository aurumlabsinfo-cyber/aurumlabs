"""Historical import: parsing, validation and replay.

The fixtures below are hand-built ZIPs in Binance's documented archive layout.
They test the parser, not the market - nothing here says anything about BTC.

The validation tests matter most: a silently mis-parsed column would poison
every study run afterwards, so the importer must fail loudly instead.
"""

from __future__ import annotations

import io
import zipfile
from datetime import date

import pytest

from app.core.bus import EventBus
from app.features.engine import FeatureEngine
from app.marketdata.types import BookTicker, DataSource
from app.ml.importer import (
    BOOK_TICKER_COLUMNS,
    ImportError_,
    ReplayMarket,
    _merge_by_time,
    _parse_agg_trades,
    _parse_book_ticker,
    _sanity_check,
    _tick_from_quote,
    archive_url,
    read_csv_rows,
)

DAY = date(2026, 8, 1)
DAY_START_MS = 1785542400000  # 2026-08-01T00:00:00Z


def zip_csv(rows: list[list], header: list[str] | None = None) -> bytes:
    buf = io.StringIO()
    if header:
        buf.write(",".join(header) + "\n")
    for r in rows:
        buf.write(",".join(str(c) for c in r) + "\n")
    out = io.BytesIO()
    with zipfile.ZipFile(out, "w") as zf:
        zf.writestr("data.csv", buf.getvalue())
    return out.getvalue()


def book_rows(n: int = 100, start_ms: int = DAY_START_MS) -> list[list]:
    rows = []
    for i in range(n):
        mid = 100_000.0 + i * 0.5
        ts = start_ms + i * 100
        rows.append([1_000_000 + i, mid - 0.05, 1.2, mid + 0.05, 0.9, ts, ts])
    return rows


def trade_rows(n: int = 60, start_ms: int = DAY_START_MS) -> list[list]:
    rows = []
    for i in range(n):
        ts = start_ms + i * 150
        rows.append(
            [500 + i, 100_000.0 + i * 0.4, 0.01 + i * 0.001, 9000 + i, 9000 + i,
             ts, "false" if i % 3 else "true", "true"]
        )
    return rows


# ------------------------------------------------------------------- urls
def test_archive_url_matches_the_published_layout():
    url = archive_url("btcusdt", "bookTicker", DAY)
    assert url == (
        "https://data.binance.vision/data/spot/daily/bookTicker/BTCUSDT/"
        "BTCUSDT-bookTicker-2026-08-01.zip"
    )
    assert "aggTrades" in archive_url("BTCUSDT", "aggTrades", DAY)


# ----------------------------------------------------------------- parsing
def test_reads_files_with_and_without_a_header():
    with_header = list(read_csv_rows(zip_csv(book_rows(5), BOOK_TICKER_COLUMNS),
                                     BOOK_TICKER_COLUMNS))
    without = list(read_csv_rows(zip_csv(book_rows(5)), BOOK_TICKER_COLUMNS))
    assert len(with_header) == 5
    assert len(without) == 5
    assert with_header[0][1] == without[0][1]


def test_parses_book_ticker_into_canonical_quotes():
    quotes = list(_parse_book_ticker(zip_csv(book_rows(10)), "BTCUSDT"))
    assert len(quotes) == 10
    q = quotes[0]
    assert isinstance(q, BookTicker)
    assert q.bid_price < q.ask_price
    assert q.source is DataSource.REPLAY
    assert q.exchange_ts == DAY_START_MS
    assert q.mid == pytest.approx(100_000.0)


def test_parses_agg_trades_with_the_maker_flag():
    trades = list(_parse_agg_trades(zip_csv(trade_rows(9)), "BTCUSDT"))
    assert len(trades) == 9
    # Row 0 has is_buyer_maker=true => the aggressor was the SELLER.
    assert trades[0].is_buyer_maker is True
    assert trades[0].aggressor.value == "SELL"
    assert trades[1].aggressor.value == "BUY"
    assert all(t.source is DataSource.REPLAY for t in trades)


def test_a_short_row_is_rejected_rather_than_guessed():
    broken = [[1, 100.0, 1.0]]  # three columns where seven are expected
    with pytest.raises(ImportError_, match="expected at least"):
        list(_parse_book_ticker(zip_csv(broken), "BTCUSDT"))


def test_a_non_numeric_field_is_rejected():
    rows = book_rows(3)
    rows[1][1] = "not-a-price"
    with pytest.raises(ImportError_, match="malformed"):
        list(_parse_book_ticker(zip_csv(rows), "BTCUSDT"))


def test_an_empty_archive_is_rejected():
    empty = io.BytesIO()
    with zipfile.ZipFile(empty, "w"):
        pass
    with pytest.raises(ImportError_, match="empty"):
        list(read_csv_rows(empty.getvalue(), BOOK_TICKER_COLUMNS))


# -------------------------------------------------------------- validation
def test_sanity_check_accepts_well_formed_data():
    quotes = list(_parse_book_ticker(zip_csv(book_rows(50)), "BTCUSDT"))
    trades = list(_parse_agg_trades(zip_csv(trade_rows(30)), "BTCUSDT"))
    _sanity_check(quotes, trades, DAY)  # must not raise


def test_sanity_check_rejects_swapped_bid_and_ask_columns():
    """The most likely real-world failure: a column order change upstream."""
    rows = book_rows(50)
    for r in rows:
        r[1], r[3] = r[3], r[1]  # bid <-> ask
    quotes = list(_parse_book_ticker(zip_csv(rows), "BTCUSDT"))
    with pytest.raises(ImportError_, match="crossed"):
        _sanity_check(quotes, [], DAY)


def test_sanity_check_rejects_timestamps_from_the_wrong_day():
    quotes = list(_parse_book_ticker(zip_csv(book_rows(50)), "BTCUSDT"))
    with pytest.raises(ImportError_, match="outside"):
        _sanity_check(quotes, [], date(2026, 9, 15))


def test_sanity_check_rejects_seconds_where_milliseconds_are_expected():
    rows = book_rows(50)
    for r in rows:
        r[5] = r[5] // 1000  # seconds instead of ms
        r[6] = r[6] // 1000
    quotes = list(_parse_book_ticker(zip_csv(rows), "BTCUSDT"))
    with pytest.raises(ImportError_, match="outside"):
        _sanity_check(quotes, [], DAY)


def test_sanity_check_rejects_non_positive_trade_sizes():
    quotes = list(_parse_book_ticker(zip_csv(book_rows(50)), "BTCUSDT"))
    rows = trade_rows(30)
    rows[0][2] = 0
    trades = list(_parse_agg_trades(zip_csv(rows), "BTCUSDT"))
    with pytest.raises(ImportError_, match="non-positive"):
        _sanity_check(quotes, trades, DAY)


# ------------------------------------------------------------------ merge
def test_streams_are_interleaved_in_timestamp_order():
    quotes = list(_parse_book_ticker(zip_csv(book_rows(20)), "BTCUSDT"))
    trades = list(_parse_agg_trades(zip_csv(trade_rows(20)), "BTCUSDT"))
    merged = list(_merge_by_time(quotes, trades))
    assert len(merged) == 40
    stamps = [m.server_ts for m in merged]
    assert stamps == sorted(stamps)


def test_a_quote_wins_a_tie_so_trades_see_a_book():
    q = list(_parse_book_ticker(zip_csv(book_rows(1)), "BTCUSDT"))
    t = list(_parse_agg_trades(zip_csv(trade_rows(1)), "BTCUSDT"))
    assert q[0].server_ts == t[0].server_ts
    merged = list(_merge_by_time(q, t))
    assert isinstance(merged[0], BookTicker)


def test_merge_handles_one_empty_stream():
    q = list(_parse_book_ticker(zip_csv(book_rows(5)), "BTCUSDT"))
    assert len(list(_merge_by_time(q, []))) == 5
    assert len(list(_merge_by_time([], []))) == 0


# ----------------------------------------------------------------- replay
def test_replay_produces_features_flagged_as_real_replayed_data(settings):
    market = ReplayMarket("BTCUSDT")
    features = FeatureEngine(settings, EventBus(), market)  # type: ignore[arg-type]

    quotes = list(_parse_book_ticker(zip_csv(book_rows(400)), "BTCUSDT"))
    trades = list(_parse_agg_trades(zip_csv(trade_rows(300)), "BTCUSDT"))

    last_ts = 0
    for item in _merge_by_time(quotes, trades):
        if isinstance(item, BookTicker):
            tick = _tick_from_quote(item, market)
            market.last_tick = tick
            market.book.synced = True
            features.ingest_tick(tick)
        else:
            market.last_trade = item
            features.ingest_trade(item)
        last_ts = item.server_ts

    fv = features.compute(ts=last_ts)
    assert fv is not None
    f = fv["features"]

    # Real market data replayed - not synthetic.
    assert market.is_synthetic is False
    assert market.source is DataSource.REPLAY

    # Order flow and top-of-book come through the archive.
    assert f["volume_imbalance_1s"] is not None
    assert f["book_imbalance_l1"] is not None
    assert f["spread_bps"] > 0
    assert f["return_1000ms"] is not None

    # Depth is NOT in the archive, and must read as absent rather than zero.
    assert f["depth_imbalance_5"] is None
    assert f["bid_wall_size"] is None
    assert f["liquidity_removal_bid"] is None


def test_replay_reports_no_latency_rather_than_inventing_one(settings):
    market = ReplayMarket("BTCUSDT")
    quote = list(_parse_book_ticker(zip_csv(book_rows(1)), "BTCUSDT"))[0]
    tick = _tick_from_quote(quote, market)
    # There is no network hop in a replay; 0 is the truthful value.
    assert tick.latency_ms == 0
    assert tick.source is DataSource.REPLAY


def test_replay_market_quality_depends_on_having_a_quote():
    market = ReplayMarket("BTCUSDT")
    assert market.data_quality()["ok"] is False
    quote = list(_parse_book_ticker(zip_csv(book_rows(1)), "BTCUSDT"))[0]
    market.last_tick = _tick_from_quote(quote, market)
    assert market.data_quality()["ok"] is True
