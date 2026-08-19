"""Venue endpoint defaults.

Endpoints are configuration, never constants — but a *default* per venue is
what stops the most dangerous configuration mistake this system can make:
selecting one venue while the endpoint fields still hold another's URLs. Leave
them empty in ``config.yaml`` and the venue's own defaults are filled in;
set one explicitly and it wins.

**These values could not be checked against the venues' current documentation
when this was written** — the build environment refuses both `api.bybit.com`
and `fapi.binance.com` at the egress proxy. They are written from the
documented public API shapes and verified structurally by
``main.py diagnose``, which hits the configured host and reports exactly what
failed. Check them before a live run. That is three lines in ``config.yaml``.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass


@dataclass(frozen=True)
class VenueEndpoints:
    """Everything the adapter needs to reach one venue's public market data."""

    name: str
    label: str
    rest_base: str
    ws_base: str
    ws_path: str
    rest_depth_path: str
    rest_exchange_info_path: str
    rest_time_path: str
    #: Bybit needs a product category on every request; Binance does not.
    category: str = ""
    #: Depths the venue's WebSocket order-book topic accepts, smallest first.
    #: Empty means the venue does not make you choose one.
    ws_depths: tuple[int, ...] = ()
    #: Largest depth the REST snapshot endpoint will return.
    max_rest_depth: int = 1000
    docs: str = ""

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


BINANCE_USDM = VenueEndpoints(
    name="binance_usdm",
    label="Binance USD-M Futures",
    rest_base="https://fapi.binance.com",
    ws_base="wss://fstream.binance.com",
    ws_path="/stream",
    rest_depth_path="/fapi/v1/depth",
    rest_exchange_info_path="/fapi/v1/exchangeInfo",
    rest_time_path="/fapi/v1/time",
    max_rest_depth=1000,
    docs="https://developers.binance.com/docs/derivatives/usds-margined-futures",
)

BINANCE_USDM_TESTNET = VenueEndpoints(
    name="binance_usdm_testnet",
    label="Binance USD-M Futures (testnet)",
    rest_base="https://testnet.binancefuture.com",
    ws_base="wss://stream.binancefuture.com",
    ws_path="/stream",
    rest_depth_path="/fapi/v1/depth",
    rest_exchange_info_path="/fapi/v1/exchangeInfo",
    rest_time_path="/fapi/v1/time",
    max_rest_depth=1000,
    docs="https://developers.binance.com/docs/derivatives/usds-margined-futures",
)

BYBIT_LINEAR = VenueEndpoints(
    name="bybit_linear",
    label="Bybit V5 linear perpetuals (USDT)",
    rest_base="https://api.bybit.com",
    # Bybit publishes one socket per product category rather than encoding the
    # streams in the query string, so the category is part of the path.
    ws_base="wss://stream.bybit.com",
    ws_path="/v5/public/linear",
    rest_depth_path="/v5/market/orderbook",
    rest_exchange_info_path="/v5/market/instruments-info",
    rest_time_path="/v5/market/time",
    category="linear",
    ws_depths=(1, 50, 200, 500),
    max_rest_depth=500,
    docs="https://bybit-exchange.github.io/docs/v5/intro",
)

BYBIT_LINEAR_TESTNET = VenueEndpoints(
    name="bybit_linear_testnet",
    label="Bybit V5 linear perpetuals (testnet)",
    rest_base="https://api-testnet.bybit.com",
    ws_base="wss://stream-testnet.bybit.com",
    ws_path="/v5/public/linear",
    rest_depth_path="/v5/market/orderbook",
    rest_exchange_info_path="/v5/market/instruments-info",
    rest_time_path="/v5/market/time",
    category="linear",
    ws_depths=(1, 50, 200, 500),
    max_rest_depth=500,
    docs="https://bybit-exchange.github.io/docs/v5/intro",
)

VENUES: dict[str, VenueEndpoints] = {
    v.name: v
    for v in (BINANCE_USDM, BINANCE_USDM_TESTNET, BYBIT_LINEAR, BYBIT_LINEAR_TESTNET)
}

#: Which adapter class handles which venue.  Kept here so the list of supported
#: venues is one table rather than a chain of ``if`` statements.
VENUE_ADAPTERS: dict[str, str] = {
    "binance_usdm": "binance",
    "binance_usdm_testnet": "binance",
    "bybit_linear": "bybit",
    "bybit_linear_testnet": "bybit",
}


def endpoints_for(venue: str) -> VenueEndpoints:
    try:
        return VENUES[venue]
    except KeyError:
        raise ValueError(
            f"unknown venue {venue!r}; supported: {', '.join(sorted(VENUES))}"
        ) from None


def ws_depth_for(venue: str, wanted_levels: int) -> int:
    """Smallest order-book depth the venue offers that covers ``wanted_levels``.

    Subscribing to a deeper book than the features read costs bandwidth and, on
    Bybit, a slower push cadence — depth 50 updates every 20 ms while depth 200
    updates every 100 ms, so taking 200 "to be safe" makes the book five times
    staler for no benefit.
    """
    spec = endpoints_for(venue)
    if not spec.ws_depths:
        return wanted_levels
    for depth in spec.ws_depths:
        if depth >= wanted_levels:
            return depth
    return spec.ws_depths[-1]
