"""REST and WebSocket surface, exercised through the real ASGI app."""

from __future__ import annotations

import json
import time

import pytest
from fastapi.testclient import TestClient

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def client():
    from app.main import app

    with TestClient(app) as c:
        time.sleep(2.0)  # let the feed warm up
        yield c


# ------------------------------------------------------------------- health
def test_root_lists_the_surface(client):
    body = client.get("/").json()
    assert body["mode"] == "PAPER TRADING ONLY"
    assert "/ws/dashboard" in body["websockets"]


def test_health_exposes_components_and_synthetic_warning(client):
    body = client.get("/health").json()
    assert body["status"] in ("HEALTHY", "DEGRADED")
    assert body["is_synthetic"] is True
    assert "SYNTHETIC DATA" in body["synthetic_warning"]
    assert set(body["components"]) >= {
        "websocket", "api", "database", "market_data", "order_book", "model",
        "latency", "error_rate",
    }


def test_liveness_and_readiness(client):
    assert client.get("/health/live").json()["status"] == "alive"
    ready = client.get("/health/ready").json()
    assert "data_quality" in ready


# ------------------------------------------------------------------- market
def test_market_endpoint_returns_a_two_sided_quote(client):
    body = client.get("/market").json()
    assert body["price"] > 0
    assert body["bid"] < body["ask"]
    assert body["spread_bps"] > 0
    assert body["is_synthetic"] is True
    assert body["latency_ms"] is not None


def test_orderbook_endpoint_is_sorted_and_uncrossed(client):
    body = client.get("/orderbook?levels=10").json()
    assert body["synced"] is True
    bids = [p for p, _ in body["bids"]]
    asks = [p for p, _ in body["asks"]]
    assert bids == sorted(bids, reverse=True)
    assert asks == sorted(asks)
    assert bids[0] < asks[0]
    assert body["stats"]["applied_updates"] > 0


def test_orderbook_level_bounds_are_validated(client):
    assert client.get("/orderbook?levels=0").status_code == 422
    assert client.get("/orderbook?levels=99999").status_code == 422


def test_trades_endpoint(client):
    body = client.get("/trades?limit=20").json()
    assert body["count"] > 0
    t = body["trades"][0]
    assert t["side"] in ("BUY", "SELL")
    assert t["price"] > 0


def test_features_endpoint_has_the_documented_families(client):
    body = client.get("/features").json()
    f = body["latest"]["features"]
    for key in (
        "return_100ms", "return_5000ms", "acceleration_bps_s2",
        "book_imbalance_l1", "depth_imbalance_5", "micro_price_dev_bps",
        "volume_imbalance_1s", "consecutive_buys", "trade_intensity_1s",
        "realized_vol_5s_bps", "vol_acceleration", "rsi_14", "ema_9", "bb_z",
        "atr_14", "vwap_60s", "latency_ms",
    ):
        assert key in f, key


def test_agents_endpoint_returns_all_eight(client):
    body = client.get("/agents").json()
    assert len(body["agents"]) == 8
    for a in body["agents"]:
        assert set(a) >= {
            "agent", "direction", "confidence", "score", "reason",
            "features_used", "timestamp", "data_quality",
        }
    # The endpoint rounds each probability to 4 decimals for display, so the
    # sum can drift by ~1e-4. The unrounded invariant is asserted in
    # tests/test_agents_decision.py.
    total = body["prob_up"] + body["prob_down"] + body["prob_neutral"]
    assert abs(total - 1.0) < 1e-3


# ------------------------------------------------------------------ signals
def test_signals_endpoint_shape(client):
    body = client.get("/signals").json()
    assert "active" in body and "counters" in body
    assert body["counters"]["decisions"] > 0


def test_current_signal_endpoint(client):
    body = client.get("/signals/current").json()
    assert "signal" in body and "market" in body
    assert body["server_ts"] > 0


def test_paper_trading_states_it_never_places_orders(client):
    body = client.get("/paper-trading").json()
    assert "no order is ever sent" in body["mode"]


def test_statistics_reports_payout_unknown_by_default(client):
    body = client.get("/statistics").json()
    overall = body["overall"]
    assert overall["payout_status"] == "PAYOUT UNKNOWN"
    assert overall["pnl_units"] is None
    assert "calibration" in body
    assert "by_regime" in body


def test_calibration_and_montecarlo_endpoints(client):
    assert "buckets" in client.get("/statistics/calibration").json()
    mc = client.get("/statistics/montecarlo?simulations=200").json()
    assert "error" in mc or "win_count" in mc


# ----------------------------------------------------------------- research
def test_backtest_status_reports_readiness(client):
    body = client.get("/backtest").json()
    assert body["running"] is False
    assert body["readiness"]["ready"] in (True, False)
    assert "table_counts" in body


def test_models_endpoint_lists_algorithms(client):
    body = client.get("/models").json()
    assert "logistic_regression" in body["available_algorithms"]
    assert body["active"]["ready"] is False  # no model trained yet


def test_exchanges_endpoint_shows_the_venue_abstraction(client):
    body = client.get("/exchanges").json()
    assert {"binance_spot", "binance_futures", "coinbase", "synthetic"} <= set(
        body["available"]
    )
    assert body["primary"] == "synthetic"


# ----------------------------------------------------------------- security
def test_settings_never_leak_secrets(client):
    body = client.get("/settings").json()
    flat = json.dumps(body).lower()
    assert "admin_api_key" not in flat
    assert "password" not in flat
    assert "database_url" not in flat


def test_admin_endpoints_reject_missing_or_wrong_keys(client):
    assert client.patch(
        "/settings", json={"key": "signal_enabled", "value": False}
    ).status_code in (401, 503)
    assert client.post(
        "/backtest", json={"horizons": [5.0]},
        headers={"X-API-Key": "wrong-key"},
    ).status_code in (401, 503)


def test_admin_endpoints_accept_the_configured_key(client):
    from app.config import get_settings

    key = get_settings().admin_api_key
    if not key:
        pytest.skip("ADMIN_API_KEY not configured in this environment")
    r = client.patch(
        "/settings", json={"key": "signal_min_confidence", "value": 0.61},
        headers={"X-API-Key": key},
    )
    assert r.status_code == 200
    assert r.json()["updated"]["signal_min_confidence"] == 0.61


def test_unknown_settings_key_is_rejected(client):
    from app.config import get_settings

    key = get_settings().admin_api_key
    if not key:
        pytest.skip("ADMIN_API_KEY not configured in this environment")
    r = client.patch(
        "/settings", json={"key": "database_url", "value": "postgres://evil"},
        headers={"X-API-Key": key},
    )
    assert r.status_code == 400


def test_security_headers_are_present(client):
    r = client.get("/health/live")
    assert r.headers["X-Content-Type-Options"] == "nosniff"
    assert r.headers["X-Frame-Options"] == "DENY"


# --------------------------------------------------------------- websockets
def test_ws_market_streams_ticks(client):
    with client.websocket_connect("/ws/market") as ws:
        types = set()
        for _ in range(6):
            msg = ws.receive_json()
            types.add(msg["type"])
            assert msg["server_ts"] > 0
            if "tick" in types:
                break
        assert "tick" in types


def test_ws_orderbook_streams_depth(client):
    with client.websocket_connect("/ws/orderbook") as ws:
        msg = ws.receive_json()
        assert msg["type"] == "orderbook"
        assert msg["data"]["bids"] and msg["data"]["asks"]


def test_ws_signals_sends_a_snapshot_first(client):
    with client.websocket_connect("/ws/signals") as ws:
        msg = ws.receive_json()
        assert msg["type"] == "snapshot"
        assert "counters" in msg["data"]


def test_ws_dashboard_announces_mode_and_payout(client):
    with client.websocket_connect("/ws/dashboard") as ws:
        hello = ws.receive_json()
        assert hello["type"] == "hello"
        assert hello["data"]["is_synthetic"] is True
        assert hello["data"]["payout_status"] == "PAYOUT UNKNOWN"
        assert hello["data"]["horizon_s"] == 5.0
