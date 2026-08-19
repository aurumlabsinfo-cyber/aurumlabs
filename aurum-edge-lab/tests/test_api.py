"""API contract tests, against a real runtime on a replayed feed.

Not mocked: the app starts the actual runtime, which starts the actual data
engine, feature engine, research director and broker.  Every endpoint below is
answered from live state that came out of the pipeline, which is the only way to
catch a route that returns a shape the frontend cannot use.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from aurum.api.app import create_app, route_paths
from aurum.config import Config, load_config

ROOT = Path(__file__).resolve().parent.parent
SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]

#: Every route the blueprint's API contract names.
CONTRACT = [
    "/health", "/market", "/markets", "/market/{symbol}", "/orderbook/{symbol}",
    "/features/{symbol}", "/agents", "/research", "/hypotheses", "/strategies",
    "/champion", "/challengers", "/signals", "/positions", "/trades", "/wallet",
    "/cycles", "/statistics", "/data-quality", "/diagnostics", "/config",
]


@pytest.fixture(scope="module")
def client(tmp_path_factory, replay_file: Path):
    tmp = tmp_path_factory.mktemp("api")
    config: Config = load_config(
        ROOT / "config.yaml",
        use_env=False,
        overrides={
            "app": {"data_dir": str(tmp), "log_level": "ERROR"},
            "market": {"feed": "replay", "replay_path": str(replay_file)},
            "research": {"min_warmup_s": 0.0, "cycle_interval_s": 5.0},
            "cross_market": {"min_samples": 30, "window_s": 60},
            "diagnostics": {"export_dir": str(tmp / "exports")},
        },
    )
    config.market.symbols = [s for s in config.market.symbols if s.symbol in SYMBOLS]
    app = create_app(config)
    with TestClient(app) as test_client:
        yield test_client


def test_every_contract_route_is_served(client: TestClient) -> None:
    served = route_paths(client.app)
    missing = [path for path in CONTRACT if path not in served]
    assert not missing, f"routes missing from the API contract: {missing}"
    assert "/ws/live" in served


def test_health_reports_everything_the_blueprint_requires(client: TestClient) -> None:
    payload = client.get("/health").json()
    for key in (
        "version", "uptime_s", "market_feed", "database", "connected_symbols", "research",
        "paper_broker", "positions", "wallet", "cycle", "data_quality", "errors",
    ):
        assert key in payload, f"/health is missing {key}"

    assert payload["market_feed"]["live"] is False, "a replay feed must never claim to be live"
    assert payload["wallet"]["starting_balance"] == 100.00
    assert payload["wallet"]["currency"] == "EUR"
    assert payload["paper_broker"]["live_orders_possible"] is False
    assert payload["status"] in {
        "OK", "WARMING_UP", "NO_FEED", "NO_VALIDATED_EDGE", "AWAITING_EDGE", "DEGRADED"
    }
    assert payload["database"]["driver"] == "sqlite"


def test_markets_expose_all_configured_symbols(client: TestClient) -> None:
    payload = client.get("/markets").json()
    assert payload["count"] == len(SYMBOLS)
    symbols = {row["symbol"] for row in payload["symbols"]}
    assert symbols == set(SYMBOLS)
    for row in payload["symbols"]:
        assert row["tier"] in {"CORE", "MAJOR", "DYNAMIC"}
        assert "quality" in row


def test_market_detail_and_orderbook(client: TestClient) -> None:
    payload = client.get("/market/BTCUSDT").json()
    assert payload["symbol"] == "BTCUSDT"
    assert "book" in payload and "quality" in payload

    book = client.get("/orderbook/BTCUSDT", params={"depth": 5}).json()
    assert book["symbol"] == "BTCUSDT"
    if book["ready"]:
        assert len(book["bids"]) <= 5 and len(book["asks"]) <= 5
        assert book["bids"][0][0] < book["asks"][0][0], "a served book must not be crossed"
        assert book["mid"] > 0


def test_unknown_symbol_is_a_404_not_an_empty_result(client: TestClient) -> None:
    response = client.get("/market/NOTACOIN")
    assert response.status_code == 404
    assert "configured universe" in response.json()["detail"]


def test_features_are_available_or_explain_why_not(client: TestClient) -> None:
    payload = client.get("/features/BTCUSDT").json()
    assert payload["symbol"] == "BTCUSDT"
    if payload["available"]:
        assert payload["values"]["mid"] > 0
        assert "ofi_1s" in payload["values"]
        assert 0.0 <= payload["quality_score"] <= 1.0
    else:
        assert payload["reason"], "unavailable features must come with a reason"


def test_agents_report_real_inputs_and_metrics(client: TestClient) -> None:
    payload = client.get("/agents").json()
    names = {agent["name"] for agent in payload["agents"]}
    assert {"microstructure", "momentum", "mean_reversion", "cross_crypto", "relative_value"} <= names
    assert "cycle_postmortem" in names
    for agent in payload["agents"]:
        assert agent["description"], f"{agent['name']} has no description"
        if agent["name"] != "cycle_postmortem":
            assert agent["inputs"], f"{agent['name']} declares no inputs"
            assert "metrics" in agent


def test_champion_absence_is_explained(client: TestClient) -> None:
    payload = client.get("/champion").json()
    if payload["champion"] is None:
        assert payload["state"] == "NO_VALIDATED_EDGE"
        assert payload["reason"], "the absence of a champion must be explained"
    else:
        assert payload["champion"]["can_trade"] is True


def test_diagnostics_explains_every_gate(client: TestClient) -> None:
    payload = client.get("/diagnostics").json()
    assert payload["summary"], "diagnostics must produce a readable summary"
    assert "rejections" in payload and "gate_catalogue" in payload
    # Every gate in the catalogue carries an explanation and an action.
    for gate in payload["gate_catalogue"]:
        assert gate["explanation"], f"{gate['reason']} has no explanation"
        assert gate["action"], f"{gate['reason']} has no suggested action"
    # Percentages are present wherever counts are.
    for row in payload["rejections"]:
        assert "percent" in row and "count" in row
    assert "warmup" in payload


def test_wallet_and_cycles(client: TestClient) -> None:
    wallet = client.get("/wallet").json()
    assert wallet["wallet"]["starting_balance"] == 100.00
    assert wallet["wallet"]["balance"] <= 100.00 + 1e-9
    assert "ledger" in wallet and "equity_curve" in wallet
    assert "risk" in wallet

    cycles = client.get("/cycles").json()
    assert cycles["current"]["cycle"]["cycle_id"] == 1
    assert cycles["current"]["cycle"]["starting_balance"] == 100.00


def test_research_and_hypotheses_endpoints(client: TestClient) -> None:
    research = client.get("/research").json()
    assert "director" in research and "memory" in research and "validation" in research
    assert research["director"]["no_edge_reason"], "no-edge must always carry a reason"

    hypotheses = client.get("/hypotheses", params={"limit": 5}).json()
    assert "hypotheses" in hypotheses and "status_counts" in hypotheses


def test_strategies_signals_positions_trades(client: TestClient) -> None:
    strategies = client.get("/strategies").json()
    assert "counts" in strategies
    assert set(strategies["counts"]) >= {"RESEARCH", "CANDIDATE", "CHAMPION", "REJECTED"}

    assert "signals" in client.get("/signals").json()
    positions = client.get("/positions").json()
    assert positions["count"] == len(positions["positions"])
    assert "trades" in client.get("/trades").json()
    assert "challengers" in client.get("/challengers").json()


def test_statistics_and_data_quality(client: TestClient) -> None:
    stats = client.get("/statistics").json()
    for key in ("broker", "wallet", "research", "strategies", "features", "cross_market", "database"):
        assert key in stats

    quality = client.get("/data-quality").json()
    assert quality["summary"]["symbols"] == len(SYMBOLS)
    assert "thresholds" in quality


def test_config_exposes_bounds_and_refuses_out_of_range_settings(client: TestClient) -> None:
    payload = client.get("/config").json()
    settable = {row["path"]: row for row in payload["settable"]}
    assert settable["risk.risk_per_trade_pct"]["max"] == 2.0
    assert payload["config"]["wallet"]["starting_balance_eur"] == 100.0

    ok = client.post("/config/settings", json={"risk.risk_per_trade_pct": 1.5})
    assert ok.status_code == 200
    assert ok.json()["applied"]["risk.risk_per_trade_pct"] == 1.5

    bad = client.post("/config/settings", json={"risk.risk_per_trade_pct": 50.0})
    assert bad.status_code == 400
    assert "must be <= 2.0" in str(bad.json()["detail"])

    structural = client.post("/config/settings", json={"wallet.starting_balance_eur": 1_000_000})
    assert structural.status_code == 400
    assert client.get("/wallet").json()["wallet"]["starting_balance"] == 100.00


def test_websocket_delivers_live_state(client: TestClient) -> None:
    with client.websocket_connect("/ws/live") as websocket:
        payload = websocket.receive_json()
        assert payload["type"] == "state"
        assert "wallet" in payload and "markets" in payload and "diagnostics" in payload
        assert payload["feed"]["live"] is False
        assert len(payload["markets"]) == len(SYMBOLS)
        assert "no_edge_reason" in payload


def test_index_lists_the_served_routes(client: TestClient) -> None:
    payload = client.get("/").json()
    assert payload["paper_trading_only"] is True
    assert "/health" in payload["endpoints"]
    assert "/diagnostics" in payload["endpoints"]
