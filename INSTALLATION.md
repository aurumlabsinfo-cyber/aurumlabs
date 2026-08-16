# Installation

Two supported paths: Docker (recommended) and a local development setup.

## Requirements

| Component | Version |
|---|---|
| Docker + Compose | 24+ |
| Python (local dev) | 3.11+ |
| Node.js (local dev) | 20.9+ (Next.js 16 minimum) |
| PostgreSQL | 14+ (16 recommended) |
| Outbound network | `stream.binance.com:9443` (WSS) and `api.binance.com` (HTTPS) |

No exchange account and no API key are required. Every market-data endpoint
used is public.

---

## Option A — Docker

```bash
./setup.sh --start
docker compose logs -f backend
```

`setup.sh` writes `.env` with a generated database password and admin key,
selects the live public Binance feed, and verifies the endpoints are reachable.
It never overwrites an existing `.env`.

Manual equivalent:

```bash
cp .env.example .env
```

Edit `.env` and set at minimum:

```bash
POSTGRES_PASSWORD=<something long>
ADMIN_API_KEY=$(openssl rand -hex 32)   # needed for /settings, /backtest, /models
EXCHANGES=binance_spot
```

Then:

```bash
docker compose up -d --build
docker compose logs -f backend
```

**No exchange API key is required at any point.** The market-data endpoints are
public; the key above is your own, guarding this instance's admin routes.

* dashboard: <http://localhost:3000>
* API + OpenAPI docs: <http://localhost:8000/docs>

`NEXT_PUBLIC_API_URL` / `NEXT_PUBLIC_WS_URL` are baked into the browser bundle
at build time. If the backend is not on `localhost:8000`, set them in `.env`
**before** `docker compose build`, and rebuild the frontend after changing them.

---

## Option B — local development

### 1. PostgreSQL

```bash
# any local instance works; the schema is created automatically at boot
createdb btcquant
createdb btcquant_test      # only needed to run the tests
```

### 2. Backend

```bash
cd backend
python3.11 -m venv .venv
source .venv/bin/activate
pip install -r requirements-dev.txt

cp ../.env.example ../.env      # settings are read from the repo-root .env
uvicorn app.main:app --reload --port 8000
```

Check it:

```bash
curl -s localhost:8000/health | jq '{status, source, is_synthetic}'
```

### 3. Frontend

```bash
npm install
npm run dev        # http://localhost:3000
```

### 4. Tests

```bash
cd backend
DATABASE_URL=postgresql+asyncpg://postgres:postgres@localhost:5432/btcquant_test \
ADMIN_API_KEY=test-admin-key \
  python -m pytest -q
```

Frontend tests (Vitest + Testing Library, no server needed):

```bash
npm test
```

Tests marked `integration` need a reachable PostgreSQL. They never contact an
exchange: they run against the synthetic adapter. To skip them:

```bash
python -m pytest -q -m "not integration"
```

---

## Offline / no-connectivity mode

If the host cannot reach an exchange, the pipeline can still be exercised with
the **synthetic** feed:

```bash
EXCHANGES=synthetic
ALLOW_SYNTHETIC_SOURCE=true
```

This emits model-generated ticks, trades and depth diffs. Everything it
produces is flagged `is_synthetic = true` in the database and the API, the
dashboard shows a permanent warning banner, and the research tooling excludes
those rows by default. **It describes the simulator, not the market**, and
cannot be used to establish an edge.

To clear simulator rows before recording real data:

```bash
curl -X POST localhost:8000/admin/purge-synthetic -H "X-API-Key: $ADMIN_API_KEY"
```

---

## First-run checklist

Run through this before trusting anything the dashboard shows.

```bash
curl -s localhost:8000/health | jq '{
  source, is_synthetic,
  ws: .components.websocket.status,
  db: .components.database.status,
  book: .components.order_book.status,
  quality: .market.data_quality
}'
```

1. `source: "LIVE"` and `is_synthetic: false`.
2. `websocket: "UP"`, `database: "UP"`, `order_book: "UP"`.
3. `data_quality.score` climbing to 1.0 and `warmup_complete: true` after
   `MIN_WARMUP_SECONDS`.
4. `curl -s localhost:8000/orderbook | jq .stats` — `applied_updates` rising,
   `gaps_detected` low and `resyncs` not climbing continuously.
5. `curl -s localhost:8000/features | jq '.latest.features.return_1000ms'` —
   a number, not `null`, once enough history exists.
6. Row counts increasing: `python -m app.ml.cli status`.

If `resyncs` climbs steadily, the host is losing WebSocket frames (bad network,
CPU starvation, or an aggressive proxy). The book will keep resynchronising and
the engine will keep refusing to trade — which is the correct behaviour, but it
means no signals will ever be produced until the feed is stable.

---

## Storage planning

At the default settings the engine writes roughly:

| Table | Rate |
|---|---|
| `market_ticks` | one row per book-ticker update (~5–30/s on BTCUSDT) |
| `trades` | one row per trade (~5–50/s) |
| `features` | 10 rows/s (`FEATURE_INTERVAL_MS=100`) |
| `order_book_snapshots` | one row per `BOOK_SNAPSHOT_INTERVAL_S` |
| `order_book_updates` | one row per depth diff (~10/s) - **off by default** |
| `performance_metrics` | a few rows per `PERFORMANCE_METRICS_INTERVAL_S` |

Expect a few hundred MB per day. Turn off what you do not need with
`PERSIST_MARKET_TICKS`, `PERSIST_TRADES`, `PERSIST_FEATURES`, and see
[DEPLOYMENT.md](DEPLOYMENT.md) for retention.
