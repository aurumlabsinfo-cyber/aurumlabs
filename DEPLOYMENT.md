# Deployment

## Docker Compose (recommended)

```bash
cp .env.example .env
# set POSTGRES_PASSWORD, ADMIN_API_KEY, EXCHANGES, and the NEXT_PUBLIC_* URLs
docker compose up -d --build
docker compose ps
docker compose logs -f backend
```

Three services: `postgres` (persistent volume), `backend` (FastAPI + engine),
`frontend` (Next.js standalone). The database is not published to the host by
default.

### Changing the public URLs

`NEXT_PUBLIC_API_URL` and `NEXT_PUBLIC_WS_URL` are compiled into the browser
bundle at **build** time. Behind a domain:

```bash
NEXT_PUBLIC_API_URL=https://quant.example.com/api
NEXT_PUBLIC_WS_URL=wss://quant.example.com/api
CORS_ORIGINS=https://quant.example.com
docker compose build frontend && docker compose up -d frontend
```

## Reverse proxy

The backend speaks plain HTTP and WebSocket; terminate TLS in front of it.
WebSocket upgrade headers and a long read timeout are the two things people
forget.

```nginx
server {
    listen 443 ssl http2;
    server_name quant.example.com;

    location / {
        proxy_pass http://frontend:3000;
        proxy_set_header Host $host;
    }

    location /api/ {
        proxy_pass http://backend:8000/;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;      # WebSocket
        proxy_set_header Connection "upgrade";
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_read_timeout 3600s;                    # feeds are long-lived
        proxy_buffering off;                         # do not buffer streams
    }
}
```

Add HTTP basic auth or an identity-aware proxy on `/api/` if the instance is
reachable from the internet. There is no user system in the application.

## Scaling model

**Run exactly one backend instance per symbol.** The engine is stateful: it
owns a local order book and the signal lifecycle. Two instances would maintain
two independent books and emit duplicate signals.

To scale:

* more symbols → one process per symbol, each with its own `SYMBOL`, all
  writing to the same database;
* more viewers → the frontend and the WebSocket fan-out scale horizontally;
  the engine does not need to;
* heavy research → run the backtest CLI on a separate machine against a replica
  of the database, never on the box holding the live feed.

## Resource expectations

| Resource | Typical |
|---|---|
| Backend CPU | < 1 core steady state (spikes during a backtest) |
| Backend RAM | 300–600 MB; more while training |
| Database write rate | a few hundred rows/second at defaults |
| Disk | a few hundred MB per day per symbol |

Reduce with `PERSIST_MARKET_TICKS=false`, `PERSIST_TRADES=false`, or a slower
`FEATURE_INTERVAL_MS`. Keep `PERSIST_FEATURES=true` if you intend to do any
research — it is the table the backtester reads.

## Retention

There is no automatic pruning. Add one:

```sql
-- keep 30 days of raw ticks and trades, keep features longer
DELETE FROM market_ticks WHERE ts < (EXTRACT(EPOCH FROM now()) - 30*86400) * 1000;
DELETE FROM trades       WHERE ts < (EXTRACT(EPOCH FROM now()) - 30*86400) * 1000;
-- order_book_updates grows fastest of all; prune it hard if you enabled it
DELETE FROM order_book_updates WHERE ts < (EXTRACT(EPOCH FROM now()) - 3*86400) * 1000;
```

For long horizons, consider TimescaleDB: the schema is plain PostgreSQL and
`market_ticks` / `trades` / `features` convert cleanly to hypertables on `ts`.

Back up what is expensive to recreate: `paper_trades`, `signals`,
`model_versions` and the `models` volume. Raw ticks can be re-collected; a
month of recorded signal outcomes cannot.

## Monitoring

```bash
curl -s localhost:8000/health | jq '{status, components}'   # orchestrator probe
curl -s localhost:8000/health/live                          # liveness
curl -s localhost:8000/health/ready                         # readiness
```

Alert on:

| Condition | Why |
|---|---|
| `components.order_book.status != "UP"` for > 60s | the book cannot resynchronise |
| `orderbook.resyncs` climbing steadily | dropping WebSocket frames |
| `market.feed_age_ms > 5000` | the feed is dead but the process is alive |
| `tick_latency_ms.p95` above `MAX_LATENCY_MS` | signals will be gated off |
| `components.database.status != "UP"` | rows are queuing in memory |
| `is_synthetic == true` in production | **the simulator is running** |

Logs are structured (JSON when `ENV=prod`), so ship them straight to your log
stack. Errors also land in the `errors` table and in `/health`.

## Upgrades

The schema is created idempotently at boot (`create_all`), so adding a table or
an index is a restart. Destructive changes need a real migration — add Alembic
before you make one.

```bash
git pull
docker compose build
docker compose up -d
```

Expect a short gap in recorded data across the restart, and a fresh
`MIN_WARMUP_SECONDS` warm-up before signals resume. In-flight signals are lost
on restart; settled ones are already persisted.

## Production checklist

- [ ] `ENV=prod`, `LOG_LEVEL=INFO`
- [ ] `POSTGRES_PASSWORD` strong and unique
- [ ] `ADMIN_API_KEY` set (`openssl rand -hex 32`)
- [ ] `ALLOW_SYNTHETIC_SOURCE=false`
- [ ] `EXCHANGES` points at a real venue; `/health` shows `source: "LIVE"`
- [ ] `CORS_ORIGINS` lists only your real origins
- [ ] TLS terminated, WebSocket upgrade proxied, `/api/` authenticated if public
- [ ] Database not published to the host
- [ ] Retention job scheduled, backups of `paper_trades` and `models`
- [ ] Alerts wired to `/health`
- [ ] `BINARY_PAYOUT` set if you want monetary P&L — otherwise expect
      `PAYOUT UNKNOWN`, which is the honest default
