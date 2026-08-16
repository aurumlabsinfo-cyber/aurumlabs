# Security

## Threat model

This system reads public market data and writes paper trades. It holds no
funds, no exchange credentials and no customer data. The realistic risks are:

1. **Someone changes engine behaviour** (settings, active model, purging data).
2. **Resource exhaustion** — an open WebSocket or a hot REST loop.
3. **Database exposure** — the recorded tick history is valuable and, in a
   shared deployment, worth protecting.
4. **Operator self-deception** — synthetic data or an unvalidated model
   presented as real. This is treated as a security property here, not a
   cosmetic one.

## Secrets

* Every setting comes from environment variables. `.env` is git-ignored;
  `.env.example` is the committed template and contains no real values.
* **No exchange API key is used or accepted.** All market-data endpoints are
  public. There is no signing code and no private endpoint in the codebase.
* `ADMIN_API_KEY` guards state-changing routes. Generate it with
  `openssl rand -hex 32`. When unset, those routes return `503` and stay
  disabled — the safe default.
* `GET /settings` deliberately excludes the admin key, the database URL and
  anything password-shaped.
* The frontend receives only `NEXT_PUBLIC_*` values (the API and WebSocket
  URLs). These are compiled into the browser bundle, so **never** put a secret
  behind that prefix.

## Authentication

| Route class | Auth |
|---|---|
| Market data, signals, statistics, health | none (data is public) |
| `PATCH /settings`, `POST /backtest`, `POST /models/*`, `POST /admin/*` | `X-API-Key` |

The key is compared with `hmac.compare_digest` to avoid a timing side channel.

There is no user system. If you expose this beyond localhost, put it behind a
reverse proxy with real authentication — see [DEPLOYMENT.md](DEPLOYMENT.md).

## Input validation

* Every request body and query parameter is a Pydantic model or a constrained
  `Query` — `levels` is bounded 1–200, `simulations` 100–100,000, and so on.
  Violations return `422` before any handler runs.
* `PATCH /settings` accepts only an allow-listed set of keys and casts each
  value to its declared type. `database_url` and friends are not in that list.
* All database access goes through SQLAlchemy with bound parameters. No string
  interpolation of user input into SQL.
* Exchange payloads are parsed field by field into typed structures; a
  malformed frame is logged and dropped, never trusted.

## Rate limiting and resource control

* Per-IP sliding-window limiter (`RATE_LIMIT_REQUESTS` / `RATE_LIMIT_WINDOW_S`,
  default 120/60s), returning `429` with `Retry-After`. `/health*` is exempt so
  orchestrators can always probe. Idle clients are evicted so the limiter
  cannot itself become a memory leak.
* WebSocket connections capped at `WS_MAX_CONNECTIONS` (close code `1013`).
* Every bus subscriber has a bounded queue and drops its oldest frames when it
  falls behind — a slow client cannot apply back-pressure to the engine.
* The database writer bounds its queue and re-queues rows on failure rather
  than growing without limit.

## HTTP hardening

Both tiers set `X-Content-Type-Options: nosniff`, `X-Frame-Options: DENY`,
`Referrer-Policy: no-referrer` and a restrictive `Permissions-Policy`.
`X-Powered-By` is disabled. CORS is an explicit origin allow-list
(`CORS_ORIGINS`), credentials off, methods and headers restricted — never `*`.

## Error handling

Unhandled exceptions return a generic 500 with the exception *type* and no
stack trace or query text. Details go to the structured server log and the
`errors` table. Adapters, agents and the ingestion path all catch broadly on
purpose: one malformed message must never take down the feed.

## Containers

* Both images run as a non-root user (uid 10001).
* PostgreSQL is not published to the host by default — it is reachable only on
  the compose network.
* `POSTGRES_PASSWORD` has no default: compose fails fast if it is unset.
* Health checks on all three services.
* `.dockerignore` keeps `.env`, virtualenvs, `.git` and model artifacts out of
  the build context.

## Data integrity as a safety property

The rules that stop the system from lying to its operator:

* Anything produced by the simulator is flagged `is_synthetic = true` in every
  table, every API response and a permanent banner in the UI, and is excluded
  from research queries by default.
* `ALLOW_SYNTHETIC_SOURCE` defaults to `false`; the synthetic adapter refuses
  to start without an explicit opt-in.
* Unavailable values are `null`, never `0`.
* No monetary P&L is reported when the payout is unknown.
* Win rates carry confidence intervals, p-values and explicit
  insufficient-sample warnings.
* Leakage checks must pass before any edge verdict, and a detected leak forces
  `FAILED` no matter how good the accuracy looks.

## No trading capability

There is no order placement path. No private endpoint, no request signing, no
API secret is read. Adding real execution would require new code, new
credentials and a deliberate decision — it cannot happen by misconfiguration.

## Reporting

Open a private issue or contact the maintainer directly. Do not include `.env`
contents, keys or database dumps in a public report.
