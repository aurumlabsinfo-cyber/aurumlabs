#!/usr/bin/env bash
#
# One-shot setup for the BTC 5-Second Quant Engine.
#
#   ./setup.sh            prepare .env only
#   ./setup.sh --start    prepare .env and bring the stack up with Docker
#
# There is NO exchange API key to configure. The engine reads Binance's public
# market-data endpoints, which require no credential of any kind. The two
# secrets generated below are yours, for your own machine:
#
#   POSTGRES_PASSWORD  the database password
#   ADMIN_API_KEY      guards /settings, /backtest and /models on YOUR instance
#
set -euo pipefail

cd "$(dirname "$0")"

GREEN=$'\033[0;32m'; YELLOW=$'\033[0;33m'; RED=$'\033[0;31m'; BOLD=$'\033[1m'; OFF=$'\033[0m'
info()  { printf '%s==>%s %s\n' "$GREEN" "$OFF" "$1"; }
warn()  { printf '%s!!%s  %s\n' "$YELLOW" "$OFF" "$1"; }
fail()  { printf '%sxx%s  %s\n' "$RED" "$OFF" "$1" >&2; exit 1; }

random_secret() {
  if command -v openssl >/dev/null 2>&1; then
    openssl rand -hex 32
  elif [ -r /dev/urandom ]; then
    head -c 32 /dev/urandom | od -An -tx1 | tr -d ' \n'
  else
    fail "no way to generate a random secret (install openssl)"
  fi
}

# ----------------------------------------------------------------- .env
if [ -f .env ]; then
  warn ".env already exists - leaving it untouched."
  warn "Delete it and re-run this script if you want fresh secrets."
else
  [ -f .env.example ] || fail ".env.example is missing - is this the repo root?"

  info "Generating .env with fresh secrets"
  PG_PASS="$(random_secret)"
  ADMIN_KEY="$(random_secret)"

  # Start from the documented template so every comment survives.
  cp .env.example .env

  # Portable in-place edit (BSD sed on macOS needs the empty -i argument).
  sedi() { if sed --version >/dev/null 2>&1; then sed -i "$@"; else sed -i '' "$@"; fi; }

  sedi "s|^POSTGRES_PASSWORD=.*|POSTGRES_PASSWORD=${PG_PASS}|" .env
  sedi "s|^ADMIN_API_KEY=.*|ADMIN_API_KEY=${ADMIN_KEY}|" .env
  sedi "s|^DATABASE_URL=.*|DATABASE_URL=postgresql+asyncpg://postgres:${PG_PASS}@localhost:5432/btcquant|" .env
  # Live public Binance feed, simulator firmly off.
  sedi "s|^EXCHANGES=.*|EXCHANGES=binance_spot|" .env
  sedi "s|^ALLOW_SYNTHETIC_SOURCE=.*|ALLOW_SYNTHETIC_SOURCE=false|" .env

  chmod 600 .env
  info ".env created (mode 600). It is git-ignored and must stay that way."
fi

# --------------------------------------------------------------- checks
info "Checking connectivity to the public Binance endpoints"
if command -v curl >/dev/null 2>&1; then
  if curl -fsS --max-time 10 "https://api.binance.com/api/v3/time" >/dev/null 2>&1; then
    PRICE="$(curl -fsS --max-time 10 "https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT" 2>/dev/null || true)"
    info "Binance reachable. ${PRICE:-}"
  else
    warn "Cannot reach api.binance.com from this machine."
    warn "Causes: no internet, a corporate proxy, or Binance being blocked in"
    warn "your country. The engine will start, report source=LIVE but never"
    warn "receive data, and correctly refuse to signal (NO TRADE)."
    warn "Check /health after starting: components.market_data must be UP."
  fi
else
  warn "curl not found - skipping the connectivity check."
fi

# ---------------------------------------------------------------- start
if [ "${1:-}" = "--start" ]; then
  command -v docker >/dev/null 2>&1 || fail "docker is not installed"
  docker compose version >/dev/null 2>&1 || fail "docker compose v2 is required"
  info "Building and starting the stack (first build takes a few minutes)"
  docker compose up -d --build
  echo
  info "Waiting for the backend to answer /health ..."
  for _ in $(seq 1 60); do
    if curl -fsS --max-time 3 http://localhost:8000/health >/dev/null 2>&1; then
      break
    fi
    sleep 2
  done
  if curl -fsS --max-time 3 http://localhost:8000/health >/dev/null 2>&1; then
    info "Backend is up."
  else
    warn "Backend did not answer yet. Check: docker compose logs -f backend"
  fi
fi

# ----------------------------------------------------------------- next
cat <<'EOS'

────────────────────────────────────────────────────────────
  NEXT STEPS
────────────────────────────────────────────────────────────

  Start everything (if you did not pass --start):

      docker compose up -d --build

  Then open:

      http://localhost:3000        the dashboard
      http://localhost:8000/docs   the API

  Confirm it is on REAL data, not the simulator:

      curl -s localhost:8000/health | grep -o '"is_synthetic":[a-z]*'
      # must print  "is_synthetic":false

      curl -s localhost:8000/market
      # bid < ask, a plausible BTC price, latency_ms in the tens

  The first minute shows NO TRADE while the engine warms up and the order
  book synchronises. That is correct behaviour, not a fault.

  IMPORTANT
  · No exchange API key is used anywhere. Market data is public.
  · Paper trading only. Nothing can place a real order.
  · No edge is claimed. Collect hours of data, then run the backtest:
        docker compose exec backend python -m app.ml.cli status
        docker compose exec backend python -m app.ml.cli backtest --summary

────────────────────────────────────────────────────────────
EOS
