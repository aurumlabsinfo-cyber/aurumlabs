#!/usr/bin/env bash
#
# AURUM EDGE LAB — first start.
#
# Creates the virtualenv, installs dependencies, verifies the machine can run
# the engine, and starts it. Safe to re-run: everything it does is idempotent.
#
#   ./start.sh                      live feed
#   ./start.sh --replay FILE        replay a recorded or generated file
#   ./start.sh --demo               generate a synthetic file and replay it
#   ./start.sh --frontend           also build and serve the dashboard
#
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")"

PYTHON="${PYTHON:-python3}"
VENV=".venv"
REPLAY=""
DEMO=0
FRONTEND=0
EXTRA=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --replay)   REPLAY="$2"; shift 2 ;;
    --demo)     DEMO=1; shift ;;
    --frontend) FRONTEND=1; shift ;;
    *)          EXTRA+=("$1"); shift ;;
  esac
done

say() { printf '\n\033[1;33m==>\033[0m %s\n' "$*"; }
die() { printf '\n\033[1;31mERROR:\033[0m %s\n' "$*" >&2; exit 1; }

# ---------------------------------------------------------------- prerequisites

command -v "$PYTHON" >/dev/null || die "$PYTHON not found. Python 3.11 or newer is required."
version="$("$PYTHON" -c 'import sys; print("%d.%d" % sys.version_info[:2])')"
case "$version" in
  3.11|3.12|3.13|3.14) ;;
  *) die "Python $version found; 3.11 or newer is required." ;;
esac

# ------------------------------------------------------------------- environment

if [[ ! -d "$VENV" ]]; then
  say "Creating the virtualenv in $VENV"
  "$PYTHON" -m venv "$VENV"
fi
# shellcheck disable=SC1091
source "$VENV/bin/activate"

say "Installing dependencies"
pip install --quiet --upgrade pip
pip install --quiet -r requirements.txt

mkdir -p data data/exports

# ------------------------------------------------------------------- self-check

say "Running the self-test (no network required)"
python3 main.py selftest || die "self-test failed — do not run the engine until this passes"

# ---------------------------------------------------------------------- demo

if [[ "$DEMO" == "1" ]]; then
  REPLAY="data/replay.jsonl"
  if [[ ! -f "$REPLAY" ]]; then
    say "Generating a synthetic replay file (this is NOT market data)"
    python3 tools/make_replay.py --minutes 25 --out "$REPLAY"
  else
    say "Reusing the existing $REPLAY"
  fi
fi

# ------------------------------------------------------------------- diagnose

say "Diagnosing this machine"
if [[ -n "$REPLAY" ]]; then
  python3 main.py diagnose --feed replay --replay "$REPLAY" || true
else
  # A failure here is usually the venue being unreachable, which is worth
  # seeing in full before the engine starts rather than as silence afterwards.
  python3 main.py diagnose || {
    printf '\n\033[1;31mThe venue endpoints did not verify.\033[0m\n'
    printf 'Check market.rest_base / market.ws_base in config.yaml against the current\n'
    printf 'official Binance USD-M Futures documentation, or start with --demo to run\n'
    printf 'against a generated file instead.\n\n'
    read -r -p 'Start anyway? [y/N] ' answer
    [[ "$answer" == "y" || "$answer" == "Y" ]] || exit 1
  }
fi

# ------------------------------------------------------------------- frontend

if [[ "$FRONTEND" == "1" ]]; then
  command -v npm >/dev/null || die "npm not found, but --frontend was requested"
  say "Building the dashboard"
  (cd frontend && npm install --silent && npm run build)
  say "Serving the dashboard on http://localhost:3000"
  (cd frontend && npm run start >../data/frontend.log 2>&1 &)
fi

# ------------------------------------------------------------------------ run

say "Starting the engine"
if [[ -n "$REPLAY" ]]; then
  exec python3 main.py run --replay "$REPLAY" --replay-speed 6 "${EXTRA[@]+"${EXTRA[@]}"}"
fi
exec python3 main.py run "${EXTRA[@]+"${EXTRA[@]}"}"
