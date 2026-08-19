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
#   ./start.sh --yes                start even if the venue does not verify
#
# The engine always starts if it can. A missing npm, a dashboard that will not
# build, or an unreachable venue costs you that piece and says so — none of
# them silently take the backend down with them.
#
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")"

PYTHON="${PYTHON:-python3}"
VENV=".venv"
REPLAY=""
DEMO=0
FRONTEND=0
ASSUME_YES=0
EXTRA=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --replay)   REPLAY="$2"; shift 2 ;;
    --demo)     DEMO=1; shift ;;
    --frontend) FRONTEND=1; shift ;;
    --yes|-y)   ASSUME_YES=1; shift ;;
    *)          EXTRA+=("$1"); shift ;;
  esac
done

say()  { printf '\n\033[1;33m==>\033[0m %s\n' "$*"; }
warn() { printf '\033[1;33mWARNING:\033[0m %s\n' "$*" >&2; }
die()  { printf '\n\033[1;31mERROR:\033[0m %s\n' "$*" >&2; exit 1; }

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
    venue="$(python3 main.py config 2>/dev/null | python3 -c 'import json,sys; print(json.load(sys.stdin)["config"]["market"]["venue"])' 2>/dev/null || echo "the configured venue")"
    printf '\n\033[1;31mThe venue endpoints did not verify.\033[0m\n'
    printf 'Selected venue: %s\n' "$venue"
    printf 'The report above names the exact URL that failed. Usual causes, in order:\n'
    printf '  * no route to the venue (firewall, proxy, or a country block)\n'
    printf '  * the venue moved an endpoint — check its current official docs and set\n'
    printf '    AURUM_REST_BASE / AURUM_WS_BASE, or market.rest_base / market.ws_base\n'
    printf 'Or start with --demo to run against a generated file instead.\n\n'
    # Only ask when there is someone to answer. Run from a script, a pipe or a
    # service manager there is no terminal, and a blocking read would hang the
    # start with no output explaining why — which looks exactly like a backend
    # that failed to boot.
    if [[ "$ASSUME_YES" == "1" ]]; then
      warn "--yes given: starting anyway. Expect the feed to stay DISCONNECTED."
    elif [[ -t 0 ]]; then
      read -r -p 'Start anyway? [y/N] ' answer
      [[ "$answer" == "y" || "$answer" == "Y" ]] || exit 1
    else
      die "no terminal to ask. Re-run with --yes to start regardless, or --demo for a generated feed."
    fi
  }
fi

# ------------------------------------------------------------------- frontend

# The engine is the product; the dashboard only looks at it. Nothing in here
# may prevent the engine from starting — a missing npm or a failed build costs
# you the browser view, not the run. Every branch below warns and carries on.
if [[ "$FRONTEND" == "1" ]]; then
  if ! command -v npm >/dev/null; then
    warn "npm not found — skipping the dashboard. The engine and its API still start."
  else
    say "Building the dashboard"
    if (cd frontend && npm install --silent && npm run build); then
      say "Serving the dashboard on http://localhost:3000"
      (cd frontend && npm run start >../data/frontend.log 2>&1 &)
    else
      warn "the dashboard failed to build — see the output above. Skipping it;"
      warn "the engine and its API still start. Everything is on the API:"
      warn "  curl -s http://127.0.0.1:${AURUM_API_PORT:-8002}/health | python3 -m json.tool"
    fi
  fi
fi

# ------------------------------------------------------------------------ run

say "Starting the engine"
if [[ -n "$REPLAY" ]]; then
  exec python3 main.py run --replay "$REPLAY" --replay-speed 6 "${EXTRA[@]+"${EXTRA[@]}"}"
fi
exec python3 main.py run "${EXTRA[@]+"${EXTRA[@]}"}"
