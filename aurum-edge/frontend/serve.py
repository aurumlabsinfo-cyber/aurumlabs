#!/usr/bin/env python3
"""Static server for the AURUM EDGE dashboard.

Deliberately its own process, with no import of the trading core: the backend
runs, trades and records whether or not this is up, and this serves files
whether or not the backend is up.  The only link between them is HTTP.

    python3 serve.py --port 8200 --api http://127.0.0.1:8100
"""

from __future__ import annotations

import argparse
import http.server
import os
import socketserver
from pathlib import Path

ROOT = Path(__file__).resolve().parent


class Handler(http.server.SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(ROOT), **kwargs)

    def end_headers(self) -> None:
        self.send_header("Cache-Control", "no-store")
        super().end_headers()

    def log_message(self, fmt: str, *args) -> None:  # quieter output
        if os.environ.get("AURUM_UI_VERBOSE"):
            super().log_message(fmt, *args)


def main() -> int:
    parser = argparse.ArgumentParser(description="AURUM EDGE dashboard (static)")
    parser.add_argument("--port", type=int, default=int(os.environ.get("AURUM_UI_PORT", 8200)))
    parser.add_argument("--host", default=os.environ.get("AURUM_UI_HOST", "0.0.0.0"))
    parser.add_argument(
        "--api",
        default=os.environ.get("AURUM_API_URL", "http://127.0.0.1:8100"),
        help="backend URL the dashboard should talk to",
    )
    args = parser.parse_args()

    socketserver.TCPServer.allow_reuse_address = True
    with socketserver.TCPServer((args.host, args.port), Handler) as httpd:
        print(f"AURUM EDGE dashboard: http://127.0.0.1:{args.port}/?api={args.api}")
        print("(the backend is a separate process - this only serves files)")
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print("\nstopped")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
