"""Logging.

Named ``logging_setup`` rather than ``logging`` so that ``import logging``
inside this package keeps meaning the standard library.

Two formats: a readable console line for development, and one JSON object per
line for anything that ships logs.  Both carry the same fields, so a grep that
works in development still works in production.
"""

from __future__ import annotations

import json
import logging
import sys
import time
from typing import Any

_CONFIGURED = False

_RESERVED = frozenset(
    {
        "args", "asctime", "created", "exc_info", "exc_text", "filename", "funcName",
        "levelname", "levelno", "lineno", "module", "msecs", "message", "msg", "name",
        "pathname", "process", "processName", "relativeCreated", "stack_info",
        "thread", "threadName", "taskName",
    }
)


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(record.created))
            + f".{int(record.msecs):03d}Z",
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        for key, value in record.__dict__.items():
            if key not in _RESERVED and not key.startswith("_"):
                payload[key] = value
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


class ConsoleFormatter(logging.Formatter):
    """``12:04:31.220 INFO  market.binance  connected  symbols=10``"""

    def format(self, record: logging.LogRecord) -> str:
        stamp = time.strftime("%H:%M:%S", time.gmtime(record.created)) + f".{int(record.msecs):03d}"
        extras = " ".join(
            f"{k}={v}" for k, v in record.__dict__.items() if k not in _RESERVED and not k.startswith("_")
        )
        line = f"{stamp} {record.levelname:<7} {record.name:<26} {record.getMessage()}"
        if extras:
            line = f"{line}  {extras}"
        if record.exc_info:
            line = f"{line}\n{self.formatException(record.exc_info)}"
        return line


def setup_logging(level: str = "INFO", json_output: bool = False, *, force: bool = False) -> None:
    global _CONFIGURED
    if _CONFIGURED and not force:
        return
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(JsonFormatter() if json_output else ConsoleFormatter())
    root = logging.getLogger()
    for existing in list(root.handlers):
        root.removeHandler(existing)
    root.addHandler(handler)
    root.setLevel(getattr(logging, level.upper(), logging.INFO))
    # These two are chatty at INFO and say nothing we do not already log.
    logging.getLogger("websockets").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    _CONFIGURED = True


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(f"aurum.{name}" if not name.startswith("aurum") else name)
