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


#: Callback names whose failures happen while a socket is already being torn
#: down. The adapter has by then logged the error that caused the teardown.
_TEARDOWN_CALLBACKS = ("_call_connection_lost", "connection_lost", "run_parser")


def install_asyncio_noise_filter() -> None:
    """Stop a failed connection from printing a stack of library tracebacks.

    When a venue is unreachable — a firewall, a proxy answering 403, a country
    block — the adapter catches that cleanly, records it, and retries with
    backoff. But the websocket transport can then raise again *inside its own
    teardown*, and asyncio prints that second failure as an unhandled-callback
    traceback with no context.

    The retry loop is unaffected, so the tracebacks are noise. They are not
    harmless noise: a page of them makes a system that is behaving exactly as
    designed look like one that has crashed, and the real one-line cause
    scrolls away. So teardown failures are summarised at debug level and
    everything else is handed to the default handler untouched — this quietens
    a known-benign path, it does not swallow errors.
    """
    import asyncio

    log = get_logger("asyncio")

    def handler(loop: asyncio.AbstractEventLoop, context: dict[str, Any]) -> None:
        where = str(context.get("message", "")) + str(context.get("handle", ""))
        if any(name in where for name in _TEARDOWN_CALLBACKS):
            exc = context.get("exception")
            log.debug(
                "ignored a transport teardown failure",
                extra={"detail": f"{type(exc).__name__}: {exc}" if exc else where[:200]},
            )
            return
        loop.default_exception_handler(context)

    try:
        asyncio.get_running_loop().set_exception_handler(handler)
    except RuntimeError:
        # No loop yet: the caller is not running under asyncio, so there is
        # nothing to quieten and nothing to fail over.
        pass
