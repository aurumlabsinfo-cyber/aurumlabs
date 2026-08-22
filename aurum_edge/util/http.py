"""Client HTTP in sola lettura, su `urllib` della libreria standard.

Tre proprieta' non negoziabili:

* **solo GET**. Non c'e' un metodo per POST in questo modulo. Un client che non
  sa scrivere non puo' piazzare un ordine nemmeno per errore di programmazione.
* **niente credenziali**. Nessuna firma, nessuna chiave, nessun header di
  autenticazione. Gli endpoint usati sono tutti pubblici.
* **errori che si distinguono**. Un blocco di rete, un rifiuto del proxy e un
  errore applicativo dell'exchange sono cose diverse e vanno raccontate in
  modo diverso a chi guarda la dashboard.
"""

from __future__ import annotations

import gzip
import json
import socket
import ssl
import threading
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Any

from .. import config

_USER_AGENT = "aurum-edge-discovery/1.0 (read-only public market data)"


class FetchError(RuntimeError):
    """Errore di rete o di protocollo. Porta con se' come raccontarlo."""

    def __init__(self, kind: str, detail: str, url: str = "") -> None:
        super().__init__(f"{kind}: {detail}")
        self.kind = kind          # NETWORK | BLOCKED | HTTP | DECODE | API
        self.detail = detail
        self.url = url

    def to_dict(self) -> dict[str, Any]:
        return {"kind": self.kind, "detail": self.detail, "url": self.url}


class _RateLimiter:
    """Un passo minimo fra due richieste, condiviso fra i thread."""

    def __init__(self, min_interval: float) -> None:
        self.min_interval = min_interval
        self._lock = threading.Lock()
        self._last = 0.0

    def wait(self) -> None:
        with self._lock:
            delta = time.monotonic() - self._last
            if delta < self.min_interval:
                time.sleep(self.min_interval - delta)
            self._last = time.monotonic()


_LIMITER = _RateLimiter(config.HTTP_MIN_INTERVAL)


@dataclass
class Response:
    status: int
    body: bytes

    def json(self) -> Any:
        try:
            return json.loads(self.body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise FetchError("DECODE", str(exc)) from exc

    def text(self) -> str:
        return self.body.decode("utf-8", errors="replace")


def _opener() -> urllib.request.OpenerDirector:
    """Rispetta HTTPS_PROXY e il bundle CA se l'ambiente li impone.

    `urllib` legge da solo le variabili d'ambiente del proxy; il contesto SSL
    di default legge il trust store di sistema. Non si disabilita mai la
    verifica: un errore di certificato e' un'informazione, non un ostacolo.
    """
    ctx = ssl.create_default_context()
    return urllib.request.build_opener(urllib.request.HTTPSHandler(context=ctx))


def get(url: str, params: dict[str, Any] | None = None, *,
        timeout: float | None = None, retries: int | None = None,
        accept: str = "application/json") -> Response:
    """GET con ritentativi a passo crescente. Solo GET, per scelta."""
    timeout = config.HTTP_TIMEOUT if timeout is None else timeout
    retries = config.HTTP_RETRIES if retries is None else retries

    if params:
        from urllib.parse import urlencode
        clean = {k: v for k, v in params.items() if v is not None}
        url = f"{url}?{urlencode(clean)}"

    last: FetchError | None = None
    for attempt in range(max(1, retries)):
        if attempt:
            time.sleep(min(2.0 ** attempt, 8.0))
        _LIMITER.wait()
        req = urllib.request.Request(url, method="GET", headers={
            "User-Agent": _USER_AGENT,
            "Accept": accept,
            "Accept-Encoding": "gzip",
        })
        try:
            with _opener().open(req, timeout=timeout) as resp:
                raw = resp.read()
                if resp.headers.get("Content-Encoding") == "gzip":
                    raw = gzip.decompress(raw)
                return Response(resp.status, raw)
        except urllib.error.HTTPError as exc:
            body = ""
            try:
                body = exc.read()[:400].decode("utf-8", errors="replace")
            except Exception:                                   # noqa: BLE001
                pass
            if exc.code in (403, 407):
                # Un rifiuto di policy non si ritenta: si racconta.
                raise FetchError(
                    "BLOCKED",
                    f"HTTP {exc.code} dal proxy o dall'host. "
                    f"L'host non e' consentito da questa rete. {body}".strip(),
                    url) from exc
            last = FetchError("HTTP", f"HTTP {exc.code} {body}".strip(), url)
            if exc.code < 500 and exc.code != 429:
                raise last from exc
        except (urllib.error.URLError, socket.timeout, ssl.SSLError, OSError) as exc:
            reason = getattr(exc, "reason", exc)
            text = str(reason)
            kind = "BLOCKED" if "403" in text or "tunnel" in text.lower() else "NETWORK"
            last = FetchError(kind, text, url)
            if kind == "BLOCKED":
                raise last from exc
    raise last or FetchError("NETWORK", "richiesta fallita senza dettagli", url)


def get_json(url: str, params: dict[str, Any] | None = None, **kw) -> Any:
    return get(url, params, **kw).json()


def get_text(url: str, params: dict[str, Any] | None = None, **kw) -> str:
    return get(url, params, accept="application/xml, text/xml, */*", **kw).text()
