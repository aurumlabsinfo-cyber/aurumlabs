"""Outbound network helpers shared by the exchange adapters.

`HTTP_PROXY_URL` used to be declared in the settings and read by nothing: an
operator whose network cannot reach the venue directly would configure it,
restart, receive no data at all, and see an engine that emitted NO TRADE
forever with no indication why. Both the REST client and the WebSocket client
go through here now, so the setting means what it says.
"""

from __future__ import annotations

import inspect

import httpx
import websockets

from app.core.logging_conf import get_logger

log = get_logger(__name__)


def http_client(
    base_url: str, timeout_s: float, proxy: str | None = None, **kwargs
) -> httpx.AsyncClient:
    """REST client, routed through `proxy` when one is configured."""
    return httpx.AsyncClient(
        base_url=base_url,
        timeout=timeout_s,
        headers={"User-Agent": "btc-5s-quant-engine/1.0"},
        **({"proxy": proxy} if proxy else {}),
        **kwargs,
    )


def ws_supports_proxy() -> bool:
    try:
        return "proxy" in inspect.signature(websockets.connect).parameters
    except (TypeError, ValueError):  # pragma: no cover - defensive
        return False


def ws_connect(uri: str, proxy: str | None = None, **kwargs):
    """`websockets.connect`, with the proxy applied when one is configured.

    Proxy support landed in websockets 15. On an older build the setting cannot
    be honoured, and silently ignoring it is exactly the failure this module
    exists to remove - so it raises instead.
    """
    if proxy:
        if not ws_supports_proxy():
            raise RuntimeError(
                "HTTP_PROXY_URL is set but the installed `websockets` package "
                "is too old to use a proxy (needs >= 15). Upgrade it, or clear "
                "HTTP_PROXY_URL - it cannot be honoured as things stand."
            )
        kwargs["proxy"] = proxy
    return websockets.connect(uri, **kwargs)
