"""Rate limiting, admin auth and security headers.

The market-data endpoints are read-only and unauthenticated by design (the data
itself is public). Anything that changes engine behaviour - settings, model
activation, backtests, synthetic purges - requires `ADMIN_API_KEY`, which lives
in the environment and is never sent to the browser.
"""

from __future__ import annotations

import time
from collections import defaultdict, deque
from typing import Callable

from fastapi import Header, HTTPException, Request, status
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import Response

from app.config import Settings, get_settings


class RateLimitMiddleware(BaseHTTPMiddleware):
    """Fixed-window-per-client limiter with a sliding deque of timestamps."""

    def __init__(self, app, settings: Settings) -> None:
        super().__init__(app)
        self.limit = settings.rate_limit_requests
        self.window = settings.rate_limit_window_s
        self._hits: dict[str, deque[float]] = defaultdict(deque)

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        if request.url.path in ("/health", "/health/live", "/health/ready"):
            return await call_next(request)
        client = request.client.host if request.client else "unknown"
        now = time.monotonic()
        hits = self._hits[client]
        cutoff = now - self.window
        while hits and hits[0] < cutoff:
            hits.popleft()
        if len(hits) >= self.limit:
            retry = max(1, int(self.window - (now - hits[0])))
            return Response(
                content='{"detail":"rate limit exceeded"}',
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                media_type="application/json",
                headers={"Retry-After": str(retry)},
            )
        hits.append(now)
        # Bound memory: forget idle clients.
        if len(self._hits) > 10_000:
            for key in [k for k, v in self._hits.items() if not v][:5_000]:
                self._hits.pop(key, None)
        return await call_next(request)


class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        response = await call_next(request)
        response.headers.setdefault("X-Content-Type-Options", "nosniff")
        response.headers.setdefault("X-Frame-Options", "DENY")
        response.headers.setdefault("Referrer-Policy", "no-referrer")
        response.headers.setdefault(
            "Permissions-Policy", "geolocation=(), microphone=(), camera=()"
        )
        return response


async def require_admin(x_api_key: str | None = Header(default=None)) -> None:
    """Guard for state-changing endpoints."""
    settings = get_settings()
    if settings.admin_api_key is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=(
                "ADMIN_API_KEY is not configured, so administrative endpoints are "
                "disabled. Set it in the server environment (never in the frontend)."
            ),
        )
    if not x_api_key or not _constant_time_eq(x_api_key, settings.admin_api_key):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="invalid API key"
        )


def _constant_time_eq(a: str, b: str) -> bool:
    import hmac

    return hmac.compare_digest(a.encode(), b.encode())
