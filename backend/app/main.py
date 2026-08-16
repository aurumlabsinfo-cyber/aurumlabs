"""FastAPI application entry point."""

from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from app.api.routes import router as rest_router
from app.api.security import RateLimitMiddleware, SecurityHeadersMiddleware
from app.api.ws import router as ws_router
from app.config import get_settings
from app.core.clock import now_ms
from app.core.logging_conf import configure_logging, get_logger
from app.services.container import Services, set_services

settings = get_settings()
configure_logging(settings.log_level, json_logs=settings.env == "prod")
log = get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    services = Services(settings)
    set_services(services)
    await services.start()
    if services.market.is_synthetic:
        log.warning(
            "SYNTHETIC_MODE",
            message=(
                "Running on the SYNTHETIC simulator. Output is model-generated "
                "and describes nothing about the real market."
            ),
        )
    try:
        yield
    finally:
        await services.stop()
        set_services(None)


app = FastAPI(
    title=settings.app_name,
    version="1.0.0",
    description=(
        "Live market data, microstructure feature engineering, multi-agent "
        "analysis and PAPER-ONLY binary signal evaluation for BTC. "
        "No order is ever sent to any venue."
    ),
    lifespan=lifespan,
)

app.add_middleware(SecurityHeadersMiddleware)
app.add_middleware(RateLimitMiddleware, settings=settings)
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origin_list,
    allow_credentials=False,
    allow_methods=["GET", "POST", "PATCH", "OPTIONS"],
    allow_headers=["Content-Type", "X-API-Key"],
)

app.include_router(rest_router)
app.include_router(ws_router)


@app.exception_handler(Exception)
async def unhandled_exception(request: Request, exc: Exception) -> JSONResponse:
    log.error(
        "api.unhandled", path=request.url.path, error=f"{type(exc).__name__}: {exc}"
    )
    return JSONResponse(
        status_code=500,
        content={
            "detail": "internal error",
            "error_type": type(exc).__name__,
            "server_ts": now_ms(),
        },
    )


@app.get("/", tags=["system"])
async def root() -> dict:
    return {
        "name": settings.app_name,
        "mode": "PAPER TRADING ONLY",
        "symbol": settings.symbol,
        "docs": "/docs",
        "rest": [
            "/health", "/market", "/orderbook", "/trades", "/features", "/agents",
            "/signals", "/paper-trading", "/statistics", "/backtest", "/models",
            "/settings",
        ],
        "websockets": ["/ws/market", "/ws/orderbook", "/ws/signals", "/ws/dashboard"],
        "server_ts": now_ms(),
    }
