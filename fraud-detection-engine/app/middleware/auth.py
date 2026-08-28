"""Simple API key authentication middleware."""
import logging

from fastapi import Request
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware

from app.config import settings

logger = logging.getLogger(__name__)


class APIKeyMiddleware(BaseHTTPMiddleware):
    """Reject requests that lack a valid X-API-Key header.

    Paths in EXEMPT_PATHS bypass authentication so that health checks,
    documentation, and metrics endpoints remain publicly accessible.
    """

    EXEMPT_PATHS = {
        "/",
        "/health",
        "/api/v1/health",
        "/api/v1/webhook/beneficiary-saved",  # authenticated via X-Webhook-Secret inside handler
        "/docs",
        "/redoc",
        "/openapi.json",
        "/metrics",
        "/metrics/",  # Mount("/metrics", ...) 307-redirects bare "/metrics" here
    }

    async def dispatch(self, request: Request, call_next):
        if request.url.path in self.EXEMPT_PATHS:
            return await call_next(request)

        # Let CORS preflight pass through so the browser receives CORS headers
        if request.method == "OPTIONS":
            return await call_next(request)

        api_key = request.headers.get("X-API-Key", "")
        if api_key != settings.api_secret_key:
            logger.warning(
                "Unauthorized request from %s to %s",
                request.client.host if request.client else "unknown",
                request.url.path,
            )
            return JSONResponse(
                status_code=401,
                content={"error": "unauthorized", "detail": "Missing or invalid X-API-Key"},
            )

        return await call_next(request)
