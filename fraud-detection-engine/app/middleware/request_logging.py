"""Request/response logging middleware that emits structured JSON."""
import json
import logging
import time

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

logger = logging.getLogger("request")


class RequestLoggingMiddleware(BaseHTTPMiddleware):
    """Log every HTTP request and response as a structured JSON line."""

    async def dispatch(self, request: Request, call_next) -> Response:
        t0 = time.perf_counter()
        response: Response = await call_next(request)
        duration_ms = round((time.perf_counter() - t0) * 1000, 2)

        record = {
            "method": request.method,
            "path": request.url.path,
            "query": str(request.url.query) or None,
            "status_code": response.status_code,
            "duration_ms": duration_ms,
            "client": request.client.host if request.client else None,
        }

        level = logging.WARNING if response.status_code >= 400 else logging.INFO
        logger.log(level, json.dumps(record))

        return response
