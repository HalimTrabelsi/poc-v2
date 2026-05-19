"""Fraud Detection Engine — FastAPI Application (v2)."""
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from prometheus_client import make_asgi_app

from app.api.errors import register_error_handlers
from app.api.routes import router as v1_router
from app.api.webhook import router as webhook_router
from app.logging_config import setup_logging
from app.middleware.auth import APIKeyMiddleware
from app.middleware.request_logging import RequestLoggingMiddleware

setup_logging()


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Start background scanner on startup
    try:
        from app.services.scanner_service import get_scanner
        get_scanner().start()
    except Exception as exc:
        import logging
        logging.getLogger(__name__).warning("Scanner could not start: %s", exc)
    try:
        from app.services.retraining_service import get_retrainer
        get_retrainer().start()
    except Exception as exc:
        import logging
        logging.getLogger(__name__).warning("Retraining scheduler could not start: %s", exc)
    yield
    try:
        from app.services.scanner_service import get_scanner
        get_scanner().stop()
    except Exception:
        pass
    try:
        from app.services.retraining_service import get_retrainer
        get_retrainer().stop()
    except Exception:
        pass


from fastapi.openapi.utils import get_openapi
from fastapi.security import APIKeyHeader

api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)

app = FastAPI(
    title="Fraud Detection Engine — OpenG2P",
    description="Production-grade fraud risk scoring for OpenG2P social protection programs.",
    version="2.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan,
)


def custom_openapi():
    if app.openapi_schema:
        return app.openapi_schema
    schema = get_openapi(
        title=app.title,
        version=app.version,
        description=app.description,
        routes=app.routes,
    )
    schema["components"]["securitySchemes"] = {
        "ApiKeyAuth": {"type": "apiKey", "in": "header", "name": "X-API-Key"}
    }
    schema["security"] = [{"ApiKeyAuth": []}]
    app.openapi_schema = schema
    return schema


app.openapi = custom_openapi

# ── Middleware (outermost first) ──────────────────────────────
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)
app.add_middleware(RequestLoggingMiddleware)
app.add_middleware(APIKeyMiddleware)

# ── Exception handlers ────────────────────────────────────────
register_error_handlers(app)

# ── Prometheus metrics ────────────────────────────────────────
metrics_app = make_asgi_app()
app.mount("/metrics", metrics_app)

# ── Routers ──────────────────────────────────────────────────
app.include_router(v1_router, prefix="/api", tags=["Fraud Detection v2"])
app.include_router(webhook_router, prefix="/api", tags=["Webhook"])


@app.get("/health", tags=["Health"])
async def health():
    """Lightweight liveness probe (no external dependencies checked)."""
    return {"status": "ok", "service": "fraud-detection-engine", "version": "2.0.0"}


@app.get("/", tags=["Health"])
async def root():
    return {"message": "Fraud Detection Engine v2 is running — see /docs"}
