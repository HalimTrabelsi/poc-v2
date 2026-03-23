"""
Fraud Detection Engine — FastAPI Application
PFE OpenG2P — Halim Trabelsi
"""
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from prometheus_client import make_asgi_app

from app.api.routes_scoring import router as scoring_router
from app.api.routes_graph   import router as graph_router
from app.api.routes_cases   import router as cases_router
from app.api.routes_rules   import router as rules_router

# ── App ──────────────────────────────────────────────────────
app = FastAPI(
    title="Fraud Detection Engine — OpenG2P",
    description="Moteur intelligent de detection des risques et fraudes",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Prometheus metrics ────────────────────────────────────────
metrics_app = make_asgi_app()
app.mount("/metrics", metrics_app)

# ── Routers ──────────────────────────────────────────────────
app.include_router(scoring_router, prefix="/api/v1", tags=["Scoring"])
app.include_router(graph_router,   prefix="/api/v1", tags=["Graph"])
app.include_router(cases_router,   prefix="/api/v1", tags=["Cases"])
app.include_router(rules_router,   prefix="/api/v1", tags=["Rules"])

# ── Health check ─────────────────────────────────────────────
@app.get("/health", tags=["Health"])
async def health():
    return {
        "status": "ok",
        "service": "fraud-detection-engine",
        "version": "1.0.0"
    }

@app.get("/", tags=["Health"])
async def root():
    return {"message": "Fraud Detection Engine is running"}