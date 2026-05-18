# Fraud Detection Engine for OpenG2P

A production-grade, modular fraud risk scoring system designed for OpenG2P social protection programs. It combines deterministic rule evaluation, network graph analysis, and XGBoost machine learning to produce explainable fraud risk decisions for each beneficiary.

The engine exposes a REST API that downstream case management systems can call synchronously. Every decision is persisted with full audit trail support, SHAP-based explainability, and Prometheus metrics. The system is designed to be read-only with respect to OpenG2P data — it never modifies the source database.

## Architecture

```
┌─────────────────────────────────────────────────────┐
│                  FastAPI Application                │
│  ┌──────────┐  ┌──────────┐  ┌───────────────────┐ │
│  │ Auth MW  │  │ Log MW   │  │  Error Handlers   │ │
│  └──────────┘  └──────────┘  └───────────────────┘ │
│                    API v1 Routes                    │
│  /score  /cases  /explain  /health                  │
└──────────────┬──────────────────────────────────────┘
               │
               ▼
    ┌──────────────────────┐
    │  DecisionOrchestrator│
    └──┬───┬───┬───┬───────┘
       │   │   │   │
  Features Rules Graph  ML
       │   │   │   │
  ┌────▼─┐ │ ┌─▼─┐ ▼
  │Ext.  │ │ │Nx │ XGBoost +
  │SQL   │ │ │   │ IsoForest
  └──────┘ │ └───┘
           │
      YAML Rules
    (4 scenario files)
               │
               ▼
    ┌──────────────────────┐
    │    Explainer (SHAP)  │
    └──────────────────────┘
               │
               ▼
    ┌──────────────────────┐
    │  FraudCaseRepository │  → PostgreSQL (fraud_store)
    └──────────────────────┘
```

## Quick Start

```bash
# 1. Clone and install dependencies
git clone <repo-url> && cd fraud-detection-engine
pip install -r requirements.txt

# 2. Generate synthetic training data
python scripts/generate_synthetic_data.py --n 5000 --output ml/data/synthetic/dataset_ml.csv

# 3. Train ML models
python scripts/train_ml_models.py --data ml/data/synthetic/dataset_ml.csv --output app/models_saved/

# 4. Copy environment template and configure
cp .env.example .env  # edit DB URLs and API_SECRET_KEY

# 5. Run the API
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
```

### Docker Compose (full stack)

```bash
cd docker && docker-compose up -d
# API: http://localhost:8000/docs
# Dashboard: http://localhost:8501
# Grafana: http://localhost:3000
```

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| POST | `/api/v1/score/beneficiary/{id}` | Score a beneficiary (full pipeline) |
| GET | `/api/v1/cases` | List fraud cases (filterable) |
| PATCH | `/api/v1/cases/{id}/status` | Update case status |
| GET | `/api/v1/explain/{id}` | Get full SHAP + rule explanation |
| GET | `/api/v1/health` | Service health and model readiness |
| GET | `/metrics` | Prometheus metrics |
| GET | `/docs` | Swagger UI |

All endpoints except `/health`, `/docs`, `/metrics` require `X-API-Key` header.

## Fraud Scenarios

| Scenario | Rules | Key Indicators |
|----------|-------|----------------|
| Multi-Enrollment | ME001, ME002 | 4+ simultaneous programs, rapid enrollment |
| Network Fraud | NF001–NF004 | Shared phone/bank account across beneficiaries |
| Socio-Economic Mismatch | SE001–SE003 | Low PMT score, ghost household, income underreporting |
| Payment Anomalies | PA001–PA003 | High payment gap ratio, high-amount outliers |

## Score Aggregation

```
final_score = 0.30 × rule_score + 0.50 × ml_score + 0.20 × graph_score
```

Risk thresholds: CRITICAL ≥ 0.80, HIGH ≥ 0.60, MEDIUM ≥ 0.40, LOW < 0.40

## Contributing

1. Fork the repository and create a feature branch.
2. Run `pip install -r requirements-dev.txt` and `pytest tests/` before opening a PR.
3. Follow Google-style docstrings and add type hints to all new functions.
4. New fraud rules belong in `app/rules/rules/` as YAML files following the existing schema.
