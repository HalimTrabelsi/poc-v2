# 🛡️ Fraud Detection Engine for OpenG2P

A production-grade, **hybrid fraud-risk scoring system** for OpenG2P social-protection
programs. It combines a **deterministic rule engine**, **network graph analytics**, and
**calibrated machine learning** to produce an explainable fraud-risk decision for every
beneficiary — with SHAP attributions, natural-language LLM explanations, geospatial
hotspots, and a closed feedback → retraining loop.

The engine is exposed as a REST API that case-management systems (Odoo / OpenG2P) call
synchronously. It is **read-only** with respect to the OpenG2P source database and never
modifies registry data.

---

## ✨ Features Implemented

### 1. Hybrid Scoring Pipeline
Three complementary detectors are fused into a single risk score:

```
final_score = 0.30 × rule_score  +  0.50 × ml_score  +  0.20 × graph_score
```
| Layer | Engine | What it catches |
|-------|--------|-----------------|
| **Rules** | YAML + safe AST eval (10+ rules) | Known fraud patterns, policy violations |
| **ML** | XGBoost (isotonic-calibrated) + Isolation Forest | Subtle multivariate anomalies |
| **Graph** | NetworkX (PageRank / shared-attribute clustering) | Collusion rings, shared phones/accounts |

Risk bands: **CRITICAL ≥ 0.80 · HIGH ≥ 0.60 · MEDIUM ≥ 0.40 · LOW < 0.40**

### 2. Rule Engine (`app/rules/rules/`)
Five hot-reloadable YAML scenario packs — edit and reload without redeploy:

| File | Scenario | Example indicators |
|------|----------|--------------------|
| `multi_enrollment.yaml` | Multi-Enrollment | 4+ simultaneous programs, rapid enrollment |
| `network_fraud.yaml` | Network / Collusion | Shared phone or bank account across beneficiaries |
| `socio_economic.yaml` | Socio-Economic Mismatch | Income/asset inconsistency, ghost household |
| `payment_anomalies.yaml` | Payment Anomalies | High payment-gap ratio, high-amount outliers |
| `temporal_anomalies.yaml` | Temporal Anomalies | Suspicious enrollment/payout timing |

### 3. Machine Learning (`ml/`, `app/services/ml_service.py`)
- **XGBoost** classifier with `scale_pos_weight` + **isotonic calibration** for trustworthy probabilities.
- **Isolation Forest** for unsupervised anomaly signal.
- Trained & benchmarked across **multiple datasets** (synthetic OpenG2P, PaySim transactions, OpenG2P demo).
- Honest, defensible metrics (**AUC ≈ 0.86** on realistic overlapping data — not an over-fit 0.99).
- Reproducible analysis scripts in `ml/notebooks/` (figures + PDF report in `ml/outputs/`).

### 4. Explainability
- **SHAP** (`app/core/shap_explainer.py`) — per-decision feature attributions.
- **LLM natural-language explanations** (`app/services/llm_explainer_service.py`) — Ollama / Mistral generates a French analyst-friendly summary of *why* a case was flagged.

### 5. Geospatial Analytics (`app/services/geo_service.py`)
- `/geo/heatmap` — lat/lon fraud-score points for heatmap rendering.
- `/geo/hotspots` — **DBSCAN** clustering to surface geographic fraud hotspots.

### 6. Feedback Loop & Auto-Retraining (`app/services/retraining_service.py`)
- Investigators submit verdicts on flagged cases.
- Verdicts become labels; `/retrain` re-fits XGBoost on accumulated feedback.
- Every run is versioned in **MLflow**, with one-click **rollback** to any prior model.

### 7. Experiment Tracking & Model Versioning (MLflow)
- All training runs logged (params, metrics, git commit) — see `ml/scripts/backfill_mlflow_history.py`.
- Dashboard surfaces model version history and supports artifact rollback.

### 8. Interactive Dashboard (`dashboard/streamlit_app.py`)
Streamlit UI with tabs for: **Score a Beneficiary** (single / scan-all / batch CSV),
**Fraud Cases** (review, status, verdict), **Explainability** (SHAP + rules + raw scores),
**Geospatial Hotspots**, **System Monitoring** (risk distribution), and
**Feedback & Model Retraining** with MLflow version history.

### 9. Observability
- **Prometheus** metrics at `/metrics`; **Grafana** dashboards for live monitoring.
- Structured request logging + full audit trail persisted per decision.

### 10. OpenG2P / Odoo Integration
- Read-only feature extraction from the OpenG2P registry.
- `/scan/all` to batch-score every imported beneficiary.
- Webhook (`app/api/webhook.py`) + companion Odoo `fraud.case` module for case management.

---

## 🧱 Architecture

```
                          ┌──────────────────────────┐
   Odoo / OpenG2P  ─────► │     FastAPI  (app/)       │
   Registry (RO)         │  Auth · Logging · Errors   │
                          └────────────┬──────────────┘
                                       ▼
                          ┌──────────────────────────┐
                          │   DecisionOrchestrator    │
                          └──┬─────────┬─────────┬────┘
                  Features ──┘    Rules │     Graph │     ML
                 (extractor)   (YAML AST) (NetworkX) (XGBoost+IsoForest)
                                       ▼
                       SHAP  +  LLM explanation (Ollama/Mistral)
                                       ▼
                       FraudCaseRepository ─► PostgreSQL (fraud_store)
                                       ▼
                   Streamlit dashboard · Prometheus/Grafana · MLflow
```

---

## 🚀 Quick Start

### Local (API only)
```bash
pip install -r requirements.txt

# 1. Generate synthetic training data
python scripts/generate_synthetic_data.py --n 5000 --output ml/data/synthetic/dataset_ml.csv

# 2. Train the models
python scripts/train_ml_models.py --data ml/data/synthetic/dataset_ml.csv --output app/models_saved/

# 3. Configure environment
cp .env.example .env        # set DB URLs + API_SECRET_KEY

# 4. Run the API
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
#   → Swagger UI at http://localhost:8000/docs
```

### Full stack (Docker Compose)
```bash
docker compose -f ../docker-compose.full.yml up -d
```
| Service | URL |
|---------|-----|
| Fraud API (Swagger) | http://localhost:8002/docs |
| Streamlit dashboard | http://localhost:8501 |
| MLflow | http://localhost:5000 |
| Grafana | http://localhost:3000 |
| Prometheus | http://localhost:9090 |

Stack services: `fraud-engine`, `dashboard`, `fraud-db` (PostgreSQL), `mlflow`,
`ollama`, `prometheus`, `grafana`, plus the OpenG2P/Odoo containers.

---

## 📡 API Endpoints

| Method | Path | Description | Auth |
|--------|------|-------------|------|
| POST | `/api/v1/score` | Score a beneficiary payload (full pipeline) | ✅ |
| GET | `/api/v1/scan/{beneficiary_id}` | Fetch from OpenG2P and score | ✅ |
| POST | `/api/v1/scan/all` | Batch-score all imported beneficiaries | ✅ |
| POST | `/api/v1/score/upload` | Batch scoring via CSV upload | ✅ |
| GET | `/api/v1/cases` | List fraud cases (filterable) | ✅ |
| PUT | `/api/v1/cases/{id}` | Update case status / verdict | ✅ |
| GET | `/api/v1/rules` | List loaded rules | ✅ |
| POST | `/api/v1/rules/reload` | Hot-reload YAML rules from disk | ✅ |
| GET | `/api/v1/geo/heatmap` | Fraud-score points (lat/lon) | ✅ |
| GET | `/api/v1/geo/hotspots` | DBSCAN fraud cluster hotspots | ✅ |
| POST | `/api/v1/feedback` | Submit investigator verdict | ✅ |
| GET | `/api/v1/feedback/stats` | Feedback statistics | ✅ |
| POST | `/api/v1/retrain` | Trigger XGBoost retraining on feedback | ✅ |
| GET | `/api/v1/models/versions` | List recent MLflow runs | ✅ |
| POST | `/api/v1/models/rollback/{run_id}` | Restore a model from an MLflow run | ✅ |
| GET | `/api/v1/health` | Service & model readiness | — |
| GET | `/metrics` | Prometheus metrics | — |
| GET | `/docs` | Swagger UI | — |

Authenticated endpoints require the `X-API-Key` header.

---

## 📁 Project Layout

```
fraud-detection-engine/
├── app/                  # FastAPI application
│   ├── api/              # routes (scoring, cases, rules, graph, geo, webhook)
│   ├── core/             # pipeline, SHAP & LLM explainers
│   ├── services/         # ml, rules, graph, geo, llm, retraining, alert, report
│   ├── rules/rules/      # YAML rule packs (5 scenarios)
│   ├── data/             # OpenG2P feature extractors & repository
│   └── models_saved/     # trained models + *_metadata.json (metrics)
├── dashboard/            # Streamlit UI
├── ml/                   # data science workspace
│   ├── notebooks/        # analysis & report-generation scripts
│   ├── scripts/          # training, MLflow backfill
│   └── outputs/          # generated figures + PDF report (git-ignored)
├── scripts/              # data generation, training, OpenG2P import/scan
├── tests/                # pytest suite
└── requirements.txt
```

---

## 🔬 Reproduce the ML Report
```bash
python ml/notebooks/analyse_modeles.py          # single-dataset figures
python ml/notebooks/rapport_pfe_simple.py       # multi-dataset comparison
python ml/notebooks/evolution_modeles.py        # model evolution 0.99 → 0.86
python ml/notebooks/generer_pdf_rapport.py      # assemble everything into one PDF
# → results land in ml/outputs/ (git-ignored)
```

---

## 🧪 Development
```bash
pip install -r requirements-dev.txt
pytest tests/
```
- Google-style docstrings + type hints on new functions.
- New fraud rules → add a YAML file under `app/rules/rules/` following the existing schema, then `POST /api/v1/rules/reload`.

---

## 📝 Notes
- Trained models, datasets, databases, and generated figures/PDF are **git-ignored**
  (regenerate them with the scripts above). Only the small `*_metadata.json` files —
  which document the trained-model metrics — are committed.
- Designed for academic / PoC use on synthetic and public datasets (PaySim).
