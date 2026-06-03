# OpenG2P Fraud Detection Engine v2 — Full Technical Report

## Table of Contents
1. [Executive Summary](#1-executive-summary)
2. [System Architecture](#2-system-architecture)
3. [Technology Stack](#3-technology-stack)
4. [Feature Implementation Details](#4-feature-implementation-details)
5. [Algorithm Explanations](#5-algorithm-explanations)
6. [API Reference](#6-api-reference)
7. [Data Flow](#7-data-flow)
8. [Configuration & Deployment](#8-configuration--deployment)

---

## 1. Executive Summary

The OpenG2P Fraud Detection Engine v2 is a production-grade, AI-powered fraud detection system integrated with the OpenG2P social protection platform (built on Odoo). It scores every registered beneficiary across five detection dimensions and persists results for investigator review.

**10 major capabilities implemented:**

| # | Feature | Impact |
|---|---------|--------|
| 1 | Real-Time Webhook & PostgreSQL NOTIFY trigger | Sub-second scoring on registration |
| 2 | Deduplication table as ML feature | Leverages OpenG2P's built-in dedup results |
| 3 | Graph Neural Network / Personalized PageRank | Network fraud propagation |
| 4 | Temporal Anomaly Detection | Velocity & timing pattern rules |
| 5 | Geospatial Fraud Clustering (DBSCAN) | Geographic hotspot detection |
| 6 | Investigator Feedback Loop | Human-in-the-loop XGBoost retraining |
| 7 | PDF/CSV Audit Report Export | Downloadable professional reports |
| 8 | Batch CSV Upload Scoring | Score 10,000 beneficiaries at once |
| 9 | MLflow Model Versioning | Full experiment tracking + rollback |
| 10 | Alert System | Slack/Teams/Odoo inbox notifications |

---

## 2. System Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        OpenG2P / Odoo                           │
│  res_partner  g2p_program_membership  g2p_payment  g2p_dedup   │
│       │                                                          │
│  PostgreSQL NOTIFY trigger ──────────────────────────────┐      │
└──────────────────────────────────────────────────────────│──────┘
                                                           │
┌──────────────────────────────────────────────────────────▼──────┐
│              Fraud Detection Engine (FastAPI / Python)           │
│                                                                  │
│  ┌─────────────┐  ┌──────────────┐  ┌────────────────────────┐ │
│  │  Scanner    │  │  Webhook     │  │  REST API              │ │
│  │  Service   │  │  Endpoint   │  │  /api/v1/score/...     │ │
│  │ (LISTEN +  │  │  POST       │  │  /api/v1/cases/...     │ │
│  │  poll)     │  │  /webhook/  │  │  /api/v1/geo/...       │ │
│  └──────┬─────┘  └──────┬──────┘  └───────────┬────────────┘ │
│         └───────────────┴──────────────────────┘              │
│                          │                                      │
│              DecisionOrchestrator                               │
│         ┌────────────────┼────────────────┐                    │
│         ▼                ▼                ▼                     │
│   FeatureEngineer   RuleService      MLScorer                  │
│   (46 features)     (19 YAML rules)  (XGBoost + IsoForest)    │
│         │                │                │                     │
│         └────────────────┼────────────────┘                    │
│                          ▼                                      │
│                   GraphAnalyzer                                 │
│              (PageRank + Community)                             │
│                          │                                      │
│                   Explainer (SHAP)                              │
│                          │                                      │
│                   FraudCaseRepository                           │
│                   (PostgreSQL fraud_store)                      │
│                          │                                      │
│                   AlertService ──► Slack / Odoo Inbox          │
└─────────────────────────────────────────────────────────────────┘
              │                          │
     ┌────────▼──────┐          ┌────────▼──────┐
     │   Streamlit   │          │    MLflow     │
     │   Dashboard   │          │   Tracking    │
     │  (6 pages)    │          │  (SQLite DB)  │
     └───────────────┘          └───────────────┘
```

### Scoring Formula

```
final_score = 0.30 × rule_score + 0.50 × ml_score + 0.20 × graph_score
```

| Component | Weight | Source |
|-----------|--------|--------|
| Rule score | 30% | Weighted sum of triggered YAML rules |
| ML score | 50% | 0.70 × XGBoost + 0.30 × Isolation Forest |
| Graph score | 20% | 0.50 × PageRank + 0.30 × community_risk + 0.20 × density |

### Risk Thresholds

| Level | Threshold | Action |
|-------|-----------|--------|
| CRITICAL | ≥ 0.80 | BLOCK_PAYMENT |
| HIGH | ≥ 0.60 | MANUAL_REVIEW |
| MEDIUM | ≥ 0.40 | MONITOR |
| LOW | < 0.40 | CLEAR |

---

## 3. Technology Stack

### Core Framework
- **FastAPI** — Async Python API framework. Chosen for automatic OpenAPI docs, Pydantic validation, BackgroundTasks, and high throughput via uvicorn ASGI.
- **Pydantic v2** — Data validation and settings management. All request/response models are typed Pydantic classes.
- **SQLAlchemy Core** — Thin DB access layer used with raw SQL (not ORM) for precision and performance on complex analytical queries.
- **psycopg2** — Low-level PostgreSQL driver. Used directly (not via SQLAlchemy) for the NOTIFY/LISTEN real-time channel because SQLAlchemy does not expose `select.select()` on the connection socket.

### Machine Learning
- **XGBoost** — Gradient-boosted decision trees. Primary fraud classifier (70% of ML score). Trained on 46 features with `scale_pos_weight` to handle class imbalance.
- **Isolation Forest** — Unsupervised anomaly detector (30% of ML score). Detects unusual feature combinations without needing labels. Score is inverted so higher = more anomalous.
- **SHAP (SHapley Additive exPlanations)** — Computes how much each feature contributed to a specific prediction. Uses `TreeExplainer` for fast tree-model explanations.
- **scikit-learn** — Provides Isolation Forest, DBSCAN, preprocessing utilities.
- **joblib** — Model serialization format (`.joblib` files). Faster than pickle for NumPy arrays.

### Graph Analysis
- **NetworkX** — Graph construction and algorithms. Nodes = beneficiaries, edges = shared phone/bank account. Ego-network expanded to 2 hops.
- **Personalized PageRank** — `nx.pagerank(G, personalization=fraud_scores)` seeds known fraud scores into the random walk. Fraud risk propagates through the network.
- **Label Propagation** — `nx.community.label_propagation_communities()` for community detection. Falls back to connected components.

### Geospatial
- **scikit-learn DBSCAN** — Density-Based Spatial Clustering. Groups beneficiaries by geographic proximity. `eps=0.15` degrees (~17 km), `min_samples=2`.
- **pydeck** — WebGL-based geospatial visualization bundled with Streamlit. Used for `HeatmapLayer` + `ScatterplotLayer`.

### Data & Features
- **pandas** — Feature matrix construction, CSV parsing, data alignment.
- **numpy** — Numerical operations, array concatenation, haversine distance.

### Reports
- **reportlab** — Pure-Python PDF generation library. Builds full A4 PDF reports with tables, banners, and embedded matplotlib charts (PNG bytes).
- **matplotlib** — Chart generation for score breakdown bars and SHAP waterfall charts, rendered to PNG and embedded in PDF.

### ML Experiment Tracking
- **MLflow** — Tracks every training run: parameters, metrics, and model artifacts. Uses local SQLite backend (`mlflow.db`) — no server needed. Supports rollback by downloading artifacts from a previous run.

### Dashboard
- **Streamlit** — Python-native web dashboard. Multi-page layout via `st.sidebar.radio`. Auto-reloads on file save.
- **plotly** — Interactive charts (used in Monitoring page).

### Alerting
- **requests** — HTTP client used to POST webhook payloads to Slack/Teams.
- **SQLAlchemy + direct SQL** — Writes `mail_message` rows directly into Odoo's database for internal inbox delivery.

### Infrastructure
- **Docker** — Multi-stage build: `builder` stage runs `pip install`, `runtime` stage copies site-packages only. Minimizes image size.
- **PostgreSQL NOTIFY/LISTEN** — Native pub/sub built into PostgreSQL. The trigger function calls `pg_notify('fraud_queue', partner_id)` on every INSERT/UPDATE to `res_partner`.
- **threading** — Background daemon threads for the NOTIFY listener, weekly retraining scheduler, and fire-and-forget alert dispatch.

---

## 4. Feature Implementation Details

### Feature 1: Real-Time Webhook & PostgreSQL NOTIFY

**Problem**: Polling OpenG2P every N minutes creates detection lag. A beneficiary could receive payment before fraud is detected.

**Solution**: A PostgreSQL trigger fires `pg_notify('fraud_queue', partner_id::text)` immediately on `res_partner` INSERT/UPDATE. The engine's scanner service holds a raw psycopg2 connection with `LISTEN fraud_queue` and wakes up within milliseconds.

**Files**:
- `scripts/install_pg_trigger.py` — Installs the plpgsql trigger function and trigger
- `app/services/scanner_service.py` — `_listen_loop()` runs `select.select()` with 5s timeout on the connection socket; `_poll_loop()` runs every 60s as fallback
- `app/api/webhook.py` — `POST /api/v1/webhook/beneficiary-saved` — HTTP webhook for external integrations (validates `X-Webhook-Secret` header)

**Latency**: ~1 second from registration to completed fraud score.

---

### Feature 2: Deduplication as ML Feature

**Problem**: OpenG2P runs its own deduplication checks. Ignoring those results misses a direct fraud signal.

**Solution**: The feature extractor reads `g2p_program_membership.deduplication_status` and the many-to-many duplicate relationship table. Three new features feed into both the rule engine and XGBoost:

- `is_dedup_flagged` — binary: any dedup status is 'duplicate'
- `dedup_programs_count` — how many programs flagged this beneficiary
- `has_duplicate_record` — binary: appears in the duplicate relationship table

Two new rules (NF005, NF006) in `network_fraud.yaml` trigger on these features.

**Files**:
- `app/data/extractors.py` — `dedup_agg` CTE
- `app/rules/rules/network_fraud.yaml` — NF005, NF006

---

### Feature 3: Graph Neural Network / Personalized PageRank

**Problem**: Simple shared-count formulas (`shared_phone * 0.35 + shared_account * 0.50`) don't propagate risk transitively. If A and B share a phone, and B is a known fraudster, A should score higher.

**Solution**: Build an ego-network (2 hops) for each beneficiary from shared phone/bank edges. Run Personalized PageRank seeded by known fraud scores from the database. The random walk converges such that nodes adjacent to confirmed fraudsters receive higher scores.

**Algorithm**:
```python
personalization = {node: fraud_scores.get(node, 1/N) for node in G.nodes}
pagerank = nx.pagerank(G, alpha=0.85, personalization=personalization)
```

Community detection then identifies whether the beneficiary's cluster has high fraud density:
```python
community_risk = (fraudulent nodes in community) / (community size)
```

**Network score** = `0.50 × pagerank + 0.30 × community_risk + 0.20 × density`

**Files**: `app/services/graph_service.py`

---

### Feature 4: Temporal Anomaly Detection

**Problem**: Fraud rings register many beneficiaries rapidly and receive payments quickly before detection.

**Solution**: Seven new temporal features extracted via SQL window aggregations:

| Feature | Description |
|---------|-------------|
| `enroll_last_30d` | Enrollments in last 30 days for this program |
| `enroll_last_90d` | Enrollments in last 90 days |
| `enrollment_velocity` | Enrollments per day since registration |
| `days_reg_to_first_enroll` | Days from registration to first enrollment |
| `days_reg_to_first_payment` | Days from registration to first payment received |
| `payments_within_7d_of_enroll` | Payments received within 7 days of enrollment |
| `payment_cycle_count` | Number of payment cycles participated in |

Five YAML rules (TA001–TA005) detect: rapid enrollment, immediate payment after enrollment (< 7 days), high enrollment velocity, and suspicious registration age.

**Files**:
- `app/data/extractors.py` — `temporal_enroll_agg` and `temporal_payment_agg` CTEs
- `app/rules/rules/temporal_anomalies.yaml`

---

### Feature 5: Geospatial Fraud Clustering (DBSCAN)

**Problem**: Fraud rings often operate from specific geographic areas. Detecting geographic clusters identifies coordinated activity.

**Solution**: DBSCAN clusters beneficiaries by latitude/longitude (or synthetic coordinates derived deterministically from `partner_id` when real GPS is missing). Clusters with high fraud rates become labeled hotspots.

**Algorithm Parameters**:
- `eps = 0.15` degrees (~17 km at equatorial latitudes)
- `min_samples = 2` (as few as 2 nearby beneficiaries form a cluster)
- `metric = 'euclidean'` on (lat, lon) — fast; precise enough for Guinea-scale distances

Cluster risk is labeled based on fraud rate: ≥ 50% = CRITICAL, ≥ 30% = HIGH, etc.

**Dashboard**: pydeck 3D HeatmapLayer (weight = fraud score) + ScatterplotLayer for HIGH/CRITICAL points.

**Files**: `app/services/geo_service.py`

---

### Feature 6: Investigator Feedback Loop

**Problem**: ML models trained on synthetic data drift from real fraud patterns over time.

**Solution**: Investigators submit verdicts (`confirmed_fraud` / `false_positive` / `uncertain`) via the dashboard or API. Verdicts are stored in the `fraud_feedback` table with the case's feature vector. Weekly, or on-demand, XGBoost is retrained on confirmed labels augmented with the original synthetic training CSV.

**Retraining pipeline**:
1. Pull all `confirmed_fraud` and `false_positive` records from `fraud_feedback`
2. Reconstruct feature matrix from stored JSONB feature vectors
3. Augment with `training_data.csv` (original synthetic data) if available
4. Compute `scale_pos_weight = n_clean / n_fraud` to handle class imbalance
5. Fit `XGBClassifier(n_estimators=400, max_depth=6, learning_rate=0.05)`
6. Persist to `models_saved/xgboost.joblib`
7. Hot-reload the `MLScorer` singleton — new model takes effect immediately

**Files**: `app/services/retraining_service.py`, `app/data/repository.py`

---

### Feature 7: PDF/CSV Audit Report Export

**Problem**: Investigators need printable reports for compliance submissions and audit trails.

**Solution**: `reportlab` generates a multi-section A4 PDF per case including:
- Case header (ID, beneficiary, date, status)
- Risk banner (color-coded: red/orange/yellow/green)
- Score breakdown horizontal bar chart (matplotlib → PNG → embedded)
- Rules triggered table
- SHAP feature contributions bar chart
- Full feature vector appendix

CSV export uses Python's `csv.DictWriter` for bulk compliance exports.

**Files**: `app/services/report_service.py`  
**Endpoints**: `GET /api/v1/cases/{case_id}/report/pdf`, `GET /api/v1/cases/export/csv`

---

### Feature 8: Batch CSV Upload Scoring

**Problem**: Investigators need to score a custom list of beneficiaries (e.g., from a program enrollment export) without clicking one-by-one.

**Solution**: `POST /api/v1/score/batch` accepts a `.csv` file upload. It auto-detects the beneficiary ID column (supports `beneficiary_id`, `partner_id`, or `id`), scores all rows in parallel using `ThreadPoolExecutor(max_workers=8)`, and returns a results CSV.

**Limits**: 10,000 beneficiaries per request, 8 parallel workers.

**Dashboard**: File uploader widget in "Score / Scan / Batch" → "Batch CSV Upload" tab with download button for results.

**Files**: `app/api/routes.py` (`score_batch_csv` endpoint)

---

### Feature 9: MLflow Model Versioning

**Problem**: After retraining, there is no way to compare runs or revert to a previous model if the new one performs worse.

**Solution**: Every retraining run is logged to MLflow with:
- **Parameters**: n_estimators, max_depth, learning_rate, scale_pos_weight, n_features, n_feedback_labels
- **Metrics**: feedback_accuracy, n_samples
- **Artifact**: the full XGBoost model (`mlflow.sklearn.log_model`)

The MLflow backend is a local SQLite file (`app/models_saved/mlflow.db`) — no server required.

**Rollback**: `POST /api/v1/models/rollback/{run_id}` downloads the model artifact from a specific run via `mlflow.sklearn.load_model(f"runs:/{run_id}/xgboost_model")`, saves it to disk, and hot-reloads the scorer.

**Dashboard**: Model History table in Monitoring page with one-click rollback selector.

**Files**: `app/services/retraining_service.py` (`_mlflow_log`, `get_mlflow_runs`, `rollback_to_run`)

---

### Feature 10: Alert System

**Problem**: Investigators only see fraud when they open the dashboard. CRITICAL cases need immediate notification.

**Solution**: After every fraud decision, `AlertService.send(decision)` is called in a fire-and-forget background thread. It dispatches to three channels:

**Channel 1 — Structured log** (always active):
```
ALERT [CRITICAL] beneficiary=42 score=0.926 recommendation=BLOCK_PAYMENT case=abc123...
```

**Channel 2 — HTTP Webhook** (configured via `ALERT_WEBHOOK_URL`):
Slack-compatible Block Kit JSON payload. Works with Slack, Microsoft Teams, PagerDuty, and any generic webhook receiver.

**Channel 3 — Odoo mail.message** (configured via `ALERT_ODOO_ENABLED=true`):
Direct SQL INSERT into the `mail_message` table, linked to the `res.partner` record of the flagged beneficiary. Appears in the beneficiary's Odoo internal chat/inbox without any Odoo module installation.

**Files**: `app/services/alert_service.py`, `app/services/decision_service.py` (step 9)

---

## 5. Algorithm Explanations

### XGBoost (Gradient Boosted Trees)

XGBoost trains an ensemble of decision trees sequentially. Each new tree corrects the errors of the previous ones. For fraud detection:
- **Input**: 46 numerical features per beneficiary
- **Output**: probability of fraud (0.0–1.0)
- **`scale_pos_weight`**: automatically set to `n_clean / n_fraud` to compensate for class imbalance (fraud is rare)
- **`eval_metric="logloss"`**: optimizes log-likelihood, appropriate for probability calibration

XGBoost is fast enough for real-time scoring (< 5ms per prediction) while consistently outperforming logistic regression and random forests on tabular fraud datasets.

### Isolation Forest

An unsupervised anomaly detection algorithm. It randomly partitions the feature space into half-spaces. Anomalies (unusual feature combinations) require fewer partitions to isolate — they have shorter average path lengths. The score is inverted and normalized to [0, 1]. This catches novel fraud patterns that labeled training data might not cover.

Combined score: `ml_score = 0.70 × xgboost_prob + 0.30 × iso_score`

### SHAP (SHapley Additive exPlanations)

SHAP values explain individual predictions by computing the average marginal contribution of each feature across all possible feature orderings (from cooperative game theory). For XGBoost, `TreeExplainer` computes exact SHAP values efficiently without sampling.

A positive SHAP value means the feature pushed the score higher (toward fraud). A negative value means it pushed toward legitimate. The dashboard shows the top 10 features by |SHAP value|.

### Personalized PageRank

Standard PageRank models a random web surfer who clicks random links and occasionally teleports to a random page. In the social graph:
- **Nodes** = beneficiaries
- **Edges** = shared phone numbers or bank accounts
- **Personalization** = instead of teleporting to a random node uniformly, the surfer teleports to nodes proportional to their known fraud score

This makes PageRank converge to high values for nodes that are:
1. Directly connected to many known fraudsters, OR
2. Close to known fraudsters in the network even if not directly connected

`alpha=0.85` means 85% probability of following an edge, 15% probability of teleporting.

### DBSCAN

Density-Based Spatial Clustering of Applications with Noise. Unlike k-means, DBSCAN:
- Does not require specifying the number of clusters
- Can find arbitrarily shaped clusters
- Labels outliers as noise (not forced into a cluster)

A point is a "core point" if it has ≥ `min_samples` neighbors within `eps` distance. Points reachable from core points form a cluster. For fraud detection, geographic clusters with high fraud rates indicate coordinated regional activity.

### Label Propagation (Community Detection)

A simple but effective algorithm: each node is initially its own community. Iteratively, each node adopts the most common community label among its neighbors. Convergence produces communities that are internally dense. The fraction of a community that are confirmed/high-score fraudsters becomes the `community_risk` signal.

---

## 6. API Reference

### Scoring
| Method | Path | Description |
|--------|------|-------------|
| POST | `/api/v1/score/beneficiary/{id}` | Score a single beneficiary from OpenG2P |
| POST | `/api/v1/score/features` | Score from a pre-built feature dict |
| POST | `/api/v1/score/batch` | Upload CSV, score all, download results CSV |

### Cases
| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/v1/cases` | List cases with optional filters |
| PATCH | `/api/v1/cases/{id}/status` | Update case status |
| GET | `/api/v1/cases/{id}/report/pdf` | Download PDF audit report |
| GET | `/api/v1/cases/export/csv` | Export all cases as CSV |

### Explainability
| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/v1/explain/{beneficiary_id}` | Full SHAP + rule explanation for latest decision |

### Geospatial
| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/v1/geo/heatmap` | Lat/lon + weight points for pydeck |
| GET | `/api/v1/geo/hotspots` | DBSCAN cluster summaries |

### Feedback & Retraining
| Method | Path | Description |
|--------|------|-------------|
| POST | `/api/v1/cases/{id}/feedback` | Submit investigator verdict |
| GET | `/api/v1/feedback/stats` | Verdict counts + model precision estimate |
| POST | `/api/v1/retrain` | Trigger background retraining |

### Model Versioning
| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/v1/models/versions` | List recent MLflow training runs |
| POST | `/api/v1/models/rollback/{run_id}` | Restore model from MLflow run |

### Infrastructure
| Method | Path | Description |
|--------|------|-------------|
| POST | `/api/v1/scan/now` | Immediate full-table scan |
| GET | `/api/v1/scan/status` | Unscored beneficiary count |
| GET | `/api/v1/beneficiaries` | List beneficiary IDs |
| POST | `/api/v1/webhook/beneficiary-saved` | HTTP webhook endpoint |
| GET | `/api/v1/health` | Service health + model readiness |

### Authentication
All endpoints require `X-API-Key` header (configured via `API_SECRET_KEY` in `.env`).  
The webhook endpoint additionally accepts `X-Webhook-Secret`.

---

## 7. Data Flow

### Real-Time Scoring (NOTIFY path)
```
Beneficiary registered in OpenG2P
    → PostgreSQL trigger fires pg_notify('fraud_queue', partner_id)
    → scanner_service._listen_loop() wakes up via select.select()
    → _score_one(partner_id) called
    → DecisionOrchestrator.score_beneficiary(partner_id)
        → FeatureEngineer: 46-feature SQL extraction from 8 CTEs
        → RuleService: YAML rule evaluation → rule_score + triggered_rules
        → GraphAnalyzer: 2-hop network → PageRank → network_score
        → MLScorer: XGBoost + IsoForest → ml_score
        → final_score = 0.30×rule + 0.50×ml + 0.20×graph
        → Explainer: SHAP values + natural language summary
        → FraudCaseRepository.save_decision() → fraud_cases table
        → AlertService.send() → background thread
            → log WARNING
            → POST Slack webhook (if configured)
            → INSERT mail_message into Odoo (if enabled)
```

### Feedback-Driven Retraining
```
Investigator submits verdict via dashboard
    → POST /api/v1/cases/{id}/feedback
    → fraud_feedback table (case_id, verdict, features JSONB, investigator)

Weekly (or manual POST /api/v1/retrain):
    → get_confirmed_labels() from fraud_feedback
    → Reconstruct X_feedback, y_feedback from stored JSONB features
    → Augment with training_data.csv if available
    → XGBClassifier.fit(X_combined, y_combined)
    → joblib.dump(model, 'xgboost.joblib')
    → mlflow.start_run() → log params, metrics, model artifact
    → MLScorer singleton hot-reloaded (new model active immediately)
```

### Feature Extraction (46 features across 8 SQL CTEs)
```sql
WITH
  base          AS (beneficiary demographics, registration age, address fields),
  prog_agg      AS (program membership counts, earliest enrollment date),
  phone_agg     AS (phone count, shared-phone network size),
  bank_agg      AS (bank account count, shared-bank network size),
  dedup_agg     AS (dedup status, duplicate program count),
  temporal_enroll_agg AS (enrollment velocity, 30d/90d counts, days-to-enroll),
  temporal_payment_agg AS (days-to-payment, payments-within-7d, cycle count),
  network_risk  AS (composite: shared_phone×0.35 + shared_bank×0.50 + dedup×0.15)
SELECT ... (46 columns)
```

---

## 8. Configuration & Deployment

### Environment Variables (`.env`)
```ini
# Databases
OPENG2P_DB_URL=postgresql://odoo:odoo@db:5432/openg2p
FEATURE_STORE_URL=postgresql://fraud:fraud@fraud-db:5432/fraud_store

# Thresholds
CRITICAL_THRESHOLD=0.80
HIGH_THRESHOLD=0.60
MEDIUM_THRESHOLD=0.40

# Alerts
ALERT_WEBHOOK_URL=https://hooks.slack.com/services/...  # leave blank to disable
ALERT_MIN_RISK_LEVEL=CRITICAL
ALERT_ODOO_ENABLED=true

# MLflow
MLFLOW_TRACKING_URI=sqlite:///app/models_saved/mlflow.db
MLFLOW_EXPERIMENT_NAME=fraud-detection-engine

# API Security
API_SECRET_KEY=change-this-in-production
```

### Docker Deployment
```bash
# Build
docker compose build fraud-engine

# Start all services
docker compose up -d

# Install PostgreSQL trigger (one-time)
docker compose exec fraud-engine python scripts/install_pg_trigger.py

# Run demo scenarios
docker compose exec fraud-engine python scripts/demo_fraud_scenarios.py
```

### Services
| Service | Port | Description |
|---------|------|-------------|
| fraud-engine | 8000 | FastAPI REST API |
| dashboard | 8501 | Streamlit dashboard |
| fraud-db | 5433 | Dedicated PostgreSQL for fraud_store |
| mlflow | 5000 | (Optional) MLflow UI server |

### Model Files (`app/models_saved/`)
| File | Description |
|------|-------------|
| `xgboost.joblib` | Active XGBoost classifier |
| `isolation_forest.joblib` | Active Isolation Forest |
| `metadata.json` | Feature column list, training date, metrics |
| `training_data.csv` | Original synthetic training data (augments retraining) |
| `mlflow.db` | SQLite MLflow experiment tracking database |

---

*Generated: OpenG2P Fraud Detection Engine v2.0 — Full Implementation Report*
