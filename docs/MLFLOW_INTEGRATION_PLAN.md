# MLflow Integration Plan — Fraud Detection Engine

## Overview

Integrate MLflow to **track, visualize, and compare** all ML models (XGBoost, Random Forest, LightGBM, Logistic Regression, Isolation Forest, SHAP) with full experiment history, metrics, and data lineage.

---

## Architecture

### 1. **MLflow Tracking Server** (New Docker Service)
- **Service**: `mlflow` in docker-compose
- **Port**: 5000 (web UI)
- **Backend**: SQLite (local) or PostgreSQL (for scale)
- **Artifact Store**: Local filesystem (bind-mount) or S3
- **Container**: Official MLflow image

### 2. **Integration Points**

#### A. Training Scripts
- `ml/scripts/train_robust.py` (main) → **Log all models + metrics**
- `ml/train.py` (wrapper) → **Log params, dataset info**
- `scripts/train_ml_models.py` → **Log batch training**

#### B. Metrics to Track per Model
```
Per-Model Metrics:
  - ROC-AUC, PR-AUC, F1, Precision, Recall
  - Confusion Matrix (TP, FP, FN, TN)
  - Threshold sweep (precision/recall at 0.3–0.9)
  - Calibration error (if calibrated)
  - Feature importance (top 10)

Per-Dataset:
  - Row count, fraud rate, feature count
  - Feature statistics (mean, std, min, max, nulls)
  - Class distribution

Per-Run:
  - Training time, inference time
  - Model size (joblib bytes)
  - Hyperparameters (learning_rate, max_depth, etc.)
  - Data split ratios (60/20/20)
```

#### C. SHAP Integration
- Log SHAP summary plots (bar, dependence for top-5 features)
- Log feature importance comparison (SHAP vs model native)
- Store SHAP values as artifact (JSON for reproducibility)

#### D. Retraining Service
- Hook `app/services/retraining_service.py` to MLflow
- Auto-log metrics after each retrain
- Compare new model vs baseline in MLflow UI

---

## Data Artifacts to Log

### 1. Dataset Summary (JSON)
```json
{
  "name": "dataset_ml.csv",
  "rows": 5000,
  "fraud_rate": 0.05,
  "features": 18,
  "feature_names": ["age", "income", ...],
  "feature_stats": {
    "age": {"mean": 40.5, "std": 15.2, "min": 18, "max": 85},
    "shared_phone_count": {"mean": 0.2, "std": 0.8, "nulls": 0}
  },
  "splits": {"train": 3000, "val": 1000, "test": 1000}
}
```

### 2. Feature Importance Matrix (CSV)
```
feature,xgboost_importance,random_forest,lightgbm,shap_mean
shared_phone_count,50.1,48.3,52.1,0.34
shared_account_count,23.4,25.1,22.5,0.21
network_risk,29.2,31.5,28.9,0.18
```

### 3. Confusion Matrices (PNG per threshold)
- ROC curve, PR curve, calibration curve

### 4. Model Artifacts
- Serialized XGBoost / RandomForest (joblib)
- Metadata (training date, feature names)

---

## Visualization Dashboard (MLflow UI)

### Main Views

1. **Experiments Tab**
   - List all training runs with auto-generated names (date + dataset + model type)
   - Filter by model type, date range, F1 score threshold
   - Compare 2+ runs side-by-side

2. **Metrics Charts**
   - Parallel coordinates: ROC-AUC vs Precision vs F1 vs training_time
   - Line charts: How metrics evolved across runs (trend)
   - Scatter: Model performance vs hyperparameter values

3. **Dataset Profile**
   - Table: Feature statistics (mean, std, distribution)
   - Heatmap: Feature correlations
   - Bar chart: Fraud distribution across demographic groups

4. **Feature Importance**
   - Stacked bar: Top features per model
   - Comparison: XGBoost vs RF vs SHAP
   - SHAP summary plots (bar, violin, dependence)

5. **Model Registry**
   - Production model (latest validated)
   - Staging candidates
   - Archived versions
   - Transition history (dev → staging → prod)

6. **Comparison View**
   - Select 2–5 runs
   - Side-by-side: params, metrics, data, performance curves
   - "Winner" highlighted (best F1 / ROC-AUC)

---

## Proposed Enhancements

### 1. **Hyperparameter Tuning Dashboard**
Track grid-search results visually:
- Heatmap: `learning_rate` vs `max_depth` vs F1-score
- Contour plot: 2D hyperparameter space with score isocontours
- Recommendation: "Best params at (lr=0.05, depth=6, F1=0.88)"

### 2. **Data Drift Detection**
Log dataset statistics at each run to detect:
- Fraud rate drift (e.g., 5% → 8%)
- Feature distribution shift (mean/std changes)
- Missing data increases
- Warnings: "Fraud rate up 2% vs baseline"

### 3. **Cost-Benefit Analysis**
Log business metrics alongside ML metrics:
- False positive rate → cost to investigate
- False negative rate → cost of missed fraud
- Total cost = FP_cost × FPR + FN_cost × FNR
- Chart: Cost vs threshold (show optimal operating point)

### 4. **Model Explainability Report**
Auto-generate per-model:
- Top 5 features by importance
- SHAP summary plot (top 10 features)
- Feature interaction plots (SHAP dependence)
- Actual fraud case explanations (test set)

### 5. **A/B Test Framework**
Pre-prod testing:
- Candidate model on holdout 10% of data
- Compare metrics vs current production
- Decision rule: "Deploy if F1 ≥ prod F1 AND Precision ≥ prod Precision"
- Rollback link: "Revert to run X" (one-click)

### 6. **Anomaly Detection Dashboard**
Monitor inference-time performance:
- Live: How many CRITICAL/HIGH cases detected per day
- Drift: Are today's cases similar to training data?
- Alert: "High fraud rate detected (15% vs 5% baseline)"

---

## Implementation Steps

### Phase 1: MLflow Setup (1–2 hours)
1. Add MLflow service to docker-compose.full.yml
2. Update `ml/scripts/train_robust.py` to log metrics/models
3. Create `ml/utils/mlflow_utils.py` (reusable logging functions)
4. Manual test: Run training, verify runs appear in MLflow UI

### Phase 2: Dashboard Enhancements (2–4 hours)
1. Create Python script: `ml/dashboards/mlflow_custom.py`
   - Query MLflow API
   - Generate custom visualizations (SHAP, feature importance, cost-benefit)
   - Embed in Streamlit or standalone Flask

2. Add "Model Comparison" tab to Streamlit dashboard
   - Latest 5 runs displayed
   - Metrics/params side-by-side

### Phase 3: Retraining Integration (1 hour)
1. Hook `app/services/retraining_service.py` → MLflow
2. Auto-log on `POST /api/v1/retrain`

### Phase 4: Production Monitoring (2–3 hours)
1. Add inference-time logging to `app/core/pipeline.py`
2. Log daily aggregates: case counts, fraud rates, top rules triggered
3. Create "Monitoring" dashboard in Streamlit

---

## Benefits

| Benefit | Impact |
|---------|--------|
| **Reproducibility** | Every model version + data + params tracked; revert to any run |
| **Debugging** | When accuracy drops, compare current run to last good run instantly |
| **Collaboration** | Team sees all experiments; no scattered notebooks/emails |
| **Compliance** | Audit trail: who trained what, when, on what data, with what results |
| **A/B Testing** | Validate new models before production swap (one-click deploy) |
| **Data Drift** | Detect when live data diverges from training (early warning) |

---

## Timeline

- **Week 1**: Phase 1 (setup) + Phase 2 (basic dashboards)
- **Week 2**: Phase 3 (retrain hook) + Phase 4 (monitoring)

---

## Cost / Infrastructure

- **MLflow Server**: ~50 MB RAM, negligible CPU (runs on fraud-engine host)
- **Storage**: ~500 MB for 50 model runs + artifacts (bind-mounted, local disk)
- **Scaling**: If > 1000 runs, migrate backend to PostgreSQL (5 min config change)

---

## Next Steps

1. Review this plan with the team
2. Approve scope (all 4 phases or subset?)
3. I implement Phase 1 immediately (MLflow service + train_robust.py logging)
4. Test with a demo training run
5. Proceed with phases 2–4 based on feedback

