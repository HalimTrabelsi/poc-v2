# MLflow Integration Setup & Usage

## Overview

MLflow tracks all model training runs, metrics, parameters, and artifacts. This enables:
- **Reproducibility**: Every model version is tracked with its exact data, code, and hyperparameters
- **Comparison**: Compare models side-by-side to identify the best performer
- **Visualization**: Dashboard shows trends, feature importance, dataset statistics
- **Audit Trail**: Who trained what, when, on what data, with what results

---

## Quick Start

### 1. Start MLflow Server

Already in `docker-compose.full.yml`:

```bash
docker compose up mlflow -d
```

MLflow UI: **http://localhost:5000**

### 2. Run Training with MLflow Tracking

```bash
cd fraud-detection-engine
python ml/scripts/train_robust.py
```

Logs to MLflow automatically (if installed). Verify in http://localhost:5000 → "fraud_detection_xgboost" experiment.

### 3. View MLflow Experiments Dashboard

```bash
streamlit run ml/dashboards/mlflow_dashboard.py
```

Opens at **http://localhost:8502** (custom port to avoid conflict with fraud dashboard).

---

## What Gets Logged

### Per Training Run

| What | Where | Details |
|------|-------|---------|
| **Dataset Info** | `dataset_info.json` | Rows, features, fraud rate, splits |
| **Feature Stats** | `feature_statistics.json` | Mean, std, min, max per feature |
| **XGBoost Model** | `xgboost_model/` | Serialized model (joblib) |
| **Isolation Forest** | `isolation_forest_model/` | Anomaly detector |
| **Metrics** | MLflow UI | AUC-ROC, F1, Precision, Recall, Calibration |
| **Params** | MLflow UI | Learning rate, max depth, ensemble weights |
| **Tags** | MLflow UI | Dataset name, model type, purpose |

### Example Metrics Logged

```
xgboost/roc_auc = 0.9951
xgboost/f1 = 0.8817
xgboost/precision = 0.9535
xgboost/recall = 0.82
ensemble_weight_ml = 0.30
ensemble_weight_rules = 0.25
ensemble_weight_graph = 0.45
```

---

## Dashboard Features

### 📈 Runs Tab
- List all training runs in chronological order
- View metrics and parameters for each run
- Click a run ID to see detailed logs

### 🔬 Comparison Tab
- Select 2+ runs to compare side-by-side
- Parallel metrics table
- Radar chart showing metrics profiles
- Identify best model by ROC-AUC, F1, etc.

### 📊 Metrics Trend Tab
- Line chart of any metric over time
- Spot performance improvements/regressions
- Example: "Is F1 stable or degrading across runs?"

### 🎯 Dataset Info Tab
- Data split (train/val/test %)
- Feature statistics (mean, std, nulls)
- Fraud rate per split
- Helps detect data drift

---

## Integration with CI/CD

### Auto-Log on Retrain

When you trigger a retrain via the API:

```bash
curl -X POST http://localhost:8002/api/v1/retrain \
  -H "X-API-Key: dev-secret-change-in-prod"
```

The new model automatically:
1. Trains on the latest data
2. Logs all metrics/artifacts to MLflow
3. Compares against previous best
4. Promotes to staging if metrics improve

---

## Querying MLflow Programmatically

### Get Best Model by ROC-AUC

```python
import mlflow

experiment = mlflow.get_experiment_by_name("fraud_detection_xgboost")
runs = mlflow.search_runs(
    experiment_ids=[experiment.experiment_id],
    order_by=["metrics.xgboost/roc_auc DESC"],
    max_results=1,
)
best_run = runs.iloc[0]
print(f"Best ROC-AUC: {best_run['metrics.xgboost/roc_auc']:.4f}")
```

### Load Artifacts

```python
mlflow.artifacts.download_artifacts(
    run_id=best_run["run_id"],
    artifact_path="xgboost_model",
    dst_path="./best_model",
)
```

---

## Data Artifacts Structure

```
mlflow/
├── mlruns/
│   ├── 0/  (default experiment)
│   └── 1/  (fraud_detection_xgboost)
│       ├── abc123/  (run ID)
│       │   ├── artifacts/
│       │   │   ├── dataset_info.json
│       │   │   ├── feature_statistics.json
│       │   │   ├── feature_importance.json
│       │   │   ├── shap_summary.json (if SHAP logged)
│       │   │   ├── xgboost_model/
│       │   │   │   ├── model.pkl
│       │   │   │   └── requirements.txt
│       │   │   └── isolation_forest_model/
│       │   ├── params.yaml
│       │   ├── metrics/
│       │   └── tags.yaml
│       └── def456/  (another run)
└── mlflow.db  (SQLite backend)
```

---

## Scale to Production

### Switch from SQLite to PostgreSQL

For 1000+ runs, use PostgreSQL for reliability:

```bash
# Start MLflow with PostgreSQL backend
mlflow server \
  --backend-store-uri postgresql://user:pass@db:5432/mlflow \
  --default-artifact-root s3://bucket/mlflow \
  --host 0.0.0.0 --port 5000
```

Update `docker-compose.yml` to pass these env vars to the MLflow service.

### S3 Artifact Store

For cloud deployments, store artifacts in S3:

```bash
export AWS_ACCESS_KEY_ID=xxx
export AWS_SECRET_ACCESS_KEY=xxx
mlflow server --default-artifact-root s3://my-bucket/mlflow
```

---

## Troubleshooting

### MLflow UI shows "No Experiments"

1. Check MLflow server is running:
   ```bash
   curl http://localhost:5000/health
   ```
   Expected: `{"status":"ok"}`

2. Check tracking URI in training script:
   ```python
   mlflow.set_tracking_uri("http://localhost:5000")
   ```

3. Verify training actually ran:
   ```bash
   docker logs fraud-engine | grep MLflow
   ```

### Artifacts Not Uploading

1. Check MLflow data volume exists:
   ```bash
   docker volume ls | grep mlflow
   ```

2. Check container can write to volume:
   ```bash
   docker exec mlflow-server ls -la /mlflow
   ```

### Streamlit Dashboard Crashes

Install dependencies:
```bash
pip install mlflow plotly pandas streamlit
```

---

## Next: Advanced Visualizations

See `MLFLOW_INTEGRATION_PLAN.md` for Phase 2+ enhancements:
- Cost-benefit analysis dashboard (false positive cost vs. recall)
- Data drift detection
- A/B testing framework
- Anomaly detection monitor
- SHAP summary plots

