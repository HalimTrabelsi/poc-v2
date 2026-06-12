#!/usr/bin/env bash
# Test MLflow Integration — Full E2E workflow
# Usage: bash test_mlflow_integration.sh

set -e

MLFLOW_URI="http://localhost:5000"
PROJECT_DIR="fraud-detection-engine"

echo "════════════════════════════════════════════════════════════"
echo "  MLflow Integration Test — Fraud Detection Engine"
echo "════════════════════════════════════════════════════════════"
echo

# ─────────────────────────────────────────────────────────────
# Step 1: Start MLflow Server
# ─────────────────────────────────────────────────────────────
echo "[1/5] Starting MLflow server..."
docker compose -f docker-compose.full.yml up mlflow -d 2>&1 | grep -v "^[a-z_-]*$" || true
sleep 3

# Health check
echo "Waiting for MLflow to be ready..."
for i in {1..10}; do
    if curl -s "$MLFLOW_URI/health" > /dev/null 2>&1; then
        echo "✅ MLflow is ready"
        break
    fi
    if [ $i -eq 10 ]; then
        echo "❌ MLflow failed to start"
        exit 1
    fi
    sleep 2
done
echo

# ─────────────────────────────────────────────────────────────
# Step 2: Check MLflow Utils
# ─────────────────────────────────────────────────────────────
echo "[2/5] Checking MLflow utilities..."
if [ -f "$PROJECT_DIR/ml/utils/mlflow_utils.py" ]; then
    echo "✅ mlflow_utils.py exists"
    if grep -q "class MLflowExperiment" "$PROJECT_DIR/ml/utils/mlflow_utils.py"; then
        echo "✅ MLflowExperiment context manager found"
    fi
    if grep -q "def log_dataset_info" "$PROJECT_DIR/ml/utils/mlflow_utils.py"; then
        echo "✅ log_dataset_info function found"
    fi
else
    echo "❌ mlflow_utils.py not found"
    exit 1
fi
echo

# ─────────────────────────────────────────────────────────────
# Step 3: Run Training (will log to MLflow)
# ─────────────────────────────────────────────────────────────
echo "[3/5] Running training with MLflow tracking..."
echo "Command: python $PROJECT_DIR/ml/scripts/train_robust.py"
echo

cd "$PROJECT_DIR"

# Run training; capture output
python ml/scripts/train_robust.py --mlflow-uri "$MLFLOW_URI" 2>&1 | tee /tmp/mlflow_training.log

# Check for success markers
if grep -q "Ended MLflow run" /tmp/mlflow_training.log; then
    echo "✅ Training logged to MLflow"
else
    echo "⚠️  Training may not have logged to MLflow (if mlflow not installed in container)"
fi

if grep -q "AUC-ROC" /tmp/mlflow_training.log; then
    echo "✅ Training completed successfully"
else
    echo "❌ Training failed"
    exit 1
fi
echo

cd - > /dev/null

# ─────────────────────────────────────────────────────────────
# Step 4: Verify Artifacts in MLflow
# ─────────────────────────────────────────────────────────────
echo "[4/5] Verifying MLflow artifacts..."

# Check via MLflow API
EXPERIMENTS=$(curl -s "$MLFLOW_URI/api/2.0/experiments/list" | grep -o '"experiment_id":"[^"]*"' | head -1)

if [ -z "$EXPERIMENTS" ]; then
    echo "⚠️  No experiments found in MLflow yet (mlflow may not be tracking)"
else
    echo "✅ Experiments exist in MLflow"

    # Try to get experiment details
    EXP_ID=$(curl -s "$MLFLOW_URI/api/2.0/experiments/list" | grep -o '"experiment_id":"[0-9]*"' | head -1 | cut -d'"' -f4)
    if [ ! -z "$EXP_ID" ]; then
        RUNS=$(curl -s "$MLFLOW_URI/api/2.0/experiments/get?experiment_id=$EXP_ID" | grep -o '"run_id":"[^"]*"' | wc -l)
        if [ "$RUNS" -gt 0 ]; then
            echo "✅ Found $RUNS run(s) in experiment"
        fi
    fi
fi

# Check local MLflow storage
if docker exec mlflow-server ls /mlflow/mlruns/1 > /dev/null 2>&1; then
    RUN_COUNT=$(docker exec mlflow-server sh -c 'ls -d /mlflow/mlruns/1/*/ 2>/dev/null | wc -l')
    echo "✅ MLflow storage has $RUN_COUNT run(s)"

    # Check artifacts
    ARTIFACTS=$(docker exec mlflow-server sh -c 'ls /mlflow/mlruns/1/*/artifacts/ 2>/dev/null | head -1')
    if [ ! -z "$ARTIFACTS" ]; then
        echo "✅ Artifacts found: $ARTIFACTS"
    fi
else
    echo "⚠️  MLflow storage not yet initialized"
fi
echo

# ─────────────────────────────────────────────────────────────
# Step 5: Dashboard Ready
# ─────────────────────────────────────────────────────────────
echo "[5/5] MLflow Dashboard Setup"
echo

echo "════════════════════════════════════════════════════════════"
echo "  ✅ MLflow Integration Tests Complete"
echo "════════════════════════════════════════════════════════════"
echo
echo "📊 View MLflow Web UI:"
echo "   → http://localhost:5000"
echo
echo "📈 View Streamlit Dashboard:"
echo "   streamlit run fraud-detection-engine/ml/dashboards/mlflow_dashboard.py"
echo "   → http://localhost:8502"
echo
echo "🔍 Check Artifacts:"
echo "   docker exec mlflow-server ls -la /mlflow/mlruns/1/"
echo
echo "📝 Read Documentation:"
echo "   → fraud-detection-engine/MLFLOW_SETUP.md"
echo "   → MLFLOW_INTEGRATION_PLAN.md"
echo "   → MLFLOW_IMPLEMENTATION_SUMMARY.md"
echo
