"""Train an XGBoost baseline on the cleaned PaySim training set.

This is the secondary 'transaction fraud' model — complementary to the
existing beneficiary model. Outputs metrics + saved joblib.
"""
import json
from pathlib import Path

import joblib
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.calibration import CalibratedClassifierCV
from sklearn.metrics import (
    roc_auc_score, precision_score, recall_score, f1_score,
    confusion_matrix, average_precision_score,
)
from xgboost import XGBClassifier

DATA = Path(r"C:\Users\Mega Pc\Desktop\poc-v2\poc-v2\fraud-detection-engine\data\paysim_clean_balanced.parquet")
OUT = Path(r"C:\Users\Mega Pc\Desktop\poc-v2\poc-v2\fraud-detection-engine\app\models_saved")

df = pd.read_parquet(DATA)
y = df["is_fraud"].astype(int)
X = df.drop(columns=["is_fraud"])
feat_cols = list(X.columns)
print(f"Training on {len(X):,} rows, {len(feat_cols)} features")
print(f"Fraud rate in training set: {y.mean()*100:.2f}%")

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, stratify=y, random_state=42
)

base = XGBClassifier(
    n_estimators=300, max_depth=6, learning_rate=0.1,
    scale_pos_weight=(len(y_train) - y_train.sum()) / y_train.sum(),
    objective="binary:logistic", eval_metric="aucpr",
    tree_method="hist", random_state=42, n_jobs=-1,
)
print("\nFitting calibrated XGBoost...")
model = CalibratedClassifierCV(base, method="isotonic", cv=3)
model.fit(X_train, y_train)

probs = model.predict_proba(X_test)[:, 1]
preds = (probs >= 0.5).astype(int)

metrics = {
    "roc_auc": round(roc_auc_score(y_test, probs), 4),
    "pr_auc": round(average_precision_score(y_test, probs), 4),
    "precision": round(precision_score(y_test, preds), 4),
    "recall": round(recall_score(y_test, preds), 4),
    "f1": round(f1_score(y_test, preds), 4),
    "confusion_matrix": confusion_matrix(y_test, preds).tolist(),
    "n_train": int(len(X_train)),
    "n_test": int(len(X_test)),
    "fraud_rate": round(float(y.mean()), 6),
}
print("\nMetrics:")
for k, v in metrics.items():
    print(f"  {k}: {v}")

# Feature importance via underlying estimator
inner = model.calibrated_classifiers_[0].estimator
fi = sorted(zip(feat_cols, inner.feature_importances_), key=lambda x: -x[1])
print("\nTop 10 feature importances:")
for name, imp in fi[:10]:
    print(f"  {name:30s}  {imp:.4f}")

OUT.mkdir(parents=True, exist_ok=True)
joblib.dump(model, OUT / "xgboost_paysim.joblib")
with (OUT / "paysim_metadata.json").open("w") as f:
    json.dump({
        "feature_columns": feat_cols,
        "metrics": metrics,
        "feature_importance": {n: float(v) for n, v in fi},
        "source_dataset": "PaySim (Kaggle AIML Dataset)",
        "model_purpose": "Transaction-level fraud detection (complementary to beneficiary model)",
        "training_strategy": "TRANSFER+CASH_OUT only, 1:20 undersampled, isotonic calibration",
    }, f, indent=2)
print(f"\nSaved to {OUT/'xgboost_paysim.joblib'}")
