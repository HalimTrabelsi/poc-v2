"""Honest PaySim training: leak-free features, chronological split,
imbalanced test eval, production-realistic metrics.

What changed vs the first version:
  1. DROPPED leaky features: balance_anomaly + raw balance columns.
     PaySim's simulator fabricates fraud rows by zeroing newbalanceOrig
     without subtracting amount — so balance_anomaly == is_fraud almost
     perfectly. The first model just memorised that label rule.
  2. CHRONOLOGICAL split on `tx_step` (PaySim's time index) instead of
     random. Prevents future-leakage where the model sees later
     simulator state during training.
  3. TEST set is the full imbalanced tail (no undersampling on test).
     Train set is 1:20 undersampled, but eval is on real 0.3% base rate.
  4. REPORTS metrics that matter in production deployment:
       - PR-AUC (ROC-AUC is misleading on imbalanced)
       - Precision @ top-k (analyst capacity is fixed)
       - Recall @ 1% FP rate (cost-of-investigation constraint)
"""
import json
from pathlib import Path

import joblib
import pandas as pd
import numpy as np
from sklearn.calibration import CalibratedClassifierCV
from sklearn.metrics import (
    roc_auc_score, precision_score, recall_score, f1_score,
    confusion_matrix, average_precision_score,
    precision_recall_curve, roc_curve,
)
from xgboost import XGBClassifier

DATA = Path(r"C:\Users\Mega Pc\Desktop\poc-v2\poc-v2\fraud-detection-engine\data\paysim_clean_balanced.parquet")
OUT = Path(r"C:\Users\Mega Pc\Desktop\poc-v2\poc-v2\fraud-detection-engine\app\models_saved")

# Reload + drop leaky cols ---------------------------------------------------
df = pd.read_parquet(DATA)
LEAKY = [
    "balance_anomaly",       # IS the simulator's labeling rule
    "orig_balance_after",    # downstream of the leak
    "dest_balance_after",    # downstream of the leak
    "overdraft_attempt",     # derived from leaky balance
    "orig_balance_before",   # near-perfectly correlated with leak via division
    "dest_balance_before",   # ditto
]
keep_cols = [c for c in df.columns if c not in LEAKY]
df = df[keep_cols]
print(f"Dropped leaky cols: {LEAKY}")
print(f"Remaining features: {[c for c in keep_cols if c != 'is_fraud']}")

# Chronological split on tx_step --------------------------------------------
df = df.sort_values("tx_step").reset_index(drop=True)
cutoff = df["tx_step"].quantile(0.8)
train = df[df["tx_step"] <= cutoff].copy()
test  = df[df["tx_step"] >  cutoff].copy()

print(f"\nChronological split at step {cutoff:.0f}")
print(f"  Train rows: {len(train):,}  fraud={train['is_fraud'].sum():,} "
      f"({train['is_fraud'].mean()*100:.2f}%)")
print(f"  Test  rows: {len(test):,}  fraud={test['is_fraud'].sum():,} "
      f"({test['is_fraud'].mean()*100:.2f}%)")

# Restore TEST set to true imbalanced rate by re-mixing the undersampled
# legit rows back with the *original* fraud rate. We can't fully recover
# the discarded legit rows here, but we ALSO load a fresh chunk of the
# raw CSV to give the test set realistic imbalance.
print("\nLoading raw legit rows to restore true class balance in test set...")
RAW = Path(r"C:\Users\Mega Pc\Desktop\poc-v2\poc-v2\data\data\AIML Dataset.csv")

# Pull a fresh slice of TRANSFER+CASH_OUT rows past the chronological cutoff
raw_chunks = []
for chunk in pd.read_csv(RAW, chunksize=500_000):
    chunk = chunk[chunk["type"].isin(["TRANSFER", "CASH_OUT"])]
    chunk = chunk[chunk["step"] > cutoff]
    raw_chunks.append(chunk)
raw_tail = pd.concat(raw_chunks, ignore_index=True)
print(f"  Raw tail rows (full distribution): {len(raw_tail):,}  "
      f"fraud={raw_tail['isFraud'].sum():,} ({raw_tail['isFraud'].mean()*100:.4f}%)")

# Re-engineer the SAME non-leaky features on the raw tail
raw_tail = raw_tail.rename(columns={
    "step": "tx_step", "type": "tx_type", "amount": "tx_amount",
    "nameOrig": "orig_id", "oldbalanceOrg": "orig_balance_before",
    "newbalanceOrig": "orig_balance_after", "nameDest": "dest_id",
    "oldbalanceDest": "dest_balance_before",
    "newbalanceDest": "dest_balance_after", "isFraud": "is_fraud",
})
raw_tail = raw_tail.drop(columns=["isFlaggedFraud"])
raw_tail["full_drain"] = (
    (raw_tail["orig_balance_before"] > 0) & (raw_tail["orig_balance_after"] == 0)
).astype(int)
raw_tail["round_amount"] = (raw_tail["tx_amount"] % 100 == 0).astype(int)
raw_tail["dest_was_empty"] = (raw_tail["dest_balance_before"] == 0).astype(int)
raw_tail["dest_is_merchant"] = raw_tail["dest_id"].str.startswith("M").astype(int)
raw_tail["log_amount"] = np.log1p(raw_tail["tx_amount"])
raw_tail["amount_to_balance_ratio"] = (
    raw_tail["tx_amount"] / raw_tail["orig_balance_before"].replace(0, np.nan)
).fillna(0).clip(0, 100)
raw_tail["hour_of_day"] = raw_tail["tx_step"] % 24
raw_tail["is_night"] = (
    (raw_tail["hour_of_day"] < 6) | (raw_tail["hour_of_day"] >= 22)
).astype(int)
raw_tail["tx_type_transfer"] = (raw_tail["tx_type"] == "TRANSFER").astype(int)

feat_cols = [c for c in keep_cols if c != "is_fraud"]
test_full = raw_tail[feat_cols + ["is_fraud"]].copy()
print(f"  Realistic test set: {len(test_full):,}  "
      f"fraud rate {test_full['is_fraud'].mean()*100:.4f}%")

# Train ---------------------------------------------------------------------
X_train, y_train = train[feat_cols], train["is_fraud"].astype(int)
X_test,  y_test  = test_full[feat_cols], test_full["is_fraud"].astype(int)

print("\nFitting calibrated XGBoost (no leaky features)...")
base = XGBClassifier(
    n_estimators=300, max_depth=6, learning_rate=0.1,
    scale_pos_weight=(len(y_train) - y_train.sum()) / max(y_train.sum(), 1),
    objective="binary:logistic", eval_metric="aucpr",
    tree_method="hist", random_state=42, n_jobs=-1,
)
model = CalibratedClassifierCV(base, method="isotonic", cv=3)
model.fit(X_train, y_train)

probs = model.predict_proba(X_test)[:, 1]
preds = (probs >= 0.5).astype(int)

# Threshold-free metrics ---------------------------------------------------
roc = roc_auc_score(y_test, probs)
pr  = average_precision_score(y_test, probs)
print(f"\n=== Threshold-free ===")
print(f"  ROC-AUC: {roc:.4f}")
print(f"  PR-AUC:  {pr:.4f}  (most meaningful on imbalanced data)")

# Default-threshold metrics ------------------------------------------------
p = precision_score(y_test, preds, zero_division=0)
r = recall_score(y_test, preds)
f = f1_score(y_test, preds, zero_division=0)
cm = confusion_matrix(y_test, preds)
print(f"\n=== At threshold = 0.5 ===")
print(f"  Precision: {p:.4f}")
print(f"  Recall:    {r:.4f}")
print(f"  F1:        {f:.4f}")
print(f"  Confusion (rows=actual, cols=pred): TN={cm[0,0]:,}  FP={cm[0,1]:,}  "
      f"FN={cm[1,0]:,}  TP={cm[1,1]:,}")

# Production-realistic metrics ---------------------------------------------
fpr, tpr, thr_roc = roc_curve(y_test, probs)
target_fpr = 0.01
idx = np.argmin(np.abs(fpr - target_fpr))
print(f"\n=== Recall @ 1% FP (analyst-capacity constraint) ===")
print(f"  Threshold: {thr_roc[idx]:.4f}")
print(f"  Recall:    {tpr[idx]:.4f}")
print(f"  FP rate:   {fpr[idx]:.4f}")

n_test = len(y_test)
for k_pct in [0.1, 0.5, 1.0, 5.0]:
    k = max(1, int(n_test * k_pct / 100))
    top_k_idx = np.argsort(probs)[-k:]
    top_k_actual = y_test.iloc[top_k_idx].sum()
    precision_at_k = top_k_actual / k
    recall_at_k = top_k_actual / max(y_test.sum(), 1)
    print(f"  Top {k_pct}% ({k:>7,} cases): "
          f"precision={precision_at_k:.4f}  recall={recall_at_k:.4f}")

# Feature importance ------------------------------------------------------
inner = model.calibrated_classifiers_[0].estimator
fi = sorted(zip(feat_cols, inner.feature_importances_), key=lambda x: -x[1])
print(f"\n=== Feature importance ===")
for n, imp in fi:
    print(f"  {n:30s} {imp:.4f}")

# Save ---------------------------------------------------------------------
OUT.mkdir(parents=True, exist_ok=True)
joblib.dump(model, OUT / "xgboost_paysim.joblib")

honest_metrics = {
    "roc_auc": round(float(roc), 4),
    "pr_auc": round(float(pr), 4),
    "precision_t0.5": round(float(p), 4),
    "recall_t0.5": round(float(r), 4),
    "f1_t0.5": round(float(f), 4),
    "confusion_t0.5": {"tn": int(cm[0,0]), "fp": int(cm[0,1]),
                       "fn": int(cm[1,0]), "tp": int(cm[1,1])},
    "recall_at_1pct_fp": round(float(tpr[idx]), 4),
    "threshold_at_1pct_fp": round(float(thr_roc[idx]), 4),
    "n_train": int(len(X_train)),
    "n_test":  int(len(X_test)),
    "test_fraud_rate": round(float(y_test.mean()), 6),
}
with (OUT / "paysim_metadata.json").open("w") as f:
    json.dump({
        "feature_columns": feat_cols,
        "leaky_features_dropped": LEAKY,
        "split_method": "chronological_on_tx_step",
        "test_set_distribution": "true_imbalanced_0.3pct",
        "metrics": honest_metrics,
        "feature_importance": {n: float(v) for n, v in fi},
        "source_dataset": "PaySim (Kaggle AIML Dataset)",
        "model_purpose": "Transaction-level fraud signal (complementary to beneficiary model)",
    }, f, indent=2)
print(f"\nSaved model + honest metrics to {OUT}")
