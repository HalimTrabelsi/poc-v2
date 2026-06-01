"""Train a beneficiary-fraud model on the OpenG2P-aligned demo data.

This is the model that actually matters for the demo: trained on data
that matches the schema the fraud-engine sees in production, using
features extracted by the same logic as features_service.py.

Honest methodology:
  * Stratified random split (no temporal axis on beneficiaries here)
  * Class-weighted training (no oversampling)
  * Evaluate on the held-out 20% with the same fraud rate as the full set
  * Report PR-AUC + per-pattern recall (does the model catch each
    fraud archetype, or only some?)
"""
import json
from pathlib import Path
from collections import Counter

import pandas as pd
import numpy as np
import joblib
from sklearn.model_selection import train_test_split
from sklearn.calibration import CalibratedClassifierCV
from sklearn.metrics import (
    roc_auc_score, average_precision_score,
    precision_score, recall_score, f1_score,
    confusion_matrix, classification_report,
)
from xgboost import XGBClassifier

DEMO_DIR = Path(r"C:\Users\Mega Pc\Desktop\poc-v2\poc-v2\fraud-detection-engine\data\openg2p_demo")
OUT = Path(r"C:\Users\Mega Pc\Desktop\poc-v2\poc-v2\fraud-detection-engine\app\models_saved")

# Load the three demo CSVs and join into one feature table -------------------
beneficiaries = pd.read_csv(DEMO_DIR / "openg2p_beneficiaries_import.csv")
phones = pd.read_csv(DEMO_DIR / "openg2p_phones_import.csv")
payments = pd.read_csv(DEMO_DIR / "openg2p_payments_import.csv")

print(f"Loaded {len(beneficiaries)} beneficiaries, {len(phones)} phones, "
      f"{len(payments)} payments")
print(f"Fraud rate in source data: {beneficiaries['_fraud_label'].mean()*100:.2f}%")

# === Feature engineering (mirror features_service.py logic) ================
# Demographic features
beneficiaries["dob"] = pd.to_datetime(beneficiaries["Date of Birth"], format="%d-%m-%Y")
beneficiaries["age"] = ((pd.Timestamp("2026-06-01") - beneficiaries["dob"]).dt.days / 365.25).astype(int)
beneficiaries["age_under_18"] = (beneficiaries["age"] < 18).astype(int)
beneficiaries["age_over_75"] = (beneficiaries["age"] > 75).astype(int)
beneficiaries["age_invalid"] = ((beneficiaries["age"] < 0) | (beneficiaries["age"] > 110)).astype(int)

# Income mapping
income_map = {"0_5000": 2500, "5001_10000": 7500, "10001_20000": 15000,
              "20001_50000": 35000, "50001_plus": 75000}
beneficiaries["income_numeric"] = beneficiaries["Annual Household Income (USD)"].map(income_map).fillna(0)

# Employment / home / vehicle flags
beneficiaries["is_unemployed"] = (beneficiaries["Employement Status"] == "unemployed").astype(int)
beneficiaries["owns_home"] = (beneficiaries["Home Owned"] == "yes").astype(int)
beneficiaries["owns_vehicle"] = (~beneficiaries["Vehicles Owned"].isin(["none", "two_wheeler"])).astype(int)
beneficiaries["income_vehicle_mismatch"] = (
    (beneficiaries["income_numeric"] < 10000) & (beneficiaries["owns_vehicle"] == 1)
).astype(int)
beneficiaries["income_home_mismatch"] = (
    (beneficiaries["income_numeric"] < 10000) & (beneficiaries["owns_home"] == 1)
).astype(int)

# Shared-phone collision count
phone_counts = phones["phone_sanitized"].value_counts()
phone_lookup = phones.set_index("partner_id")["phone_sanitized"]
beneficiaries["phone_collision_count"] = beneficiaries["Registrant ID"].map(
    lambda rid: phone_counts.get(phone_lookup.get(rid), 1)
)
beneficiaries["shared_phone_flag"] = (beneficiaries["phone_collision_count"] > 1).astype(int)

# Shared-account collision count
acct_counts = beneficiaries["Account Number"].astype(str).value_counts()
beneficiaries["account_collision_count"] = beneficiaries["Account Number"].astype(str).map(acct_counts)
beneficiaries["shared_account_flag"] = (beneficiaries["account_collision_count"] > 1).astype(int)

# Duplicate-name flag (exact-match)
name_counts = beneficiaries["Full Name"].value_counts()
beneficiaries["duplicate_name_flag"] = (
    beneficiaries["Full Name"].map(name_counts) > 1
).astype(int)

# Mass-enrollment flag (same enrollment minute, 5+ partners)
enrol_minute = pd.to_datetime(beneficiaries["_enrollment_date"]).dt.floor("min")
enrol_counts = enrol_minute.value_counts()
beneficiaries["mass_enrollment_flag"] = (
    enrol_minute.map(enrol_counts) >= 5
).astype(int)

# Payment-derived features
pay = payments.set_index("partner_id")
beneficiaries["payment_amount"] = beneficiaries["Registrant ID"].map(pay["amount_issued"]).fillna(0)
beneficiaries["payment_paid"] = beneficiaries["Registrant ID"].map(pay["amount_paid"]).fillna(0)
beneficiaries["payment_round"] = (beneficiaries["payment_amount"] % 100 == 0).astype(int)
beneficiaries["payment_gap_ratio"] = (
    (beneficiaries["payment_amount"] - beneficiaries["payment_paid"])
    / beneficiaries["payment_amount"].replace(0, np.nan)
).fillna(0).clip(0, 1)

# Rapid-payout: enrollment-to-payment delta in days
enrol_dt = pd.to_datetime(beneficiaries["_enrollment_date"])
pay_dt = pd.to_datetime(beneficiaries["Registrant ID"].map(pay["create_date"]))
beneficiaries["days_to_payment"] = (pay_dt - enrol_dt).dt.total_seconds() / 86400.0
beneficiaries["rapid_payout_flag"] = (beneficiaries["days_to_payment"] < 1).astype(int)

# Identity-cluster proxy: same DOB + same last name (5+ partners)
beneficiaries["last_name"] = beneficiaries["Full Name"].str.split().str[-1]
cluster_key = beneficiaries["dob"].astype(str) + "|" + beneficiaries["last_name"]
cluster_counts = cluster_key.value_counts()
beneficiaries["identity_cluster_flag"] = (
    cluster_key.map(cluster_counts) >= 5
).astype(int)

# === Build feature matrix ==================================================
FEATURES = [
    "age", "age_under_18", "age_over_75", "age_invalid",
    "income_numeric", "is_unemployed", "owns_home", "owns_vehicle",
    "income_vehicle_mismatch", "income_home_mismatch",
    "phone_collision_count", "shared_phone_flag",
    "account_collision_count", "shared_account_flag",
    "duplicate_name_flag", "mass_enrollment_flag",
    "payment_amount", "payment_paid", "payment_round", "payment_gap_ratio",
    "days_to_payment", "rapid_payout_flag", "identity_cluster_flag",
]
X = beneficiaries[FEATURES].fillna(0)
y = beneficiaries["_fraud_label"]
patterns = beneficiaries["_fraud_pattern"]

# Stratified split ---------------------------------------------------------
X_train, X_test, y_train, y_test, p_train, p_test = train_test_split(
    X, y, patterns, test_size=0.2, stratify=y, random_state=42
)

print(f"\nTrain: {len(X_train)} ({y_train.mean()*100:.1f}% fraud)  "
      f"Test: {len(X_test)} ({y_test.mean()*100:.1f}% fraud)")

# Train --------------------------------------------------------------------
base = XGBClassifier(
    n_estimators=200, max_depth=4, learning_rate=0.1,
    scale_pos_weight=(len(y_train) - y_train.sum()) / max(y_train.sum(), 1),
    objective="binary:logistic", eval_metric="aucpr",
    tree_method="hist", random_state=42, n_jobs=-1,
)
model = CalibratedClassifierCV(base, method="isotonic", cv=3)
model.fit(X_train, y_train)

probs = model.predict_proba(X_test)[:, 1]
preds = (probs >= 0.5).astype(int)

# Metrics ------------------------------------------------------------------
roc = roc_auc_score(y_test, probs)
pr  = average_precision_score(y_test, probs)
prec = precision_score(y_test, preds, zero_division=0)
rec  = recall_score(y_test, preds)
f1   = f1_score(y_test, preds, zero_division=0)
cm   = confusion_matrix(y_test, preds)

print("\n=== Overall metrics (threshold = 0.5) ===")
print(f"  ROC-AUC:   {roc:.4f}")
print(f"  PR-AUC:    {pr:.4f}")
print(f"  Precision: {prec:.4f}")
print(f"  Recall:    {rec:.4f}")
print(f"  F1:        {f1:.4f}")
print(f"  Confusion: TN={cm[0,0]}  FP={cm[0,1]}  FN={cm[1,0]}  TP={cm[1,1]}")

# Per-pattern recall (does each fraud archetype get caught?)
print("\n=== Per-pattern recall (test set) ===")
test_df = pd.DataFrame({"y": y_test.values, "p": preds, "pattern": p_test.values,
                         "prob": probs})
fraud_only = test_df[test_df["y"] == 1]
for pattern, group in fraud_only.groupby("pattern"):
    if not pattern:
        continue
    caught = int(group["p"].sum())
    total = len(group)
    avg_score = group["prob"].mean()
    print(f"  {pattern:20s}  {caught}/{total} caught "
          f"({caught/total*100:5.1f}%)  avg score={avg_score:.3f}")

# Top-k precision (analyst capacity)
print("\n=== Precision @ top-k (analyst-capacity view) ===")
test_full = pd.DataFrame({"y": y_test.values, "prob": probs}).sort_values("prob", ascending=False)
for k_pct in [1, 5, 10, 20, 30]:
    k = max(1, int(len(test_full) * k_pct / 100))
    top_k = test_full.head(k)
    p_at_k = top_k["y"].mean()
    r_at_k = top_k["y"].sum() / max(y_test.sum(), 1)
    print(f"  Top {k_pct:>2}% ({k:>3} cases): precision={p_at_k:.3f}  recall={r_at_k:.3f}")

# Feature importance
inner = model.calibrated_classifiers_[0].estimator
fi = sorted(zip(FEATURES, inner.feature_importances_), key=lambda x: -x[1])
print("\n=== Feature importance (top 10) ===")
for name, imp in fi[:10]:
    print(f"  {name:30s}  {imp:.4f}")

# Save ---------------------------------------------------------------------
OUT.mkdir(parents=True, exist_ok=True)
joblib.dump(model, OUT / "xgboost_openg2p_demo.joblib")

with (OUT / "openg2p_demo_metadata.json").open("w") as f:
    json.dump({
        "feature_columns": FEATURES,
        "training_set": "OpenG2P-aligned synthetic demo data",
        "n_train": int(len(X_train)),
        "n_test": int(len(X_test)),
        "fraud_rate": float(y.mean()),
        "metrics": {
            "roc_auc": round(float(roc), 4),
            "pr_auc": round(float(pr), 4),
            "precision_t0.5": round(float(prec), 4),
            "recall_t0.5": round(float(rec), 4),
            "f1_t0.5": round(float(f1), 4),
            "confusion": {"tn": int(cm[0,0]), "fp": int(cm[0,1]),
                          "fn": int(cm[1,0]), "tp": int(cm[1,1])},
        },
        "feature_importance": {n: float(v) for n, v in fi},
    }, f, indent=2)
print(f"\nSaved model to {OUT/'xgboost_openg2p_demo.joblib'}")
