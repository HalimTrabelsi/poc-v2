"""Profile the AIML (PaySim) dataset for fraud-engine ingestion.

Reports: dtypes, null %, cardinality, fraud-label balance, per-type fraud rate,
balance-delta sanity, top-suspicious patterns.
"""
import pandas as pd
import numpy as np
from pathlib import Path

CSV = Path(r"C:\Users\Mega Pc\Desktop\poc-v2\poc-v2\data\data\AIML Dataset.csv")

print("=" * 70)
print("AIML / PAYSIM DATASET PROFILE")
print("=" * 70)

# 1) Header & dtypes via small sample
sample = pd.read_csv(CSV, nrows=10_000)
print(f"\nColumns ({len(sample.columns)}):")
for c in sample.columns:
    print(f"  {c:25s} {str(sample[c].dtype):12s}  example={sample[c].iloc[0]!r}")

# 2) Full-file aggregates via chunked pass
print("\nFull-file scan (chunked) ...")
chunks = pd.read_csv(CSV, chunksize=500_000)
total_rows = 0
null_counts = None
fraud_count = 0
flagged_count = 0
type_counts = {}
type_fraud = {}
amount_stats = []
balance_anomalies = 0

for chunk in chunks:
    total_rows += len(chunk)
    nc = chunk.isna().sum()
    null_counts = nc if null_counts is None else null_counts + nc
    fraud_count += int(chunk["isFraud"].sum())
    flagged_count += int(chunk["isFlaggedFraud"].sum())
    for t, n in chunk["type"].value_counts().items():
        type_counts[t] = type_counts.get(t, 0) + int(n)
    for t, n in chunk[chunk["isFraud"] == 1]["type"].value_counts().items():
        type_fraud[t] = type_fraud.get(t, 0) + int(n)
    amount_stats.append((chunk["amount"].sum(), len(chunk), chunk["amount"].max()))
    # balance sanity: newbalanceOrig should = oldbalanceOrg - amount (for debits)
    debits = chunk[chunk["type"].isin(["PAYMENT", "TRANSFER", "DEBIT", "CASH_OUT"])]
    delta = debits["oldbalanceOrg"] - debits["amount"] - debits["newbalanceOrig"]
    balance_anomalies += int((delta.abs() > 0.01).sum())

print(f"\nTotal rows: {total_rows:,}")
print(f"Fraud rows: {fraud_count:,} ({fraud_count/total_rows*100:.4f}%)")
print(f"Flagged-fraud rows: {flagged_count:,} ({flagged_count/total_rows*100:.4f}%)")

print(f"\nNull counts:")
for c, n in null_counts.items():
    print(f"  {c:25s} {n:>10,}  ({n/total_rows*100:.2f}%)")

print(f"\nTransaction types & fraud rate:")
for t in sorted(type_counts, key=type_counts.get, reverse=True):
    cnt = type_counts[t]
    fr = type_fraud.get(t, 0)
    print(f"  {t:12s} count={cnt:>9,}  fraud={fr:>6,}  rate={fr/cnt*100:.4f}%")

print(f"\nBalance-equation anomalies (|old - amount - new| > 0.01): "
      f"{balance_anomalies:,} ({balance_anomalies/total_rows*100:.2f}%)")
print("  → These rows have inconsistent balances — strong fraud signal.")

# 3) Class balance summary for ML
print("\nClass imbalance ratio: 1 fraud : "
      f"{(total_rows - fraud_count) / max(fraud_count, 1):,.0f} legit")
print("  → Severe imbalance — need SMOTE / class_weight / undersampling for training.")
