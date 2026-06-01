"""Clean PaySim and produce a fraud-engine-ready training set.

Decisions (justified in the data-engineering report):

1. KEEP only TRANSFER and CASH_OUT rows. Other types contain zero positive
   labels — they would teach the model that 99.87% of inputs are legit and
   inflate accuracy while collapsing recall.
2. DROP nameOrig / nameDest (high-cardinality IDs, no leak-safe signal) —
   we extract a single boolean (dest_is_merchant) from the 'M' prefix.
3. DROP isFlaggedFraud (16 positives in 6.3M rows — degenerate column).
4. ENGINEER 9 derived features that capture the actual fraud topology:
   balance_anomaly, full_drain, round_amount, dest_zero_before, etc.
5. UNDERSAMPLE legit to a 1:20 ratio (legit:fraud) so the model trains on
   meaningful examples without losing real distribution shape.
6. STANDARDIZE column names to match fraud-engine feature naming
   (snake_case, no camelCase) and emit clean parquet + csv.
"""
import pandas as pd
import numpy as np
from pathlib import Path

SRC = Path(r"C:\Users\Mega Pc\Desktop\poc-v2\poc-v2\data\data\AIML Dataset.csv")
OUT_DIR = Path(r"C:\Users\Mega Pc\Desktop\poc-v2\poc-v2\fraud-detection-engine\data")
OUT_DIR.mkdir(parents=True, exist_ok=True)

print("Loading + filtering...")
chunks = []
for chunk in pd.read_csv(SRC, chunksize=500_000):
    chunk = chunk[chunk["type"].isin(["TRANSFER", "CASH_OUT"])]
    chunks.append(chunk)
df = pd.concat(chunks, ignore_index=True)
print(f"Filtered rows (TRANSFER + CASH_OUT only): {len(df):,}")

# Drop degenerate column
df = df.drop(columns=["isFlaggedFraud"])

# Rename to snake_case
df = df.rename(columns={
    "step": "tx_step",
    "type": "tx_type",
    "amount": "tx_amount",
    "nameOrig": "orig_id",
    "oldbalanceOrg": "orig_balance_before",
    "newbalanceOrig": "orig_balance_after",
    "nameDest": "dest_id",
    "oldbalanceDest": "dest_balance_before",
    "newbalanceDest": "dest_balance_after",
    "isFraud": "is_fraud",
})

# Feature engineering -------------------------------------------------------
print("Engineering features...")

# 1. Balance-equation anomaly: |old - amount - new| > 0.01
df["orig_balance_delta"] = df["orig_balance_before"] - df["tx_amount"] - df["orig_balance_after"]
df["balance_anomaly"] = (df["orig_balance_delta"].abs() > 0.01).astype(int)

# 2. Account fully drained
df["full_drain"] = ((df["orig_balance_before"] > 0) & (df["orig_balance_after"] == 0)).astype(int)

# 3. Round-number amount (fraudsters often use round numbers)
df["round_amount"] = (df["tx_amount"] % 100 == 0).astype(int)

# 4. Destination was empty before transfer (mule account warming up)
df["dest_was_empty"] = (df["dest_balance_before"] == 0).astype(int)

# 5. Amount exceeds origin balance (overdraft attempt)
df["overdraft_attempt"] = (df["tx_amount"] > df["orig_balance_before"]).astype(int)

# 6. Destination is merchant (M-prefix) vs customer (C-prefix)
df["dest_is_merchant"] = df["dest_id"].str.startswith("M").astype(int)

# 7. Log-amount (heavy-tailed → log-transform for tree models too, helps splits)
df["log_amount"] = np.log1p(df["tx_amount"])

# 8. Amount ratio to origin balance (0 if balance=0)
df["amount_to_balance_ratio"] = df["tx_amount"] / df["orig_balance_before"].replace(0, np.nan)
df["amount_to_balance_ratio"] = df["amount_to_balance_ratio"].fillna(0).clip(0, 100)

# 9. Hour-of-day proxy (step is 1h; step % 24)
df["hour_of_day"] = df["tx_step"] % 24
df["is_night"] = ((df["hour_of_day"] < 6) | (df["hour_of_day"] >= 22)).astype(int)

# 10. Categorical encoding for tx_type
df["tx_type_transfer"] = (df["tx_type"] == "TRANSFER").astype(int)

# Validate feature signal quality
print("\nFraud rate per engineered feature:")
for feat in ["balance_anomaly", "full_drain", "round_amount", "dest_was_empty",
             "overdraft_attempt", "dest_is_merchant", "is_night", "tx_type_transfer"]:
    on = df[df[feat] == 1]
    off = df[df[feat] == 0]
    if len(on) and len(off):
        lift = (on["is_fraud"].mean() / max(off["is_fraud"].mean(), 1e-9))
        print(f"  {feat:25s}  on={on['is_fraud'].mean()*100:.2f}%  "
              f"off={off['is_fraud'].mean()*100:.2f}%  lift={lift:.1f}x")

# Sampling for ML training: 1:20 fraud:legit -------------------------------
print("\nBuilding 1:20 stratified training set...")
fraud = df[df["is_fraud"] == 1]
legit = df[df["is_fraud"] == 0].sample(n=len(fraud) * 20, random_state=42)
balanced = pd.concat([fraud, legit], ignore_index=True).sample(frac=1, random_state=42)
print(f"Balanced training set: {len(balanced):,} rows  "
      f"(fraud={len(fraud):,}, legit={len(legit):,})")

# Drop the raw ID columns now that features are extracted
balanced = balanced.drop(columns=["orig_id", "dest_id", "tx_type", "orig_balance_delta"])

# Final cast: numeric only
balanced = balanced.astype({
    "tx_step": "int32", "tx_amount": "float32",
    "orig_balance_before": "float32", "orig_balance_after": "float32",
    "dest_balance_before": "float32", "dest_balance_after": "float32",
    "is_fraud": "int8",
    "balance_anomaly": "int8", "full_drain": "int8", "round_amount": "int8",
    "dest_was_empty": "int8", "overdraft_attempt": "int8",
    "dest_is_merchant": "int8", "is_night": "int8", "tx_type_transfer": "int8",
    "log_amount": "float32", "amount_to_balance_ratio": "float32",
    "hour_of_day": "int8",
})

# Save
out_csv = OUT_DIR / "paysim_clean_balanced.csv"
out_parquet = OUT_DIR / "paysim_clean_balanced.parquet"
balanced.to_csv(out_csv, index=False)
try:
    balanced.to_parquet(out_parquet, index=False)
    print(f"Wrote: {out_parquet}")
except Exception as e:
    print(f"Parquet skipped ({e})")
print(f"Wrote: {out_csv}")
print(f"Columns: {list(balanced.columns)}")
