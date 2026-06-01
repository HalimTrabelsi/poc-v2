"""Per-feature univariate AUC to find which column is still leaking."""
import pandas as pd
from pathlib import Path
from sklearn.metrics import roc_auc_score

DATA = Path(r"C:\Users\Mega Pc\Desktop\poc-v2\poc-v2\fraud-detection-engine\data\paysim_clean_balanced.parquet")
df = pd.read_parquet(DATA)
y = df["is_fraud"]

print(f"{'feature':<30s} {'univariate AUC':>15s}")
print("-" * 50)
for col in df.columns:
    if col == "is_fraud":
        continue
    try:
        auc = roc_auc_score(y, df[col])
        # AUC < 0.5 means inverse signal — flip it for display
        marker = "  <<< LEAK" if auc > 0.99 or auc < 0.01 else ""
        print(f"{col:<30s} {auc:15.4f}{marker}")
    except Exception as e:
        print(f"{col:<30s} ERROR: {e}")
