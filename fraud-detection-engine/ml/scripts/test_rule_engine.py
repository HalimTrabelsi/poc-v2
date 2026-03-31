from pathlib import Path
import sys

BASE_DIR = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(BASE_DIR))

import pandas as pd
from sklearn.metrics import classification_report, confusion_matrix
from app.core.rule_engine import RuleEngine

DATASET_PATH = BASE_DIR / "ml" / "data" / "synthetic" / "rule_engine_test_dataset.csv"
OUTPUT_PATH = BASE_DIR / "outputs" / "rule_engine_results.csv"


def main():
    df = pd.read_csv(DATASET_PATH)

    engine = RuleEngine()
    results_df = engine.evaluate_df(df)

    final_df = df.merge(results_df, left_on="partner_id", right_on="beneficiary_id", how="left")
    final_df["pred_rule"] = (final_df["rule_score"] >= 0.30).astype(int)

    print("\n=== MATRICE DE CONFUSION ===")
    print(confusion_matrix(final_df["synthetic_label"], final_df["pred_rule"]))

    print("\n=== CLASSIFICATION REPORT ===")
    print(classification_report(final_df["synthetic_label"], final_df["pred_rule"], digits=3))

    print("\n=== TOP 20 CAS LES PLUS SUSPECTS ===")
    print(
        final_df.sort_values("rule_score", ascending=False)[
            ["partner_id", "fraud_scenario", "rule_score", "risk_level", "triggered_rules"]
        ].head(20).to_string(index=False)
    )

    print("\n=== DISTRIBUTION DES NIVEAUX ===")
    print(final_df["risk_level"].value_counts())

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    final_df.to_csv(OUTPUT_PATH, index=False)
    print(f"\nSaved: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()