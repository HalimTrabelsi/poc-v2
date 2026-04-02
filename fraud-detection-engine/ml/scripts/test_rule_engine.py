from pathlib import Path
import sys
import json

BASE_DIR = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(BASE_DIR))

import pandas as pd
from sklearn.metrics import classification_report, confusion_matrix
from app.core.rule_engine import RuleEngine

DATASET_PATH = BASE_DIR / "ml" / "data" / "synthetic" / "rule_engine_test_dataset.csv"
OUTPUT_PATH = BASE_DIR / "outputs" / "rule_engine_results.csv"

REQUIRED_COLUMNS = ["partner_id", "synthetic_label"]


def validate_dataset(df: pd.DataFrame):
    missing = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing:
        raise ValueError(f"Dataset missing required columns: {missing}")


def summarize_triggered_rules(df: pd.DataFrame):
    exploded = []

    for _, row in df.iterrows():
        rules = row.get("triggered_rules", [])
        if isinstance(rules, str):
            try:
                rules = json.loads(rules.replace("'", '"'))
            except Exception:
                rules = []

        for rule in rules:
            exploded.append({
                "partner_id": row.get("partner_id"),
                "rule_id": rule.get("rule_id"),
                "flag": rule.get("flag"),
                "severity": rule.get("severity"),
                "weight": rule.get("weight"),
            })

    if not exploded:
        print("\n=== AUCUNE RÈGLE DÉCLENCHÉE ===")
        return

    rules_df = pd.DataFrame(exploded)

    print("\n=== TOP RÈGLES DÉCLENCHÉES ===")
    print(rules_df["flag"].value_counts().to_string())

    print("\n=== RÉPARTITION PAR SÉVÉRITÉ ===")
    print(rules_df["severity"].value_counts().to_string())


def main():
    if not DATASET_PATH.exists():
        raise FileNotFoundError(f"Dataset not found: {DATASET_PATH}")

    df = pd.read_csv(DATASET_PATH)
    validate_dataset(df)

    engine = RuleEngine()
    results_df = engine.evaluate_df(df)

    final_df = df.merge(
        results_df,
        left_on="partner_id",
        right_on="beneficiary_id",
        how="left"
    )

    threshold = 0.30
    final_df["pred_rule"] = (final_df["rule_score"] >= threshold).astype(int)

    print("\n=== MATRICE DE CONFUSION ===")
    print(confusion_matrix(final_df["synthetic_label"], final_df["pred_rule"]))

    print("\n=== CLASSIFICATION REPORT ===")
    print(classification_report(final_df["synthetic_label"], final_df["pred_rule"], digits=3))

    print("\n=== TOP 20 CAS LES PLUS SUSPECTS ===")
    cols = ["partner_id", "rule_score", "risk_level", "triggered_flags"]
    optional_cols = ["fraud_scenario", "synthetic_label"]
    cols = [c for c in cols + optional_cols if c in final_df.columns]

    print(
        final_df.sort_values("rule_score", ascending=False)[cols]
        .head(20)
        .to_string(index=False)
    )

    print("\n=== DISTRIBUTION DES NIVEAUX ===")
    print(final_df["risk_level"].value_counts().to_string())

    summarize_triggered_rules(final_df)

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    final_df.to_csv(OUTPUT_PATH, index=False)
    print(f"\nSaved: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()