"""Score all synthetic beneficiaries through the fraud API and print a summary.

Usage:
    python scripts/score_synthetic_dataset.py --db-url postgresql://fraud:fraud123@localhost:5433/fraud_engine
    python scripts/score_synthetic_dataset.py --csv ml/data/synthetic/dataset_ml.csv  # skip DB, use CSV directly
"""
import argparse
import json
import logging
import sys
import time
from pathlib import Path

import pandas as pd
import requests

sys.path.insert(0, str(Path(__file__).parent.parent))

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

API_BASE = "http://localhost:8000/api/v1"
API_KEY = "dev-secret-change-in-prod"
HEADERS = {"X-API-Key": API_KEY, "Content-Type": "application/json"}

FEATURE_COLS = [
    "age", "income", "income_per_person", "household_size", "nb_children",
    "nb_elderly", "dependency_ratio", "has_disabled", "single_head",
    "nb_programs", "nb_active_programs", "pmt_score", "pmt_score_min",
    "avg_enrollment_days", "payment_count", "payment_gap_ratio",
    "payment_success_rate", "amount_variance", "cycle_count",
    "shared_phone_count", "shared_account_count", "network_risk",
    "group_membership_count", "high_amount_flag", "income_program_inconsistency",
]


def load_from_db(db_url: str) -> pd.DataFrame:
    from sqlalchemy import create_engine
    engine = create_engine(db_url)
    df = pd.read_sql("SELECT * FROM synthetic_beneficiaries ORDER BY id", engine)
    logger.info("Loaded %d rows from database.", len(df))
    return df


def load_from_csv(csv_path: str) -> pd.DataFrame:
    df = pd.read_csv(csv_path)
    df["beneficiary_id"] = ["SYN-" + str(i).zfill(5) for i in range(len(df))]
    logger.info("Loaded %d rows from CSV.", len(df))
    return df


def score_row(row: dict, api_base: str) -> dict | None:
    bid = row.get("beneficiary_id", "unknown")
    features = {k: float(row[k]) for k in FEATURE_COLS if k in row and row[k] is not None}
    features["beneficiary_id"] = bid

    try:
        r = requests.post(
            f"{api_base}/score/features",
            headers=HEADERS,
            json=features,
            timeout=15,
        )
        if r.status_code == 200:
            return r.json()
        logger.warning("HTTP %d for %s: %s", r.status_code, bid, r.text[:120])
    except requests.RequestException as exc:
        logger.warning("Request failed for %s: %s", bid, exc)
    return None


def print_summary(results: list[dict], ground_truth: list[int]) -> None:
    if not results:
        logger.error("No results to summarise.")
        return

    risk_counts: dict[str, int] = {}
    rec_counts: dict[str, int] = {}
    tp = fp = tn = fn = 0

    for res, actual in zip(results, ground_truth):
        rl = res.get("risk_level", "LOW")
        rec = res.get("recommendation", "CLEAR")
        risk_counts[rl] = risk_counts.get(rl, 0) + 1
        rec_counts[rec] = rec_counts.get(rec, 0) + 1

        predicted_fraud = rl in ("HIGH", "CRITICAL")
        if predicted_fraud and actual == 1:
            tp += 1
        elif predicted_fraud and actual == 0:
            fp += 1
        elif not predicted_fraud and actual == 0:
            tn += 1
        else:
            fn += 1

    total = len(results)
    print("\n" + "=" * 55)
    print("  BATCH SCORING SUMMARY")
    print("=" * 55)
    print(f"  Total scored:   {total}")
    print(f"\n  Risk distribution:")
    for level in ("CRITICAL", "HIGH", "MEDIUM", "LOW"):
        count = risk_counts.get(level, 0)
        bar = "█" * int(count / total * 30)
        print(f"    {level:10s} {count:5d}  {bar}")

    print(f"\n  Recommendations:")
    for rec, cnt in sorted(rec_counts.items(), key=lambda x: -x[1]):
        print(f"    {rec:20s} {cnt:5d}")

    if tp + fn > 0:
        precision = tp / (tp + fp) if (tp + fp) > 0 else 0
        recall = tp / (tp + fn)
        f1 = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0
        print(f"\n  Detection quality (vs is_fraud label):")
        print(f"    True Positives:  {tp}")
        print(f"    False Positives: {fp}")
        print(f"    True Negatives:  {tn}")
        print(f"    False Negatives: {fn}")
        print(f"    Precision:       {precision:.2%}")
        print(f"    Recall:          {recall:.2%}")
        print(f"    F1 Score:        {f1:.2%}")

    print("=" * 55)
    print("  View full results at http://localhost:8501 → Cases tab")
    print("=" * 55 + "\n")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Batch-score synthetic beneficiaries")
    p.add_argument("--csv", default=None, help="Score directly from CSV (no DB needed)")
    p.add_argument("--db-url", default="postgresql://fraud:fraud123@localhost:5433/fraud_engine")
    p.add_argument("--limit", type=int, default=0, help="Max rows to score (0 = all)")
    p.add_argument("--api", default=API_BASE, help="Fraud API base URL")
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()

    df = load_from_csv(args.csv) if args.csv else load_from_db(args.db_url)
    if args.limit > 0:
        df = df.head(args.limit)

    ground_truth = df["is_fraud"].astype(int).tolist() if "is_fraud" in df.columns else [0] * len(df)
    rows = df.to_dict(orient="records")

    logger.info("Scoring %d beneficiaries via %s …", len(rows), args.api)
    results = []
    t0 = time.time()

    for i, row in enumerate(rows):
        res = score_row(row, args.api)
        if res:
            results.append(res)
        if (i + 1) % 50 == 0:
            elapsed = time.time() - t0
            rate = (i + 1) / elapsed
            remaining = (len(rows) - i - 1) / rate
            logger.info(
                "Progress: %d/%d (%.0f/s, ~%.0fs remaining)",
                i + 1, len(rows), rate, remaining,
            )

    elapsed = time.time() - t0
    logger.info("Scored %d rows in %.1fs (%.0f/s)", len(results), elapsed, len(results) / elapsed)
    print_summary(results, ground_truth)
