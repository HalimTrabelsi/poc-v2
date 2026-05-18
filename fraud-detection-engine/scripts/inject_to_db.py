"""Inject synthetic CSV into the fraud_db for batch scoring.

Usage:
    python scripts/inject_to_db.py --csv ml/data/synthetic/dataset_ml.csv
"""
import argparse
import logging
import sys
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text

sys.path.insert(0, str(Path(__file__).parent.parent))

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

FEATURE_STORE_URL = "postgresql://fraud:fraud123@localhost:5433/fraud_engine"

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS synthetic_beneficiaries (
    id                          SERIAL PRIMARY KEY,
    beneficiary_id              TEXT UNIQUE NOT NULL,
    age                         FLOAT,
    income                      FLOAT,
    income_per_person           FLOAT,
    household_size              FLOAT,
    nb_children                 FLOAT,
    nb_elderly                  FLOAT,
    dependency_ratio            FLOAT,
    has_disabled                FLOAT,
    single_head                 FLOAT,
    nb_programs                 FLOAT,
    nb_active_programs          FLOAT,
    pmt_score                   FLOAT,
    pmt_score_min               FLOAT,
    avg_enrollment_days         FLOAT,
    payment_count               FLOAT,
    payment_gap_ratio           FLOAT,
    payment_success_rate        FLOAT,
    amount_variance             FLOAT,
    cycle_count                 FLOAT,
    shared_phone_count          FLOAT,
    shared_account_count        FLOAT,
    network_risk                FLOAT,
    group_membership_count      FLOAT,
    high_amount_flag            FLOAT,
    income_program_inconsistency FLOAT,
    is_fraud                    INTEGER DEFAULT 0,
    inserted_at                 TIMESTAMP DEFAULT NOW()
);
"""

FEATURE_COLS = [
    "age", "income", "income_per_person", "household_size", "nb_children",
    "nb_elderly", "dependency_ratio", "has_disabled", "single_head",
    "nb_programs", "nb_active_programs", "pmt_score", "pmt_score_min",
    "avg_enrollment_days", "payment_count", "payment_gap_ratio",
    "payment_success_rate", "amount_variance", "cycle_count",
    "shared_phone_count", "shared_account_count", "network_risk",
    "group_membership_count", "high_amount_flag", "income_program_inconsistency",
]


def inject(csv_path: str, db_url: str, truncate: bool = False) -> int:
    df = pd.read_csv(csv_path)
    df["beneficiary_id"] = ["SYN-" + str(i).zfill(5) for i in range(len(df))]

    engine = create_engine(db_url)

    with engine.begin() as conn:
        conn.execute(text(CREATE_TABLE_SQL))
        if truncate:
            conn.execute(text("TRUNCATE TABLE synthetic_beneficiaries RESTART IDENTITY"))
            logger.info("Table truncated.")

    insert_cols = ["beneficiary_id"] + FEATURE_COLS + ["is_fraud"]
    available = [c for c in insert_cols if c in df.columns]
    df[available].to_sql(
        "synthetic_beneficiaries",
        engine,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=500,
    )

    logger.info("Inserted %d rows into synthetic_beneficiaries.", len(df))
    return len(df)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Inject synthetic CSV into fraud_db")
    p.add_argument("--csv", default="ml/data/synthetic/dataset_ml.csv")
    p.add_argument("--db-url", default=FEATURE_STORE_URL)
    p.add_argument("--truncate", action="store_true", help="Clear table before inserting")
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    count = inject(args.csv, args.db_url, truncate=args.truncate)
    logger.info("Done. %d beneficiaries ready for batch scoring.", count)
