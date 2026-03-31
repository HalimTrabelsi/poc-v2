import random
from datetime import date, timedelta
from pathlib import Path
from sklearn.metrics import classification_report
from sklearn.metrics import classification_report, confusion_matrix

import numpy as np
import pandas as pd

random.seed(42)
np.random.seed(42)

N = 2000
FRAUD_RATE = 0.05

OUTPUT = Path("rule_engine_test_dataset.csv")


def bounded_lognormal(mean_log=6.0, sigma=0.6, low=50, high=4000):
    x = np.random.lognormal(mean=mean_log, sigma=sigma)
    return float(max(low, min(x, high)))


def make_normal_case(i: int) -> dict:
    household_size = int(np.random.choice([1, 2, 3, 4, 5, 6, 7], p=[0.08, 0.14, 0.20, 0.22, 0.18, 0.12, 0.06]))
    nb_children = int(np.random.randint(0, max(1, household_size)))
    nb_elderly = int(np.random.choice([0, 0, 0, 1]))
    adults = max(household_size - nb_children - nb_elderly, 1)

    income = round(bounded_lognormal(), 2)
    nb_programs = int(np.random.choice([1, 2, 3], p=[0.65, 0.25, 0.10]))
    pmt_score = round(float(np.random.uniform(0.35, 0.85)), 3)

    cycle_count = int(np.random.choice([1, 2, 3], p=[0.20, 0.35, 0.45]))
    payment_count = cycle_count + int(np.random.choice([0, 0, 1]))
    total_issued = round(float(np.random.uniform(150, 1200) * cycle_count), 2)
    total_paid = round(float(total_issued * np.random.uniform(0.95, 1.0)), 2)
    gap_ratio = round((total_issued - total_paid) / total_issued if total_issued > 0 else 0, 3)

    shared_phone_count = 0
    shared_account_count = 0

    income_per_person = round(income / max(household_size, 1), 2)
    dependency_ratio = round((nb_children + nb_elderly) / adults, 3)
    high_amount_flag = 0
    network_risk = 0.0

    return {
        "partner_id": 10000 + i,
        "age": int(np.random.randint(18, 75)),
        "gender_m": int(np.random.choice([0, 1])),
        "income": income,
        "household_size": household_size,
        "nb_children": nb_children,
        "nb_elderly": nb_elderly,
        "has_disabled": int(np.random.choice([0, 1], p=[0.94, 0.06])),
        "single_head": int(np.random.choice([0, 1], p=[0.82, 0.18])),
        "nb_programs": nb_programs,
        "pmt_score": pmt_score,
        "total_issued": total_issued,
        "total_paid": total_paid,
        "gap_ratio": gap_ratio,
        "payment_count": payment_count,
        "cycle_count": cycle_count,
        "shared_phone_count": shared_phone_count,
        "shared_account_count": shared_account_count,
        "income_per_person": income_per_person,
        "dependency_ratio": dependency_ratio,
        "high_amount_flag": high_amount_flag,
        "network_risk": network_risk,
        "synthetic_label": 0,
        "fraud_scenario": "none",
    }


def inject_shared_account(row: dict):
    row["shared_account_count"] = int(np.random.randint(2, 5))
    row["network_risk"] = min(1.0, 0.6 * min(row["shared_account_count"], 5))
    row["synthetic_label"] = 1
    row["fraud_scenario"] = "shared_account"


def inject_shared_phone(row: dict):
    row["shared_phone_count"] = int(np.random.randint(3, 6))
    row["network_risk"] = min(1.0, 0.4 * min(row["shared_phone_count"], 5))
    row["synthetic_label"] = 1
    row["fraud_scenario"] = "shared_phone"


def inject_multi_program(row: dict):
    row["nb_programs"] = int(np.random.randint(4, 6))
    row["pmt_score"] = round(float(np.random.uniform(0.05, 0.25)), 3)
    row["synthetic_label"] = 1
    row["fraud_scenario"] = "multi_program"


def inject_high_gap(row: dict):
    row["total_issued"] = round(float(np.random.uniform(1500, 5000)), 2)
    row["total_paid"] = round(float(row["total_issued"] * np.random.uniform(0.2, 0.6)), 2)
    row["gap_ratio"] = round((row["total_issued"] - row["total_paid"]) / row["total_issued"], 3)
    row["payment_count"] = int(np.random.randint(2, 5))
    row["cycle_count"] = int(np.random.randint(1, 3))
    row["synthetic_label"] = 1
    row["fraud_scenario"] = "high_gap"


def inject_high_amount(row: dict):
    row["total_issued"] = round(float(np.random.uniform(5000, 15000)), 2)
    row["total_paid"] = round(float(row["total_issued"] * np.random.uniform(0.9, 1.0)), 2)
    row["gap_ratio"] = round((row["total_issued"] - row["total_paid"]) / row["total_issued"], 3)
    row["high_amount_flag"] = 1
    row["synthetic_label"] = 1
    row["fraud_scenario"] = "high_amount"


def inject_combo_case(row: dict):
    row["shared_phone_count"] = int(np.random.randint(2, 4))
    row["shared_account_count"] = int(np.random.randint(2, 4))
    row["nb_programs"] = int(np.random.randint(4, 6))
    row["total_issued"] = round(float(np.random.uniform(2500, 8000)), 2)
    row["total_paid"] = round(float(row["total_issued"] * np.random.uniform(0.3, 0.7)), 2)
    row["gap_ratio"] = round((row["total_issued"] - row["total_paid"]) / row["total_issued"], 3)
    row["high_amount_flag"] = int(row["total_issued"] > 5000)
    row["network_risk"] = min(
        1.0,
        0.4 * min(row["shared_phone_count"], 5) + 0.6 * min(row["shared_account_count"], 5)
    )
    row["pmt_score"] = round(float(np.random.uniform(0.05, 0.20)), 3)
    row["synthetic_label"] = 1
    row["fraud_scenario"] = "combo"


def main():
    rows = [make_normal_case(i) for i in range(N)]

    n_suspects = int(N * FRAUD_RATE)
    suspect_indices = np.random.choice(range(N), size=n_suspects, replace=False)

    injectors = [
        inject_shared_account,
        inject_shared_phone,
        inject_multi_program,
        inject_high_gap,
        inject_high_amount,
        inject_combo_case,
    ]

    for idx in suspect_indices:
        injector = random.choice(injectors)
        injector(rows[idx])

        row = rows[idx]
        row["income_per_person"] = round(row["income"] / max(row["household_size"], 1), 2)
        adults = max(row["household_size"] - row["nb_children"] - row["nb_elderly"], 1)
        row["dependency_ratio"] = round((row["nb_children"] + row["nb_elderly"]) / adults, 3)

    df = pd.DataFrame(rows)

    # recalculer high_amount_flag si besoin
    threshold = df["total_issued"].quantile(0.95)
    df["high_amount_flag"] = (df["total_issued"] > threshold).astype(int)

    # recalculer network_risk
    df["network_risk"] = (
        df["shared_phone_count"].clip(upper=5) * 0.4
        + df["shared_account_count"].clip(upper=5) * 0.6
    ).clip(upper=1.0)

    df.to_csv(OUTPUT, index=False)

    print(f"Saved: {OUTPUT}")
    print(df["synthetic_label"].value_counts(dropna=False))
    print(df["fraud_scenario"].value_counts(dropna=False))
    print(df.head(10).to_string(index=False))


if __name__ == "__main__":
    main()

