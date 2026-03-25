import os
import json
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from datetime import datetime

# ============================================================
# CONFIG
# ============================================================

OUTPUT_DIR = os.getenv("OUTPUT_DIR", "./data")
os.makedirs(OUTPUT_DIR, exist_ok=True)


def get_engine():
    """
    Priority:
    1) OPENG2P_DB_URL from docker-compose
    2) fallback to explicit PG vars
    """
    openg2p_db_url = os.getenv("OPENG2P_DB_URL")
    if openg2p_db_url:
        print("Using OPENG2P_DB_URL from environment.")
        return create_engine(openg2p_db_url)

    pghost = os.getenv("PGHOST", "postgresql")
    pgport = os.getenv("PGPORT", "5432")
    pgdatabase = os.getenv("PGDATABASE", "openg2p")
    pguser = os.getenv("PGUSER", "odoo")
    pgpassword = os.getenv("PGPASSWORD", "openg2p")

    conn_str = f"postgresql://{pguser}:{pgpassword}@{pghost}:{pgport}/{pgdatabase}"
    print(f"Using fallback connection: postgresql://{pguser}:***@{pghost}:{pgport}/{pgdatabase}")
    return create_engine(conn_str)


# ============================================================
# HELPERS
# ============================================================

def load_table(engine, query, table_name="unknown"):
    try:
        df = pd.read_sql(query, engine)
        print(f"Loaded {table_name}: {len(df)} rows")
        return df
    except Exception as e:
        print(f"Warning: failed to load {table_name}: {e}")
        return pd.DataFrame()


def compute_age(birthdate):
    if pd.isna(birthdate):
        return np.nan
    birthdate = pd.to_datetime(birthdate, errors="coerce")
    if pd.isna(birthdate):
        return np.nan
    return round((pd.Timestamp.today() - birthdate).days / 365.25, 2)


def ensure_numeric(df, col, default=0):
    if col not in df.columns:
        df[col] = default
    df[col] = pd.to_numeric(df[col], errors="coerce").fillna(default)
    return df


def ensure_column(df, col, default=0):
    if col not in df.columns:
        df[col] = default
    return df


# ============================================================
# LOAD DATA
# ============================================================

def load_data(engine):
    print("Loading tables...")

    res_partner = load_table(
        engine,
        "SELECT * FROM res_partner",
        "res_partner"
    )

    payments = load_table(
        engine,
        "SELECT * FROM g2p_payment",
        "g2p_payment"
    )

    phones = load_table(
        engine,
        "SELECT * FROM g2p_phone_number",
        "g2p_phone_number"
    )

    programs = load_table(
        engine,
        "SELECT * FROM g2p_program_registrant_info",
        "g2p_program_registrant_info"
    )

    groups = load_table(
        engine,
        "SELECT * FROM g2p_group_membership",
        "g2p_group_membership"
    )

    cycles = load_table(
        engine,
        "SELECT * FROM g2p_cycle",
        "g2p_cycle"
    )

    banks = load_table(
        engine,
        "SELECT * FROM res_partner_bank",
        "res_partner_bank"
    )

    return res_partner, payments, phones, programs, groups, cycles, banks


# ============================================================
# FEATURE ENGINEERING
# ============================================================

def build_base_partner_features(res_partner):
    df = res_partner.copy()

    # Keep only registrants if available
    if "is_registrant" in df.columns:
        df = df[df["is_registrant"].fillna(False) == True].copy()

    # Core profile
    df["age"] = df["birthdate"].apply(compute_age) if "birthdate" in df.columns else np.nan

    df["income"] = pd.to_numeric(df["income"], errors="coerce") if "income" in df.columns else np.nan

    df["household_size"] = df["z_ind_grp_num_individuals"] if "z_ind_grp_num_individuals" in df.columns else np.nan
    df["nb_children"] = df["z_ind_grp_num_children"] if "z_ind_grp_num_children" in df.columns else 0
    df["nb_elderly"] = df["z_ind_grp_num_elderly"] if "z_ind_grp_num_elderly" in df.columns else 0

    df["household_size"] = pd.to_numeric(df["household_size"], errors="coerce")
    df["nb_children"] = pd.to_numeric(df["nb_children"], errors="coerce").fillna(0)
    df["nb_elderly"] = pd.to_numeric(df["nb_elderly"], errors="coerce").fillna(0)

    df["dependency_ratio"] = (
        df["nb_children"].fillna(0) + df["nb_elderly"].fillna(0)
    ) / df["household_size"].replace(0, np.nan)

    df["income_per_person"] = df["income"] / df["household_size"].replace(0, np.nan)

    # Extra useful flags
    if "z_ind_grp_is_single_head_hh" in df.columns:
        df["single_head_hh"] = df["z_ind_grp_is_single_head_hh"].fillna(False).astype(int)
    else:
        df["single_head_hh"] = 0

    if "z_ind_grp_is_hh_with_disabled" in df.columns:
        df["hh_with_disabled"] = df["z_ind_grp_is_hh_with_disabled"].fillna(False).astype(int)
    else:
        df["hh_with_disabled"] = 0

    if "z_cst_indv_receive_government_benefits" in df.columns:
        df["receive_government_benefits"] = df["z_cst_indv_receive_government_benefits"].fillna(False).astype(int)
    else:
        df["receive_government_benefits"] = 0

    if "program_membership_count" in df.columns:
        df["program_membership_count"] = pd.to_numeric(df["program_membership_count"], errors="coerce").fillna(0)
    else:
        df["program_membership_count"] = 0

    if "entitlements_count" in df.columns:
        df["entitlements_count"] = pd.to_numeric(df["entitlements_count"], errors="coerce").fillna(0)
    else:
        df["entitlements_count"] = 0

    if "cycles_count" in df.columns:
        df["cycles_count"] = pd.to_numeric(df["cycles_count"], errors="coerce").fillna(0)
    else:
        df["cycles_count"] = 0

    keep_cols = [
        "id",
        "gender",
        "district",
        "area_id",
        "registration_date",
        "age",
        "income",
        "household_size",
        "nb_children",
        "nb_elderly",
        "dependency_ratio",
        "income_per_person",
        "single_head_hh",
        "hh_with_disabled",
        "receive_government_benefits",
        "program_membership_count",
        "entitlements_count",
        "cycles_count",
    ]

    keep_cols = [c for c in keep_cols if c in df.columns]
    df = df[keep_cols].copy()

    return df


def add_phone_features(df, phones):
    df["shared_phone_count"] = 0

    if phones.empty:
        print("Phone features skipped: g2p_phone_number is empty.")
        return df

    required_cols = {"partner_id", "phone_sanitized"}
    if not required_cols.issubset(phones.columns):
        print("Phone features skipped: required columns missing.")
        return df

    temp = phones.copy()
    temp["phone_sanitized"] = temp["phone_sanitized"].fillna("UNKNOWN_PHONE")

    phone_counts = temp.groupby("phone_sanitized")["partner_id"].nunique()
    temp["shared_phone_count"] = temp["phone_sanitized"].map(phone_counts)

    phone_feat = temp.groupby("partner_id", as_index=False)["shared_phone_count"].max()

    df = df.merge(
        phone_feat,
        left_on="id",
        right_on="partner_id",
        how="left",
        suffixes=("", "_phone")
    )

    if "shared_phone_count_phone" in df.columns:
        df["shared_phone_count"] = df["shared_phone_count_phone"].fillna(df["shared_phone_count"]).fillna(0)
        df.drop(columns=["shared_phone_count_phone"], inplace=True)

    if "partner_id" in df.columns:
        df.drop(columns=["partner_id"], inplace=True)

    df["shared_phone_count"] = pd.to_numeric(df["shared_phone_count"], errors="coerce").fillna(0)
    return df


def add_bank_features(df, banks):
    df["shared_account_count"] = 0

    if banks.empty:
        print("Bank features skipped: res_partner_bank is empty.")
        return df

    if "partner_id" not in banks.columns:
        print("Bank features skipped: partner_id missing in res_partner_bank.")
        return df

    account_col = None
    for candidate in ["acc_number", "sanitized_acc_number", "account_number"]:
        if candidate in banks.columns:
            account_col = candidate
            break

    if account_col is None:
        print("Bank features skipped: no account number column found.")
        print("Available bank columns:", list(banks.columns))
        return df

    temp = banks.copy()
    temp[account_col] = temp[account_col].fillna("UNKNOWN_ACCOUNT")

    acc_counts = temp.groupby(account_col)["partner_id"].nunique()
    temp["shared_account_count"] = temp[account_col].map(acc_counts)

    bank_feat = temp.groupby("partner_id", as_index=False)["shared_account_count"].max()

    df = df.merge(
        bank_feat,
        left_on="id",
        right_on="partner_id",
        how="left",
        suffixes=("", "_bank")
    )

    if "shared_account_count_bank" in df.columns:
        df["shared_account_count"] = df["shared_account_count_bank"].fillna(df["shared_account_count"]).fillna(0)
        df.drop(columns=["shared_account_count_bank"], inplace=True)

    if "partner_id" in df.columns:
        df.drop(columns=["partner_id"], inplace=True)

    df["shared_account_count"] = pd.to_numeric(df["shared_account_count"], errors="coerce").fillna(0)
    return df


def add_program_features(df, programs):
    df["nb_programs"] = 0
    df["program_overlap_flag"] = 0

    if programs.empty:
        print("Program features skipped: g2p_program_registrant_info is empty.")
        return df

    required_cols = {"registrant_id", "program_id"}
    if not required_cols.issubset(programs.columns):
        print("Program features skipped: required columns missing.")
        return df

    temp = programs.copy()

    prog_count = (
        temp.groupby("registrant_id", as_index=False)["program_id"]
        .nunique()
        .rename(columns={"program_id": "nb_programs"})
    )

    df = df.merge(
        prog_count,
        left_on="id",
        right_on="registrant_id",
        how="left",
        suffixes=("", "_prog")
    )

    if "nb_programs_prog" in df.columns:
        df["nb_programs"] = df["nb_programs_prog"].fillna(df["nb_programs"]).fillna(0)
        df.drop(columns=["nb_programs_prog"], inplace=True)

    if "registrant_id" in df.columns:
        df.drop(columns=["registrant_id"], inplace=True)

    df["nb_programs"] = pd.to_numeric(df["nb_programs"], errors="coerce").fillna(0)
    df["program_overlap_flag"] = (df["nb_programs"] > 1).astype(int)

    return df


def add_group_features(df, groups):
    df["active_group_memberships"] = 0

    if groups.empty:
        print("Group features skipped: g2p_group_membership is empty.")
        return df

    required_cols = {"individual", "is_ended"}
    if not required_cols.issubset(groups.columns):
        print("Group features skipped: required columns missing.")
        return df

    temp = groups.copy()
    temp["is_ended"] = temp["is_ended"].fillna(False)

    active_groups = (
        temp[temp["is_ended"] == False]
        .groupby("individual", as_index=False)
        .size()
        .rename(columns={"size": "active_group_memberships"})
    )

    df = df.merge(
        active_groups,
        left_on="id",
        right_on="individual",
        how="left",
        suffixes=("", "_grp")
    )

    if "active_group_memberships_grp" in df.columns:
        df["active_group_memberships"] = df["active_group_memberships_grp"].fillna(df["active_group_memberships"]).fillna(0)
        df.drop(columns=["active_group_memberships_grp"], inplace=True)

    if "individual" in df.columns:
        df.drop(columns=["individual"], inplace=True)

    df["active_group_memberships"] = pd.to_numeric(df["active_group_memberships"], errors="coerce").fillna(0)

    return df


def build_payment_cycle_dataset(base_df, programs, payments, cycles):
    """
    Baseline dataset builder.
    If program/payment/cycle tables are empty, fallback to beneficiary-level dataset
    while still creating all expected columns.
    """
    fallback_cols = {
        "program_id": np.nan,
        "cycle_id": np.nan,
        "state": np.nan,
        "pmt_score": 0.0,
        "latest_pmt_score": 0.0,
        "start_date": pd.NaT,
        "end_date": pd.NaT,
        "cycle_state": np.nan,
        "total_amount_issued": 0.0,
        "total_amount_paid": 0.0,
        "total_gap": 0.0,
        "payment_count_in_cycle": 0,
        "cycle_duration_days": 0.0,
        "payment_gap_ratio": 0.0,
    }

    # Case 1: no programs -> beneficiary-level fallback
    if programs.empty or "registrant_id" not in programs.columns:
        print("Programs table empty, falling back to beneficiary-level dataset.")
        fallback = base_df.copy()
        for col, default in fallback_cols.items():
            fallback[col] = default
        return fallback

    # beneficiary x program base
    bp = programs.copy()
    keep_prog_cols = [c for c in ["registrant_id", "program_id", "state", "pmt_score", "latest_pmt_score"] if c in bp.columns]
    bp = bp[keep_prog_cols].drop_duplicates().copy()

    dataset = base_df.merge(
        bp,
        left_on="id",
        right_on="registrant_id",
        how="left"
    )

    if "registrant_id" in dataset.columns:
        dataset.drop(columns=["registrant_id"], inplace=True)

    payment_cycle_agg = pd.DataFrame()
    if not payments.empty and "cycle_id" in payments.columns:
        temp = payments.copy()
        temp["amount_issued"] = pd.to_numeric(temp["amount_issued"], errors="coerce").fillna(0)
        temp["amount_paid"] = pd.to_numeric(temp["amount_paid"], errors="coerce").fillna(0)
        temp["payment_gap"] = temp["amount_issued"] - temp["amount_paid"]

        payment_cycle_agg = (
            temp.groupby("cycle_id", as_index=False)
            .agg(
                total_amount_issued=("amount_issued", "sum"),
                total_amount_paid=("amount_paid", "sum"),
                total_gap=("payment_gap", "sum"),
                payment_count_in_cycle=("id", "count")
            )
        )

    cycle_bridge = pd.DataFrame()
    if not cycles.empty and {"id", "program_id"}.issubset(cycles.columns):
        cycle_bridge = cycles[["id", "program_id", "start_date", "end_date", "state"]].copy()
        cycle_bridge = cycle_bridge.rename(columns={"id": "cycle_id", "state": "cycle_state"})

    if not cycle_bridge.empty and "program_id" in dataset.columns:
        dataset = dataset.merge(cycle_bridge, on="program_id", how="left")
    else:
        dataset["cycle_id"] = np.nan
        dataset["start_date"] = pd.NaT
        dataset["end_date"] = pd.NaT
        dataset["cycle_state"] = np.nan

    if not payment_cycle_agg.empty and "cycle_id" in dataset.columns:
        dataset = dataset.merge(payment_cycle_agg, on="cycle_id", how="left")
    else:
        dataset["total_amount_issued"] = 0.0
        dataset["total_amount_paid"] = 0.0
        dataset["total_gap"] = 0.0
        dataset["payment_count_in_cycle"] = 0

    # ensure expected columns always exist
    for col, default in fallback_cols.items():
        if col not in dataset.columns:
            dataset[col] = default

    if "start_date" in dataset.columns:
        dataset["start_date"] = pd.to_datetime(dataset["start_date"], errors="coerce")
    if "end_date" in dataset.columns:
        dataset["end_date"] = pd.to_datetime(dataset["end_date"], errors="coerce")

    dataset["cycle_duration_days"] = (
        (dataset["end_date"] - dataset["start_date"]).dt.days
        if "start_date" in dataset.columns and "end_date" in dataset.columns
        else 0
    )

    dataset["payment_gap_ratio"] = dataset["total_gap"] / dataset["total_amount_issued"].replace(0, np.nan)
    dataset["payment_gap_ratio"] = pd.to_numeric(dataset["payment_gap_ratio"], errors="coerce").fillna(0)

    return dataset

def add_weak_labels(df):
    """
    Baseline weak labeling to bootstrap the project.
    Works even when payment/program tables are empty.
    """
    required_defaults = {
        "shared_account_count": 0,
        "shared_phone_count": 0,
        "nb_programs": 0,
        "dependency_ratio": 0,
        "payment_gap_ratio": 0,
        "payment_count_in_cycle": 0,
    }

    for col, default in required_defaults.items():
        if col not in df.columns:
            df[col] = default

    df["shared_account_count"] = pd.to_numeric(df["shared_account_count"], errors="coerce").fillna(0)
    df["shared_phone_count"] = pd.to_numeric(df["shared_phone_count"], errors="coerce").fillna(0)
    df["nb_programs"] = pd.to_numeric(df["nb_programs"], errors="coerce").fillna(0)
    df["dependency_ratio"] = pd.to_numeric(df["dependency_ratio"], errors="coerce").fillna(0)
    df["payment_gap_ratio"] = pd.to_numeric(df["payment_gap_ratio"], errors="coerce").fillna(0)
    df["payment_count_in_cycle"] = pd.to_numeric(df["payment_count_in_cycle"], errors="coerce").fillna(0)

    df["risk_score_rule"] = (
        (df["shared_account_count"] > 2).astype(int)
        + (df["shared_phone_count"] > 2).astype(int)
        + (df["nb_programs"] > 2).astype(int)
        + (df["dependency_ratio"] > 1).astype(int)
        + (df["payment_gap_ratio"] > 0.3).astype(int)
        + (df["payment_count_in_cycle"] > 3).astype(int)
    )

    df["is_suspicious"] = (df["risk_score_rule"] >= 2).astype(int)

    def risk_level(score):
        if score >= 4:
            return "HIGH"
        if score >= 2:
            return "MEDIUM"
        return "LOW"

    df["risk_level"] = df["risk_score_rule"].apply(risk_level)
    return df


def build_full_dataset(res_partner, payments, phones, programs, groups, cycles, banks):
    df = build_base_partner_features(res_partner)
    df = add_phone_features(df, phones)
    df = add_bank_features(df, banks)
    df = add_program_features(df, programs)
    df = add_group_features(df, groups)
    df = build_payment_cycle_dataset(df, programs, payments, cycles)
    df = add_weak_labels(df)

    # final cleanup
    numeric_cols = [
        "age",
        "income",
        "household_size",
        "nb_children",
        "nb_elderly",
        "dependency_ratio",
        "income_per_person",
        "shared_phone_count",
        "shared_account_count",
        "nb_programs",
        "active_group_memberships",
        "pmt_score",
        "latest_pmt_score",
        "total_amount_issued",
        "total_amount_paid",
        "total_gap",
        "payment_count_in_cycle",
        "cycle_duration_days",
        "payment_gap_ratio",
        "risk_score_rule",
        "is_suspicious",
    ]

    for col in numeric_cols:
        df = ensure_numeric(df, col, 0)

    return df


# ============================================================
# EXPORT
# ============================================================

def export_dataset(df):
    supervised_cols = [
        "id",
        "program_id",
        "cycle_id",
        "gender",
        "district",
        "area_id",
        "age",
        "income",
        "household_size",
        "nb_children",
        "nb_elderly",
        "dependency_ratio",
        "income_per_person",
        "single_head_hh",
        "hh_with_disabled",
        "receive_government_benefits",
        "program_membership_count",
        "entitlements_count",
        "cycles_count",
        "shared_phone_count",
        "shared_account_count",
        "nb_programs",
        "program_overlap_flag",
        "active_group_memberships",
        "pmt_score",
        "latest_pmt_score",
        "total_amount_issued",
        "total_amount_paid",
        "total_gap",
        "payment_count_in_cycle",
        "cycle_duration_days",
        "payment_gap_ratio",
        "risk_score_rule",
        "is_suspicious",
        "risk_level",
    ]

    anomaly_cols = [
        "id",
        "program_id",
        "cycle_id",
        "age",
        "income",
        "household_size",
        "nb_children",
        "nb_elderly",
        "dependency_ratio",
        "income_per_person",
        "shared_phone_count",
        "shared_account_count",
        "nb_programs",
        "active_group_memberships",
        "pmt_score",
        "latest_pmt_score",
        "total_amount_issued",
        "total_gap",
        "payment_count_in_cycle",
        "payment_gap_ratio",
    ]

    supervised_cols = [c for c in supervised_cols if c in df.columns]
    anomaly_cols = [c for c in anomaly_cols if c in df.columns]

    supervised_path = os.path.join(OUTPUT_DIR, "dataset_supervised.csv")
    anomaly_path = os.path.join(OUTPUT_DIR, "dataset_anomaly.csv")
    schema_path = os.path.join(OUTPUT_DIR, "dataset_schema.json")

    df[supervised_cols].to_csv(supervised_path, index=False)
    df[anomaly_cols].to_csv(anomaly_path, index=False)

    schema = {
        "supervised_features": supervised_cols,
        "anomaly_features": anomaly_cols,
        "notes": {
            "granularity": "baseline beneficiary x cycle when cycle bridge is available via program_id",
            "label_type": "weak labels",
            "important_warning": "payment-to-beneficiary linkage is still approximate and should be improved later"
        }
    }

    with open(schema_path, "w", encoding="utf-8") as f:
        json.dump(schema, f, indent=2)

    print("Datasets exported successfully.")
    print(f"Supervised dataset: {supervised_path}")
    print(f"Anomaly dataset: {anomaly_path}")
    print(f"Schema: {schema_path}")


# ============================================================
# MAIN
# ============================================================

def main():
    engine = get_engine()

    res_partner, payments, phones, programs, groups, cycles, banks = load_data(engine)

    if not banks.empty:
        print("res_partner_bank columns:", list(banks.columns))

    df = build_full_dataset(
        res_partner=res_partner,
        payments=payments,
        phones=phones,
        programs=programs,
        groups=groups,
        cycles=cycles,
        banks=banks,
    )

    print(f"Final dataset rows: {len(df)}")
    print(f"Final dataset columns: {len(df.columns)}")

    export_dataset(df)


if __name__ == "__main__":
    main()