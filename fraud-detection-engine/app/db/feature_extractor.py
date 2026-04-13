"""
feature_extractor.py — Extraction features anti-fraude depuis OpenG2P
======================================================================
Version défensive compatible avec schémas OpenG2P variables.
"""

from typing import Optional
import pandas as pd
from sqlalchemy import text
from app.db.postgres import get_openg2p_db


def extract_features(limit: Optional[int] = None) -> pd.DataFrame:
    db = get_openg2p_db()
    engine = db.engine

    params = {}
    limit_sql = ""
    if limit is not None:
        limit_sql = "LIMIT :limit"
        params["limit"] = limit

    query = text(f"""
    WITH partner_base AS (
        SELECT
            p.id AS partner_id,
            COALESCE(EXTRACT(YEAR FROM AGE(p.birthdate))::int, 35) AS age,
            LOWER(COALESCE(p.gender, 'unknown')) AS gender,
            COALESCE(p.income, 0.0) AS income,
            1 AS household_size,
            0 AS nb_children,
            0 AS nb_elderly,
            0 AS has_disabled,
            0 AS single_head,
            0 AS elderly_head,
            COALESCE(
                EXTRACT(DAY FROM NOW() - p.create_date)::int,
                365
            ) AS registration_age_days
        FROM res_partner p
        WHERE COALESCE(p.active, true) = true
          AND COALESCE(p.is_company, false) = false
          AND COALESCE(p.is_registrant, false) = true
          AND p.name NOT IN (
              'My Company', 'Administrator', 'Public user',
              'Default User Template', 'OdooBot'
          )
    ),

    prog_agg AS (
        SELECT
            pm.partner_id,
            COUNT(DISTINCT pm.program_id) AS nb_programs,
            COUNT(DISTINCT CASE
                WHEN pm.state = 'enrolled' THEN pm.program_id
            END) AS nb_active_programs,
            AVG(EXTRACT(DAY FROM NOW() - pm.enrollment_date))::int AS avg_enrollment_days
        FROM g2p_program_membership pm
        GROUP BY pm.partner_id
    ),

    /* fallback PMT si la table registrant info n'existe pas */
    pmt_agg AS (
        SELECT
            pb.partner_id,
            0.5 AS pmt_score,
            0.5 AS pmt_score_min
        FROM partner_base pb
    ),

    pay_agg AS (
        SELECT
            ent.partner_id,
            COUNT(py.id) AS payment_count,
            COALESCE(SUM(py.amount_issued), 0.0) AS total_issued,
            COALESCE(SUM(py.amount_paid), 0.0) AS total_paid,
            CASE
                WHEN COALESCE(SUM(py.amount_issued), 0) > 0
                THEN (SUM(py.amount_issued) - SUM(py.amount_paid)) / SUM(py.amount_issued)
                ELSE 0.0
            END AS payment_gap_ratio,
            CASE
                WHEN COALESCE(SUM(py.amount_issued), 0) > 0
                THEN SUM(py.amount_paid) / SUM(py.amount_issued)
                ELSE 0.0
            END AS payment_success_rate,
            COALESCE(VAR_POP(py.amount_paid), 0.0) AS amount_variance,
            COUNT(DISTINCT ent.cycle_id) AS cycle_count,
            AVG(ent.initial_amount) AS avg_entitlement_amount
        FROM g2p_payment py
        JOIN g2p_entitlement ent ON py.entitlement_id = ent.id
        GROUP BY ent.partner_id
    ),

    phone_shared AS (
        SELECT
            ph.partner_id,
            COUNT(DISTINCT ph2.partner_id) - 1 AS shared_phone_count
        FROM g2p_phone_number ph
        JOIN g2p_phone_number ph2
          ON COALESCE(ph.phone_sanitized, ph.phone_no)
           = COALESCE(ph2.phone_sanitized, ph2.phone_no)
         AND ph.partner_id != ph2.partner_id
        GROUP BY ph.partner_id
    ),

    bank_shared AS (
        SELECT
            rb.partner_id,
            COUNT(DISTINCT rb2.partner_id) - 1 AS shared_account_count
        FROM res_partner_bank rb
        JOIN res_partner_bank rb2
          ON COALESCE(NULLIF(rb.sanitized_acc_number, ''), rb.acc_number)
           = COALESCE(NULLIF(rb2.sanitized_acc_number, ''), rb2.acc_number)
         AND rb.partner_id != rb2.partner_id
         AND rb.active = true
         AND rb2.active = true
        GROUP BY rb.partner_id
    ),

    group_agg AS (
        SELECT
            gm.individual AS partner_id,
            COUNT(*) AS group_membership_count
        FROM g2p_group_membership gm
        GROUP BY gm.individual
    )

    SELECT
        pb.partner_id,
        pb.age,
        pb.gender,
        pb.income,
        pb.household_size,
        pb.nb_children,
        pb.nb_elderly,
        pb.has_disabled,
        pb.single_head,
        pb.elderly_head,
        pb.registration_age_days,

        COALESCE(pa.nb_programs, 0) AS nb_programs,
        COALESCE(pa.nb_active_programs, 0) AS nb_active_programs,
        COALESCE(pa.avg_enrollment_days, 0) AS avg_enrollment_days,
        COALESCE(pm.pmt_score, 0.5) AS pmt_score,
        COALESCE(pm.pmt_score_min, 0.5) AS pmt_score_min,

        COALESCE(py.payment_count, 0) AS payment_count,
        COALESCE(py.total_issued, 0.0) AS total_issued,
        COALESCE(py.total_paid, 0.0) AS total_paid,
        COALESCE(py.payment_gap_ratio, 0.0) AS payment_gap_ratio,
        COALESCE(py.payment_success_rate, 0.0) AS payment_success_rate,
        COALESCE(py.amount_variance, 0.0) AS amount_variance,
        COALESCE(py.cycle_count, 0) AS cycle_count,
        COALESCE(py.avg_entitlement_amount, 0.0) AS avg_entitlement_amount,

        COALESCE(ps.shared_phone_count, 0) AS shared_phone_count,
        COALESCE(bs.shared_account_count, 0) AS shared_account_count,
        COALESCE(ga.group_membership_count, 0) AS group_membership_count
    FROM partner_base pb
    LEFT JOIN prog_agg pa ON pb.partner_id = pa.partner_id
    LEFT JOIN pmt_agg pm ON pb.partner_id = pm.partner_id
    LEFT JOIN pay_agg py ON pb.partner_id = py.partner_id
    LEFT JOIN phone_shared ps ON pb.partner_id = ps.partner_id
    LEFT JOIN bank_shared bs ON pb.partner_id = bs.partner_id
    LEFT JOIN group_agg ga ON pb.partner_id = ga.partner_id
    {limit_sql}
    """)

    df = pd.read_sql(query, engine, params=params)

    if df.empty:
        return df

    df["income_per_person"] = (
        df["income"] / df["household_size"].clip(lower=1)
    ).round(2)

    adults = (
        df["household_size"] - df["nb_children"] - df["nb_elderly"]
    ).clip(lower=1)

    df["dependency_ratio"] = (
        (df["nb_children"] + df["nb_elderly"]) / adults
    ).round(3)

    df["network_risk"] = (
        df["shared_phone_count"].clip(upper=5) * 0.4
        + df["shared_account_count"].clip(upper=5) * 0.6
    ).clip(upper=1.0).round(4)

    q95 = df["total_issued"].quantile(0.95) if len(df) > 0 else 0
    df["high_amount_flag"] = (df["total_issued"] > q95).astype(int)

    q15 = df["income_per_person"].quantile(0.15) if len(df) > 0 else 0
    df["income_program_inconsistency"] = (
        (df["income_per_person"] < q15) & (df["nb_programs"] >= 3)
    ).astype(int)

    return df


ML_FEATURES = [
    "age", "income", "income_per_person",
    "household_size", "nb_children", "nb_elderly",
    "dependency_ratio",
    "has_disabled", "single_head",
    "nb_programs", "nb_active_programs",
    "pmt_score", "pmt_score_min",
    "avg_enrollment_days",
    "payment_count", "payment_gap_ratio",
    "payment_success_rate", "amount_variance",
    "cycle_count",
    "shared_phone_count", "shared_account_count",
    "network_risk",
    "group_membership_count",
    "high_amount_flag", "income_program_inconsistency",
]