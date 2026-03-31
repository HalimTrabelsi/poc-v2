from typing import Optional
import pandas as pd
from sqlalchemy import text
from app.db.postgres import get_openg2p_db


def extract_rule_features(limit: Optional[int] = None) -> pd.DataFrame:
    db = get_openg2p_db()
    engine = db.engine

    limit_clause = ""
    params = {}
    if limit is not None:
        limit_clause = "LIMIT :limit"
        params["limit"] = limit

    query = text(f"""
    WITH partner_base AS (
        SELECT
            p.id AS partner_id,
            COALESCE(EXTRACT(YEAR FROM AGE(p.birthdate))::int, 35) AS age,
            CASE WHEN LOWER(COALESCE(p.gender, '')) = 'male' THEN 1 ELSE 0 END AS gender_m,
            COALESCE(p.income, 0) AS income,
            COALESCE(p.z_ind_grp_num_individuals, 1) AS household_size,
            COALESCE(p.z_ind_grp_num_children, 0) AS nb_children,
            COALESCE(p.z_ind_grp_num_elderly, 0) AS nb_elderly,
            CASE WHEN p.z_ind_grp_is_hh_with_disabled THEN 1 ELSE 0 END AS has_disabled,
            CASE WHEN p.z_ind_grp_is_single_head_hh THEN 1 ELSE 0 END AS single_head
        FROM res_partner p
        WHERE p.active = true
          AND COALESCE(p.is_company, false) = false
          AND p.name NOT IN ('My Company', 'Administrator', 'Public user', 'Default User Template', 'OdooBot')
    ),
    prog_agg AS (
        SELECT
            registrant_id,
            COUNT(DISTINCT program_id) AS nb_programs,
            AVG(COALESCE(pmt_score, latest_pmt_score, 0.5)) AS pmt_score
        FROM g2p_program_registrant_info
        GROUP BY registrant_id
    ),
    pay_agg AS (
        SELECT
            partner_id,
            COALESCE(SUM(amount_issued), 0) AS total_issued,
            COALESCE(SUM(amount_paid), 0) AS total_paid,
            CASE
                WHEN COALESCE(SUM(amount_issued), 0) > 0
                THEN (SUM(amount_issued) - SUM(amount_paid)) / SUM(amount_issued)
                ELSE 0
            END AS gap_ratio,
            COUNT(*) AS payment_count,
            COUNT(DISTINCT cycle_id) AS cycle_count
        FROM g2p_payment
        GROUP BY partner_id
    ),
    phone_shared AS (
        SELECT
            ph.partner_id,
            COUNT(DISTINCT ph2.partner_id) - 1 AS shared_phone_count
        FROM g2p_phone_number ph
        JOIN g2p_phone_number ph2
          ON COALESCE(ph.phone_sanitized, ph.phone_no) = COALESCE(ph2.phone_sanitized, ph2.phone_no)
         AND ph.partner_id != ph2.partner_id
        GROUP BY ph.partner_id
    ),
    bank_shared AS (
        SELECT
            rb.partner_id,
            COUNT(DISTINCT rb2.partner_id) - 1 AS shared_account_count
        FROM res_partner_bank rb
        JOIN res_partner_bank rb2
          ON COALESCE(NULLIF(rb.sanitized_acc_number, ''), rb.acc_number, rb.account_number)
           = COALESCE(NULLIF(rb2.sanitized_acc_number, ''), rb2.acc_number, rb2.account_number)
         AND rb.partner_id != rb2.partner_id
        GROUP BY rb.partner_id
    )
    SELECT
        pb.partner_id,
        pb.age,
        pb.gender_m,
        pb.income,
        pb.household_size,
        pb.nb_children,
        pb.nb_elderly,
        pb.has_disabled,
        pb.single_head,
        COALESCE(pa.nb_programs, 0) AS nb_programs,
        COALESCE(pa.pmt_score, 0.5) AS pmt_score,
        COALESCE(py.total_issued, 0) AS total_issued,
        COALESCE(py.total_paid, 0) AS total_paid,
        COALESCE(py.gap_ratio, 0) AS gap_ratio,
        COALESCE(py.payment_count, 0) AS payment_count,
        COALESCE(py.cycle_count, 0) AS cycle_count,
        COALESCE(ps.shared_phone_count, 0) AS shared_phone_count,
        COALESCE(bs.shared_account_count, 0) AS shared_account_count
    FROM partner_base pb
    LEFT JOIN prog_agg pa ON pb.partner_id = pa.registrant_id
    LEFT JOIN pay_agg py ON pb.partner_id = py.partner_id
    LEFT JOIN phone_shared ps ON pb.partner_id = ps.partner_id
    LEFT JOIN bank_shared bs ON pb.partner_id = bs.partner_id
    {limit_clause}
    """)

    df = pd.read_sql(query, engine, params=params)

    df["income_per_person"] = df["income"] / df["household_size"].clip(lower=1)
    adults = (df["household_size"] - df["nb_children"] - df["nb_elderly"]).clip(lower=1)
    df["dependency_ratio"] = (df["nb_children"] + df["nb_elderly"]) / adults
    df["high_amount_flag"] = (df["total_issued"] > df["total_issued"].quantile(0.95)).astype(int)
    df["network_risk"] = (
        df["shared_phone_count"].clip(upper=5) * 0.4
        + df["shared_account_count"].clip(upper=5) * 0.6
    ).clip(upper=1.0)

    return df