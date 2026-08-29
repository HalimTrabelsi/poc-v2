"""Data extractors that wrap the OpenG2P connector with typed queries."""
import logging
from datetime import date
from typing import Optional

import pandas as pd
from sqlalchemy import text

from app.data.connector import OpenG2PConnector

logger = logging.getLogger(__name__)

_MAIN_QUERY = """
WITH partner_base AS (
    SELECT
        p.id AS partner_id,
        p.name AS name,
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

pmt_agg AS (
    SELECT
        pm.partner_id,
        0.5 AS pmt_score,
        0.5 AS pmt_score_min
    FROM g2p_program_membership pm
    WHERE false
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
        COUNT(DISTINCT ph2.partner_id) AS shared_phone_count
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
        COUNT(DISTINCT rb2.partner_id) AS shared_account_count
    FROM res_partner_bank rb
    JOIN res_partner_bank rb2
      ON COALESCE(NULLIF(rb.sanitized_acc_number, ''), rb.acc_number)
       = COALESCE(NULLIF(rb2.sanitized_acc_number, ''), rb2.acc_number)
     AND rb.partner_id != rb2.partner_id
     AND rb.active = true
     AND rb2.active = true
     AND COALESCE(NULLIF(rb.sanitized_acc_number, ''), rb.acc_number) IS NOT NULL
    GROUP BY rb.partner_id
),

group_agg AS (
    SELECT
        gm.individual AS partner_id,
        COUNT(*) AS group_membership_count
    FROM g2p_group_membership gm
    GROUP BY gm.individual
),

identity_cluster AS (
    -- Detect identity-cluster fraud: several registrants sharing the same
    -- birthdate AND the same family name (last word of the name, case-
    -- insensitive). One person enrolling multiple times under slightly
    -- different first names is a classic G2P fraud pattern that exact
    -- phone/account matching misses.
    SELECT
        p.id AS partner_id,
        COUNT(DISTINCT p2.id) AS identity_cluster_count
    FROM res_partner p
    JOIN res_partner p2
      ON p.birthdate = p2.birthdate
     AND p.birthdate IS NOT NULL
     AND lower(regexp_replace(trim(p.name),  '^.*\\s', ''))
       = lower(regexp_replace(trim(p2.name), '^.*\\s', ''))
     AND p.id != p2.id
     AND COALESCE(p2.is_registrant, false) = true
    WHERE COALESCE(p.is_registrant, false) = true
    GROUP BY p.id
),

temporal_enroll_agg AS (
    -- How fast did this beneficiary accumulate program enrollments?
    SELECT
        pm.partner_id,
        -- Enrollments in recent windows
        COUNT(CASE WHEN pm.enrollment_date >= (NOW()::date - 30)  THEN 1 END) AS enroll_last_30d,
        COUNT(CASE WHEN pm.enrollment_date >= (NOW()::date - 90)  THEN 1 END) AS enroll_last_90d,
        -- Velocity: programs per month over full history
        CASE
            WHEN MAX(EXTRACT(DAY FROM NOW() - pm.enrollment_date::timestamp)) > 0
            THEN COUNT(*) / GREATEST(
                MAX(EXTRACT(DAY FROM NOW() - pm.enrollment_date::timestamp)) / 30.0, 1
            )
            ELSE 0.0
        END AS enrollment_velocity,
        -- Days between partner registration and earliest program enrollment
        MIN(EXTRACT(DAY FROM pm.enrollment_date - p.create_date::date)::int) AS days_reg_to_first_enroll
    FROM g2p_program_membership pm
    JOIN res_partner p ON p.id = pm.partner_id
    GROUP BY pm.partner_id
),

temporal_payment_agg AS (
    -- Payment timing anomalies
    SELECT
        ent.partner_id,
        -- Days from partner registration to first payment (very low = suspicious)
        MIN(
            EXTRACT(DAY FROM py.payment_datetime - p.create_date)::int
        ) AS days_reg_to_first_payment,
        -- Payments received within 7 days of any enrollment (rushed disbursement)
        COUNT(CASE
            WHEN EXTRACT(DAY FROM py.payment_datetime - pm.enrollment_date::timestamp) < 7
            THEN 1
        END) AS payments_within_7d_of_enroll,
        -- Distinct cycle count (high = beneficiary spans many disbursement windows)
        COUNT(DISTINCT ent.cycle_id) AS payment_cycle_count
    FROM g2p_payment py
    JOIN g2p_entitlement ent   ON py.entitlement_id = ent.id
    JOIN res_partner p         ON p.id = ent.partner_id
    LEFT JOIN g2p_program_membership pm
        ON pm.partner_id = ent.partner_id
       AND pm.program_id = (
           SELECT c.program_id FROM g2p_cycle c WHERE c.id = ent.cycle_id LIMIT 1
       )
    GROUP BY ent.partner_id
),

dedup_agg AS (
    -- OpenG2P's own deduplication engine results: flagged memberships + duplicate records
    SELECT
        pm.partner_id,
        -- 1 if OpenG2P's dedup engine marked any membership as a duplicate
        MAX(CASE
            WHEN pm.deduplication_status IS NOT NULL
             AND pm.deduplication_status != ''
            THEN 1 ELSE 0
        END) AS is_dedup_flagged,
        -- How many memberships triggered dedup
        COUNT(CASE
            WHEN pm.deduplication_status IS NOT NULL
             AND pm.deduplication_status != ''
            THEN 1
        END) AS dedup_flag_count,
        -- Count of formal duplicate records linked to this partner's memberships
        COUNT(DISTINCT rel.g2p_program_membership_duplicate_id) AS dedup_record_count
    FROM g2p_program_membership pm
    LEFT JOIN g2p_program_membership_g2p_program_membership_duplicate_rel rel
        ON pm.id = rel.g2p_program_membership_id
    GROUP BY pm.partner_id
)

SELECT
    pb.partner_id,
    pb.name,
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
    COALESCE(ga.group_membership_count, 0) AS group_membership_count,
    COALESCE(ic.identity_cluster_count, 0) AS identity_cluster_count,
    COALESCE(da.is_dedup_flagged, 0) AS is_dedup_flagged,
    COALESCE(da.dedup_flag_count, 0) AS dedup_flag_count,
    COALESCE(da.dedup_record_count, 0) AS dedup_record_count,
    -- temporal enrollment features
    COALESCE(te.enroll_last_30d, 0) AS enroll_last_30d,
    COALESCE(te.enroll_last_90d, 0) AS enroll_last_90d,
    COALESCE(te.enrollment_velocity, 0.0) AS enrollment_velocity,
    COALESCE(te.days_reg_to_first_enroll, 999) AS days_reg_to_first_enroll,
    -- temporal payment features
    COALESCE(tp.days_reg_to_first_payment, 999) AS days_reg_to_first_payment,
    COALESCE(tp.payments_within_7d_of_enroll, 0) AS payments_within_7d_of_enroll,
    COALESCE(tp.payment_cycle_count, 0) AS payment_cycle_count
FROM partner_base pb
LEFT JOIN prog_agg pa ON pb.partner_id = pa.partner_id
LEFT JOIN pmt_agg pm ON pb.partner_id = pm.partner_id
LEFT JOIN pay_agg py ON pb.partner_id = py.partner_id
LEFT JOIN phone_shared ps ON pb.partner_id = ps.partner_id
LEFT JOIN bank_shared bs ON pb.partner_id = bs.partner_id
LEFT JOIN group_agg ga ON pb.partner_id = ga.partner_id
LEFT JOIN identity_cluster ic ON pb.partner_id = ic.partner_id
LEFT JOIN dedup_agg da ON pb.partner_id = da.partner_id
LEFT JOIN temporal_enroll_agg te ON pb.partner_id = te.partner_id
LEFT JOIN temporal_payment_agg tp ON pb.partner_id = tp.partner_id
"""


def _apply_derived_features(df: pd.DataFrame) -> pd.DataFrame:
    """Compute derived columns that require the full dataset for quantiles."""
    df["income_per_person"] = (
        df["income"] / df["household_size"].clip(lower=1)
    ).round(2)

    adults = (df["household_size"] - df["nb_children"] - df["nb_elderly"]).clip(lower=1)
    df["dependency_ratio"] = (
        (df["nb_children"] + df["nb_elderly"]) / adults
    ).round(3)

    if "identity_cluster_count" not in df.columns:
        df["identity_cluster_count"] = 0
    df["network_risk"] = (
        df["shared_phone_count"].clip(upper=5) * 0.35
        + df["shared_account_count"].clip(upper=5) * 0.50
        + df["is_dedup_flagged"] * 0.15          # OpenG2P dedup engine confirmation
        + df["identity_cluster_count"].clip(upper=5) * 0.40  # same DOB + family name
    ).clip(upper=1.0).round(4)

    q95 = df["total_issued"].quantile(0.95) if len(df) > 0 else 0
    df["high_amount_flag"] = (df["total_issued"] > q95).astype(int)

    q15 = df["income_per_person"].quantile(0.15) if len(df) > 0 else 0
    df["income_program_inconsistency"] = (
        (df["income_per_person"] < q15) & (df["nb_programs"] >= 3)
    ).astype(int)

    # Temporal anomaly flags
    # Rapid multi-enrollment: 3+ programs enrolled within 30 days
    df["rapid_enrollment_flag"] = (df["enroll_last_30d"] >= 3).astype(int)

    # Ghost beneficiary: received payment within 14 days of registration
    df["fast_payment_flag"] = (
        (df["days_reg_to_first_payment"] < 14) & (df["days_reg_to_first_payment"] >= 0)
    ).astype(int)

    # Suspicious velocity: more than 0.5 new program enrollments per month on average
    df["high_velocity_flag"] = (df["enrollment_velocity"] > 0.5).astype(int)

    # Rushed disbursement: any payment issued within 7 days of enrollment
    df["rushed_payment_flag"] = (df["payments_within_7d_of_enroll"] > 0).astype(int)

    return df


class BeneficiaryExtractor:
    """Extract beneficiary feature rows from OpenG2P PostgreSQL."""

    def __init__(self, connector: Optional[OpenG2PConnector] = None) -> None:
        self._connector = connector or OpenG2PConnector()

    def get_beneficiary_features(
        self, beneficiary_id: str, snapshot_date: Optional[date] = None
    ) -> dict:
        """Return a feature dict for a single beneficiary.

        Args:
            beneficiary_id: The OpenG2P partner_id.
            snapshot_date: Unused currently; reserved for time-travel queries.

        Returns:
            Feature dict with all raw SQL columns plus derived columns.

        Raises:
            ValueError: If no partner with that ID exists.
        """
        # Filter is applied inside the CTE's partner_base via AND clause
        filtered_query = _MAIN_QUERY.replace(
            "AND p.name NOT IN (\n"
            "          'My Company', 'Administrator', 'Public user',\n"
            "          'Default User Template', 'OdooBot'\n"
            "      )",
            "AND p.name NOT IN (\n"
            "          'My Company', 'Administrator', 'Public user',\n"
            "          'Default User Template', 'OdooBot'\n"
            "      )\n      AND p.id = :pid",
        )
        query = text(filtered_query)
        df = pd.read_sql(query, self._connector.engine, params={"pid": int(beneficiary_id)})

        if df.empty:
            raise ValueError(f"No beneficiary found with partner_id={beneficiary_id}")

        df = _apply_derived_features(df)
        return df.iloc[0].to_dict()

    def get_all_features(self, limit: Optional[int] = None) -> pd.DataFrame:
        """Return features for every beneficiary, used for batch scoring and training.

        Args:
            limit: Cap number of rows returned (useful for development).
        """
        limit_clause = f"LIMIT {int(limit)}" if limit is not None else ""
        query = text(_MAIN_QUERY + f" {limit_clause}")
        df = pd.read_sql(query, self._connector.engine)

        if df.empty:
            return df

        return _apply_derived_features(df)


class PaymentExtractor:
    """Extract raw payment history rows for a single beneficiary."""

    def __init__(self, connector: Optional[OpenG2PConnector] = None) -> None:
        self._connector = connector or OpenG2PConnector()

    def get_payment_history(self, beneficiary_id: str) -> list[dict]:
        """Return chronological payment records for a beneficiary."""
        query = text("""
            SELECT
                py.id AS payment_id,
                py.amount_issued,
                py.amount_paid,
                py.payment_datetime,
                py.status,
                ent.cycle_id,
                ent.initial_amount
            FROM g2p_payment py
            JOIN g2p_entitlement ent ON py.entitlement_id = ent.id
            WHERE ent.partner_id = :pid
            ORDER BY py.payment_datetime ASC
        """)
        df = pd.read_sql(query, self._connector.engine, params={"pid": int(beneficiary_id)})
        return df.to_dict(orient="records")


class RelationshipExtractor:
    """Extract network edges (shared phone / shared account) for graph analysis."""

    def __init__(self, connector: Optional[OpenG2PConnector] = None) -> None:
        self._connector = connector or OpenG2PConnector()

    def get_network_edges(self, beneficiary_id: str) -> list[dict]:
        """Return all neighbours connected via shared phone, bank account,
        or geographic proximity (same city or same region).

        Each edge dict contains: source, target, edge_type, weight.
        Geographic edges are deliberately LOW weight (0.05 / 0.02) — never
        dominant over identity signals (shared phone/account) — and capped
        at 20 matches each to avoid graph blow-up in dense cities/regions.
        """
        phone_query = text("""
            SELECT
                ph.partner_id AS source,
                ph2.partner_id AS target,
                'shared_phone' AS edge_type,
                1.0 AS weight
            FROM g2p_phone_number ph
            JOIN g2p_phone_number ph2
              ON COALESCE(ph.phone_sanitized, ph.phone_no)
               = COALESCE(ph2.phone_sanitized, ph2.phone_no)
             AND ph.partner_id != ph2.partner_id
            WHERE ph.partner_id = :pid
        """)
        bank_query = text("""
            SELECT
                rb.partner_id AS source,
                rb2.partner_id AS target,
                'shared_account' AS edge_type,
                1.5 AS weight
            FROM res_partner_bank rb
            JOIN res_partner_bank rb2
              ON COALESCE(NULLIF(rb.sanitized_acc_number, ''), rb.acc_number)
               = COALESCE(NULLIF(rb2.sanitized_acc_number, ''), rb2.acc_number)
             AND rb.partner_id != rb2.partner_id
             AND rb.active = true AND rb2.active = true
            WHERE rb.partner_id = :pid
        """)
        # city-level: same city AND region (city alone is too broad in
        # large countries — e.g. "Springfield" isn't unique enough).
        city_query = text("""
            SELECT
                p.id AS source,
                p2.id AS target,
                'same_city' AS edge_type,
                0.05 AS weight
            FROM res_partner p
            JOIN res_partner p2
              ON p2.city = p.city AND p2.state_id IS NOT DISTINCT FROM p.state_id
             AND p.id != p2.id
             AND p2.is_registrant = true
            WHERE p.id = :pid
              AND p.city IS NOT NULL AND p.city != ''
            LIMIT 20
        """)
        # region-level: region only — weaker/broader signal, used for
        # heatmap density rather than as a fraud indicator on its own.
        region_query = text("""
            SELECT
                p.id AS source,
                p2.id AS target,
                'same_region' AS edge_type,
                0.02 AS weight
            FROM res_partner p
            JOIN res_partner p2
              ON p2.state_id = p.state_id
             AND p.id != p2.id
             AND p2.is_registrant = true
             AND (p2.city IS DISTINCT FROM p.city)  -- same_city already covers city matches
            WHERE p.id = :pid
              AND p.state_id IS NOT NULL
            LIMIT 20
        """)
        pid = int(beneficiary_id)
        df_phone = pd.read_sql(phone_query, self._connector.engine, params={"pid": pid})
        df_bank = pd.read_sql(bank_query, self._connector.engine, params={"pid": pid})
        try:
            df_city = pd.read_sql(city_query, self._connector.engine, params={"pid": pid})
            df_region = pd.read_sql(region_query, self._connector.engine, params={"pid": pid})
        except Exception:
            # state_id (region) may be unpopulated/absent on older data —
            # geographic edges are an enhancement, never a hard requirement.
            df_city = pd.DataFrame(columns=["source", "target", "edge_type", "weight"])
            df_region = pd.DataFrame(columns=["source", "target", "edge_type", "weight"])
        frames = [d for d in (df_phone, df_bank, df_city, df_region) if not d.empty]
        if not frames:
            return []
        combined = pd.concat(frames, ignore_index=True)
        return combined.to_dict(orient="records")

    def get_shared_entities(self, beneficiary_id: str) -> dict:
        """Return the real names of beneficiaries sharing a phone or bank account.

        Used to build human-readable explanations (e.g. "shares a bank
        account with Karim") instead of an anonymous count — see
        app.core.explanation_templates and app.services.llm_explainer_service.
        """
        edges = self.get_network_edges(beneficiary_id)
        phone_ids = [e["target"] for e in edges if e.get("edge_type") == "shared_phone"]
        bank_ids = [e["target"] for e in edges if e.get("edge_type") == "shared_account"]

        names = self._get_partner_names(phone_ids + bank_ids)
        return {
            "shared_phone_with": [names.get(pid, f"Beneficiary #{pid}") for pid in phone_ids],
            "shared_bank_with": [names.get(pid, f"Beneficiary #{pid}") for pid in bank_ids],
        }

    def get_partner_name(self, partner_id: str) -> Optional[str]:
        """Return a single beneficiary's display name, or None if not found."""
        names = self._get_partner_names([partner_id])
        return names.get(int(partner_id))

    def _get_partner_names(self, partner_ids: list) -> dict:
        if not partner_ids:
            return {}
        query = text("SELECT id, name FROM res_partner WHERE id = ANY(:ids)")
        with self._connector.engine.connect() as conn:
            rows = conn.execute(query, {"ids": list({int(p) for p in partner_ids})}).fetchall()
        return {row[0]: row[1] for row in rows}
