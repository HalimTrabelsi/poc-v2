import os
from typing import Any, Dict, List, Optional

import pandas as pd
from sqlalchemy import create_engine, text

OPENG2P_DB_URL = os.getenv(
    "OPENG2P_DB_URL",
    "postgresql://odoo:openg2p@postgresql:5432/openg2p"
)

FRAUD_DB_URL = os.getenv(
    "DB_URL",
    "postgresql://fraud:fraud123@fraud-db:5432/fraud_engine"
)


class OpenG2PDatabase:
    def __init__(self):
        self.engine = create_engine(
            OPENG2P_DB_URL,
            pool_pre_ping=True,
            connect_args={"connect_timeout": 10},
        )

    def test_connection(self) -> bool:
        try:
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return True
        except Exception as e:
            print(f"[OpenG2P DB Error] {e}")
            return False

    def get_all_beneficiaries(self, limit: int = 100) -> pd.DataFrame:
        query = text("""
            WITH phone_counts AS (
                SELECT
                    p.partner_id,
                    MAX(cnt.shared_phone_count) AS shared_phone_count
                FROM g2p_phone_number p
                JOIN (
                    SELECT
                        phone_sanitized,
                        COUNT(DISTINCT partner_id) AS shared_phone_count
                    FROM g2p_phone_number
                    WHERE phone_sanitized IS NOT NULL
                    GROUP BY phone_sanitized
                ) cnt
                    ON p.phone_sanitized = cnt.phone_sanitized
                GROUP BY p.partner_id
            ),
            bank_counts AS (
                SELECT
                    b.partner_id,
                    MAX(cnt.shared_account_count) AS shared_account_count
                FROM res_partner_bank b
                JOIN (
                    SELECT
                        COALESCE(NULLIF(sanitized_acc_number, ''), acc_number) AS acc_key,
                        COUNT(DISTINCT partner_id) AS shared_account_count
                    FROM res_partner_bank
                    WHERE COALESCE(NULLIF(sanitized_acc_number, ''), acc_number) IS NOT NULL
                    GROUP BY COALESCE(NULLIF(sanitized_acc_number, ''), acc_number)
                ) cnt
                    ON COALESCE(NULLIF(b.sanitized_acc_number, ''), b.acc_number) = cnt.acc_key
                GROUP BY b.partner_id
            )
            SELECT
                rp.id AS beneficiary_id,
                rp.name AS beneficiary_name,
                rp.gender,
                CASE
                    WHEN rp.birthdate IS NOT NULL
                    THEN DATE_PART('year', AGE(CURRENT_DATE, rp.birthdate))
                    ELSE 0
                END AS age,
                COALESCE(rp.income, 0) AS income,
                COALESCE(rp.z_ind_grp_num_individuals, 0) AS household_size,
                COALESCE(rp.z_ind_grp_num_children, 0) AS nb_children,
                COALESCE(rp.z_ind_grp_num_elderly, 0) AS nb_elderly,
                CASE
                    WHEN rp.z_ind_grp_is_hh_with_disabled IS TRUE THEN 1
                    ELSE 0
                END AS disability_flag,
                CASE
                    WHEN rp.z_cst_indv_receive_government_benefits IS TRUE THEN 1
                    ELSE 0
                END AS receive_government_benefits,
                CASE
                    WHEN rp.z_ind_grp_is_single_head_hh IS TRUE THEN 1
                    ELSE 0
                END AS single_head_hh,
                0 AS vehicles_owned,
                COALESCE(pc.shared_phone_count, 0) AS shared_phone_count,
                COALESCE(bc.shared_account_count, 0) AS shared_account_count
            FROM res_partner rp
            LEFT JOIN phone_counts pc ON rp.id = pc.partner_id
            LEFT JOIN bank_counts bc ON rp.id = bc.partner_id
            LIMIT :limit
        """)
        try:
            return pd.read_sql(query, self.engine, params={"limit": limit})
        except Exception as e:
            print(f"[OpenG2P DB Error] {e}")
            return pd.DataFrame()

    def get_beneficiary(self, beneficiary_id: str) -> Optional[dict]:
        query = text("""
            WITH phone_counts AS (
                SELECT
                    p.partner_id,
                    MAX(cnt.shared_phone_count) AS shared_phone_count
                FROM g2p_phone_number p
                JOIN (
                    SELECT
                        phone_sanitized,
                        COUNT(DISTINCT partner_id) AS shared_phone_count
                    FROM g2p_phone_number
                    WHERE phone_sanitized IS NOT NULL
                    GROUP BY phone_sanitized
                ) cnt
                    ON p.phone_sanitized = cnt.phone_sanitized
                GROUP BY p.partner_id
            ),
            bank_counts AS (
                SELECT
                    b.partner_id,
                    MAX(cnt.shared_account_count) AS shared_account_count
                FROM res_partner_bank b
                JOIN (
                    SELECT
                        COALESCE(NULLIF(sanitized_acc_number, ''), acc_number) AS acc_key,
                        COUNT(DISTINCT partner_id) AS shared_account_count
                    FROM res_partner_bank
                    WHERE COALESCE(NULLIF(sanitized_acc_number, ''), acc_number) IS NOT NULL
                    GROUP BY COALESCE(NULLIF(sanitized_acc_number, ''), acc_number)
                ) cnt
                    ON COALESCE(NULLIF(b.sanitized_acc_number, ''), b.acc_number) = cnt.acc_key
                GROUP BY b.partner_id
            )
            SELECT
                rp.id AS beneficiary_id,
                rp.name AS beneficiary_name,
                rp.gender,
                CASE
                    WHEN rp.birthdate IS NOT NULL
                    THEN DATE_PART('year', AGE(CURRENT_DATE, rp.birthdate))
                    ELSE 0
                END AS age,
                COALESCE(rp.income, 0) AS income,
                COALESCE(rp.z_ind_grp_num_individuals, 0) AS household_size,
                COALESCE(rp.z_ind_grp_num_children, 0) AS nb_children,
                COALESCE(rp.z_ind_grp_num_elderly, 0) AS nb_elderly,
                CASE
                    WHEN rp.z_ind_grp_is_hh_with_disabled IS TRUE THEN 1
                    ELSE 0
                END AS disability_flag,
                CASE
                    WHEN rp.z_cst_indv_receive_government_benefits IS TRUE THEN 1
                    ELSE 0
                END AS receive_government_benefits,
                CASE
                    WHEN rp.z_ind_grp_is_single_head_hh IS TRUE THEN 1
                    ELSE 0
                END AS single_head_hh,
                0 AS vehicles_owned,
                COALESCE(pc.shared_phone_count, 0) AS shared_phone_count,
                COALESCE(bc.shared_account_count, 0) AS shared_account_count
            FROM res_partner rp
            LEFT JOIN phone_counts pc ON rp.id = pc.partner_id
            LEFT JOIN bank_counts bc ON rp.id = bc.partner_id
            WHERE rp.id = :bid
            LIMIT 1
        """)
        try:
            df = pd.read_sql(query, self.engine, params={"bid": int(beneficiary_id)})
            if df.empty:
                return None
            return df.iloc[0].to_dict()
        except Exception as e:
            print(f"[OpenG2P DB Error] {e}")
            return None


class FraudDatabase:
    def __init__(self):
        self.engine = create_engine(
            FRAUD_DB_URL,
            pool_pre_ping=True,
            connect_args={"connect_timeout": 10},
        )

    def save_alert(self, result: dict) -> bool:
        query = text("""
            INSERT INTO fraud_alerts (
                beneficiary_id, risk_score, risk_level,
                action, rule_flags, explanation, status
            ) VALUES (
                :beneficiary_id, :risk_score, :risk_level,
                :action, :rule_flags, :explanation, 'pending'
            )
        """)
        try:
            with self.engine.begin() as conn:
                conn.execute(query, {
                    "beneficiary_id": result["beneficiary_id"],
                    "risk_score": result["final_score"],
                    "risk_level": result["risk_level"],
                    "action": result.get("action", "No immediate action"),
                    "rule_flags": ", ".join(result.get("rule_flags", [])),
                    "explanation": result.get("explanation", ""),
                })
            return True
        except Exception as e:
            print(f"[Fraud DB Error] {e}")
            return False

    def get_alerts(self, status: str = "pending") -> List[dict]:
        query = text("""
            SELECT * FROM fraud_alerts
            WHERE status = :status
            ORDER BY risk_score DESC
            LIMIT 100
        """)
        try:
            df = pd.read_sql(query, self.engine, params={"status": status})
            return df.to_dict(orient="records")
        except Exception as e:
            print(f"[Fraud DB Error] {e}")
            return []

    def _normalize_gender(self, value: Any) -> int:
        if value is None:
            return 0
        s = str(value).strip().lower()
        if s in ["female", "f", "1"]:
            return 1
        return 0

    def _safe_float(self, value: Any, default: float = 0.0) -> float:
        try:
            if value is None or value == "":
                return default
            return float(value)
        except Exception:
            return default

    def _row_to_features(self, row: Dict[str, Any]) -> Dict[str, Any]:
        household_size = self._safe_float(row.get("household_size"), 0.0)
        nb_children = self._safe_float(row.get("nb_children"), 0.0)
        nb_elderly = self._safe_float(row.get("nb_elderly"), 0.0)
        income = self._safe_float(row.get("income"), 0.0)

        dependants = nb_children + nb_elderly
        nb_adults = max(household_size - dependants, 0.0)

        dependency_ratio = (dependants / nb_adults) if nb_adults > 0 else 0.0
        income_per_person = (income / household_size) if household_size > 0 else 0.0

        return {
            "beneficiary_id": row.get("beneficiary_id"),
            "gender": self._normalize_gender(row.get("gender")),
            "age": self._safe_float(row.get("age"), 0.0),
            "income": income,
            "household_size": household_size,
            "nb_children": nb_children,
            "vehicles_owned": self._safe_float(row.get("vehicles_owned"), 0.0),
            "dependency_ratio": dependency_ratio,
            "income_per_person": income_per_person,
            "disability_flag": int(self._safe_float(row.get("disability_flag"), 0.0)),
            "immigration_flag": 0,
            "own_home_flag": 0,
            "shared_phone_count": self._safe_float(row.get("shared_phone_count"), 0.0),
            "shared_account_count": self._safe_float(row.get("shared_account_count"), 0.0),
            "receive_government_benefits": int(self._safe_float(row.get("receive_government_benefits"), 0.0)),
            "single_head_hh": int(self._safe_float(row.get("single_head_hh"), 0.0)),
        }

    def get_beneficiary_features(self, beneficiary_id: int) -> Optional[Dict[str, Any]]:
        openg2p = get_openg2p_db()
        row = openg2p.get_beneficiary(str(beneficiary_id))
        if not row:
            return None
        return self._row_to_features(row)

    def get_all_beneficiaries_features(self, limit: int = 50) -> List[Dict[str, Any]]:
        openg2p = get_openg2p_db()
        df = openg2p.get_all_beneficiaries(limit=limit)
        if df.empty:
            return []
        rows = df.to_dict(orient="records")
        return [self._row_to_features(row) for row in rows]

    def save_case_result(self, result: Dict[str, Any]) -> bool:
        return self.save_alert({
            "beneficiary_id": result["beneficiary_id"],
            "final_score": result["final_score"],
            "risk_level": result["risk_level"],
            "action": result.get("recommended_action", "No immediate action"),
            "rule_flags": result.get("rule_flags", []),
            "explanation": result.get("explanation", ""),
        })


_openg2p_db: OpenG2PDatabase | None = None
_fraud_db: FraudDatabase | None = None


def get_openg2p_db() -> OpenG2PDatabase:
    global _openg2p_db
    if _openg2p_db is None:
        _openg2p_db = OpenG2PDatabase()
    return _openg2p_db


def get_fraud_db() -> FraudDatabase:
    global _fraud_db
    if _fraud_db is None:
        _fraud_db = FraudDatabase()
    return _fraud_db