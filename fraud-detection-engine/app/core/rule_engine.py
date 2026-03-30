import json
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd
from sqlalchemy import create_engine, text

DB_URL = "postgresql://odoo:openg2p@postgresql:5432/openg2p"
RULES_PATH = Path("rules/fraud_rules.json")


class RuleEngine:
    def __init__(self, rules_path: str | Path = RULES_PATH):
        with open(rules_path, "r", encoding="utf-8") as f:
            self.rules = json.load(f)["rules"]

    def _safe_eval(self, condition: str, data: Dict[str, Any]) -> bool:
        try:
            return bool(eval(condition, {"__builtins__": {}}, data))
        except Exception:
            return False

    def evaluate_one(self, row: Dict[str, Any]) -> Dict[str, Any]:
        triggered = []
        total_score = 0.0

        for rule in self.rules:
            if self._safe_eval(rule["condition"], row):
                triggered.append({
                    "rule_id": rule["id"],
                    "flag": rule["flag"],
                    "severity": rule["severity"],
                    "weight": rule["weight"],
                })
                total_score += float(rule["weight"])

        rule_score = min(total_score, 1.0)

        risk_level = (
            "CRITICAL" if rule_score >= 0.80 else
            "HIGH" if rule_score >= 0.55 else
            "MEDIUM" if rule_score >= 0.30 else
            "LOW"
        )

        return {
            "beneficiary_id": row.get("partner_id"),
            "rule_score": round(rule_score, 3),
            "risk_level": risk_level,
            "triggered_rules": triggered,
            "pass_to_ml": rule_score < 0.80,
        }

    def evaluate_df(self, df: pd.DataFrame) -> pd.DataFrame:
        outputs: List[Dict[str, Any]] = []
        for _, row in df.iterrows():
            outputs.append(self.evaluate_one(row.to_dict()))
        return pd.DataFrame(outputs)


def extract_rule_features(engine) -> pd.DataFrame:
    query = text("""
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
    """)

    df = pd.read_sql(query, engine)

    df["income_per_person"] = df["income"] / df["household_size"].clip(lower=1)
    df["dependency_ratio"] = (df["nb_children"] + df["nb_elderly"]) / (
        df["household_size"] - df["nb_children"] - df["nb_elderly"]
    ).replace(0, 1)
    df["high_amount_flag"] = (
        df["total_issued"] > df["total_issued"].quantile(0.95)
    ).astype(int)
    df["network_risk"] = (
        df["shared_phone_count"].clip(upper=5) * 0.4 +
        df["shared_account_count"].clip(upper=5) * 0.6
    ).clip(upper=1.0)

    return df


def main():
    engine = create_engine(DB_URL, pool_pre_ping=True)
    df = extract_rule_features(engine)

    engine_rules = RuleEngine()
    results = engine_rules.evaluate_df(df)

    final_df = df.merge(results, on="beneficiary_id", how="left")
    final_df.to_csv("rule_engine_results.csv", index=False)

    print("\nTop suspicious cases:")
    print(
        final_df.sort_values(["rule_score"], ascending=False)[
            ["beneficiary_id", "rule_score", "risk_level", "triggered_rules"]
        ].head(20).to_string(index=False)
    )

    print("\nDistribution:")
    print(final_df["risk_level"].value_counts(dropna=False))


if __name__ == "__main__":
    main()