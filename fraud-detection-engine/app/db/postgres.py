"""
PostgreSQL connections
- OPENG2P_DB : lecture seule → données bénéficiaires du POC
- FRAUD_DB   : lecture/écriture → alertes et scores du moteur fraude
"""
import os
import pandas as pd
from sqlalchemy import create_engine, text

# ── Connexion 1 : Base OpenG2P du POC (lecture seule) ────────
OPENG2P_DB_URL = os.getenv(
    "OPENG2P_DB_URL",
    "postgresql://odoo:odoo@postgresql:5432/openg2p"
)

# ── Connexion 2 : Base Fraud Engine (lecture/écriture) ───────
FRAUD_DB_URL = os.getenv(
    "DB_URL",
    "postgresql://fraud:fraud123@fraud-db:5432/fraud_engine"
)


class OpenG2PDatabase:
    """Lecture des données bénéficiaires depuis le POC OpenG2P"""

    def __init__(self):
        self.engine = create_engine(
            OPENG2P_DB_URL,
            pool_pre_ping=True,
            connect_args={"connect_timeout": 10}
        )

    def get_all_beneficiaries(self) -> pd.DataFrame:
        """Récupère tous les bénéficiaires pour scan complet"""
        query = text("""
            SELECT
                r.id::text                                    AS beneficiary_id,
                r.name                                        AS beneficiary_name,
                COUNT(DISTINCT pm.program_id)                 AS nb_programs,
                COALESCE(SUM(p.amount_paid), 0)               AS total_amount,
                COUNT(DISTINCT p.cycle_id)                    AS nb_cycles,
                EXTRACT(DAY FROM NOW() - r.create_date)::int  AS days_since_enrollment,
                COUNT(DISTINCT gm.individual_id)              AS household_size,
                r.active
            FROM res_partner r
            LEFT JOIN g2p_program_membership pm ON r.id = pm.partner_id
            LEFT JOIN g2p_payment p             ON r.id = p.partner_id
            LEFT JOIN g2p_group_membership gm   ON r.id = gm.group_id
            WHERE r.active = true
              AND EXISTS (
                SELECT 1 FROM g2p_program_membership pm2
                WHERE pm2.partner_id = r.id
              )
            GROUP BY r.id, r.name, r.create_date, r.active
            LIMIT 1000
        """)
        try:
            df = pd.read_sql(query, self.engine)
            if df.empty:
                return pd.DataFrame()
            # Compute derived features
            df["amount_ratio"]        = df["total_amount"] / 500.0
            df["account_changes_30d"] = 0
            df["nb_payment_failures"] = 0
            df["location_risk_score"] = 0.3
            return df
        except Exception as e:
            print(f"[OpenG2P DB Error] {e}")
            return pd.DataFrame()

    def get_beneficiary(self, beneficiary_id: str) -> dict | None:
        """Récupère un bénéficiaire spécifique par ID"""
        query = text("""
            SELECT
                r.id::text                                    AS beneficiary_id,
                r.name                                        AS beneficiary_name,
                COUNT(DISTINCT pm.program_id)                 AS nb_programs,
                COALESCE(SUM(p.amount_paid), 0)               AS total_amount,
                COUNT(DISTINCT p.cycle_id)                    AS nb_cycles,
                EXTRACT(DAY FROM NOW() - r.create_date)::int  AS days_since_enrollment,
                COUNT(DISTINCT gm.individual_id)              AS household_size,
                r.active
            FROM res_partner r
            LEFT JOIN g2p_program_membership pm ON r.id = pm.partner_id
            LEFT JOIN g2p_payment p             ON r.id = p.partner_id
            LEFT JOIN g2p_group_membership gm   ON r.id = gm.group_id
            WHERE r.id = :bid
            GROUP BY r.id, r.name, r.create_date, r.active
        """)
        try:
            df = pd.read_sql(
                query, self.engine, params={"bid": int(beneficiary_id)}
            )
            if df.empty:
                return None
            row = df.iloc[0].to_dict()
            row["amount_ratio"]        = row["total_amount"] / 500.0
            row["account_changes_30d"] = 0
            row["nb_payment_failures"] = 0
            row["location_risk_score"] = 0.3
            return row
        except Exception as e:
            print(f"[OpenG2P DB Error] {e}")
            return None

    def test_connection(self) -> bool:
        try:
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return True
        except Exception:
            return False


class FraudDatabase:
    """Stockage des alertes et résultats du moteur fraude"""

    def __init__(self):
        self.engine = create_engine(
            FRAUD_DB_URL,
            pool_pre_ping=True,
            connect_args={"connect_timeout": 10}
        )

    def save_alert(self, result: dict) -> bool:
        """Sauvegarde une alerte de fraude"""
        query = text("""
            INSERT INTO fraud_alerts (
                beneficiary_id, risk_score, risk_level,
                action, rule_flags, explanation, status
            ) VALUES (
                :beneficiary_id, :risk_score, :risk_level,
                :action, :rule_flags, :explanation, 'pending'
            )
            ON CONFLICT (beneficiary_id)
            DO UPDATE SET
                risk_score  = EXCLUDED.risk_score,
                risk_level  = EXCLUDED.risk_level,
                action      = EXCLUDED.action,
                rule_flags  = EXCLUDED.rule_flags,
                explanation = EXCLUDED.explanation,
                updated_at  = NOW()
        """)
        try:
            with self.engine.begin() as conn:
                conn.execute(query, {
                    "beneficiary_id": result["beneficiary_id"],
                    "risk_score":     result["final_score"],
                    "risk_level":     result["risk_level"],
                    "action":         result["action"],
                    "rule_flags":     ", ".join(result.get("rule_flags", [])),
                    "explanation":    result.get("explanation", ""),
                })
            return True
        except Exception as e:
            print(f"[Fraud DB Error] {e}")
            return False

    def get_alerts(self, status: str = "pending") -> list[dict]:
        """Récupère les alertes par statut"""
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


# ── Singletons ───────────────────────────────────────────────
_openg2p_db: OpenG2PDatabase | None = None
_fraud_db:   FraudDatabase   | None = None


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
def get_cases(self, limit: int = 50) -> list[dict]:
    query = text("""
        SELECT
            fc.id,
            fc.alert_id,
            fc.assigned_to,
            fc.resolution,
            fc.notes,
            fc.resolved_at,
            fc.created_at,
            fa.beneficiary_id,
            fa.risk_score,
            fa.risk_level,
            fa.action,
            fa.rule_flags,
            fa.explanation,
            fa.status
        FROM fraud_cases fc
        JOIN fraud_alerts fa ON fa.id = fc.alert_id
        ORDER BY fc.created_at DESC
        LIMIT :limit
    """)
    try:
        df = pd.read_sql(query, self.engine, params={"limit": limit})
        return df.to_dict(orient="records")
    except Exception as e:
        print(f"[Fraud DB Error] {e}")
        return []