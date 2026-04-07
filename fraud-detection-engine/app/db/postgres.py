"""
postgres.py — Connexions DB et accès données
=============================================
Rôles :
  - OpenG2PDatabase : connexion + test vers OpenG2P PostgreSQL.
  - FraudDatabase   : écriture/lecture des alertes dans la Fraud DB interne.

Extraction des features ML → déléguée à feature_extractor.py (source unique).
"""

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


# ─────────────────────────────────────────────────────────────
class OpenG2PDatabase:
    """Connexion vers la base OpenG2P. Le moteur est exposé à feature_extractor."""

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


# ─────────────────────────────────────────────────────────────
class FraudDatabase:
    """Accès à la base interne du moteur de fraude (alertes, résultats)."""

    def __init__(self):
        self.engine = create_engine(
            FRAUD_DB_URL,
            pool_pre_ping=True,
            connect_args={"connect_timeout": 10},
        )

    # ── Lecture des features ML ────────────────────────────────
    # Déléguée à feature_extractor.py pour éviter toute duplication.
    # Import lazy pour éviter l'import circulaire (feature_extractor → postgres).

    def get_beneficiary_features(self, beneficiary_id: int) -> Optional[Dict[str, Any]]:
        """Retourne les features ML d'un bénéficiaire via feature_extractor."""
        from app.db.feature_extractor import extract_features
        df = extract_features()
        row = df[df["partner_id"] == beneficiary_id]
        if row.empty:
            return None
        return row.iloc[0].to_dict()

    def get_all_beneficiaries_features(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Retourne les features ML de tous les bénéficiaires via feature_extractor."""
        from app.db.feature_extractor import extract_features
        df = extract_features(limit=limit)
        if df.empty:
            return []
        return df.to_dict(orient="records")

    # ── Alertes ───────────────────────────────────────────────

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
                    "risk_score":     result["final_score"],
                    "risk_level":     result["risk_level"],
                    "action":         result.get("action", "No immediate action"),
                    "rule_flags":     ", ".join(result.get("rule_flags", [])),
                    "explanation":    result.get("explanation", ""),
                })
            return True
        except Exception as e:
            print(f"[Fraud DB Error] {e}")
            return False

    def save_case_result(self, result: Dict[str, Any]) -> bool:
        return self.save_alert({
            "beneficiary_id": result["beneficiary_id"],
            "final_score":    result["final_score"],
            "risk_level":     result["risk_level"],
            "action":         result.get("recommended_action", "No immediate action"),
            "rule_flags":     result.get("rule_flags", []),
            "explanation":    result.get("explanation", ""),
        })

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


# ─────────────────────────────────────────────────────────────
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
