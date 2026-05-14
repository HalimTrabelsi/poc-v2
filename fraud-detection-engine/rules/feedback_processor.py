"""
feedback_processor.py — Traiter les feedback des agents
Utilisé par l'API pour recalibrer les seuils automatiquement
"""

import sqlite3
from pathlib import Path
from collections import Counter
import logging
import os
import time

logger = logging.getLogger(__name__)

# ✅ MÊME LOGIQUE QUE rule_engine.py
_current_dir = Path(__file__).parent.parent.parent  
RULES_DB = _current_dir / "rules" / "rules_history.db"

# En cas de Docker
if not RULES_DB.exists():
    RULES_DB = Path("/app/fraud-detection-engine/rules/rules_history.db")

print(f"[feedback_processor] Using DB: {RULES_DB} (exists: {RULES_DB.exists()})")

def analyze_feedback(rule_id: str, last_n_days: int = 30) -> dict:
    """Analyser les feedback pour un règle spécifique"""
    try:
        # ✅ AJOUTER UN PETIT DÉLAI (SQLite lock)
        time.sleep(0.1)
        
        # ✅ VÉRIFIER QUE LE CHEMIN EXISTE
        if not RULES_DB.exists():
            logger.error(f"DB NOT FOUND at {RULES_DB}")
            return {
                "rule_id": rule_id,
                "total_feedback": 0,
                "errors": 0,
                "error_rate": 0.0,
                "recommendation": "OK",
            }
        
        # ✅ TIMEOUT et ISOLATION_LEVEL
        conn = sqlite3.connect(str(RULES_DB), timeout=5.0)
        conn.isolation_level = None  # Autocommit mode
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT original_decision, agent_decision, COUNT(*) as count
            FROM feedback_log
            WHERE rule_id = ?
            AND datetime(feedback_timestamp) >= datetime('now', '-' || ? || ' days')
            GROUP BY original_decision, agent_decision
        """, (rule_id, last_n_days))
        
        results = cursor.fetchall()
        cursor.close()
        conn.close()
        
        # Calculer le taux d'erreur
        total = sum(r[2] for r in results) if results else 0
        errors = sum(r[2] for r in results if r[0] != r[1]) if results else 0
        
        error_rate = errors / total if total > 0 else 0.0
        
        return {
            "rule_id": rule_id,
            "total_feedback": total,
            "errors": errors,
            "error_rate": error_rate,
            "recommendation": "RECALIBRATE" if error_rate > 0.15 else "OK",
        }
    
    except Exception as e:
        logger.error(f"Erreur analyse feedback : {e}")
        # ✅ RETOURNER UN DICT VALIDE
        return {
            "rule_id": rule_id,
            "total_feedback": 0,
            "errors": 0,
            "error_rate": 0.0,
            "recommendation": "OK",
        }


def suggest_threshold_adjustment(rule_id: str, current_threshold: float) -> dict:
    """Suggérer un ajustement du seuil basé sur feedback"""
    feedback = analyze_feedback(rule_id)
    
    # ✅ VÉRIFIER QUE feedback est valide
    if not feedback or feedback.get("error_rate") is None:
        return {
            "rule_id": rule_id,
            "current_threshold": current_threshold,
            "suggested_threshold": current_threshold,
            "direction": "STABLE",
            "confidence": 0.0,
        }
    
    error_rate = feedback.get("error_rate", 0.0)
    
    if error_rate > 0.15:
        new_threshold = current_threshold * 1.1
        direction = "INCREASE"
    elif error_rate < 0.05:
        new_threshold = current_threshold * 0.9
        direction = "DECREASE"
    else:
        new_threshold = current_threshold
        direction = "STABLE"
    
    return {
        "rule_id": rule_id,
        "current_threshold": current_threshold,
        "suggested_threshold": round(new_threshold, 2),
        "direction": direction,
        "confidence": error_rate,
    }


if __name__ == "__main__":
    # Test
    result = analyze_feedback("R001")
    print(result)