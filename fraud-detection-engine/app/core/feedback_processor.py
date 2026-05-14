"""
feedback_processor.py — Traiter les feedback des agents
Utilisé par l'API pour recalibrer les seuils automatiquement
"""

import sqlite3
from pathlib import Path
from collections import Counter
import logging

logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parents[2]  # fraud-detection-engine/
RULES_DB = BASE_DIR / "rules" / "rules_history.db"

if not RULES_DB.exists():
    RULES_DB = Path("/app/rules/rules_history.db")


def analyze_feedback(rule_id: str, last_n_days: int = 30) -> dict:
    """Analyser les feedback pour un règle spécifique"""
    try:
        conn = sqlite3.connect(str(RULES_DB))
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT original_decision, agent_decision, COUNT(*) as count
            FROM feedback_log
            WHERE rule_id = ?
            AND datetime(feedback_timestamp) >= datetime('now', '-' || ? || ' days')
            GROUP BY original_decision, agent_decision
        """, (rule_id, last_n_days))
        
        results = cursor.fetchall()
        conn.close()
        
        # Calculer le taux d'erreur
        total = sum(r[2] for r in results)
        errors = sum(r[2] for r in results if r[0] != r[1])
        
        error_rate = errors / total if total > 0 else 0
        
        return {
            "rule_id": rule_id,
            "total_feedback": total,
            "errors": errors,
            "error_rate": error_rate,
            "recommendation": "RECALIBRATE" if error_rate > 0.15 else "OK",
        }
    
    except Exception as e:
        logger.error(f"Erreur analyse feedback : {e}")
        return {}


def suggest_threshold_adjustment(rule_id: str, current_threshold: float) -> dict:
    """Suggérer un ajustement du seuil basé sur feedback"""
    feedback = analyze_feedback(rule_id)
    
    if feedback.get("error_rate", 0) > 0.15:
        # Si trop de faux positifs, augmenter le seuil (être moins strict)
        new_threshold = current_threshold * 1.1
        direction = "INCREASE"
    elif feedback.get("error_rate", 0) < 0.05:
        # Si trop de faux négatifs, diminuer le seuil (être plus strict)
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
        "confidence": feedback.get("error_rate", 0),
    }


if __name__ == "__main__":
    # Test
    result = analyze_feedback("R001")
    print(result)