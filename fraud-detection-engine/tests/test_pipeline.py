"""
test_pipeline.py — Tests complets du moteur de fraude v2.0
Tests Rule Engine + Feedback Processor + Pipeline Integration
"""

import sys
import json
import sqlite3
from pathlib import Path
from datetime import datetime

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.core.rule_engine import RuleEngine
from app.core.feedback_processor import analyze_feedback, suggest_threshold_adjustment


class TestColors:
    """ANSI colors pour terminal"""
    GREEN = '\033[92m'
    RED = '\033[91m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    END = '\033[0m'
    BOLD = '\033[1m'


def print_header(text):
    """Imprimer un header"""
    print(f"\n{TestColors.BLUE}{TestColors.BOLD}{'='*70}")
    print(f"{text.center(70)}")
    print(f"{'='*70}{TestColors.END}\n")


def print_success(text):
    """Imprimer succès"""
    print(f"{TestColors.GREEN}✅ {text}{TestColors.END}")


def print_error(text):
    """Imprimer erreur"""
    print(f"{TestColors.RED}❌ {text}{TestColors.END}")


def print_warning(text):
    """Imprimer warning"""
    print(f"{TestColors.YELLOW}⚠️  {text}{TestColors.END}")


def print_info(text):
    """Imprimer info"""
    print(f"{TestColors.BLUE}ℹ️  {text}{TestColors.END}")


# ═══════════════════════════════════════════════════════════════════════════════
# TEST 1 : Intégrité de la Base de Données
# ═══════════════════════════════════════════════════════════════════════════════

def test_database_integrity():
    """Tester l'intégrité de la base de données"""
    print_header("TEST 1 : INTÉGRITÉ BASE DE DONNÉES")
    
    try:
        # Chemin DB
        db_path = Path(__file__).parent.parent / "rules" / "rules_history.db"
        
        if not db_path.exists():
            print_error(f"Base de données NOT found : {db_path}")
            return False
        
        print_success(f"Base de données existe : {db_path}")
        
        # Vérifier les tables
        conn = sqlite3.connect(str(db_path))
        cursor = conn.cursor()
        
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = cursor.fetchall()
        
        if len(tables) < 2:
            print_error(f"Seulement {len(tables)} table(s) trouvée(s), attendu 2+")
            return False
        
        print_success(f"Tables créées : {len(tables)}")
        for table in tables:
            print_info(f"  - {table[0]}")
        
        # Vérifier rules_history table
        cursor.execute("PRAGMA table_info(rules_history)")
        cols = cursor.fetchall()
        required_cols = ['id', 'version', 'rule_id', 'timestamp']
        found_cols = [col[1] for col in cols]
        
        for req in required_cols:
            if req not in found_cols:
                print_error(f"Colonne manquante dans rules_history : {req}")
                return False
        
        print_success(f"rules_history a {len(cols)} colonnes (OK)")
        
        # Vérifier feedback_log table
        cursor.execute("PRAGMA table_info(feedback_log)")
        cols = cursor.fetchall()
        required_cols = ['id', 'beneficiary_id', 'rule_id', 'original_decision', 'agent_decision']
        found_cols = [col[1] for col in cols]
        
        for req in required_cols:
            if req not in found_cols:
                print_error(f"Colonne manquante dans feedback_log : {req}")
                return False
        
        print_success(f"feedback_log a {len(cols)} colonnes (OK)")
        
        conn.close()
        print_success("TEST 1 RÉUSSI")
        return True
        
    except Exception as e:
        print_error(f"TEST 1 ÉCHOUÉ : {e}")
        return False


# ═══════════════════════════════════════════════════════════════════════════════
# TEST 2 : Validation Règles JSON
# ═══════════════════════════════════════════════════════════════════════════════

def test_rules_json_validation():
    """Tester la validation des règles JSON"""
    print_header("TEST 2 : VALIDATION RÈGLES JSON")
    
    try:
        rules_path = Path(__file__).parent.parent / "rules" / "fraud_rules.json"
        
        if not rules_path.exists():
            print_error(f"Fichier rules JSON NOT found : {rules_path}")
            return False
        
        with open(rules_path) as f:
            payload = json.load(f)
        
        # Vérifier version
        version = payload.get('version')
        if version != "2.0":
            print_warning(f"Version attendue : 2.0, trouvée : {version}")
        else:
            print_success(f"Version : {version}")
        
        # Vérifier règles
        rules = payload.get('rules', [])
        if len(rules) == 0:
            print_error("Aucune règle trouvée dans le JSON")
            return False
        
        print_success(f"Nombre de règles : {len(rules)}")
        
        # Vérifier champs nouveaux (v2.0)
        adaptive_count = 0
        for rule in rules:
            if 'adaptive' in rule:
                adaptive_count += 1
            else:
                print_warning(f"Règle {rule.get('id')} manque le champ 'adaptive'")
        
        print_success(f"Règles avec 'adaptive' : {adaptive_count}/{len(rules)}")
        
        # Vérifier seuils adaptatifs
        threshold_rules = []
        for rule in rules:
            if rule.get('adaptive'):
                if 'min_threshold' in rule and 'max_threshold' in rule:
                    threshold_rules.append(rule['id'])
        
        print_success(f"Règles avec seuils adaptatifs : {len(threshold_rules)}")
        
        print_success("TEST 2 RÉUSSI")
        return True
        
    except Exception as e:
        print_error(f"TEST 2 ÉCHOUÉ : {e}")
        return False


# ═══════════════════════════════════════════════════════════════════════════════
# TEST 3 : Rule Engine v2.0
# ═══════════════════════════════════════════════════════════════════════════════

def test_rule_engine_v2():
    """Tester Rule Engine v2.0"""
    print_header("TEST 3 : RULE ENGINE v2.0")
    
    try:
        engine = RuleEngine()
        
        # Vérifier version
        if engine.current_version != "2.0":
            print_error(f"Version attendue : 2.0, trouvée : {engine.current_version}")
            return False
        
        print_success(f"Version des règles : {engine.current_version}")
        print_success(f"Nombre de règles actives : {engine.active_rule_count}")
        print_success(f"DB path existe : {engine._db_path.exists()}")
        
        # TEST 3.1 : Cas Légitime
        print_info("\n--- Test cas LÉGITIME ---")
        legit = {
            "beneficiary_id": 1001,
            "nb_programs": 1,
            "shared_account_count": 0,
            "shared_phone_count": 0,
            "payment_gap_ratio": 0.05,
            "network_risk": 0.0,
            "pmt_score": 0.8,
            "household_size": 3,
            "income_per_person": 500,
        }
        
        result = engine.evaluate_one(legit)
        
        print(f"  Rule Score : {result.rule_score}")
        print(f"  Risk Level : {result.risk_level}")
        print(f"  Rules Version : {result.rules_version}")
        
        if result.risk_level == "LOW" and result.rule_score == 0.0:
            print_success("Cas légitime : CORRECT (LOW)")
        else:
            print_error(f"Cas légitime : INCORRECT (attendu LOW, trouvé {result.risk_level})")
            return False
        
        # TEST 3.2 : Cas Frauduleux
        print_info("\n--- Test cas FRAUDULEUX ---")
        fraud = {
            "beneficiary_id": 1002,
            "nb_programs": 5,
            "shared_account_count": 3,
            "shared_phone_count": 3,
            "payment_gap_ratio": 0.6,
            "network_risk": 0.8,
            "pmt_score": 0.15,
            "household_size": 10,
            "income_per_person": 28,
        }
        
        result = engine.evaluate_one(fraud)
        
        print(f"  Rule Score : {result.rule_score}")
        print(f"  Risk Level : {result.risk_level}")
        
        if result.risk_level in ["HIGH", "CRITICAL"] and result.rule_score > 0.5:
            print_success(f"Cas frauduleux : CORRECT ({result.risk_level})")
        else:
            print_error(f"Cas frauduleux : INCORRECT (attendu HIGH/CRITICAL, trouvé {result.risk_level})")
            return False
        
        # TEST 3.3 : Cas Borderline
        print_info("\n--- Test cas BORDERLINE ---")
        borderline = {
            "beneficiary_id": 1003,
            "nb_programs": 2,
            "shared_account_count": 1,
            "shared_phone_count": 1,
            "payment_gap_ratio": 0.3,
            "network_risk": 0.35,
            "pmt_score": 0.5,
            "household_size": 5,
            "income_per_person": 150,
        }
        
        result = engine.evaluate_one(borderline)
        
        print(f"  Rule Score : {result.rule_score}")
        print(f"  Risk Level : {result.risk_level}")
        
        if result.risk_level == "MEDIUM":
            print_success("Cas borderline : CORRECT (MEDIUM)")
        else:
            print_warning(f"Cas borderline : attendu MEDIUM, trouvé {result.risk_level}")
        
        print_success("TEST 3 RÉUSSI")
        return True
        
    except Exception as e:
        print_error(f"TEST 3 ÉCHOUÉ : {e}")
        import traceback
        traceback.print_exc()
        return False


# ═══════════════════════════════════════════════════════════════════════════════
# TEST 4 : Feedback Processor
# ═══════════════════════════════════════════════════════════════════════════════

def test_feedback_processor():
    """Tester Feedback Processor"""
    print_header("TEST 4 : FEEDBACK PROCESSOR")
    
    try:
        engine = RuleEngine()
        
        # Enregistrer des feedbacks
        print_info("--- Enregistrement des feedbacks ---")
        for i in range(5):
            engine.log_feedback(
                beneficiary_id=2000 + i,
                rule_id="R001",
                original_decision="BLOCK",
                agent_decision="ALLOW",
                notes=f"Feedback {i+1} — Faux positif"
            )
        
        print_success(f"5 feedbacks enregistrés pour R001")
        
        # Analyser les feedbacks
        print_info("\n--- Analyse des feedbacks ---")
        analysis = analyze_feedback("R001")
        
        print(f"  Total feedback : {analysis.get('total_feedback')}")
        print(f"  Erreurs : {analysis.get('errors')}")
        print(f"  Error Rate : {analysis.get('error_rate'):.1%}")
        print(f"  Recommendation : {analysis.get('recommendation')}")
        
        if analysis.get('error_rate', 0) > 0.15:
            expected = "RECALIBRATE"
        else:
            expected = "OK"
        
        if analysis.get('recommendation') == expected:
            print_success(f"Recommendation correcte : {expected}")
        else:
            print_error(f"Recommendation incorrecte : attendu {expected}, trouvé {analysis.get('recommendation')}")
            return False
        
        # Suggérer ajustement
        print_info("\n--- Suggestion d'ajustement ---")
        adjustment = suggest_threshold_adjustment("R001", current_threshold=2.0)
        
        print(f"  Current threshold : {adjustment.get('current_threshold')}")
        print(f"  Suggested threshold : {adjustment.get('suggested_threshold')}")
        print(f"  Direction : {adjustment.get('direction')}")
        
        if adjustment.get('direction') not in ['INCREASE', 'DECREASE', 'STABLE']:
            print_error(f"Direction invalide : {adjustment.get('direction')}")
            return False
        
        print_success("Ajustement valide")
        
        # Appliquer l'ajustement
        print_info("\n--- Application de l'ajustement ---")
        engine.update_threshold(
            rule_id="R001",
            new_threshold=adjustment.get('suggested_threshold'),
            reason="Test feedback loop"
        )
        
        if "R001" in engine.adaptive_thresholds:
            print_success(f"Seuil appliqué : {engine.adaptive_thresholds['R001']}")
        else:
            print_error("Seuil non appliqué dans adaptive_thresholds")
            return False
        
        print_success("TEST 4 RÉUSSI")
        return True
        
    except Exception as e:
        print_error(f"TEST 4 ÉCHOUÉ : {e}")
        import traceback
        traceback.print_exc()
        return False


# ═══════════════════════════════════════════════════════════════════════════════
# TEST 5 : Cohérence et Versionning
# ═══════════════════════════════════════════════════════════════════════════════

def test_consistency_and_versioning():
    """Tester cohérence et versionning"""
    print_header("TEST 5 : COHÉRENCE ET VERSIONNING")
    
    try:
        engine = RuleEngine()
        
        print_info("--- Évaluation multiple ---")
        for i in range(3):
            result = engine.evaluate_one({"beneficiary_id": 3000 + i})
            
            if result.rules_version != "2.0":
                print_error(f"Version mismatch : {result.rules_version}")
                return False
            
            if not isinstance(result.feedback_applied, bool):
                print_error("feedback_applied doit être booléen")
                return False
        
        print_success("Tous les résultats ont rules_version = 2.0")
        print_success("Tous les résultats ont feedback_applied valide")
        
        # Vérifier la DB
        print_info("\n--- Vérification DB ---")
        conn = sqlite3.connect(str(engine._db_path))
        cursor = conn.cursor()
        
        cursor.execute("SELECT COUNT(*) FROM feedback_log")
        count = cursor.fetchone()[0]
        print_success(f"Feedbacks en DB : {count}")
        
        cursor.execute("SELECT COUNT(*) FROM rules_history")
        count = cursor.fetchone()[0]
        print_success(f"Historique en DB : {count}")
        
        conn.close()
        
        print_success("TEST 5 RÉUSSI")
        return True
        
    except Exception as e:
        print_error(f"TEST 5 ÉCHOUÉ : {e}")
        import traceback
        traceback.print_exc()
        return False


# ═══════════════════════════════════════════════════════════════════════════════
# TEST 6 : Performance
# ═══════════════════════════════════════════════════════════════════════════════

def test_performance():
    """Tester performance"""
    print_header("TEST 6 : PERFORMANCE")
    
    try:
        import time
        engine = RuleEngine()
        
        test_case = {
            "beneficiary_id": 1,
            "nb_programs": 2,
            "shared_account_count": 0,
            "shared_phone_count": 0,
        }
        
        print_info("--- Évaluation 100 cas ---")
        start = time.time()
        for i in range(100):
            test_case["beneficiary_id"] = i
            engine.evaluate_one(test_case)
        elapsed = time.time() - start
        
        avg_ms = (elapsed / 100) * 1000
        print(f"  Temps total : {elapsed:.3f} sec")
        print(f"  Temps moyen : {avg_ms:.2f} ms/cas")
        print(f"  Throughput : {100/elapsed:.0f} cas/sec")
        
        if avg_ms < 10:
            print_success(f"Performance OK (< 10 ms) : {avg_ms:.2f} ms")
        elif avg_ms < 50:
            print_warning(f"Performance acceptable (10-50 ms) : {avg_ms:.2f} ms")
        else:
            print_error(f"Performance faible (> 50 ms) : {avg_ms:.2f} ms")
            return False
        
        print_success("TEST 6 RÉUSSI")
        return True
        
    except Exception as e:
        print_error(f"TEST 6 ÉCHOUÉ : {e}")
        return False


# ═══════════════════════════════════════════════════════════════════════════════
# TEST 7 : Imports et Dépendances
# ═══════════════════════════════════════════════════════════════════════════════

def test_imports_and_dependencies():
    """Tester imports et dépendances"""
    print_header("TEST 7 : IMPORTS ET DÉPENDANCES")
    
    try:
        print_info("--- Import rule_engine ---")
        from app.core.rule_engine import RuleEngine, RuleResult, TriggeredRule
        print_success("RuleEngine importé")
        print_success("RuleResult importé")
        print_success("TriggeredRule importé")
        
        print_info("\n--- Import feedback_processor ---")
        from app.core.feedback_processor import analyze_feedback, suggest_threshold_adjustment
        print_success("analyze_feedback importé")
        print_success("suggest_threshold_adjustment importé")
        
        print_info("\n--- Imports de dépendances ---")
        import sqlite3
        import json
        import pandas as pd
        print_success("sqlite3 OK")
        print_success("json OK")
        print_success("pandas OK")
        
        print_success("TEST 7 RÉUSSI")
        return True
        
    except ImportError as e:
        print_error(f"TEST 7 ÉCHOUÉ — Erreur d'import : {e}")
        return False


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    """Exécuter tous les tests"""
    print("\n")
    print(f"{TestColors.BOLD}{TestColors.BLUE}")
    print("╔" + "═"*68 + "╗")
    print("║" + " "*15 + "SUITE DE TESTS — RULE ENGINE v2.0" + " "*19 + "║")
    print("╚" + "═"*68 + "╝")
    print(f"{TestColors.END}\n")
    
    tests = [
        ("Intégrité BD", test_database_integrity),
        ("Validation JSON", test_rules_json_validation),
        ("Rule Engine v2.0", test_rule_engine_v2),
        ("Feedback Processor", test_feedback_processor),
        ("Cohérence", test_consistency_and_versioning),
        ("Performance", test_performance),
        ("Imports", test_imports_and_dependencies),
    ]
    
    results = []
    for test_name, test_func in tests:
        try:
            result = test_func()
            results.append((test_name, result))
        except Exception as e:
            print_error(f"Erreur non capturée dans {test_name} : {e}")
            results.append((test_name, False))
    
    # Résumé final
    print("\n")
    print(f"{TestColors.BOLD}{TestColors.BLUE}")
    print("╔" + "═"*68 + "╗")
    print("║" + " "*20 + "RÉSUMÉ FINAL" + " "*36 + "║")
    print("╚" + "═"*68 + "╝")
    print(f"{TestColors.END}\n")
    
    all_pass = True
    for test_name, result in results:
        status = "✅" if result else "❌"
        print(f"{status} {test_name}")
        all_pass = all_pass and result
    
    print("\n" + "="*70)
    if all_pass:
        print(f"{TestColors.GREEN}{TestColors.BOLD}🎉 TOUS LES TESTS RÉUSSIS — SYSTÈME FIABLE ET PRÊT{TestColors.END}")
    else:
        print(f"{TestColors.RED}{TestColors.BOLD}⚠️ CERTAINS TESTS ONT ÉCHOUÉ — VÉRIFIER LES DÉTAILS{TestColors.END}")
    print("="*70 + "\n")
    
    return 0 if all_pass else 1


if __name__ == "__main__":
    exit(main())