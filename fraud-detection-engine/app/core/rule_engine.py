"""
rule_engine.py — Version 2.0
Moteur de règles avec apprentissage dynamique et versionning
"""

import os
import ast
import json
import logging
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime
import sqlite3

import pandas as pd

logger = logging.getLogger(__name__)

_current_dir = Path(__file__).parent.parent.parent  # Remonte à fraud-detection-engine/
DEFAULT_RULES_PATH = _current_dir / "rules" / "fraud_rules.json"
RULES_HISTORY_DB = _current_dir / "rules" / "rules_history.db"

# En cas de Docker, utiliser /app
if not DEFAULT_RULES_PATH.exists():
    DEFAULT_RULES_PATH = Path("/app/fraud-detection-engine/rules/fraud_rules.json")
if not RULES_HISTORY_DB.exists():
    RULES_HISTORY_DB = Path("/app/fraud-detection-engine/rules/rules_history.db")


@dataclass
class TriggeredRule:
    rule_id: str
    name: str
    flag: str
    severity: str
    weight: float
    condition: str
    threshold_used: float = 0.0  # NOUVEAU : tracer le seuil utilisé


@dataclass
class RuleResult:
    beneficiary_id: Any
    rule_score: float
    risk_level: str
    triggered_rules: List[TriggeredRule] = field(default_factory=list)
    triggered_flags: List[str] = field(default_factory=list)
    explanations: List[str] = field(default_factory=list)
    pass_to_ml: bool = True
    eval_time_ms: float = 0.0
    rules_version: str = "1.0"  # NOUVEAU : version des règles utilisées
    feedback_applied: bool = False  # NOUVEAU : feedback a-t-il changé les seuils?

    def to_dict(self) -> dict:
        return {
            "beneficiary_id": self.beneficiary_id,
            "rule_score": self.rule_score,
            "risk_level": self.risk_level,
            "triggered_rules": [
                {
                    "rule_id": r.rule_id,
                    "name": r.name,
                    "flag": r.flag,
                    "severity": r.severity,
                    "weight": r.weight,
                    "condition": r.condition,
                    "threshold_used": r.threshold_used,  # NOUVEAU
                }
                for r in self.triggered_rules
            ],
            "triggered_flags": self.triggered_flags,
            "explanations": self.explanations,
            "pass_to_ml": self.pass_to_ml,
            "eval_time_ms": self.eval_time_ms,
            "rules_version": self.rules_version,  # NOUVEAU
            "feedback_applied": self.feedback_applied,  # NOUVEAU
        }


class SafeExpressionEvaluator(ast.NodeVisitor):
    """Même chose qu'avant — pas de changement"""
    ALLOWED_NODES = (
        ast.Expression, ast.BoolOp, ast.BinOp, ast.UnaryOp, ast.Compare,
        ast.Name, ast.Load, ast.Constant,
        ast.And, ast.Or, ast.Not,
        ast.Eq, ast.NotEq, ast.Gt, ast.GtE, ast.Lt, ast.LtE,
        ast.Add, ast.Sub, ast.Mult, ast.Div, ast.Mod, ast.Pow,
        ast.USub, ast.UAdd,
    )

    def __init__(self, context: Dict[str, Any]):
        self.context = context

    def visit(self, node: ast.AST):
        if not isinstance(node, self.ALLOWED_NODES):
            raise ValueError(f"Nœud AST non autorisé : {type(node).__name__}")
        return super().visit(node)

    def visit_Expression(self, node): return self.visit(node.body)
    def visit_Constant(self, node): return node.value
    def visit_Name(self, node): return self.context.get(node.id, 0)

    def visit_BoolOp(self, node):
        if isinstance(node.op, ast.And):
            return all(self.visit(v) for v in node.values)
        if isinstance(node.op, ast.Or):
            return any(self.visit(v) for v in node.values)
        raise ValueError("Opérateur booléen non supporté")

    def visit_UnaryOp(self, node):
        val = self.visit(node.operand)
        if isinstance(node.op, ast.Not):
            return not val
        if isinstance(node.op, ast.USub):
            return -val
        if isinstance(node.op, ast.UAdd):
            return +val
        raise ValueError("Opérateur unaire non supporté")

    def visit_BinOp(self, node):
        left = self.visit(node.left)
        right = self.visit(node.right)
        ops = {
            ast.Add: lambda a, b: a + b,
            ast.Sub: lambda a, b: a - b,
            ast.Mult: lambda a, b: a * b,
            ast.Div: lambda a, b: a / b if b != 0 else 0.0,
            ast.Mod: lambda a, b: a % b if b != 0 else 0.0,
            ast.Pow: lambda a, b: a ** b,
        }
        op_fn = ops.get(type(node.op))
        if op_fn is None:
            raise ValueError(f"Opérateur binaire non supporté : {type(node.op).__name__}")
        return op_fn(left, right)

    def visit_Compare(self, node):
        left = self.visit(node.left)
        for op, comp in zip(node.ops, node.comparators):
            right = self.visit(comp)
            ops = {
                ast.Eq: lambda a, b: a == b,
                ast.NotEq: lambda a, b: a != b,
                ast.Gt: lambda a, b: a > b,
                ast.GtE: lambda a, b: a >= b,
                ast.Lt: lambda a, b: a < b,
                ast.LtE: lambda a, b: a <= b,
            }
            fn = ops.get(type(op))
            if fn is None:
                raise ValueError(f"Opérateur de comparaison non supporté : {type(op).__name__}")
            if not fn(left, right):
                return False
            left = right
        return True


class RuleEngine:
    """Version 2.0 avec apprentissage dynamique"""

    FEATURE_DEFAULTS: Dict[str, Any] = {
        "partner_id": None,
        "beneficiary_id": None,
        "age": 35,
        "income": 0.0,
        "household_size": 1,
        "nb_children": 0,
        "nb_elderly": 0,
        "nb_programs": 0,
        "nb_active_programs": 0,
        "pmt_score": 1.0,
        "pmt_score_min": 1.0,
        "payment_gap_ratio": 0.0,
        "gap_ratio": 0.0,
        "shared_phone_count": 0,
        "shared_account_count": 0,
        "network_risk_score": 0.0,
        "network_risk": 0.0,
        "high_amount_flag": 0,
        "dependency_ratio": 0.0,
        "income_per_person": 999_999.0,
        "single_head": 0,
        "has_disabled": 0,
        "income_program_inconsistency": 0,
    }

    def __init__(self, rules_path: Optional[Path] = None):
        self._rules_path = Path(rules_path) if rules_path else DEFAULT_RULES_PATH
        self._db_path = RULES_HISTORY_DB
        self.rules: List[dict] = []
        self._load_time: float = 0.0
        self.current_version: str = "1.0"
        self.adaptive_thresholds: Dict[str, float] = {}  # NOUVEAU
        
        # Initialiser la base de données
        self._init_rules_db()
        
        self.reload_rules()
        self._load_adaptive_thresholds()  # NOUVEAU

    def _init_rules_db(self):
        """NOUVEAU : Initialiser la base de données d'historique"""
        try:
            conn = sqlite3.connect(str(self._db_path))
            cursor = conn.cursor()
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS rules_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    version TEXT NOT NULL,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                    rule_id TEXT NOT NULL,
                    rule_json TEXT,
                    threshold_value FLOAT,
                    change_reason TEXT
                )
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS rule_performance (
                    rule_id TEXT PRIMARY KEY,
                    true_positive INTEGER DEFAULT 1,
                    false_positive INTEGER DEFAULT 1
                )
            """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS feedback_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    beneficiary_id INTEGER,
                    rule_id TEXT,
                    original_decision TEXT,
                    agent_decision TEXT,
                    feedback_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                    notes TEXT
                )
            """)
            
            conn.commit()
            conn.close()
        except Exception as e:
            logger.warning(f"Erreur initialisation DB rules : {e}")

    def reload_rules(self) -> None:
        """Charger les règles + versionning"""
        if not self._rules_path.exists():
            logger.error(f"Fichier de règles introuvable : {self._rules_path}")
            self.rules = []
            return

        with open(self._rules_path, encoding="utf-8") as f:
            payload = json.load(f)

        self.rules = [r for r in payload.get("rules", []) if r.get("enabled", True)]
        self.current_version = payload.get("_meta", {}).get("version", "1.0")  
        self._load_time = time.time()
        
        logger.info(f"✅ {len(self.rules)} règles chargées (v{self.current_version})")

    def _load_adaptive_thresholds(self):
        """NOUVEAU : Charger les seuils adaptatifs depuis la DB"""
        try:
            conn = sqlite3.connect(str(self._db_path))
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT rule_id, threshold_value 
                FROM rules_history 
                WHERE threshold_value IS NOT NULL
                ORDER BY timestamp DESC
            """)
            
            rows = cursor.fetchall()
            for rule_id, threshold in rows:
                if rule_id not in self.adaptive_thresholds:
                    self.adaptive_thresholds[rule_id] = threshold
            
            conn.close()
        except Exception as e:
            logger.warning(f"Erreur chargement seuils adaptatifs : {e}")

    def log_feedback(self, beneficiary_id: int, rule_id: str, 
                     original_decision: str, agent_decision: str, notes: str = ""):
        """NOUVEAU : Enregistrer le feedback des agents"""
        try:
            conn = sqlite3.connect(str(self._db_path))
            cursor = conn.cursor()
            
            cursor.execute("""
                INSERT INTO feedback_log 
                (beneficiary_id, rule_id, original_decision, agent_decision, notes)
                VALUES (?, ?, ?, ?, ?)
            """, (beneficiary_id, rule_id, original_decision, agent_decision, notes))
            
            conn.commit()
            conn.close()
            
            logger.info(f"Feedback enregistré : {rule_id} pour {beneficiary_id}")
        except Exception as e:
            logger.warning(f"Erreur enregistrement feedback : {e}")

    def update_threshold(self, rule_id: str, new_threshold: float, reason: str = ""):
        """NOUVEAU : Mettre à jour un seuil adaptatif"""
        try:
            conn = sqlite3.connect(str(self._db_path))
            cursor = conn.cursor()
            
            # Récupérer le JSON de la règle si elle existe
            rule_json = None
            for rule in self.rules:
                if rule.get("id") == rule_id:
                    rule_json = json.dumps(rule)
                    break
            
            cursor.execute("""
                INSERT INTO rules_history 
                (version, rule_id, rule_json, threshold_value, change_reason)
                VALUES (?, ?, ?, ?, ?)
            """, (self.current_version, rule_id, rule_json, new_threshold, reason))
            
            conn.commit()
            conn.close()
            
            self.adaptive_thresholds[rule_id] = new_threshold
            logger.info(f"Seuil mis à jour : {rule_id} = {new_threshold}")
        except Exception as e:
            logger.warning(f"Erreur mise à jour seuil : {e}")

    @property
    def active_rule_count(self) -> int:
        return len(self.rules)

    def _build_context(self, row: Dict[str, Any]) -> Dict[str, Any]:
        """Même logique qu'avant"""
        ctx = self.FEATURE_DEFAULTS.copy()

        for k, v in row.items():
            if v is not None:
                ctx[k] = v

        if ctx.get("payment_gap_ratio", 0) and not ctx.get("gap_ratio", 0):
            ctx["gap_ratio"] = ctx["payment_gap_ratio"]
        if ctx.get("gap_ratio", 0) and not ctx.get("payment_gap_ratio", 0):
            ctx["payment_gap_ratio"] = ctx["gap_ratio"]

        if ctx.get("network_risk_score", 0) and not ctx.get("network_risk", 0):
            ctx["network_risk"] = ctx["network_risk_score"]
        if ctx.get("network_risk", 0) and not ctx.get("network_risk_score", 0):
            ctx["network_risk_score"] = ctx["network_risk"]

        return ctx
    
    
    def _safe_eval(self, condition: str, context: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        """Même logique qu'avant"""
        try:
            tree = ast.parse(condition, mode="eval")
            evaluator = SafeExpressionEvaluator(context)
            result = evaluator.visit(tree)
            return bool(result), None
        except ZeroDivisionError:
            return False, "ZeroDivisionError"
        except Exception as e:
            logger.debug(f"Erreur évaluation règle '{condition}': {e}")
            return False, str(e)

    @staticmethod
    def _score_to_level(score: float) -> str:
        """Même logique qu'avant"""
        if score >= 0.80:
            return "CRITICAL"
        if score >= 0.55:
            return "HIGH"
        if score >= 0.30:
            return "MEDIUM"
        return "LOW"
    
    def _get_rule_precision(self, rule_id: str) -> float:
        try:
            conn = sqlite3.connect(str(self._db_path))
            cursor = conn.cursor()

            cursor.execute("""
                SELECT true_positive, false_positive
                FROM rule_performance
                WHERE rule_id = ?
            """, (rule_id,))

            row = cursor.fetchone()
            conn.close()

            if row:
                tp, fp = row
                return tp / (tp + fp + 1e-6)

        except:
            pass

        return 0.5  # défaut neutre

    def evaluate_one(self, row: Dict[str, Any]) -> RuleResult:
        """Évaluation avec tracking du seuil utilisé"""
        t0 = time.perf_counter()
        ctx = self._build_context(row)

        triggered: List[TriggeredRule] = []
        total_weight: float = 0.0
        explanations: List[str] = []
        feedback_applied_flag = False

        for rule in self.rules:
            matched, err = self._safe_eval(rule["condition"], ctx)

            if err:
                logger.warning(f"Règle {rule.get('id', '?')} - erreur : {err}")
                continue

            if matched:
                # Créer TriggeredRule
                tr = TriggeredRule(
                    rule_id=rule["id"],
                    name=rule["name"],
                    flag=rule["flag"],
                    severity=rule["severity"],
                    weight=float(rule["weight"]),
                    condition=rule["condition"],
                    threshold_used=self.adaptive_thresholds.get(rule["id"], 0.0),
                )
                triggered.append(tr)
                total_weight += tr.weight
                explanations.append(
                    f"[{rule['id']}] {rule['flag']} — {rule['condition']} "
                    f"(poids={rule['weight']}, sévérité={rule['severity']})"
                )

        rule_score = round(min(total_weight, 1.0), 4)
        risk_level = self._score_to_level(rule_score)
        eval_ms = round((time.perf_counter() - t0) * 1000, 2)

        return RuleResult(
            beneficiary_id=row.get("beneficiary_id") or row.get("partner_id") or row.get("partner_idx"),
            rule_score=rule_score,
            risk_level=risk_level,
            triggered_rules=triggered,
            triggered_flags=[r.flag for r in triggered],
            explanations=explanations,
            pass_to_ml=rule_score < 0.80,
            eval_time_ms=eval_ms,
            rules_version=self.current_version,
            feedback_applied=feedback_applied_flag,
        )
    def evaluate_df(self, df: pd.DataFrame) -> pd.DataFrame:
        """Même logique qu'avant + colonnes nouvelles"""
        results = []
        for _, row in df.iterrows():
            r = self.evaluate_one(row.to_dict())
            results.append({
                "beneficiary_id": r.beneficiary_id,
                "rule_score": r.rule_score,
                "risk_level": r.risk_level,
                "triggered_flags": "|".join(r.triggered_flags),
                "n_rules_triggered": len(r.triggered_rules),
                "pass_to_ml": r.pass_to_ml,
                "eval_time_ms": r.eval_time_ms,
                "rules_version": r.rules_version,  # NOUVEAU
                "feedback_applied": r.feedback_applied,  # NOUVEAU
            })
        return pd.DataFrame(results)

    def get_rules_summary(self) -> List[dict]:
        """Résumé des règles + seuils adaptatifs"""
        return [
            {
                "id": r["id"],
                "name": r["name"],
                "flag": r["flag"],
                "severity": r["severity"],
                "weight": r["weight"],
                "condition": r["condition"],
                "adaptive_threshold": self.adaptive_thresholds.get(r["id"], None),  # NOUVEAU
            }
            for r in self.rules
        ]

    def validate_rules(self) -> List[dict]:
        """Même logique qu'avant"""
        errors = []
        dummy_ctx = {k: 0 for k in self.FEATURE_DEFAULTS}
        for rule in self.rules:
            _, err = self._safe_eval(rule["condition"], dummy_ctx)
            if err:
                errors.append({
                    "rule_id": rule["id"],
                    "condition": rule["condition"],
                    "error": err,
                })
        return errors


if __name__ == "__main__":
    engine = RuleEngine()
    print(f"\n✅ {engine.active_rule_count} règles chargées (v{engine.current_version})")
    print(f"[RuleEngine] Loading rules from: {engine._rules_path}")

    errors = engine.validate_rules()
    if errors:
        print(f"⚠️  {len(errors)} règle(s) invalide(s) :")
        for e in errors:
            print(f"   {e}")
    else:
        print("✅ Toutes les règles sont syntaxiquement valides")