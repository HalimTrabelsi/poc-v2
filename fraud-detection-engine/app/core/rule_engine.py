"""
=============================================================
rule_engine.py  —  Moteur de règles métier v2
=============================================================
Évalue un bénéficiaire à travers un ensemble de règles JSON.
Chaque règle a : id, name, condition, weight, severity, flag.

Architecture :
  ┌──────────────┐
  │  JSON Rules  │  ← fraud_rules.json (hot-reload possible)
  └──────┬───────┘
         │
  ┌──────▼──────────────────────┐
  │  SafeExpressionEvaluator    │  ← AST sécurisé (pas d'eval())
  └──────┬──────────────────────┘
         │
  ┌──────▼──────────────────────┐
  │  RuleEngine.evaluate_one()  │  → RuleResult (score, flags, ...)
  └─────────────────────────────┘

Score agrégé :
  rule_score = Σ(weight_i) pour chaque règle déclenchée, capped à 1.0
  
Niveau de risque :
  CRITICAL ≥ 0.80
  HIGH     ≥ 0.55
  MEDIUM   ≥ 0.30
  LOW      < 0.30
=============================================================
"""

import ast
import json
import logging
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

logger = logging.getLogger(__name__)

# Chemin par défaut des règles
DEFAULT_RULES_PATH = Path(__file__).resolve().parents[1] / "rules" / "fraud_rules.json"


# ════════════════════════════════════════════════════════════
# TYPES
# ════════════════════════════════════════════════════════════

@dataclass
class TriggeredRule:
    rule_id   : str
    name      : str
    flag      : str
    severity  : str
    weight    : float
    condition : str


@dataclass
class RuleResult:
    beneficiary_id  : Any
    rule_score      : float
    risk_level      : str
    triggered_rules : List[TriggeredRule] = field(default_factory=list)
    triggered_flags : List[str]           = field(default_factory=list)
    explanations    : List[str]           = field(default_factory=list)
    pass_to_ml      : bool                = True
    eval_time_ms    : float               = 0.0

    def to_dict(self) -> dict:
        return {
            "beneficiary_id" : self.beneficiary_id,
            "rule_score"     : self.rule_score,
            "risk_level"     : self.risk_level,
            "triggered_rules": [
                {
                    "rule_id"  : r.rule_id,
                    "name"     : r.name,
                    "flag"     : r.flag,
                    "severity" : r.severity,
                    "weight"   : r.weight,
                    "condition": r.condition,
                }
                for r in self.triggered_rules
            ],
            "triggered_flags": self.triggered_flags,
            "explanations"   : self.explanations,
            "pass_to_ml"     : self.pass_to_ml,
            "eval_time_ms"   : self.eval_time_ms,
        }


# ════════════════════════════════════════════════════════════
# ÉVALUATEUR D'EXPRESSIONS SÉCURISÉ
# ════════════════════════════════════════════════════════════

class SafeExpressionEvaluator(ast.NodeVisitor):
    """
    Évalue une expression Python de façon sécurisée via l'AST.
    N'utilise PAS eval() → pas d'injection possible.
    
    Supporte : comparaisons, opérateurs booléens/arithmétiques, constantes, noms.
    """

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

    # ── Dispatch ─────────────────────────────────────────────
    def visit(self, node: ast.AST):
        if not isinstance(node, self.ALLOWED_NODES):
            raise ValueError(f"Nœud AST non autorisé : {type(node).__name__}")
        return super().visit(node)

    def visit_Expression(self, node): return self.visit(node.body)
    def visit_Constant  (self, node): return node.value
    def visit_Name      (self, node): return self.context.get(node.id, 0)

    def visit_BoolOp(self, node):
        if isinstance(node.op, ast.And):
            return all(self.visit(v) for v in node.values)
        if isinstance(node.op, ast.Or):
            return any(self.visit(v) for v in node.values)
        raise ValueError("Opérateur booléen non supporté")

    def visit_UnaryOp(self, node):
        val = self.visit(node.operand)
        if isinstance(node.op, ast.Not):  return not val
        if isinstance(node.op, ast.USub): return -val
        if isinstance(node.op, ast.UAdd): return +val
        raise ValueError("Opérateur unaire non supporté")

    def visit_BinOp(self, node):
        left  = self.visit(node.left)
        right = self.visit(node.right)
        ops   = {
            ast.Add : lambda a, b: a + b,
            ast.Sub : lambda a, b: a - b,
            ast.Mult: lambda a, b: a * b,
            ast.Div : lambda a, b: a / b if b != 0 else 0.0,
            ast.Mod : lambda a, b: a % b if b != 0 else 0.0,
            ast.Pow : lambda a, b: a ** b,
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
                ast.Eq  : lambda a, b: a == b,
                ast.NotEq: lambda a, b: a != b,
                ast.Gt  : lambda a, b: a >  b,
                ast.GtE : lambda a, b: a >= b,
                ast.Lt  : lambda a, b: a <  b,
                ast.LtE : lambda a, b: a <= b,
            }
            fn = ops.get(type(op))
            if fn is None:
                raise ValueError(f"Opérateur de comparaison non supporté : {type(op).__name__}")
            if not fn(left, right):
                return False
            left = right
        return True


# ════════════════════════════════════════════════════════════
# MOTEUR DE RÈGLES PRINCIPAL
# ════════════════════════════════════════════════════════════

class RuleEngine:
    """
    Moteur de règles métier anti-fraude.
    
    Utilisation :
        engine = RuleEngine()                    # charge rules/fraud_rules.json
        result = engine.evaluate_one(row_dict)   # → RuleResult
        df_out = engine.evaluate_df(df)          # → DataFrame
        
    Hot-reload :
        engine.reload_rules()
    """

    # Valeurs par défaut si une feature est absente du contexte
    FEATURE_DEFAULTS: Dict[str, Any] = {
        "partner_id"          : None,
        "age"                 : 35,
        "income"              : 0.0,
        "income_log"          : 0.0,
        "household_size"      : 1,
        "nb_children"         : 0,
        "nb_elderly"          : 0,
        "nb_programs"         : 0,
        "pmt_score"           : 1.0,
        "payment_gap_ratio"   : 0.0,
        "gap_ratio"           : 0.0,       # alias
        "shared_phone_count"  : 0,
        "shared_account_count": 0,
        "network_risk_score"  : 0.0,
        "network_risk"        : 0.0,       # alias
        "high_amount_flag"    : 0,
        "dependency_ratio"    : 0.0,
        "income_per_person"   : 999_999.0,
        "is_single_head"      : 0,
        "has_disabled"        : 0,
    }

    # Seuils de risque
    RISK_THRESHOLDS: Dict[str, float] = {
        "CRITICAL": 0.80,
        "HIGH"    : 0.55,
        "MEDIUM"  : 0.30,
    }

    def __init__(self, rules_path: Optional[Path] = None):
        self._rules_path = Path(rules_path) if rules_path else DEFAULT_RULES_PATH
        self.rules: List[dict] = []
        self._load_time: float = 0.0
        self.reload_rules()

    # ── Chargement ───────────────────────────────────────────

    def reload_rules(self) -> None:
        """Recharge les règles depuis le fichier JSON (hot-reload)."""
        if not self._rules_path.exists():
            logger.error(f"Fichier de règles introuvable : {self._rules_path}")
            self.rules = []
            return

        with open(self._rules_path, encoding="utf-8") as f:
            payload = json.load(f)

        self.rules = [r for r in payload.get("rules", []) if r.get("enabled", True)]
        self._load_time = time.time()
        logger.info(f"✅ {len(self.rules)} règles chargées depuis {self._rules_path}")

    @property
    def active_rule_count(self) -> int:
        return len(self.rules)

    # ── Normalisation du contexte ────────────────────────────

    def _build_context(self, row: Dict[str, Any]) -> Dict[str, Any]:
        """
        Fusionne les valeurs du row avec les défauts.
        Gère les alias (gap_ratio ↔ payment_gap_ratio, etc.).
        """
        ctx = self.FEATURE_DEFAULTS.copy()
        # Copie propre (None → défaut)
        for k, v in row.items():
            if v is not None:
                ctx[k] = v

        # Alias : aligne les deux noms vers la même valeur
        if ctx.get("payment_gap_ratio", 0) and not ctx.get("gap_ratio", 0):
            ctx["gap_ratio"] = ctx["payment_gap_ratio"]
        if ctx.get("gap_ratio", 0) and not ctx.get("payment_gap_ratio", 0):
            ctx["payment_gap_ratio"] = ctx["gap_ratio"]

        if ctx.get("network_risk_score", 0) and not ctx.get("network_risk", 0):
            ctx["network_risk"] = ctx["network_risk_score"]
        if ctx.get("network_risk", 0) and not ctx.get("network_risk_score", 0):
            ctx["network_risk_score"] = ctx["network_risk"]

        return ctx

    # ── Évaluation d'une règle ───────────────────────────────

    def _safe_eval(self, condition: str, context: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        """
        Évalue une condition de façon sécurisée.
        Retourne (matched: bool, error: str | None).
        """
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

    # ── Score → niveau de risque ─────────────────────────────

    @staticmethod
    def _score_to_level(score: float) -> str:
        if score >= 0.80: return "CRITICAL"
        if score >= 0.55: return "HIGH"
        if score >= 0.30: return "MEDIUM"
        return "LOW"

    # ── Évaluation principale ────────────────────────────────

    def evaluate_one(self, row: Dict[str, Any]) -> RuleResult:
        """
        Évalue un bénéficiaire contre toutes les règles actives.
        
        Args:
            row: dict avec les features du bénéficiaire
            
        Returns:
            RuleResult avec score, niveau, flags déclenchés
        """
        t0  = time.perf_counter()
        ctx = self._build_context(row)

        triggered   : List[TriggeredRule] = []
        total_weight: float = 0.0
        explanations: List[str] = []

        for rule in self.rules:
            matched, err = self._safe_eval(rule["condition"], ctx)

            if err:
                logger.warning(f"Règle {rule['id']} - erreur : {err}")
                continue

            if matched:
                tr = TriggeredRule(
                    rule_id  = rule["id"],
                    name     = rule["name"],
                    flag     = rule["flag"],
                    severity = rule["severity"],
                    weight   = float(rule["weight"]),
                    condition= rule["condition"],
                )
                triggered.append(tr)
                total_weight += tr.weight
                explanations.append(
                    f"[{rule['id']}] {rule['flag']} — {rule['condition']} "
                    f"(poids={rule['weight']}, sévérité={rule['severity']})"
                )

        rule_score = round(min(total_weight, 1.0), 4)
        risk_level = self._score_to_level(rule_score)
        eval_ms    = round((time.perf_counter() - t0) * 1000, 2)

        return RuleResult(
            beneficiary_id  = row.get("partner_id") or row.get("partner_idx"),
            rule_score      = rule_score,
            risk_level      = risk_level,
            triggered_rules = triggered,
            triggered_flags = [r.flag for r in triggered],
            explanations    = explanations,
            pass_to_ml      = rule_score < 0.80,
            eval_time_ms    = eval_ms,
        )

    def evaluate_df(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Évalue un DataFrame complet.
        Retourne un DataFrame avec les colonnes :
          beneficiary_id, rule_score, risk_level,
          triggered_flags, pass_to_ml
        """
        results = []
        for _, row in df.iterrows():
            r = self.evaluate_one(row.to_dict())
            results.append({
                "beneficiary_id"  : r.beneficiary_id,
                "rule_score"      : r.rule_score,
                "risk_level"      : r.risk_level,
                "triggered_flags" : "|".join(r.triggered_flags),
                "n_rules_triggered": len(r.triggered_rules),
                "pass_to_ml"      : r.pass_to_ml,
                "eval_time_ms"    : r.eval_time_ms,
            })
        return pd.DataFrame(results)

    # ── Stats & debug ────────────────────────────────────────

    def get_rules_summary(self) -> List[dict]:
        """Retourne un résumé lisible des règles actives."""
        return [
            {
                "id"       : r["id"],
                "name"     : r["name"],
                "flag"     : r["flag"],
                "severity" : r["severity"],
                "weight"   : r["weight"],
                "condition": r["condition"],
            }
            for r in self.rules
        ]

    def validate_rules(self) -> List[dict]:
        """
        Valide la syntaxe de toutes les règles.
        Retourne les erreurs éventuelles.
        """
        errors = []
        dummy_ctx = {k: 0 for k in self.FEATURE_DEFAULTS}
        for rule in self.rules:
            _, err = self._safe_eval(rule["condition"], dummy_ctx)
            if err:
                errors.append({"rule_id": rule["id"], "condition": rule["condition"], "error": err})
        return errors


# ════════════════════════════════════════════════════════════
# TEST RAPIDE (python -m app.core.rule_engine)
# ════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import sys, json

    engine = RuleEngine()
    print(f"\n✅ {engine.active_rule_count} règles chargées")

    # Validation syntaxe
    errors = engine.validate_rules()
    if errors:
        print(f"⚠️  {len(errors)} règle(s) invalide(s) :")
        for e in errors:
            print(f"   {e}")
    else:
        print("✅ Toutes les règles sont syntaxiquement valides")

    # Cas de test
    test_cases = [
        {
            "name": "Légitime normal",
            "data": {
                "partner_id": 1, "nb_programs": 1,
                "shared_phone_count": 0, "shared_account_count": 0,
                "pmt_score": 0.75, "payment_gap_ratio": 0.1,
                "network_risk_score": 0.05, "household_size": 4,
                "nb_children": 2, "income_per_person": 200,
            }
        },
        {
            "name": "Multi-inscription",
            "data": {
                "partner_id": 2, "nb_programs": 5,
                "shared_phone_count": 1, "shared_account_count": 0,
                "pmt_score": 0.40, "payment_gap_ratio": 0.10,
                "network_risk_score": 0.20, "household_size": 3,
                "nb_children": 1, "income_per_person": 300,
            }
        },
        {
            "name": "Téléphone partagé + Réseau",
            "data": {
                "partner_id": 3, "nb_programs": 2,
                "shared_phone_count": 5, "shared_account_count": 3,
                "pmt_score": 0.30, "payment_gap_ratio": 0.55,
                "network_risk_score": 0.70, "household_size": 6,
                "nb_children": 4, "income_per_person": 55,
            }
        },
    ]

    print("\n" + "="*60)
    for tc in test_cases:
        result = engine.evaluate_one(tc["data"])
        print(f"\n[{tc['name']}]")
        print(f"  Score    : {result.rule_score}  →  {result.risk_level}")
        print(f"  Flags    : {result.triggered_flags}")
        print(f"  Pass ML  : {result.pass_to_ml}")
        print(f"  Time     : {result.eval_time_ms} ms")
