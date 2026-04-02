import ast
import json
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd

RULES_PATH = Path(__file__).resolve().parents[2] / "rules" / "fraud_rules.json"


class SafeExpressionEvaluator(ast.NodeVisitor):
    ALLOWED_NODES = (
        ast.Expression,
        ast.BoolOp,
        ast.BinOp,
        ast.UnaryOp,
        ast.Compare,
        ast.Name,
        ast.Load,
        ast.Constant,
        ast.And,
        ast.Or,
        ast.Not,
        ast.Eq,
        ast.NotEq,
        ast.Gt,
        ast.GtE,
        ast.Lt,
        ast.LtE,
        ast.Add,
        ast.Sub,
        ast.Mult,
        ast.Div,
        ast.Mod,
        ast.USub,
    )

    def __init__(self, context: Dict[str, Any]):
        self.context = context

    def visit(self, node):
        if not isinstance(node, self.ALLOWED_NODES):
            raise ValueError(f"Unsupported expression node: {type(node).__name__}")
        return super().visit(node)

    def visit_Expression(self, node):
        return self.visit(node.body)

    def visit_Constant(self, node):
        return node.value

    def visit_Name(self, node):
        return self.context.get(node.id, 0)

    def visit_BoolOp(self, node):
        values = [self.visit(v) for v in node.values]
        if isinstance(node.op, ast.And):
            return all(values)
        if isinstance(node.op, ast.Or):
            return any(values)
        raise ValueError("Unsupported boolean operator")

    def visit_UnaryOp(self, node):
        operand = self.visit(node.operand)
        if isinstance(node.op, ast.Not):
            return not operand
        if isinstance(node.op, ast.USub):
            return -operand
        raise ValueError("Unsupported unary operator")

    def visit_BinOp(self, node):
        left = self.visit(node.left)
        right = self.visit(node.right)

        if isinstance(node.op, ast.Add):
            return left + right
        if isinstance(node.op, ast.Sub):
            return left - right
        if isinstance(node.op, ast.Mult):
            return left * right
        if isinstance(node.op, ast.Div):
            return left / right if right != 0 else 0
        if isinstance(node.op, ast.Mod):
            return left % right if right != 0 else 0

        raise ValueError("Unsupported binary operator")

    def visit_Compare(self, node):
        left = self.visit(node.left)

        for op, comparator in zip(node.ops, node.comparators):
            right = self.visit(comparator)

            if isinstance(op, ast.Eq):
                ok = left == right
            elif isinstance(op, ast.NotEq):
                ok = left != right
            elif isinstance(op, ast.Gt):
                ok = left > right
            elif isinstance(op, ast.GtE):
                ok = left >= right
            elif isinstance(op, ast.Lt):
                ok = left < right
            elif isinstance(op, ast.LtE):
                ok = left <= right
            else:
                raise ValueError(f"Unsupported comparison operator: {type(op).__name__}")

            if not ok:
                return False
            left = right

        return True


class RuleEngine:
    def __init__(self, rules_path: str | Path = RULES_PATH):
        with open(rules_path, "r", encoding="utf-8") as f:
            payload = json.load(f)

        self.rules = payload["rules"]

        self.default_features = {
            "partner_id": None,
            "shared_account_count": 0,
            "shared_phone_count": 0,
            "nb_programs": 0,
            "pmt_score": 1.0,
            "gap_ratio": 0.0,
            "high_amount_flag": 0,
            "dependency_ratio": 0.0,
            "income_per_person": 999999.0,
            "network_risk": 0.0,
            "household_size": 1,
            "nb_children": 0,
        }

    def _normalize_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        normalized = self.default_features.copy()
        normalized.update({k: v for k, v in row.items() if v is not None})
        return normalized

    def _safe_eval(self, condition: str, data: Dict[str, Any]) -> bool:
        try:
            tree = ast.parse(condition, mode="eval")
            evaluator = SafeExpressionEvaluator(data)
            return bool(evaluator.visit(tree))
        except Exception:
            return False

    def evaluate_one(self, row: Dict[str, Any]) -> Dict[str, Any]:
        row = self._normalize_row(row)

        triggered = []
        total_score = 0.0
        explanations = []

        for rule in self.rules:
            if not rule.get("enabled", True):
                continue

            matched = self._safe_eval(rule["condition"], row)
            if matched:
                triggered.append({
                    "rule_id": rule["id"],
                    "rule_name": rule["name"],
                    "flag": rule["flag"],
                    "severity": rule["severity"],
                    "weight": rule["weight"],
                    "condition": rule["condition"],
                })
                explanations.append(f"{rule['flag']} triggered by condition: {rule['condition']}")
                total_score += float(rule["weight"])

        rule_score = min(total_score, 1.0)

        if rule_score >= 0.80:
            risk_level = "CRITICAL"
        elif rule_score >= 0.55:
            risk_level = "HIGH"
        elif rule_score >= 0.30:
            risk_level = "MEDIUM"
        else:
            risk_level = "LOW"

        return {
            "beneficiary_id": row.get("partner_id"),
            "rule_score": round(rule_score, 3),
            "risk_level": risk_level,
            "triggered_rules": triggered,
            "triggered_flags": [r["flag"] for r in triggered],
            "explanations": explanations,
            "pass_to_ml": rule_score < 0.80,
        }

    def evaluate_df(self, df: pd.DataFrame) -> pd.DataFrame:
        outputs: List[Dict[str, Any]] = []
        for _, row in df.iterrows():
            outputs.append(self.evaluate_one(row.to_dict()))
        return pd.DataFrame(outputs)