"""Rule Engine — Evaluation des regles metier JSON"""
import json
import operator
from pathlib import Path


OPERATORS = {
    "gt":  operator.gt,
    "lt":  operator.lt,
    "gte": operator.ge,
    "lte": operator.le,
    "eq":  operator.eq,
    "ne":  operator.ne,
}


class RuleEngine:
    def __init__(self, rules_path: str = "ml/rules/fraud_rules.json"):
        with open(rules_path, encoding="utf-8") as f:
            data = json.load(f)
        self.rules   = data["rules"]
        self.version = data.get("version", "1.0")

    def evaluate(self, features: dict) -> dict:
        triggered   = []
        total_score = 0.0

        for rule in self.rules:
            field = rule["field"]
            value = features.get(field, 0)
            op_fn = OPERATORS.get(rule["op"], operator.gt)

            if op_fn(value, rule["value"]):
                triggered.append({
                    "rule_id":  rule["id"],
                    "name":     rule["name"],
                    "flag":     rule["flag"],
                    "severity": rule["severity"],
                })
                total_score += rule["weight"]

        rule_score = min(total_score, 1.0)
        return {
            "rule_score":      round(rule_score, 3),
            "triggered_rules": triggered,
            "flags":           [r["flag"] for r in triggered],
            "pass_to_ml":      rule_score < 0.85,
        }