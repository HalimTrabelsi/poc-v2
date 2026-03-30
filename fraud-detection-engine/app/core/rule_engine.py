import json
from pathlib import Path

RULES_PATH = Path(__file__).resolve().parents[2] / "config" / "rules.json"

class RuleEngine:
    def __init__(self, rules_path=RULES_PATH):
        with open(rules_path, "r", encoding="utf-8") as f:
            self.rules = json.load(f)

    def evaluate(self, row: dict) -> dict:
        total = 0.0
        matched = []

        safe_row = dict(row)

        for rule in self.rules:
            try:
                if eval(rule["condition"], {"__builtins__": {}}, safe_row):
                    total += float(rule["score"])
                    matched.append({
                        "code": rule["code"],
                        "reason": rule["reason"],
                        "score": rule["score"]
                    })
            except Exception:
                continue

        return {
            "rule_score": min(total, 1.0),
            "rule_flags": matched
        }