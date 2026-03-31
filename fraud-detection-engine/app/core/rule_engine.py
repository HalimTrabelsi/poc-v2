import json
from pathlib import Path
from typing import Any, Dict, List
import pandas as pd

RULES_PATH = Path(__file__).resolve().parents[2] / "rules" / "fraud_rules.json"


class RuleEngine:
    def __init__(self, rules_path: str | Path = RULES_PATH):
        with open(rules_path, "r", encoding="utf-8") as f:
            payload = json.load(f)
        self.rules = payload["rules"]

    def _safe_eval(self, condition: str, data: Dict[str, Any]) -> bool:
        try:
            return bool(eval(condition, {"__builtins__": {}}, data))
        except Exception:
            return False

    def evaluate_one(self, row: Dict[str, Any]) -> Dict[str, Any]:
        triggered = []
        total_score = 0.0

        for rule in self.rules:
            if self._safe_eval(rule["condition"], row):
                triggered.append({
                    "rule_id": rule["id"],
                    "flag": rule["flag"],
                    "severity": rule["severity"],
                    "weight": rule["weight"],
                })
                total_score += float(rule["weight"])

        rule_score = min(total_score, 1.0)

        risk_level = (
            "CRITICAL" if rule_score >= 0.80 else
            "HIGH" if rule_score >= 0.55 else
            "MEDIUM" if rule_score >= 0.30 else
            "LOW"
        )

        return {
            "beneficiary_id": row.get("partner_id"),
            "rule_score": round(rule_score, 3),
            "risk_level": risk_level,
            "triggered_rules": triggered,
            "pass_to_ml": rule_score < 0.80,
        }

    def evaluate_df(self, df: pd.DataFrame) -> pd.DataFrame:
        outputs: List[Dict[str, Any]] = []
        for _, row in df.iterrows():
            outputs.append(self.evaluate_one(row.to_dict()))
        return pd.DataFrame(outputs)