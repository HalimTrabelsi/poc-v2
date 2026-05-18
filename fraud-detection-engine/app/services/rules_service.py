"""YAML-configured rule evaluation service."""
import logging
from pathlib import Path
from typing import Optional

from app.config import settings
from app.rules.engine import RuleEngine
from app.rules.loader import RuleLoader

logger = logging.getLogger(__name__)


class RuleService:
    """Load YAML rules and expose a simple evaluate interface."""

    def __init__(self, rules_dir: Optional[Path] = None) -> None:
        self._rules_dir = rules_dir or settings.rules_dir
        self._loader = RuleLoader()
        self._rules: list[dict] = []
        self._engine: Optional[RuleEngine] = None
        self.reload_rules()

    def reload_rules(self) -> None:
        """Re-read all YAML files from disk and rebuild the rule engine."""
        self._rules = self._loader.load_from_dir(self._rules_dir)
        self._engine = RuleEngine(self._rules)
        logger.info("RuleService loaded %d rules from %s", len(self._rules), self._rules_dir)

    def evaluate(self, features: dict) -> dict:
        """Evaluate all rules against features.

        Returns:
            Dict with keys: rule_score (float), triggered_rules (list), explanations (list).
        """
        if self._engine is None:
            return {"rule_score": 0.0, "triggered_rules": [], "explanations": []}

        score, triggered = self._engine.evaluate(features)

        explanations = [r["explanation"] for r in triggered]

        return {
            "rule_score": score,
            "triggered_rules": triggered,
            "explanations": explanations,
        }

    def get_rules_summary(self) -> list[dict]:
        """Return metadata for all loaded rules without evaluated conditions."""
        return [
            {
                "id": r.get("id"),
                "name": r.get("name"),
                "weight": r.get("weight"),
                "alert_level": r.get("alert_level"),
                "condition": r.get("condition"),
            }
            for r in self._rules
        ]
