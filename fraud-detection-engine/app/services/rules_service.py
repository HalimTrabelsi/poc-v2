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
        """Return metadata for all loaded (enabled) rules without evaluated conditions."""
        return [
            {
                "id": r.get("id"),
                "name": r.get("name"),
                "weight": r.get("weight"),
                "alert_level": r.get("alert_level"),
                "condition": r.get("condition"),
                "enabled": r.get("enabled", True),
                "scenario": r.get("_scenario"),
                "file": r.get("_file"),
            }
            for r in self._rules
        ]

    def get_all_rules_admin(self) -> list[dict]:
        """Return every rule (enabled or not) across all scenario files, for the admin UI."""
        rules = self._loader.load_all_raw(self._rules_dir)
        return [
            {
                "id": r.get("id"),
                "name": r.get("name"),
                "description": r.get("description", ""),
                "weight": r.get("weight"),
                "alert_level": r.get("alert_level", "MEDIUM"),
                "condition": r.get("condition"),
                "evidence_template": r.get("evidence_template", ""),
                "enabled": r.get("enabled", True),
                "scenario": r.get("_scenario"),
                "file": r.get("_file"),
            }
            for r in rules
        ]

    def create_or_update_rule(self, scenario_file: str, rule: dict) -> dict:
        """Create a new rule or replace an existing one (matched by id), then hot-reload."""
        self._loader.upsert_rule(self._rules_dir, scenario_file, rule)
        self.reload_rules()
        return {"status": "saved", "id": rule.get("id"), "file": scenario_file}

    def delete_rule(self, rule_id: str) -> bool:
        """Delete a rule by id from wherever it lives, then hot-reload."""
        found = self._loader.delete_rule(self._rules_dir, rule_id)
        if found:
            self.reload_rules()
        return found

    def set_rule_enabled(self, rule_id: str, enabled: bool) -> bool:
        """Enable/disable a rule in place, then hot-reload."""
        found = self._loader.set_rule_enabled(self._rules_dir, rule_id, enabled)
        if found:
            self.reload_rules()
        return found

    def list_scenario_files(self) -> list[str]:
        """Return the scenario YAML filenames available under the rules dir."""
        if not self._rules_dir.exists():
            return []
        return sorted(p.name for p in self._rules_dir.glob("*.yaml"))
