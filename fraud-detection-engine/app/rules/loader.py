"""Load and validate YAML rule files from a directory."""
import logging
from pathlib import Path
from typing import Any

import yaml

logger = logging.getLogger(__name__)

_RULE_REQUIRED_KEYS = {"id", "name", "weight", "condition"}

_RULE_SCHEMA = {
    "type": "object",
    "required": list(_RULE_REQUIRED_KEYS),
    "properties": {
        "id": {"type": "string"},
        "name": {"type": "string"},
        "description": {"type": "string"},
        "weight": {"type": "number", "minimum": 0.0, "maximum": 1.0},
        "alert_level": {"type": "string", "enum": ["LOW", "MEDIUM", "HIGH", "CRITICAL"]},
        "condition": {"type": "string"},
        "evidence_template": {"type": "string"},
    },
}


class RuleLoader:
    """Load all YAML rule files from a directory and return a flat rule list."""

    def load_from_dir(self, rules_dir: Path) -> list[dict]:
        """Read every *.yaml file in rules_dir and merge all rule lists.

        Args:
            rules_dir: Path to the directory containing scenario YAML files.

        Returns:
            Flat list of valid rule dicts, skipping files that fail to parse.
        """
        rules: list[dict] = []

        if not rules_dir.exists():
            logger.warning("Rules directory does not exist: %s", rules_dir)
            return rules

        for yaml_file in sorted(rules_dir.glob("*.yaml")):
            try:
                with yaml_file.open(encoding="utf-8") as fh:
                    payload = yaml.safe_load(fh)

                file_rules = payload.get("rules", []) if isinstance(payload, dict) else []
                valid = [r for r in file_rules if self.validate_rule(r)]
                logger.info("Loaded %d rules from %s", len(valid), yaml_file.name)
                rules.extend(valid)
            except Exception as exc:
                logger.error("Failed to load rule file %s: %s", yaml_file, exc)

        return rules

    def validate_rule(self, rule: Any) -> bool:
        """Return True if rule contains all required keys with correct types."""
        if not isinstance(rule, dict):
            return False
        missing = _RULE_REQUIRED_KEYS - rule.keys()
        if missing:
            logger.warning("Rule missing keys %s: %s", missing, rule.get("id", "?"))
            return False
        try:
            float(rule["weight"])
            str(rule["condition"])
        except (TypeError, ValueError):
            return False
        return True

    def get_rule_schema(self) -> dict:
        """Return the JSON Schema used to validate individual rule objects."""
        return _RULE_SCHEMA
