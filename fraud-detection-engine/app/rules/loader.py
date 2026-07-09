"""Load, validate and persist YAML rule files from a directory."""
import ast
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
        "enabled": {"type": "boolean"},
    },
}


class RuleValidationError(ValueError):
    """Raised when a rule payload fails schema or condition validation."""


class RuleLoader:
    """Load all YAML rule files from a directory and return a flat rule list."""

    def load_from_dir(self, rules_dir: Path) -> list[dict]:
        """Read every *.yaml file in rules_dir and merge all rule lists.

        Args:
            rules_dir: Path to the directory containing scenario YAML files.

        Returns:
            Flat list of valid, enabled rule dicts (each tagged with its
            source scenario/file), skipping files that fail to parse.
        """
        rules: list[dict] = []

        if not rules_dir.exists():
            logger.warning("Rules directory does not exist: %s", rules_dir)
            return rules

        for yaml_file in sorted(rules_dir.glob("*.yaml")):
            try:
                payload = self._read_file(yaml_file)
                scenario = payload.get("scenario", yaml_file.stem)
                file_rules = payload.get("rules", []) if isinstance(payload, dict) else []
                for r in file_rules:
                    if not self.validate_rule(r):
                        continue
                    r.setdefault("enabled", True)
                    if not r["enabled"]:
                        continue
                    r["_scenario"] = scenario
                    r["_file"] = yaml_file.name
                    rules.append(r)
                logger.info("Loaded rules from %s", yaml_file.name)
            except Exception as exc:
                logger.error("Failed to load rule file %s: %s", yaml_file, exc)

        return rules

    def load_all_raw(self, rules_dir: Path) -> list[dict]:
        """Like load_from_dir, but includes disabled rules (for admin UIs)."""
        rules: list[dict] = []
        if not rules_dir.exists():
            return rules
        for yaml_file in sorted(rules_dir.glob("*.yaml")):
            try:
                payload = self._read_file(yaml_file)
                scenario = payload.get("scenario", yaml_file.stem)
                file_rules = payload.get("rules", []) if isinstance(payload, dict) else []
                for r in file_rules:
                    if not isinstance(r, dict):
                        continue
                    r = dict(r)
                    r.setdefault("enabled", True)
                    r["_scenario"] = scenario
                    r["_file"] = yaml_file.name
                    rules.append(r)
            except Exception as exc:
                logger.error("Failed to load rule file %s: %s", yaml_file, exc)
        return rules

    def _read_file(self, yaml_file: Path) -> dict:
        with yaml_file.open(encoding="utf-8") as fh:
            return yaml.safe_load(fh) or {}

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

    def validate_rule_strict(self, rule: dict) -> None:
        """Raise RuleValidationError with a precise reason, for API-facing writes.

        Beyond validate_rule's shape check, this also parses the condition as
        a Python expression (catches syntax errors before they hit the AST
        evaluator at scoring time) and enforces the weight/alert_level ranges.
        """
        if not isinstance(rule, dict):
            raise RuleValidationError("Rule must be a JSON object")

        missing = _RULE_REQUIRED_KEYS - rule.keys()
        if missing:
            raise RuleValidationError(f"Missing required fields: {sorted(missing)}")

        if not isinstance(rule["id"], str) or not rule["id"].strip():
            raise RuleValidationError("'id' must be a non-empty string")
        if not isinstance(rule["name"], str) or not rule["name"].strip():
            raise RuleValidationError("'name' must be a non-empty string")

        try:
            weight = float(rule["weight"])
        except (TypeError, ValueError):
            raise RuleValidationError("'weight' must be a number") from None
        if not (0.0 <= weight <= 1.0):
            raise RuleValidationError("'weight' must be between 0.0 and 1.0")

        alert_level = rule.get("alert_level", "MEDIUM")
        if alert_level not in ("LOW", "MEDIUM", "HIGH", "CRITICAL"):
            raise RuleValidationError("'alert_level' must be one of LOW/MEDIUM/HIGH/CRITICAL")

        condition = rule.get("condition")
        if not isinstance(condition, str) or not condition.strip():
            raise RuleValidationError("'condition' must be a non-empty string")
        try:
            ast.parse(condition, mode="eval")
        except SyntaxError as exc:
            raise RuleValidationError(f"'condition' is not a valid expression: {exc}") from None

    def get_rule_schema(self) -> dict:
        """Return the JSON Schema used to validate individual rule objects."""
        return _RULE_SCHEMA

    # ── Persistence (admin CRUD) ────────────────────────────────────────

    def upsert_rule(self, rules_dir: Path, scenario_file: str, rule: dict) -> None:
        """Insert or replace a rule (matched by id) inside scenario_file.

        Creates the file with an empty rule list if it doesn't exist yet.
        """
        self.validate_rule_strict(rule)
        path = rules_dir / scenario_file
        payload = self._read_file(path) if path.exists() else {
            "version": "1.0",
            "scenario": scenario_file.removesuffix(".yaml"),
            "rules": [],
        }
        payload.setdefault("rules", [])

        clean_rule = {k: v for k, v in rule.items() if not k.startswith("_")}
        clean_rule.setdefault("enabled", True)

        existing_ids = [r.get("id") for r in payload["rules"]]
        if clean_rule["id"] in existing_ids:
            idx = existing_ids.index(clean_rule["id"])
            payload["rules"][idx] = clean_rule
        else:
            payload["rules"].append(clean_rule)

        rules_dir.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8") as fh:
            yaml.safe_dump(payload, fh, allow_unicode=True, sort_keys=False)

    def delete_rule(self, rules_dir: Path, rule_id: str) -> bool:
        """Remove a rule by id from whichever scenario file contains it.

        Returns True if a rule was found and removed, False otherwise.
        """
        for yaml_file in sorted(rules_dir.glob("*.yaml")):
            payload = self._read_file(yaml_file)
            file_rules = payload.get("rules", []) if isinstance(payload, dict) else []
            new_rules = [r for r in file_rules if r.get("id") != rule_id]
            if len(new_rules) != len(file_rules):
                payload["rules"] = new_rules
                with yaml_file.open("w", encoding="utf-8") as fh:
                    yaml.safe_dump(payload, fh, allow_unicode=True, sort_keys=False)
                return True
        return False

    def set_rule_enabled(self, rules_dir: Path, rule_id: str, enabled: bool) -> bool:
        """Toggle a rule's enabled flag in place. Returns True if found."""
        for yaml_file in sorted(rules_dir.glob("*.yaml")):
            payload = self._read_file(yaml_file)
            file_rules = payload.get("rules", []) if isinstance(payload, dict) else []
            found = False
            for r in file_rules:
                if r.get("id") == rule_id:
                    r["enabled"] = enabled
                    found = True
            if found:
                payload["rules"] = file_rules
                with yaml_file.open("w", encoding="utf-8") as fh:
                    yaml.safe_dump(payload, fh, allow_unicode=True, sort_keys=False)
                return True
        return False
