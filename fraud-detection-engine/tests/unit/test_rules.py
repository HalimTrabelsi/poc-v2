"""Unit tests for RuleService and RuleEngine."""
from pathlib import Path

import pytest

from app.rules.engine import RuleEngine, SafeExpressionEvaluator
from app.rules.loader import RuleLoader

RULES_DIR = Path(__file__).parent.parent.parent / "app" / "rules" / "rules"

_SAMPLE_RULES = [
    {
        "id": "TEST001",
        "name": "Test Rule",
        "weight": 0.50,
        "alert_level": "HIGH",
        "condition": "nb_programs >= 4",
        "evidence_template": "Programs: {nb_programs}",
    },
    {
        "id": "TEST002",
        "name": "Network Test",
        "weight": 0.40,
        "alert_level": "CRITICAL",
        "condition": "shared_phone_count >= 3 and shared_account_count >= 2",
        "evidence_template": "Network fraud detected",
    },
]


def test_rule_not_triggered_for_clean_beneficiary(sample_features):
    """Clean beneficiary should trigger no rules."""
    engine = RuleEngine(_SAMPLE_RULES)
    score, triggered = engine.evaluate(sample_features)
    assert score == 0.0
    assert len(triggered) == 0


def test_rule_triggered_for_multi_enrollment(fraud_features):
    """Fraud features should trigger TEST001 (nb_programs >= 4)."""
    engine = RuleEngine([_SAMPLE_RULES[0]])
    score, triggered = engine.evaluate(fraud_features)
    assert score > 0.0
    assert len(triggered) == 1
    assert triggered[0]["rule_id"] == "TEST001"


def test_compound_rule_requires_both_conditions():
    """TEST002 requires shared_phone_count >= 3 AND shared_account_count >= 2."""
    engine = RuleEngine([_SAMPLE_RULES[1]])

    # Only phone condition met
    features_partial = {"shared_phone_count": 3.0, "shared_account_count": 1.0}
    _, triggered = engine.evaluate(features_partial)
    assert len(triggered) == 0

    # Both conditions met
    features_full = {"shared_phone_count": 4.0, "shared_account_count": 2.0}
    _, triggered = engine.evaluate(features_full)
    assert len(triggered) == 1


def test_score_capped_at_one():
    """Aggregate score must not exceed 1.0 even when all rules fire."""
    heavy_rules = [
        {"id": f"H{i}", "name": f"Heavy {i}", "weight": 0.9,
         "condition": "nb_programs >= 0", "evidence_template": "x"}
        for i in range(5)
    ]
    engine = RuleEngine(heavy_rules)
    score, _ = engine.evaluate({"nb_programs": 1})
    assert score == 1.0


def test_explanation_generated_from_template():
    """Triggered rule explanation must format the evidence_template."""
    engine = RuleEngine([_SAMPLE_RULES[0]])
    _, triggered = engine.evaluate({"nb_programs": 5.0})
    assert "5" in triggered[0]["explanation"]


def test_yaml_rules_load_correctly():
    """At least one YAML rule file should load without errors."""
    loader = RuleLoader()
    rules = loader.load_from_dir(RULES_DIR)
    assert len(rules) > 0


def test_yaml_rules_have_required_keys():
    """Every loaded YAML rule must have id, name, weight, condition."""
    loader = RuleLoader()
    rules = loader.load_from_dir(RULES_DIR)
    for rule in rules:
        assert "id" in rule
        assert "name" in rule
        assert "weight" in rule
        assert "condition" in rule


def test_safe_evaluator_blocks_arbitrary_code():
    """SafeExpressionEvaluator must raise ValueError on disallowed nodes."""
    import pytest

    evaluator = SafeExpressionEvaluator({})
    import ast

    malicious = ast.parse("__import__('os').system('ls')", mode="eval")
    with pytest.raises(ValueError):
        evaluator.visit(malicious)


def test_rule_service_evaluate_returns_correct_keys(sample_features):
    """RuleService.evaluate must return rule_score, triggered_rules, explanations."""
    from app.services.rules_service import RuleService

    svc = RuleService(rules_dir=RULES_DIR)
    result = svc.evaluate(sample_features)
    assert "rule_score" in result
    assert "triggered_rules" in result
    assert "explanations" in result
    assert 0.0 <= result["rule_score"] <= 1.0
