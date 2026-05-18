"""Unit tests for the Explainer service."""
import pytest

from app.services.explainability_service import Explainer, _RISK_SUMMARIES


def test_explain_returns_required_keys(mock_decision):
    """explain() must always return the four required keys."""
    explainer = Explainer()
    result = explainer.explain(mock_decision)
    assert "summary" in result
    assert "top_reasons" in result
    assert "rule_explanations" in result
    assert "feature_contributions" in result


def test_explain_summary_not_empty(mock_decision):
    """Summary string must not be empty."""
    explainer = Explainer()
    result = explainer.explain(mock_decision)
    assert len(result["summary"]) > 0


def test_explain_rule_explanations_match_triggered_rules(mock_decision):
    """rule_explanations count must equal triggered_rules count."""
    explainer = Explainer()
    result = explainer.explain(mock_decision)
    assert len(result["rule_explanations"]) == len(mock_decision["rules_triggered"])


def test_synthesize_summary_contains_risk_level_language():
    """_synthesize_summary must embed risk-level-appropriate language."""
    explainer = Explainer()
    for level in ["LOW", "MEDIUM", "HIGH", "CRITICAL"]:
        summary = explainer._synthesize_summary([], [], level)
        expected_fragment = _RISK_SUMMARIES[level][:30]
        assert expected_fragment in summary


def test_explain_rules_formats_rule_id(mock_decision):
    """Each rule explanation must include the rule_id prefix."""
    explainer = Explainer()
    result = explainer.explain(mock_decision)
    for expl in result["rule_explanations"]:
        assert "[" in expl and "]" in expl


def test_explain_with_no_model_does_not_raise(mock_decision):
    """Explainer must handle missing model gracefully (no SHAP)."""
    mock_decision["model"] = None
    mock_decision["features"] = {"shared_phone_count": 3.0, "pmt_score": 0.1}
    explainer = Explainer()
    result = explainer.explain(mock_decision)
    assert isinstance(result["feature_contributions"], list)


def test_top_reasons_capped_at_five(mock_decision):
    """top_reasons must never exceed 5 items."""
    explainer = Explainer()
    result = explainer.explain(mock_decision)
    assert len(result["top_reasons"]) <= 5
