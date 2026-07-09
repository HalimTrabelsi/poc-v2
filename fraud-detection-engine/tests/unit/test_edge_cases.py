"""Edge-case and robustness tests for the ML scoring slice."""
import pytest
import pandas as pd


def test_ml_scoring_empty_features():
    """Scoring an empty feature dict must return safe defaults, not crash."""
    from app.core.ml_scorer import MultiModelScorer
    scorer = MultiModelScorer()
    if not scorer.ready:
        pytest.skip("Model files not found")
    result = scorer.score({})
    assert result is not None
    assert result.get("ready") is True or "error" in result
    assert isinstance(result.get("ml_score", 0.0), float)
    assert 0.0 <= result.get("ml_score", 0.0) <= 1.0


def test_ml_scoring_none_values():
    """None feature values should be treated as zeros, not crash."""
    from app.core.ml_scorer import MultiModelScorer
    scorer = MultiModelScorer()
    if not scorer.ready:
        pytest.skip("Model files not found")
    features = {f: None for f in scorer.features}
    result = scorer.score(features)
    assert result is not None
    assert isinstance(result.get("ml_score", 0.0), float)


def test_ml_scoring_negative_values():
    """Negative feature values should not crash the pipeline."""
    from app.core.ml_scorer import MultiModelScorer
    scorer = MultiModelScorer()
    if not scorer.ready:
        pytest.skip("Model files not found")
    features = {f: -999.0 for f in scorer.features}
    result = scorer.score(features)
    assert result is not None
    assert isinstance(result.get("ml_score", 0.0), float)


def test_ml_scoring_extreme_outliers():
    """Extreme values (large magnitudes) must not crash."""
    from app.core.ml_scorer import MultiModelScorer
    scorer = MultiModelScorer()
    if not scorer.ready:
        pytest.skip("Model files not found")
    features = {f: 999999.0 if "count" in f or "size" in f else 100.0 for f in scorer.features}
    result = scorer.score(features)
    assert result is not None
    assert 0.0 <= result.get("ml_score", 0.0) <= 1.0


def test_ml_scoring_high_fraud_profile():
    """A clearly fraudulent profile should score above 0.5."""
    from app.core.ml_scorer import MultiModelScorer
    scorer = MultiModelScorer()
    if not scorer.ready:
        pytest.skip("Model files not found")
    high_risk = {
        "age": 25, "income": 200.0, "income_per_person": 50.0,
        "household_size": 10, "nb_children": 7, "nb_elderly": 2,
        "dependency_ratio": 0.9, "has_disabled": 1, "single_head": 1,
        "nb_programs": 5, "nb_active_programs": 5,
        "pmt_score": 0.15, "pmt_score_min": 0.10, "avg_enrollment_days": 30,
        "payment_count": 15, "payment_gap_ratio": 0.60,
        "payment_success_rate": 0.40, "amount_variance": 5000.0,
        "cycle_count": 5,
        "shared_phone_count": 5, "shared_account_count": 4,
        "network_risk": 0.85, "group_membership_count": 3,
        "high_amount_flag": 1, "income_program_inconsistency": 1,
    }
    features = {f: high_risk.get(f, 0.0) for f in scorer.features}
    result = scorer.score(features)
    ml_score = result.get("ml_score", 0.0)
    assert ml_score > 0.5, f"Expected high-risk score > 0.5, got {ml_score:.4f}"


def test_feature_names_consistency():
    """Feature names are exactly 25 and match metadata.json column order."""
    from app.core.ml_scorer import MultiModelScorer
    scorer = MultiModelScorer()
    assert len(scorer.features) == 25
    expected = [
        "age", "income", "income_per_person", "household_size", "nb_children",
        "nb_elderly", "dependency_ratio", "has_disabled", "single_head",
        "nb_programs", "nb_active_programs", "pmt_score", "pmt_score_min",
        "avg_enrollment_days", "payment_count", "payment_gap_ratio",
        "payment_success_rate", "amount_variance", "cycle_count",
        "shared_phone_count", "shared_account_count", "network_risk",
        "group_membership_count", "high_amount_flag", "income_program_inconsistency",
    ]
    assert scorer.features == expected


def test_base_model_scores_present():
    """Score result must include per-model breakdown (xgboost, rf, logreg)."""
    from app.core.ml_scorer import MultiModelScorer
    scorer = MultiModelScorer()
    if not scorer.ready:
        pytest.skip("Model files not found")
    result = scorer.score({"income": 400, "age": 35, "household_size": 4})
    base = result.get("base_model_scores", {})
    for name in ("random_forest", "xgboost", "logreg"):
        assert name in base, f"Missing base model score: {name}"
        assert 0.0 <= base[name] <= 1.0, f"Score for {name} out of range: {base[name]}"