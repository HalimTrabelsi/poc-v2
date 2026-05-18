"""Unit tests for FeatureEngineer."""
import pytest

from app.services.features_service import FEATURE_NAMES, FeatureEngineer, _DEFAULTS


class _MockExtractor:
    """Return a controlled raw feature dict without hitting the DB."""

    def __init__(self, raw: dict) -> None:
        self._raw = raw

    def get_beneficiary_features(self, beneficiary_id, snapshot_date=None) -> dict:
        return self._raw.copy()


def _make_engineer(raw: dict) -> FeatureEngineer:
    return FeatureEngineer(extractor=_MockExtractor(raw))


def test_all_feature_names_present(sample_features):
    """validate_features must return all 25 canonical feature keys."""
    engineer = _make_engineer(sample_features)
    result = engineer.validate_features(sample_features)
    for name in FEATURE_NAMES:
        assert name in result, f"Missing feature: {name}"


def test_income_per_person_calculated():
    """income_per_person = income / household_size."""
    raw = {**_DEFAULTS, "income": 400.0, "household_size": 4.0}
    engineer = _make_engineer(raw)
    features = engineer.get_features("1")
    assert abs(features["income_per_person"] - 100.0) < 0.01


def test_zero_income_does_not_raise():
    """Zero income should produce income_per_person = 0, not ZeroDivisionError."""
    raw = {**_DEFAULTS, "income": 0.0, "household_size": 3.0}
    engineer = _make_engineer(raw)
    features = engineer.get_features("1")
    assert features["income_per_person"] == 0.0


def test_zero_household_size_clamped():
    """Household size of 0 must be clamped to 1 to prevent division by zero."""
    raw = {**_DEFAULTS, "income": 200.0, "household_size": 0.0}
    engineer = _make_engineer(raw)
    features = engineer.get_features("1")
    assert features["income_per_person"] == 200.0


def test_dependency_ratio_calculation():
    """dependency_ratio = (nb_children + nb_elderly) / adults."""
    raw = {**_DEFAULTS, "household_size": 5.0, "nb_children": 2.0, "nb_elderly": 1.0}
    engineer = _make_engineer(raw)
    features = engineer.get_features("1")
    # adults = 5 - 2 - 1 = 2; ratio = 3 / 2 = 1.5
    assert abs(features["dependency_ratio"] - 1.5) < 0.01


def test_network_risk_capped_at_one():
    """network_risk must never exceed 1.0 regardless of input extremes."""
    raw = {**_DEFAULTS, "shared_phone_count": 100.0, "shared_account_count": 100.0}
    engineer = _make_engineer(raw)
    features = engineer.get_features("1")
    assert features["network_risk"] <= 1.0


def test_missing_fields_filled_with_defaults():
    """validate_features must fill absent keys with _DEFAULTS values."""
    engineer = _make_engineer({})
    result = engineer.validate_features({})
    assert result["pmt_score"] == _DEFAULTS["pmt_score"]
    assert result["nb_programs"] == _DEFAULTS["nb_programs"]


def test_none_values_replaced_by_defaults():
    """None values in the raw dict should revert to default."""
    engineer = _make_engineer({"pmt_score": None})
    result = engineer.validate_features({"pmt_score": None})
    assert result["pmt_score"] == _DEFAULTS["pmt_score"]
