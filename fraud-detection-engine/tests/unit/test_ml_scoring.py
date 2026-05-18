"""Unit tests for MLScorer."""
from pathlib import Path
from unittest.mock import MagicMock, patch

import numpy as np
import pytest

from app.services.ml_service import MLScorer


def test_is_ready_false_when_no_models(tmp_path):
    """MLScorer.is_ready must be False when the models directory is empty."""
    scorer = MLScorer(models_dir=tmp_path)
    assert scorer.is_ready is False


def test_score_returns_safe_defaults_when_not_ready(tmp_path, sample_features):
    """score() must return combined_score=0.0 when models are not loaded."""
    scorer = MLScorer(models_dir=tmp_path)
    result = scorer.score(sample_features)
    assert result["combined_score"] == 0.0
    assert "error" in result


def test_score_in_unit_interval(sample_features):
    """combined_score must always be in [0, 1]."""
    # Create a mock model that returns a probability
    mock_model = MagicMock()
    mock_model.predict_proba.return_value = np.array([[0.3, 0.7]])
    mock_model.predict.return_value = np.array([1])

    mock_iso = MagicMock()
    mock_iso.score_samples.return_value = np.array([-0.3])

    scorer = MLScorer.__new__(MLScorer)
    scorer._model = mock_model
    scorer._iso_model = mock_iso
    scorer._features = list(sample_features.keys())
    scorer._ready = True
    scorer._models_dir = Path(".")

    # Patch settings for weights
    with patch("app.services.ml_service.settings") as mock_settings:
        mock_settings.xgboost_weight = 0.70
        mock_settings.isolation_forest_weight = 0.30
        result = scorer.score(sample_features)

    assert 0.0 <= result["combined_score"] <= 1.0


def test_normalize_isolation_score_bounds():
    """_normalize_isolation_score must clamp output to [0, 1]."""
    scorer = MLScorer.__new__(MLScorer)
    scorer._model = None
    scorer._iso_model = None
    scorer._features = []
    scorer._ready = False
    scorer._models_dir = Path(".")

    assert scorer._normalize_isolation_score(-0.5) == 1.0  # very anomalous
    assert scorer._normalize_isolation_score(0.5) == 0.0   # very normal
    val = scorer._normalize_isolation_score(-0.3)
    assert 0.0 <= val <= 1.0


def test_feature_names_property(tmp_path):
    """feature_names must return the list loaded from metadata.json."""
    import json

    metadata = {"feature_columns": ["age", "income"], "training_date": "2025-01-01", "metrics": {}}
    (tmp_path / "metadata.json").write_text(json.dumps(metadata))
    # No actual model files — scorer won't be ready but features load
    scorer = MLScorer(models_dir=tmp_path)
    assert scorer.feature_names == ["age", "income"]
