"""ML Scoring service — delegates to MultiModelScorer (stacking architecture)."""
import logging
from typing import Optional

from app.core.ml_scorer import get_scorer

logger = logging.getLogger(__name__)


class MLScorer:
    """Thin wrapper over the shared MultiModelScorer singleton.

    DecisionOrchestrator expects .score(features) → dict with 'combined_score'.
    MultiModelScorer already produces this via the stacking meta-model.

    Uses the process-wide singleton (app.core.ml_scorer.get_scorer) instead of
    constructing its own MultiModelScorer, since each construction re-runs
    joblib.load() on every model file — doing that per request/thread caused
    concurrent native heap corruption (glibc "free(): invalid next size").
    """

    def __init__(self, models_dir: Optional[str] = None) -> None:
        self._multi = get_scorer()
        self._features = self._multi.features
        self._ready = self._multi.ready

    def score(self, features: dict) -> dict:
        result = self._multi.score(features)
        return {
            "xgboost_score": result.get("base_model_scores", {}).get("xgboost", 0.0),
            "isolation_score": result.get("anomaly_score", 0.0),
            "combined_score": result.get("ml_score", 0.0),
            "features_used": self._features,
            "model": self._multi.get_base_model_for_shap("xgboost"),
            "base_model_scores": result.get("base_model_scores", {}),
        }

    @property
    def is_ready(self) -> bool:
        return self._ready

    @property
    def feature_names(self) -> list:
        return self._features