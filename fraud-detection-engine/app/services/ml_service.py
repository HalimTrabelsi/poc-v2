"""XGBoost + Isolation Forest scoring service."""
import json
import logging
from pathlib import Path
from typing import Optional

import joblib
import numpy as np
import pandas as pd

from app.config import settings

logger = logging.getLogger(__name__)


class MLScorer:
    """Load trained models and produce a combined fraud probability score."""

    def __init__(self, models_dir: Optional[Path] = None) -> None:
        self._models_dir = models_dir or settings.models_dir
        self._model = None
        self._iso_model = None
        self._features: list[str] = []
        self._ready = False
        self._load_models()

    def _load_models(self) -> None:
        """Try XGBoost first, fall back to random_forest.joblib."""
        metadata_path = self._models_dir / "metadata.json"
        if not metadata_path.exists():
            logger.warning("metadata.json not found at %s", metadata_path)
            return

        try:
            with metadata_path.open(encoding="utf-8") as fh:
                metadata = json.load(fh)
            self._features = metadata.get("feature_columns", [])
        except Exception as exc:
            logger.error("Failed to read metadata.json: %s", exc)
            return

        # Prefer XGBoost artifact, fall back gracefully
        for candidate in ("xgboost.joblib", "random_forest.joblib"):
            model_path = self._models_dir / candidate
            if model_path.exists():
                try:
                    self._model = joblib.load(model_path)
                    logger.info("Loaded supervised model: %s", candidate)
                    break
                except Exception as exc:
                    logger.warning("Could not load %s: %s", candidate, exc)

        iso_path = self._models_dir / "isolation_forest.joblib"
        if iso_path.exists():
            try:
                self._iso_model = joblib.load(iso_path)
                logger.info("Loaded Isolation Forest: %s", iso_path)
            except Exception as exc:
                logger.warning("Could not load isolation_forest.joblib: %s", exc)

        self._ready = self._model is not None

    def score(self, features: dict) -> dict:
        """Score a beneficiary with both models and return a combined score.

        Returns:
            Dict with xgboost_score, isolation_score, combined_score, features_used.
            combined = 0.70 * xgboost_score + 0.30 * isolation_normalized.
        """
        if not self._ready or self._model is None:
            return {
                "xgboost_score": 0.0,
                "isolation_score": 0.0,
                "combined_score": 0.0,
                "features_used": self._features,
                "error": "Models not loaded",
            }

        row = {f: float(features.get(f, 0.0) or 0.0) for f in self._features}
        X = pd.DataFrame([row], columns=self._features)

        try:
            if hasattr(self._model, "predict_proba"):
                xgb_score = float(self._model.predict_proba(X)[0][1])
            else:
                xgb_score = float(self._model.predict(X)[0])
        except Exception as exc:
            logger.error("Supervised scoring failed: %s", exc)
            xgb_score = 0.0

        iso_score = 0.0
        if self._iso_model is not None:
            try:
                raw = float(self._iso_model.score_samples(X)[0])
                iso_score = self._normalize_isolation_score(raw)
            except Exception as exc:
                logger.warning("Isolation Forest scoring failed: %s", exc)

        combined = round(
            settings.xgboost_weight * xgb_score
            + settings.isolation_forest_weight * iso_score,
            6,
        )

        return {
            "xgboost_score": round(xgb_score, 6),
            "isolation_score": round(iso_score, 6),
            "combined_score": combined,
            "features_used": self._features,
            "model": self._model,  # exposed for SHAP in Explainer
        }

    def _normalize_isolation_score(self, raw: float) -> float:
        """Map the Isolation Forest score_samples output to [0, 1].

        score_samples returns negative values; more negative = more anomalous.
        The typical range is approximately [-0.5, 0.1], so we invert and clip.
        """
        return float(np.clip((-raw - 0.1) / 0.4, 0.0, 1.0))

    @property
    def is_ready(self) -> bool:
        """True when at least the supervised model is loaded."""
        return self._ready

    @property
    def feature_names(self) -> list[str]:
        """Feature names expected by the model, in order."""
        return self._features
