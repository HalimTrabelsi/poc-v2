import json
import os
from pathlib import Path
from typing import Dict, Any

import joblib
import pandas as pd


BASE_DIR = Path(__file__).resolve().parents[2]
MODELS_DIR = Path(os.getenv("MODELS_DIR", str(BASE_DIR / "models_saved")))
DEFAULT_MODEL_NAME = os.getenv("DEFAULT_MODEL_NAME", "random_forest")

FEATURES_FILE = MODELS_DIR / "features.json"
RF_MODEL_FILE = MODELS_DIR / "random_forest.pkl"
XGB_MODEL_FILE = MODELS_DIR / "xgboost.pkl"


class MLScorer:
    def __init__(self, model_name: str | None = None):
        self.model_name = model_name or DEFAULT_MODEL_NAME
        self.ready = False
        self.model = None
        self.features = []
        self._load()

    def _get_model_path(self) -> Path:
        if self.model_name == "random_forest":
            return RF_MODEL_FILE
        if self.model_name == "xgboost":
            return XGB_MODEL_FILE
        raise ValueError(f"Unsupported model_name: {self.model_name}")

    def _load(self):
        try:
            if not FEATURES_FILE.exists():
                raise FileNotFoundError(f"features.json not found at {FEATURES_FILE}")

            with open(FEATURES_FILE, "r", encoding="utf-8") as f:
                payload = json.load(f)

            self.features = payload.get("features", [])
            if not self.features:
                raise ValueError("No features found in features.json")

            model_path = self._get_model_path()
            if not model_path.exists():
                raise FileNotFoundError(f"Model file not found: {model_path}")

            self.model = joblib.load(model_path)
            self.ready = True

            print(f"[MLScorer] Loaded model: {self.model_name}")
            print(f"[MLScorer] Model path: {model_path}")
            print(f"[MLScorer] Features: {self.features}")

        except Exception as e:
            self.ready = False
            self.model = None
            print(f"[MLScorer] Failed to load model: {e}")

    def _normalize_payload(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        Ensure payload contains all features expected by the model.
        Missing features are filled with 0.
        """
        normalized = {}

        for feat in self.features:
            value = payload.get(feat, 0)

            # Basic normalization
            if value is None:
                value = 0

            normalized[feat] = value

        return normalized

    def _build_dataframe(self, payload: Dict[str, Any]) -> pd.DataFrame:
        normalized = self._normalize_payload(payload)
        df = pd.DataFrame([normalized], columns=self.features)
        df = df.fillna(0)
        return df

    def score(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        if not self.ready or self.model is None:
            return {
                "ready": False,
                "model_name": self.model_name,
                "ml_prediction": None,
                "ml_probability": 0.0,
                "ml_score": 0.0,
                "error": "Model is not loaded",
            }

        try:
            X = self._build_dataframe(payload)

            prediction = int(self.model.predict(X)[0])

            if hasattr(self.model, "predict_proba"):
                proba = float(self.model.predict_proba(X)[0][1])
            else:
                # fallback if model doesn't expose predict_proba
                proba = float(prediction)

            return {
                "ready": True,
                "model_name": self.model_name,
                "ml_prediction": prediction,
                "ml_probability": round(proba, 6),
                "ml_score": round(proba, 6),
                "features_used": self.features,
            }

        except Exception as e:
            return {
                "ready": False,
                "model_name": self.model_name,
                "ml_prediction": None,
                "ml_probability": 0.0,
                "ml_score": 0.0,
                "error": str(e),
            }