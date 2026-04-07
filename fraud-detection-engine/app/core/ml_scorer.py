import json
import os
from pathlib import Path
from typing import Any, Dict, Optional

import joblib
import pandas as pd


BASE_DIR = Path(__file__).resolve().parents[2]
MODELS_DIR = Path(os.getenv("MODELS_DIR", str(BASE_DIR / "models_saved")))
DEFAULT_MODEL_NAME = os.getenv("DEFAULT_MODEL_NAME", "random_forest")

METADATA_FILE = MODELS_DIR / "metadata.json"
RF_MODEL_FILE = MODELS_DIR / "random_forest.joblib"
LOGREG_MODEL_FILE = MODELS_DIR / "logreg.joblib"
ISO_MODEL_FILE = MODELS_DIR / "isolation_forest.joblib"

SUPPORTED_SUPERVISED = {"random_forest": RF_MODEL_FILE, "logreg": LOGREG_MODEL_FILE}


class MLScorer:
    def __init__(self, model_name: Optional[str] = None):
        self.model_name = model_name or DEFAULT_MODEL_NAME
        self.ready = False
        self.model = None
        self.iso_model = None
        self.features: list[str] = []
        self._load()

    def _load(self):
        try:
            if not METADATA_FILE.exists():
                raise FileNotFoundError(
                    f"metadata.json not found at {METADATA_FILE}. "
                    "Run ml/scripts/train_openg2p.py to generate model artifacts."
                )

            with open(METADATA_FILE, "r", encoding="utf-8") as f:
                metadata = json.load(f)

            self.features = metadata.get("feature_columns", [])
            if not self.features:
                raise ValueError("No feature_columns found in metadata.json")

            if self.model_name not in SUPPORTED_SUPERVISED:
                raise ValueError(
                    f"Unsupported model_name: '{self.model_name}'. "
                    f"Supported: {sorted(SUPPORTED_SUPERVISED)}"
                )

            model_path = SUPPORTED_SUPERVISED[self.model_name]
            if not model_path.exists():
                raise FileNotFoundError(
                    f"Model artifact not found: {model_path}. "
                    "Run ml/scripts/train_openg2p.py to generate model artifacts."
                )

            self.model = joblib.load(model_path)

            if ISO_MODEL_FILE.exists():
                self.iso_model = joblib.load(ISO_MODEL_FILE)
            else:
                self.iso_model = None

            self.ready = True
            print(f"[MLScorer] Loaded model: {self.model_name} ({model_path})")
            print(f"[MLScorer] Features ({len(self.features)}): {self.features}")
            if self.iso_model:
                print(f"[MLScorer] Isolation Forest loaded: {ISO_MODEL_FILE}")

        except Exception as e:
            self.ready = False
            self.model = None
            self.iso_model = None
            print(f"[MLScorer] Failed to load model: {e}")

    def _build_dataframe(self, payload: Dict[str, Any]) -> pd.DataFrame:
        row = {feat: (payload.get(feat) or 0) for feat in self.features}
        return pd.DataFrame([row], columns=self.features).fillna(0)

    def score(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        if not self.ready or self.model is None:
            return {
                "ready": False,
                "model_name": self.model_name,
                "ml_prediction": None,
                "ml_probability": 0.0,
                "ml_score": 0.0,
                "error": "Model is not loaded. Check server logs for details.",
            }

        try:
            X = self._build_dataframe(payload)

            prediction = int(self.model.predict(X)[0])

            if hasattr(self.model, "predict_proba"):
                proba = float(self.model.predict_proba(X)[0][1])
            else:
                proba = float(prediction)

            result: Dict[str, Any] = {
                "ready": True,
                "model_name": self.model_name,
                "ml_prediction": prediction,
                "ml_probability": round(proba, 6),
                "ml_score": round(proba, 6),
                "features_used": self.features,
            }

            if self.iso_model is not None:
                # score_samples returns negative values; more negative = more anomalous.
                # Invert and normalise to [0, 1] using the expected range [-0.5, 0.5].
                raw_score = float(self.iso_model.score_samples(X)[0])
                anomaly_score = round(max(0.0, min(1.0, (-raw_score - 0.1) / 0.4)), 6)
                result["anomaly_score"] = anomaly_score

            return result

        except Exception as e:
            return {
                "ready": False,
                "model_name": self.model_name,
                "ml_prediction": None,
                "ml_probability": 0.0,
                "ml_score": 0.0,
                "error": str(e),
            }
