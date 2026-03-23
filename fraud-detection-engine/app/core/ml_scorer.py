"""ML Scorer — XGBoost + IsolationForest"""
import os
import numpy as np
import pandas as pd
import joblib
from pathlib import Path

FEATURES = [
    "nb_programs",
    "total_amount",
    "amount_ratio",
    "nb_cycles",
    "days_since_enrollment",
    "account_changes_30d",
    "household_size",
    "nb_payment_failures",
    "location_risk_score",
]


class MLScorer:
    def __init__(self, models_dir: str = "models_saved"):
        p = Path(models_dir)
        xgb_path = p / "xgboost.pkl"
        iso_path  = p / "isoforest.pkl"

        self.xgb   = joblib.load(xgb_path) if xgb_path.exists() else None
        self.iso   = joblib.load(iso_path)  if iso_path.exists()  else None
        self.ready = self.xgb is not None

    def score(self, features: dict) -> dict:
        if not self.ready:
            return {
                "ml_score":      0.0,
                "fraud_prob":    0.0,
                "anomaly_score": 0.0,
                "ready":         False,
            }

        X = pd.DataFrame([{f: features.get(f, 0) for f in FEATURES}])

        fraud_prob    = float(self.xgb.predict_proba(X)[0][1])
        anomaly_raw   = float(-self.iso.score_samples(X)[0]) if self.iso else 0.0
        anomaly_score = min(anomaly_raw / 0.5, 1.0)
        ml_score      = 0.6 * fraud_prob + 0.4 * anomaly_score

        return {
            "ml_score":      round(ml_score, 3),
            "fraud_prob":    round(fraud_prob, 3),
            "anomaly_score": round(anomaly_score, 3),
            "ready":         True,
        }