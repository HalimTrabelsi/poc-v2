"""End-to-end XGBoost + Isolation Forest training pipeline."""
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import joblib
import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split

logger = logging.getLogger(__name__)

FEATURES = [
    "age", "income", "income_per_person",
    "household_size", "nb_children", "nb_elderly",
    "dependency_ratio",
    "has_disabled", "single_head",
    "nb_programs", "nb_active_programs",
    "pmt_score", "pmt_score_min",
    "avg_enrollment_days",
    "payment_count", "payment_gap_ratio",
    "payment_success_rate", "amount_variance",
    "cycle_count",
    "shared_phone_count", "shared_account_count",
    "network_risk",
    "group_membership_count",
    "high_amount_flag", "income_program_inconsistency",
]

DEFAULT_DATA_PATH = Path("ml/data/synthetic/dataset_ml.csv")
DEFAULT_MODELS_DIR = Path("app/models_saved")


class MLTrainingPipeline:
    """Train XGBoost and Isolation Forest on beneficiary data."""

    FEATURES = FEATURES

    def __init__(self, models_dir: Optional[Path] = None) -> None:
        self._models_dir = models_dir or DEFAULT_MODELS_DIR
        self._models_dir.mkdir(parents=True, exist_ok=True)

    def train(self, data_path: Optional[str] = None) -> dict:
        """Full training pipeline.

        Args:
            data_path: CSV file path. Defaults to the synthetic dataset.

        Returns:
            Metrics dict with xgboost and isolation_forest sub-dicts.
        """
        csv_path = Path(data_path) if data_path else DEFAULT_DATA_PATH
        logger.info("Loading training data from %s", csv_path)
        df = pd.read_csv(csv_path)

        df = self._feature_engineering(df)

        available = [f for f in self.FEATURES if f in df.columns]
        X = df[available].fillna(0)
        y = df["is_fraud"].astype(int) if "is_fraud" in df.columns else pd.Series(np.zeros(len(df)))

        X_temp, X_test, y_temp, y_test = train_test_split(
            X, y, test_size=0.20, random_state=42, stratify=y
        )
        X_train, X_val, y_train, y_val = train_test_split(
            X_temp, y_temp, test_size=0.25, random_state=42, stratify=y_temp
        )
        logger.info(
            "Split: train=%d  val=%d  test=%d", len(X_train), len(X_val), len(X_test)
        )

        xgb_model = self._train_xgboost(X_train, y_train, X_val, y_val)
        iso_model = self._train_isolation_forest(X_train)

        xgb_metrics = self._evaluate(xgb_model, X_test, y_test)
        iso_metrics = self._evaluate_isolation_forest(iso_model, X_test, y_test)

        all_metrics = {"xgboost": xgb_metrics, "isolation_forest": iso_metrics}
        self._save_models(xgb_model, iso_model, available, all_metrics)

        logger.info("Training complete. XGBoost ROC-AUC: %.4f", xgb_metrics.get("roc_auc", 0))
        return all_metrics

    def _feature_engineering(self, df: pd.DataFrame) -> pd.DataFrame:
        """Derive compound features if not already present in the CSV."""
        if "income_per_person" not in df.columns:
            df["income_per_person"] = (
                df["income"] / df.get("household_size", pd.Series(np.ones(len(df)))).clip(lower=1)
            )
        if "dependency_ratio" not in df.columns:
            adults = (
                df.get("household_size", 1) - df.get("nb_children", 0) - df.get("nb_elderly", 0)
            ).clip(lower=1)
            df["dependency_ratio"] = (df.get("nb_children", 0) + df.get("nb_elderly", 0)) / adults
        if "network_risk" not in df.columns:
            df["network_risk"] = (
                df.get("shared_phone_count", 0).clip(upper=5) * 0.4
                + df.get("shared_account_count", 0).clip(upper=5) * 0.6
            ).clip(upper=1.0)
        return df

    def _train_xgboost(self, X_train, y_train, X_val, y_val):
        """Train XGBoost classifier with early stopping on validation set."""
        try:
            import xgboost as xgb

            scale_pos_weight = float((y_train == 0).sum() / max((y_train == 1).sum(), 1))
            model = xgb.XGBClassifier(
                n_estimators=400,
                learning_rate=0.05,
                max_depth=6,
                min_child_weight=3,
                subsample=0.8,
                colsample_bytree=0.8,
                scale_pos_weight=scale_pos_weight,
                use_label_encoder=False,
                eval_metric="auc",
                early_stopping_rounds=30,
                random_state=42,
                n_jobs=-1,
            )
            model.fit(
                X_train, y_train,
                eval_set=[(X_val, y_val)],
                verbose=False,
            )
            return model
        except ImportError:
            logger.warning("xgboost not installed; falling back to RandomForest")
            from sklearn.ensemble import RandomForestClassifier

            model = RandomForestClassifier(
                n_estimators=300, max_depth=8, class_weight="balanced",
                random_state=42, n_jobs=-1,
            )
            model.fit(X_train, y_train)
            return model

    def _train_isolation_forest(self, X_train):
        """Train Isolation Forest on the full training set (unsupervised)."""
        from sklearn.ensemble import IsolationForest

        model = IsolationForest(
            n_estimators=200, contamination=0.05, random_state=42, n_jobs=-1
        )
        model.fit(X_train)
        return model

    def _evaluate(self, model, X_test, y_test) -> dict:
        """Return classification metrics for a supervised model."""
        from sklearn.metrics import (
            accuracy_score, f1_score, precision_score,
            recall_score, roc_auc_score, confusion_matrix,
        )

        y_pred = model.predict(X_test)
        y_proba = (
            model.predict_proba(X_test)[:, 1]
            if hasattr(model, "predict_proba")
            else y_pred.astype(float)
        )

        return {
            "accuracy": round(float(accuracy_score(y_test, y_pred)), 4),
            "precision": round(float(precision_score(y_test, y_pred, zero_division=0)), 4),
            "recall": round(float(recall_score(y_test, y_pred, zero_division=0)), 4),
            "f1": round(float(f1_score(y_test, y_pred, zero_division=0)), 4),
            "roc_auc": round(float(roc_auc_score(y_test, y_proba)), 4),
            "confusion_matrix": confusion_matrix(y_test, y_pred).tolist(),
        }

    def _evaluate_isolation_forest(self, model, X_test, y_test) -> dict:
        """Score Isolation Forest using ROC-AUC on anomaly scores."""
        from sklearn.metrics import roc_auc_score

        raw_scores = model.score_samples(X_test)
        anomaly_scores = np.clip((-raw_scores - 0.1) / 0.4, 0.0, 1.0)

        try:
            auc = float(roc_auc_score(y_test, anomaly_scores))
        except Exception:
            auc = 0.0

        return {"roc_auc": round(auc, 4)}

    def _save_models(
        self,
        xgb_model,
        iso_model,
        feature_columns: list[str],
        metrics: dict,
    ) -> None:
        """Persist model artifacts and metadata.json."""
        xgb_path = self._models_dir / "xgboost.joblib"
        iso_path = self._models_dir / "isolation_forest.joblib"
        meta_path = self._models_dir / "metadata.json"

        joblib.dump(xgb_model, xgb_path)
        joblib.dump(iso_model, iso_path)

        metadata = {
            "feature_columns": feature_columns,
            "training_date": datetime.now(timezone.utc).isoformat(),
            "metrics": metrics,
            "model_files": {
                "supervised": "xgboost.joblib",
                "anomaly": "isolation_forest.joblib",
            },
        }
        with meta_path.open("w", encoding="utf-8") as fh:
            json.dump(metadata, fh, indent=2)

        logger.info("Models saved to %s", self._models_dir)
