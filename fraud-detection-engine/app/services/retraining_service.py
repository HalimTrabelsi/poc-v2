"""Feedback-driven model retraining service.

Investigators submit verdicts (confirmed_fraud / false_positive) via the API.
These are stored in the fraud_feedback table and used as ground-truth labels
to periodically retrain XGBoost, improving precision over time.

Retraining strategy
-------------------
- Pull all cases with a confirmed verdict from fraud_feedback.
- Fetch their stored feature vectors from fraud_cases (stored as JSONB).
- Augment with the original synthetic training data if available.
- Retrain XGBoost (same hyper-params as original training).
- Save to models_saved/ and reload the MLScorer singleton.

The weekly background thread fires automatically; the API endpoint
POST /api/v1/retrain allows investigators to trigger it manually.
"""
import json
import logging
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


class RetrainingService:
    """Collect investigator feedback and periodically retrain the XGBoost model."""

    RETRAIN_INTERVAL_HOURS = 168    # 7 days
    MIN_FEEDBACK_TO_RETRAIN = 10    # wait until we have at least this many verdicts

    def __init__(self) -> None:
        self._running = False
        self._thread: threading.Thread | None = None
        self._last_retrain: datetime | None = None
        self._lock = threading.Lock()

    # ── background scheduler ─────────────────────────────────────────────────

    def start(self) -> None:
        self._running = True
        self._thread = threading.Thread(
            target=self._weekly_loop, daemon=True, name="RetrainingScheduler"
        )
        self._thread.start()
        logger.info(
            "RetrainingScheduler started — auto-retrain every %d hours",
            self.RETRAIN_INTERVAL_HOURS,
        )

    def stop(self) -> None:
        self._running = False

    def _weekly_loop(self) -> None:
        interval_s = self.RETRAIN_INTERVAL_HOURS * 3600
        while self._running:
            time.sleep(interval_s)
            try:
                result = self.retrain()
                logger.info("Scheduled retrain complete: %s", result)
            except Exception:
                logger.exception("Scheduled retrain failed")

    # ── feedback persistence ─────────────────────────────────────────────────

    def save_feedback(self, case_id: str, verdict: str, notes: str, investigator: str) -> str:
        """Persist an investigator verdict and return the feedback record id."""
        from app.data.repository import FraudCaseRepository
        repo = FraudCaseRepository()
        return repo.save_feedback(case_id, verdict, notes, investigator)

    def get_feedback_stats(self) -> dict:
        """Return counts of verdicts, model version, and retraining history."""
        from app.data.repository import FraudCaseRepository
        repo = FraudCaseRepository()
        stats = repo.get_feedback_stats()
        stats["last_retrain"] = (
            self._last_retrain.isoformat() if self._last_retrain else None
        )
        return stats

    # ── retraining ───────────────────────────────────────────────────────────

    def retrain(self) -> dict:
        """Pull confirmed labels, retrain XGBoost, reload the scorer.

        Returns a summary dict with: status, samples_used, metrics.
        """
        with self._lock:
            return self._do_retrain()

    def _do_retrain(self) -> dict:
        from app.config import settings
        from app.data.repository import FraudCaseRepository

        repo = FraudCaseRepository()
        labeled = repo.get_confirmed_labels()

        if len(labeled) < self.MIN_FEEDBACK_TO_RETRAIN:
            return {
                "status": "skipped",
                "reason": f"Only {len(labeled)} confirmed labels — need at least {self.MIN_FEEDBACK_TO_RETRAIN}",
                "confirmed_labels": len(labeled),
            }

        logger.info("Retraining on %d confirmed labels…", len(labeled))

        try:
            import joblib
            import numpy as np
            import pandas as pd
            from xgboost import XGBClassifier

            models_dir = settings.models_dir
            metadata_path = models_dir / "metadata.json"
            with metadata_path.open() as fh:
                metadata = json.load(fh)
            feature_cols: list[str] = metadata["feature_columns"]

            # Build feature matrix from confirmed feedback
            rows, y = [], []
            for rec in labeled:
                features = rec.get("features") or {}
                if isinstance(features, str):
                    try:
                        features = json.loads(features)
                    except Exception:
                        features = {}
                row = {f: float(features.get(f, 0.0) or 0.0) for f in feature_cols}
                rows.append(row)
                y.append(1 if rec["verdict"] == "confirmed_fraud" else 0)

            X_feedback = pd.DataFrame(rows, columns=feature_cols)
            y_feedback = np.array(y)

            # Augment with synthetic data if available
            X_train, y_train = self._load_synthetic_data(feature_cols)
            if X_train is not None:
                import pandas as pd
                X_combined = pd.concat([X_train, X_feedback], ignore_index=True)
                y_combined = np.concatenate([y_train, y_feedback])
                logger.info(
                    "Augmented: %d synthetic + %d feedback = %d total",
                    len(X_train), len(X_feedback), len(X_combined),
                )
            else:
                X_combined, y_combined = X_feedback, y_feedback

            # Retrain XGBoost
            n_fraud = int(y_combined.sum())
            n_clean = len(y_combined) - n_fraud
            scale = max(1, n_clean // max(n_fraud, 1))

            model = XGBClassifier(
                n_estimators=400,
                max_depth=6,
                learning_rate=0.05,
                scale_pos_weight=scale,
                use_label_encoder=False,
                eval_metric="logloss",
                random_state=42,
            )
            model.fit(X_combined, y_combined)

            # Evaluate on feedback set
            probs = model.predict_proba(X_feedback)[:, 1]
            preds = (probs >= 0.5).astype(int)
            acc = float((preds == y_feedback).mean()) if len(y_feedback) > 0 else 0.0

            # Persist model artifact
            joblib.dump(model, models_dir / "xgboost.joblib")
            self._last_retrain = datetime.now(timezone.utc)
            metadata["training_date"] = self._last_retrain.isoformat()
            metadata["feedback_samples"] = len(labeled)
            metadata["metrics"]["xgboost_feedback"] = {"accuracy": round(acc, 4)}
            with (models_dir / "metadata.json").open("w") as fh:
                json.dump(metadata, fh, indent=2)

            # Log to MLflow
            run_id = self._mlflow_log(model, metadata, {
                "n_samples": len(X_combined),
                "n_feedback": len(labeled),
                "feedback_accuracy": acc,
                "n_features": len(feature_cols),
                "scale_pos_weight": scale,
            })

            # Reload in-process scorer
            self._reload_scorer()
            logger.info("Retrain complete — accuracy=%.3f mlflow_run=%s", acc, run_id)

            return {
                "status": "success",
                "samples_used": len(X_combined),
                "feedback_labels": len(labeled),
                "feedback_accuracy": round(acc, 4),
                "retrained_at": self._last_retrain.isoformat(),
                "mlflow_run_id": run_id,
            }

        except Exception as exc:
            logger.exception("Retrain failed: %s", exc)
            return {"status": "error", "error": str(exc)}

    def _load_synthetic_data(self, feature_cols: list[str]):
        """Try to load the original synthetic training CSV if it exists."""
        try:
            from app.config import settings
            csv_path = settings.models_dir / "training_data.csv"
            if not csv_path.exists():
                return None, None
            import pandas as pd
            df = pd.read_csv(csv_path)
            if "fraud_label" not in df.columns:
                return None, None
            X = df[[c for c in feature_cols if c in df.columns]].fillna(0.0)
            # Align columns
            for col in feature_cols:
                if col not in X.columns:
                    X[col] = 0.0
            X = X[feature_cols]
            y = df["fraud_label"].values
            return X, y
        except Exception as exc:
            logger.debug("No synthetic training data found: %s", exc)
            return None, None

    def _mlflow_log(self, model, metadata: dict, metrics: dict) -> str:
        """Log the training run to MLflow. Returns run_id or empty string on failure."""
        try:
            import mlflow
            from app.config import settings

            mlflow.set_tracking_uri(settings.mlflow_tracking_uri)
            mlflow.set_experiment(settings.mlflow_experiment_name)

            with mlflow.start_run(run_name="feedback_retrain") as run:
                # Params
                mlflow.log_param("n_estimators", 400)
                mlflow.log_param("max_depth", 6)
                mlflow.log_param("learning_rate", 0.05)
                mlflow.log_param("scale_pos_weight", metrics.get("scale_pos_weight"))
                mlflow.log_param("n_features", metrics.get("n_features"))
                mlflow.log_param("n_feedback_labels", metrics.get("n_feedback"))

                # Metrics
                mlflow.log_metric("feedback_accuracy", metrics.get("feedback_accuracy", 0))
                mlflow.log_metric("n_samples", metrics.get("n_samples", 0))

                # Model artifact
                mlflow.sklearn.log_model(model, artifact_path="xgboost_model")

                return run.info.run_id
        except Exception as exc:
            logger.warning("MLflow logging failed (non-fatal): %s", exc)
            return ""

    def get_mlflow_runs(self) -> list[dict]:
        """Return recent MLflow runs for the dashboard model registry view."""
        try:
            import mlflow
            from app.config import settings
            mlflow.set_tracking_uri(settings.mlflow_tracking_uri)
            client = mlflow.tracking.MlflowClient()
            exp = client.get_experiment_by_name(settings.mlflow_experiment_name)
            if not exp:
                return []
            runs = client.search_runs(
                experiment_ids=[exp.experiment_id],
                order_by=["start_time DESC"],
                max_results=20,
            )
            return [
                {
                    "run_id":    r.info.run_id[:12],
                    "status":    r.info.status,
                    "started":   str(r.info.start_time)[:10],
                    "accuracy":  r.data.metrics.get("feedback_accuracy", 0),
                    "n_samples": int(r.data.metrics.get("n_samples", 0)),
                    "n_feedback":int(r.data.params.get("n_feedback_labels", 0)),
                }
                for r in runs
            ]
        except Exception as exc:
            logger.warning("Could not fetch MLflow runs: %s", exc)
            return []

    def rollback_to_run(self, run_id: str) -> dict:
        """Download model artifact from an MLflow run and restore it as active model."""
        try:
            import mlflow
            from app.config import settings
            mlflow.set_tracking_uri(settings.mlflow_tracking_uri)

            import tempfile, shutil, joblib
            with tempfile.TemporaryDirectory() as tmpdir:
                model = mlflow.sklearn.load_model(f"runs:/{run_id}/xgboost_model")
                joblib.dump(model, settings.models_dir / "xgboost.joblib")

            self._reload_scorer()
            logger.info("Rolled back to MLflow run %s", run_id)
            return {"status": "success", "run_id": run_id}
        except Exception as exc:
            logger.error("Rollback failed: %s", exc)
            return {"status": "error", "error": str(exc)}

    def _reload_scorer(self) -> None:
        """Hot-reload the MultiModelScorer singleton so new model takes effect immediately.

        Builds the new instance off to the side (it does its own joblib.load()
        against the freshly written model files) then atomically swaps it in.
        In-flight requests keep using the old instance until the swap completes,
        so no thread ever reads a half-written file or races the swap.
        """
        try:
            from app.core.ml_scorer import MultiModelScorer, replace_scorer
            new_scorer = MultiModelScorer()
            if new_scorer.ready:
                replace_scorer(new_scorer)
                logger.info("MultiModelScorer reloaded — new model is active")
            else:
                logger.warning("Reloaded scorer not ready — keeping previous model active")
        except Exception as exc:
            logger.warning("Failed to reload scorer: %s", exc)


# Module-level singleton
_retrainer: RetrainingService | None = None


def get_retrainer() -> RetrainingService:
    global _retrainer
    if _retrainer is None:
        _retrainer = RetrainingService()
    return _retrainer
