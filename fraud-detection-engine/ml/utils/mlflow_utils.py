"""
MLflow utilities for experiment tracking, metrics logging, and artifact management.

Provides a clean interface to log models, metrics, datasets, and SHAP artifacts
to MLflow for reproducibility and visualization.
"""
import json
import logging
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import mlflow
import numpy as np
import pandas as pd
from sklearn.metrics import (
    auc,
    confusion_matrix,
    precision_recall_curve,
    roc_curve,
)

logger = logging.getLogger(__name__)

# MLflow allows: alphanumerics, underscores (_), dashes (-), periods (.),
# spaces ( ), colons (:) and slashes (/). Everything else must be replaced
# or the log-metric / log-param API call fails with INVALID_PARAMETER_VALUE.
_INVALID_KEY_CHARS = re.compile(r"[^A-Za-z0-9_./ :-]")


def sanitize_key(key: str) -> str:
    """Make a metric/param key safe for the MLflow tracking API.

    Replaces parentheses, accents, and other disallowed characters with
    underscores so that model names like "Logistic Regression (baseline)"
    don't blow up the run.
    """
    return _INVALID_KEY_CHARS.sub("_", key).strip("_") or "unnamed"


class MLflowExperiment:
    """Context manager for MLflow experiment runs with automatic cleanup."""

    def __init__(
        self,
        experiment_name: str,
        run_name: Optional[str] = None,
        tags: Optional[Dict[str, str]] = None,
    ):
        """Initialize MLflow experiment context.

        Args:
            experiment_name: Experiment name (e.g., "fraud_detection_xgboost")
            run_name: Optional custom run name (auto-generated if None)
            tags: Optional dict of tags to attach to run (e.g., {"dataset": "openg2p"})
        """
        self.experiment_name = experiment_name
        self.run_name = run_name or f"{experiment_name}_{datetime.now():%Y%m%d_%H%M%S}"
        self.tags = tags or {}

    def __enter__(self):
        """Start MLflow run."""
        mlflow.set_experiment(self.experiment_name)
        self.run = mlflow.start_run(run_name=self.run_name)
        for key, value in self.tags.items():
            mlflow.set_tag(key, value)
        logger.info(f"Started MLflow run: {self.run_name} in experiment {self.experiment_name}")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """End MLflow run."""
        if exc_type is not None:
            mlflow.set_tag("status", "failed")
            logger.error(f"Run failed: {exc_type.__name__}: {exc_val}")
        else:
            mlflow.set_tag("status", "success")
        mlflow.end_run()
        logger.info(f"Ended MLflow run: {self.run.info.run_id}")


def log_params(params: Dict[str, Any]) -> None:
    """Log hyperparameters (keys are sanitized for the MLflow API)."""
    safe = {sanitize_key(str(k)): v for k, v in params.items()}
    mlflow.log_params(safe)
    logger.info(f"Logged {len(safe)} parameters")


def log_metrics(metrics: Dict[str, float]) -> None:
    """Log evaluation metrics.

    Keys are sanitized for the MLflow API. Non-scalar values (lists, arrays
    such as a confusion matrix) are skipped — metrics must be single numbers.
    """
    safe = {}
    skipped = []
    for k, v in metrics.items():
        if isinstance(v, bool) or not isinstance(v, (int, float)):
            skipped.append(k)
            continue
        safe[sanitize_key(str(k))] = float(v)
    if safe:
        mlflow.log_metrics(safe)
    logger.info(f"Logged {len(safe)} metrics"
                + (f" (skipped non-scalar: {skipped})" if skipped else ""))


def log_model(
    model: Any,
    artifact_path: str = "model",
    framework: str = "sklearn",
) -> None:
    """Log a trained model to MLflow.

    Args:
        model: Trained model (sklearn, xgboost, etc.)
        artifact_path: Where to store in MLflow (e.g., "model" → artifacts/model/)
        framework: Framework name (sklearn, xgboost, lightgbm)
    """
    if framework == "xgboost":
        mlflow.xgboost.log_model(model, artifact_path=artifact_path)
    else:
        mlflow.sklearn.log_model(model, artifact_path=artifact_path)
    logger.info(f"Logged {framework} model to {artifact_path}")


def log_dataset_info(
    X_train: pd.DataFrame,
    X_val: pd.DataFrame,
    X_test: pd.DataFrame,
    y_train: np.ndarray,
    y_val: np.ndarray,
    y_test: np.ndarray,
    dataset_name: str = "unknown",
) -> None:
    """Log dataset statistics and summary.

    Args:
        X_train, X_val, X_test: Feature dataframes
        y_train, y_val, y_test: Target arrays
        dataset_name: Name of the dataset
    """
    dataset_info = {
        "dataset_name": dataset_name,
        "n_features": X_train.shape[1],
        "feature_names": X_train.columns.tolist(),
        "train_rows": X_train.shape[0],
        "val_rows": X_val.shape[0],
        "test_rows": X_test.shape[0],
        "total_rows": X_train.shape[0] + X_val.shape[0] + X_test.shape[0],
        "fraud_rate_train": float(y_train.mean()),
        "fraud_rate_val": float(y_val.mean()),
        "fraud_rate_test": float(y_test.mean()),
    }

    mlflow.log_dict(dataset_info, "dataset_info.json")
    logger.info(f"Logged dataset info: {dataset_info['total_rows']} rows, "
                f"{dataset_info['fraud_rate_train']:.1%} fraud")

    # Log feature statistics
    feature_stats = {}
    for col in X_train.columns:
        feature_stats[col] = {
            "mean": float(X_train[col].mean()),
            "std": float(X_train[col].std()),
            "min": float(X_train[col].min()),
            "max": float(X_train[col].max()),
            "nulls": int(X_train[col].isna().sum()),
        }
    mlflow.log_dict(feature_stats, "feature_statistics.json")


def log_confusion_matrix_metrics(
    y_true: np.ndarray,
    y_pred: np.ndarray,
    y_proba: np.ndarray,
) -> Dict[str, float]:
    """Calculate and log confusion matrix metrics.

    Args:
        y_true: True labels
        y_pred: Predicted labels (binary: 0/1)
        y_proba: Predicted probabilities (fraud class)

    Returns:
        Dict of computed metrics
    """
    tn, fp, fn, tp = confusion_matrix(y_true, y_pred).ravel()

    metrics = {
        "confusion_matrix/true_positives": float(tp),
        "confusion_matrix/false_positives": float(fp),
        "confusion_matrix/true_negatives": float(tn),
        "confusion_matrix/false_negatives": float(fn),
        "accuracy": float((tp + tn) / (tp + tn + fp + fn)),
        "precision": float(tp / (tp + fp)) if (tp + fp) > 0 else 0.0,
        "recall": float(tp / (tp + fn)) if (tp + fn) > 0 else 0.0,
        "specificity": float(tn / (tn + fp)) if (tn + fp) > 0 else 0.0,
    }

    # Add F1
    precision = metrics["precision"]
    recall = metrics["recall"]
    metrics["f1_score"] = float(2 * (precision * recall) / (precision + recall)) \
        if (precision + recall) > 0 else 0.0

    # ROC-AUC and PR-AUC
    fpr, tpr, _ = roc_curve(y_true, y_proba)
    metrics["roc_auc"] = float(auc(fpr, y_proba))

    precision_curve, recall_curve, _ = precision_recall_curve(y_true, y_proba)
    metrics["pr_auc"] = float(auc(recall_curve, precision_curve))

    mlflow.log_metrics(metrics)
    logger.info(f"Logged metrics: F1={metrics['f1_score']:.3f}, "
                f"ROC-AUC={metrics['roc_auc']:.3f}, PR-AUC={metrics['pr_auc']:.3f}")

    return metrics


def log_feature_importance(
    feature_names: List[str],
    importances: Dict[str, float],
    model_type: str = "xgboost",
) -> None:
    """Log feature importance scores.

    Args:
        feature_names: List of feature names
        importances: Dict of {feature_name: importance_score}
        model_type: Type of model (xgboost, random_forest, lightgbm)
    """
    # Sort by importance descending
    sorted_importance = sorted(importances.items(), key=lambda x: x[1], reverse=True)

    importance_dict = {
        "model_type": model_type,
        "top_features": [f[0] for f in sorted_importance[:10]],
        "feature_importance": {name: float(score) for name, score in sorted_importance},
    }

    mlflow.log_dict(importance_dict, "feature_importance.json")
    logger.info(f"Logged feature importance: top feature = {sorted_importance[0][0]}")


def log_shap_summary(
    shap_values: np.ndarray,
    feature_names: List[str],
    X_test: pd.DataFrame,
) -> None:
    """Log SHAP summary statistics and top features.

    Args:
        shap_values: SHAP values array (n_samples, n_features)
        feature_names: Feature names
        X_test: Test feature data
    """
    # Mean absolute SHAP values
    shap_mean = np.abs(shap_values).mean(axis=0)
    shap_summary = {
        "feature_names": feature_names,
        "shap_mean": [float(v) for v in shap_mean],
        "shap_std": [float(np.std(shap_values[:, i])) for i in range(len(feature_names))],
        "top_features_by_shap": sorted(
            zip(feature_names, shap_mean),
            key=lambda x: x[1],
            reverse=True
        )[:10],
    }

    mlflow.log_dict(shap_summary, "shap_summary.json")
    logger.info(f"Logged SHAP summary: top feature = {shap_summary['top_features_by_shap'][0][0]}")


def log_calibration_metrics(
    y_true: np.ndarray,
    y_proba: np.ndarray,
) -> Dict[str, float]:
    """Log calibration curve data and metrics.

    Args:
        y_true: True labels
        y_proba: Predicted probabilities

    Returns:
        Dict of calibration metrics (Expected Calibration Error, etc.)
    """
    from sklearn.metrics import log_loss
    from sklearn.calibration import calibration_curve

    # Brier score (MSE between predicted and true)
    brier_score = np.mean((y_proba - y_true) ** 2)

    # Log loss (cross-entropy)
    log_loss_score = log_loss(y_true, y_proba)

    # Expected Calibration Error (ECE)
    prob_true, prob_pred = calibration_curve(y_true, y_proba, n_bins=10)
    ece = np.mean(np.abs(prob_true - prob_pred))

    metrics = {
        "calibration/brier_score": float(brier_score),
        "calibration/log_loss": float(log_loss_score),
        "calibration/expected_calibration_error": float(ece),
    }

    mlflow.log_metrics(metrics)
    logger.info(f"Logged calibration: Brier={brier_score:.3f}, ECE={ece:.3f}")

    return metrics


def compare_runs(experiment_name: str, n_runs: int = 5) -> pd.DataFrame:
    """Retrieve and compare recent runs from an experiment.

    Args:
        experiment_name: Experiment name
        n_runs: Number of recent runs to retrieve

    Returns:
        DataFrame with run parameters and metrics
    """
    experiment = mlflow.get_experiment_by_name(experiment_name)
    if not experiment:
        logger.warning(f"Experiment {experiment_name} not found")
        return pd.DataFrame()

    runs = mlflow.search_runs(
        experiment_ids=[experiment.experiment_id],
        order_by=["start_time DESC"],
        max_results=n_runs,
    )
    logger.info(f"Retrieved {len(runs)} runs from {experiment_name}")
    return runs
