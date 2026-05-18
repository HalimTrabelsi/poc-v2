"""Model evaluation utilities."""
import logging
from pathlib import Path

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


class ModelEvaluator:
    """Compute metrics and produce evaluation artefacts for a trained model."""

    def evaluate_model(self, model, X_test, y_test) -> dict:
        """Return a comprehensive metrics dict.

        Returns:
            Dict with accuracy, precision, recall, f1, roc_auc, confusion_matrix.
        """
        from sklearn.metrics import (
            accuracy_score, confusion_matrix, f1_score,
            precision_score, recall_score, roc_auc_score,
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

    def generate_report(self, metrics: dict, output_dir: Path) -> None:
        """Write a plain-text classification report to output_dir/report.txt."""
        output_dir.mkdir(parents=True, exist_ok=True)
        report_path = output_dir / "report.txt"
        lines = [
            "=== Model Evaluation Report ===",
            f"Accuracy:  {metrics.get('accuracy', 0):.4f}",
            f"Precision: {metrics.get('precision', 0):.4f}",
            f"Recall:    {metrics.get('recall', 0):.4f}",
            f"F1:        {metrics.get('f1', 0):.4f}",
            f"ROC-AUC:   {metrics.get('roc_auc', 0):.4f}",
            "",
            "Confusion Matrix:",
            str(metrics.get("confusion_matrix", [])),
        ]
        report_path.write_text("\n".join(lines), encoding="utf-8")
        logger.info("Report written to %s", report_path)

    def plot_roc_curve(self, model, X_test, y_test, output_path: Path) -> None:
        """Save a ROC curve PNG to output_path."""
        try:
            import matplotlib.pyplot as plt
            from sklearn.metrics import RocCurveDisplay

            fig, ax = plt.subplots(figsize=(6, 5))
            RocCurveDisplay.from_estimator(model, X_test, y_test, ax=ax)
            ax.set_title("ROC Curve")
            fig.tight_layout()
            output_path.parent.mkdir(parents=True, exist_ok=True)
            fig.savefig(output_path, dpi=120)
            plt.close(fig)
            logger.info("ROC curve saved to %s", output_path)
        except ImportError:
            logger.warning("matplotlib not installed; skipping ROC curve plot")

    def plot_feature_importance(
        self, model, feature_names: list[str], output_path: Path
    ) -> None:
        """Save a horizontal bar chart of feature importances."""
        try:
            import matplotlib.pyplot as plt

            importances: np.ndarray | None = None
            if hasattr(model, "feature_importances_"):
                importances = model.feature_importances_
            elif hasattr(model, "coef_"):
                importances = np.abs(model.coef_[0])

            if importances is None:
                logger.warning("Model has no feature importance attribute")
                return

            indices = np.argsort(importances)[::-1]
            sorted_names = [feature_names[i] for i in indices]
            sorted_vals = importances[indices]

            fig, ax = plt.subplots(figsize=(8, 6))
            ax.barh(sorted_names[:20][::-1], sorted_vals[:20][::-1])
            ax.set_xlabel("Importance")
            ax.set_title("Feature Importances (top 20)")
            fig.tight_layout()
            output_path.parent.mkdir(parents=True, exist_ok=True)
            fig.savefig(output_path, dpi=120)
            plt.close(fig)
            logger.info("Feature importance chart saved to %s", output_path)
        except ImportError:
            logger.warning("matplotlib not installed; skipping feature importance plot")
