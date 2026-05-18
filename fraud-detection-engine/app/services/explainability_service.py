"""Transform ML/Rule outputs into human-readable explanations."""
import logging
from typing import Any, Optional

import numpy as np

logger = logging.getLogger(__name__)

_RISK_SUMMARIES = {
    "CRITICAL": "This beneficiary shows multiple strong fraud indicators requiring immediate action.",
    "HIGH": "Significant fraud signals detected. Manual review is strongly recommended.",
    "MEDIUM": "Moderate risk indicators present. The case should be monitored closely.",
    "LOW": "No significant fraud signals detected. Beneficiary appears legitimate.",
}


class Explainer:
    """Generate structured explanations from a decision result dict."""

    def explain(self, decision_result: dict) -> dict:
        """Generate explanation for a stored or freshly computed decision.

        Args:
            decision_result: The dict returned by DecisionOrchestrator or the repository.

        Returns:
            Dict with summary, top_reasons, rule_explanations, feature_contributions.
        """
        risk_level = str(decision_result.get("risk_level", "LOW"))
        triggered_rules = decision_result.get("rules_triggered", []) or []
        features = decision_result.get("features", {}) or {}
        model = decision_result.get("model")

        rule_explanations = self._explain_rules(triggered_rules)

        shap_factors: list[dict] = []
        if model is not None and features:
            try:
                shap_factors = self._explain_shap(features, model)
            except Exception as exc:
                logger.warning("SHAP explanation failed: %s", exc)

        feature_contributions = shap_factors or self._explain_from_features(features)

        top_reasons = [r["explanation"] for r in triggered_rules[:3]]
        top_reasons += [
            f"{fc['feature']} = {fc['value']:.2f} ({fc['direction']})"
            for fc in feature_contributions[:3]
            if fc["feature"] not in {r.get("rule_id") for r in triggered_rules}
        ]

        summary = self._synthesize_summary(rule_explanations, shap_factors, risk_level)

        return {
            "summary": summary,
            "top_reasons": top_reasons[:5],
            "rule_explanations": rule_explanations,
            "feature_contributions": feature_contributions,
        }

    def _explain_rules(self, triggered_rules: list) -> list[str]:
        """Convert triggered rule dicts to human-readable strings."""
        explanations: list[str] = []
        for rule in triggered_rules:
            if isinstance(rule, dict):
                text = rule.get("explanation") or rule.get("name") or str(rule)
                explanations.append(f"[{rule.get('rule_id', '?')}] {text}")
        return explanations

    def _explain_shap(self, features: dict, model) -> list[dict]:
        """Compute SHAP values using TreeExplainer and return top-5 features.

        Args:
            features: Feature name → value mapping.
            model: Trained scikit-learn / XGBoost compatible model.

        Returns:
            List of dicts sorted by absolute SHAP value descending.
        """
        import shap
        import pandas as pd

        feature_names = list(features.keys())
        X = pd.DataFrame([features])[feature_names]

        try:
            explainer = shap.TreeExplainer(model)
            shap_values = explainer.shap_values(X)

            # For binary classifiers shap_values may be a list [class0, class1]
            if isinstance(shap_values, list):
                vals = shap_values[1][0]
            else:
                vals = shap_values[0]
        except Exception:
            # Fall back to LinearExplainer for non-tree models
            explainer = shap.LinearExplainer(model, X)
            shap_values = explainer.shap_values(X)
            vals = shap_values[0] if isinstance(shap_values, list) else shap_values[0]

        contributions = [
            {
                "feature": name,
                "value": float(features[name]),
                "shap_value": float(sv),
                "direction": "increases_risk" if sv > 0 else "decreases_risk",
            }
            for name, sv in zip(feature_names, vals)
        ]

        contributions.sort(key=lambda x: abs(x["shap_value"]), reverse=True)
        return contributions[:5]

    def _explain_from_features(self, features: dict) -> list[dict]:
        """Fallback when SHAP is unavailable: rank by heuristic importance."""
        high_importance = [
            "network_risk", "shared_account_count", "shared_phone_count",
            "payment_gap_ratio", "nb_programs", "pmt_score", "income_per_person",
        ]
        result: list[dict] = []
        for feat in high_importance:
            if feat in features:
                val = float(features[feat])
                result.append(
                    {
                        "feature": feat,
                        "value": val,
                        "shap_value": 0.0,
                        "direction": "increases_risk" if val > 0 else "decreases_risk",
                    }
                )
        return result[:5]

    def _synthesize_summary(
        self,
        rule_explanations: list[str],
        shap_factors: list[dict],
        risk_level: str,
    ) -> str:
        """Build a single natural-language summary sentence."""
        base = _RISK_SUMMARIES.get(risk_level, _RISK_SUMMARIES["LOW"])
        if rule_explanations:
            top_rule = rule_explanations[0]
            base += f" Key trigger: {top_rule}"
        elif shap_factors:
            top_feat = shap_factors[0]["feature"]
            base += f" Primary risk driver: {top_feat}."
        return base
