"""SHAP Explainer — Feature importance"""
import pandas as pd

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

FEATURE_LABELS = {
    "nb_programs":           "Nombre de programmes",
    "total_amount":          "Montant total recu",
    "amount_ratio":          "Ratio montant/moyenne zone",
    "nb_cycles":             "Nombre de cycles",
    "days_since_enrollment": "Jours depuis inscription",
    "account_changes_30d":   "Changements compte (30j)",
    "household_size":        "Taille du menage",
    "nb_payment_failures":   "Echecs de paiement",
    "location_risk_score":   "Score risque localisation",
}


class SHAPExplainer:
    def __init__(self, model):
        self.explainer = None
        if model is not None:
            try:
                import shap
                self.explainer = shap.TreeExplainer(model)
            except Exception:
                pass

    def get_top_factors(self, features: dict, n: int = 3) -> list[dict]:
        if self.explainer is None:
            return self._fallback_factors(features, n)

        try:
            import shap
            X  = pd.DataFrame([{f: features.get(f, 0) for f in FEATURES}])
            sv = self.explainer.shap_values(X)[0]
            pairs = sorted(
                zip(FEATURES, sv),
                key=lambda x: abs(x[1]),
                reverse=True
            )
            return [
                {
                    "feature":   FEATURE_LABELS.get(f, f),
                    "impact":    round(float(v), 3),
                    "direction": "+" if v > 0 else "-",
                }
                for f, v in pairs[:n]
            ]
        except Exception:
            return self._fallback_factors(features, n)

    def _fallback_factors(self, features: dict, n: int) -> list[dict]:
        """Fallback when SHAP not available — rank by feature value"""
        scores = {
            FEATURE_LABELS.get(f, f): features.get(f, 0)
            for f in FEATURES
        }
        top = sorted(scores.items(), key=lambda x: abs(x[1]), reverse=True)
        return [
            {"feature": k, "impact": round(float(v), 3),
             "direction": "+" if v > 0 else "-"}
            for k, v in top[:n]
        ]