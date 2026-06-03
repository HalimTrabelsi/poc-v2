import logging
from typing import Dict, Any, List, Optional
import pandas as pd
import numpy as np

try:
    import shap
    SHAP_AVAILABLE = True
except ImportError:
    SHAP_AVAILABLE = False

logger = logging.getLogger(__name__)


class SHAPExplainer:
    """
    Explicabilité SHAP pour Random Forest sklearn Pipeline.
    Extrait le clf du Pipeline pour TreeExplainer.
    Gère le préprocesseur StandardScaler (inverse transform).
    """

    def __init__(self, model: Any):
        self.model        = model
        self.explainer    = None
        self.enabled      = False
        self.feature_names: List[str] = []
        self.preprocessor = None

        if not SHAP_AVAILABLE:
            logger.warning("SHAP non installé — pip install shap")
            return

        if model is None:
            logger.warning("Modèle None fourni à SHAPExplainer")
            return

        try:
            clf = None

            # Cas 1 : sklearn Pipeline
            if hasattr(model, "named_steps"):
                if "clf" in model.named_steps:
                    clf = model.named_steps["clf"]
                if "prep" in model.named_steps:
                    self.preprocessor = model.named_steps["prep"]

            # Cas 2 : modèle brut (RandomForest, XGBoost...)
            elif hasattr(model, "estimators_") or hasattr(model, "get_booster"):
                clf = model

            if clf is None:
                logger.warning(f"Type modèle non supporté : {type(model)}")
                return

            # Extraction feature names
            if hasattr(clf, "feature_names_in_"):
                self.feature_names = list(clf.feature_names_in_)
            elif self.preprocessor is not None and hasattr(self.preprocessor, "get_feature_names_out"):
                self.feature_names = list(self.preprocessor.get_feature_names_out())

            self.explainer = shap.TreeExplainer(clf)
            self.enabled   = True
            logger.info(f"SHAP TreeExplainer OK — {len(self.feature_names)} features")

        except Exception as e:
            logger.error(f"Erreur init SHAP : {e}")
            self.enabled = False

    def _prepare_input(self, features: Dict[str, Any]) -> Optional[pd.DataFrame]:
        """Prépare et préprocesse les features pour SHAP."""
        try:
            if not self.feature_names:
                self.feature_names = list(features.keys())

            row = {feat: float(features.get(feat, 0) or 0) for feat in self.feature_names}
            X   = pd.DataFrame([row], columns=self.feature_names)

            # Appliquer le même préprocesseur que pendant l'entraînement
            if self.preprocessor is not None:
                try:
                    X_arr = self.preprocessor.transform(X)
                    X     = pd.DataFrame(X_arr, columns=self.feature_names)
                except Exception:
                    pass  # Si transform échoue, utiliser les valeurs brutes

            return X

        except Exception as e:
            logger.error(f"Erreur préparation input SHAP : {e}")
            return None

    def get_top_factors(self, features: Dict[str, Any], top_n: int = 5) -> List[Dict]:
        """
        Retourne les top N features les plus influentes.

        Returns:
            [{"feature": str, "value": Any, "impact": float, "direction": str}, ...]
        """
        if not self.enabled or self.explainer is None:
            return []

        try:
            X = self._prepare_input(features)
            if X is None:
                return []

            shap_values = self.explainer.shap_values(X)

            # Gestion format : list (binary clf) ou array
            if isinstance(shap_values, list):
                # shap_values[1] = classe positive (fraude)
                values = shap_values[1][0] if len(shap_values) == 2 else shap_values[0][0]
            elif isinstance(shap_values, np.ndarray):
                if shap_values.ndim == 3:
                    values = shap_values[0, :, 1]  # (samples, features, classes)
                else:
                    values = shap_values[0]
            else:
                values = shap_values[0]

            impacts = []
            for i, feat in enumerate(self.feature_names):
                if i >= len(values):
                    break

                impact     = float(values[i])
                abs_impact = abs(impact)

                if abs_impact < 0.005:
                    continue

                impacts.append({
                    "feature":   feat,
                    "value":     features.get(feat),
                    "impact":    round(abs_impact, 4),
                    # Signed SHAP under the canonical key so the LLM prompt and
                    # PDF report (both read 'shap_value') stay consistent with
                    # services/explainability_service.py.
                    "shap_value": round(impact, 4),
                    "direction": "increases_risk" if impact > 0 else "decreases_risk",
                })

            impacts.sort(key=lambda x: x["impact"], reverse=True)
            return impacts[:top_n]

        except Exception as e:
            logger.error(f"Erreur calcul SHAP : {e}")
            return []