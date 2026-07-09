"""
ml_scorer.py — Scoring multi-agent avec méta-modèle appris
=============================================================
Remplace l'ancien MLScorer mono-modèle (Random Forest seul).
Charge 3 modèles de base (RF, XGBoost, LogReg) + un méta-modèle
qui combine leurs prédictions avec des poids APPRIS (pas fixés à la main).
"""
import json
import os
import threading
from pathlib import Path
from typing import Any, Dict, Optional

import joblib
import numpy as np
import pandas as pd


MODELS_DIR = Path(os.getenv("MODELS_DIR", "/app/models_saved"))
METADATA_FILE = MODELS_DIR / "metadata.json"


class MultiModelScorer:
    """
    Remplace MLScorer. Charge tous les modèles de base + le méta-modèle,
    et produit un score final = combinaison apprise des 3 modèles.
    """

    def __init__(self):
        self.ready = False
        self.base_models: Dict[str, Any] = {}
        self.meta_model = None
        self.meta_model_names: list[str] = []
        self.features: list[str] = []
        self.iso_model = None
        self._load()

    def _load(self):
        try:
            if not METADATA_FILE.exists():
                raise FileNotFoundError(
                    f"metadata.json introuvable à {METADATA_FILE}. "
                    "Lancez ml/scripts/train_openg2p.py d'abord."
                )

            with open(METADATA_FILE, "r", encoding="utf-8") as f:
                metadata = json.load(f)

            self.features = metadata.get("feature_columns", [])
            base_model_names = metadata.get("base_models", [])
            self.meta_model_names = metadata.get("meta_model_input_order", [])

            if not self.features or not base_model_names or not self.meta_model_names:
                raise ValueError(
                    "metadata.json incomplet — relancez l'entraînement avec "
                    "le nouveau train_openg2p.py (architecture multi-modèles)."
                )

            # Charger chaque modèle de base
            for name in base_model_names:
                model_path = MODELS_DIR / f"{name}.joblib"
                if not model_path.exists():
                    raise FileNotFoundError(f"Modèle manquant : {model_path}")
                self.base_models[name] = joblib.load(model_path)
                print(f"[MultiModelScorer] Modèle de base chargé : {name}")

            # Charger le méta-modèle (l'arbitre)
            meta_path = MODELS_DIR / "meta_ensemble.joblib"
            if not meta_path.exists():
                raise FileNotFoundError(f"Méta-modèle manquant : {meta_path}")
            self.meta_model = joblib.load(meta_path)
            print(f"[MultiModelScorer] Méta-modèle chargé : meta_ensemble.joblib")
            print(f"[MultiModelScorer] Ordre d'entrée : {self.meta_model_names}")

            # Isolation Forest (anomalie, indépendant)
            iso_path = MODELS_DIR / "isolation_forest.joblib"
            if iso_path.exists():
                self.iso_model = joblib.load(iso_path)
                print(f"[MultiModelScorer] Isolation Forest chargé")

            self.ready = True
            print(f"[MultiModelScorer] ✅ Prêt — {len(self.base_models)} modèles + méta-modèle")

        except Exception as e:
            self.ready = False
            print(f"[MultiModelScorer] ❌ Échec du chargement : {e}")

    def _build_dataframe(self, payload: Dict[str, Any]) -> pd.DataFrame:
        row = {
            feat: (0 if payload.get(feat) is None else payload.get(feat))
            for feat in self.features
        }
        return pd.DataFrame([row], columns=self.features).fillna(0)

    def score(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        if not self.ready:
            return {
                "ready": False, "ml_score": 0.0, "anomaly_score": 0.0,
                "error": "MultiModelScorer non initialisé. Vérifiez les logs.",
            }

        try:
            X = self._build_dataframe(payload)

            # 1 — Chaque modèle de base donne son avis
            base_scores: Dict[str, float] = {}
            for name, model in self.base_models.items():
                proba = float(model.predict_proba(X)[0][1])
                base_scores[name] = proba

            # 2 — Le méta-modèle combine les avis (poids appris, PAS 0.7/0.2/0.1 à la main)
            S = np.array([[base_scores[name] for name in self.meta_model_names]])
            ml_score = float(self.meta_model.predict_proba(S)[0][1])
            ml_prediction = int(ml_score >= 0.5)

            result: Dict[str, Any] = {
                "ready": True,
                "ml_score": round(ml_score, 6),
                "ml_prediction": ml_prediction,
                "base_model_scores": {k: round(v, 6) for k, v in base_scores.items()},
                "features_used": self.features,
                "anomaly_score": 0.0,
            }

            # 3 — Isolation Forest (score d'anomalie, indépendant du méta-modèle)
            if self.iso_model is not None:
                raw_score = float(self.iso_model.score_samples(X)[0])
                anomaly_score = round(max(0.0, min(1.0, (-raw_score - 0.1) / 0.4)), 6)
                result["anomaly_score"] = anomaly_score

            return result

        except Exception as e:
            return {
                "ready": False, "ml_score": 0.0, "anomaly_score": 0.0,
                "error": str(e),
            }

    def get_base_model_for_shap(self, name: str = "random_forest"):
        """
        SHAP a besoin d'UN modèle arbre spécifique (TreeExplainer).
        On expose le Random Forest (ou XGBoost) pour l'explicabilité,
        même si le score final vient du méta-modèle.
        """
        return self.base_models.get(name)


# Alias rétrocompatible — au cas où d'autres fichiers importent encore MLScorer
MLScorer = MultiModelScorer


# ---------------------------------------------------------------------------
# Singleton accessor
# ---------------------------------------------------------------------------
# MultiModelScorer.__init__ runs several joblib.load() calls against files on
# disk. Constructing it fresh per request (as callers used to do) meant
# concurrent threads (CSV batch ThreadPoolExecutor, scanner LISTEN/poll
# threads, retrain background task) called native joblib/xgboost/numpy code
# concurrently against the same files, corrupting the C allocator's heap
# (glibc "free(): invalid next size (fast)"). This lock + cached instance
# ensures the model set is loaded exactly once and reused everywhere.
_instance: Optional["MultiModelScorer"] = None
_lock = threading.Lock()


def get_scorer() -> "MultiModelScorer":
    global _instance
    if _instance is None:
        with _lock:
            if _instance is None:
                _instance = MultiModelScorer()
    return _instance


def replace_scorer(new_instance: "MultiModelScorer") -> None:
    """Atomically swap in a freshly retrained scorer.

    Callers must fully construct new_instance (which loads its own models)
    before calling this, so in-flight scoring against the old instance is
    never interrupted mid-read.
    """
    global _instance
    with _lock:
        _instance = new_instance