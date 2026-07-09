"""
Pipeline Orchestrator — SEUL chemin de scoring du système
=============================================================
routes_scoring.py doit maintenant appeler get_pipeline().analyze(),
plus jamais recalculer son propre score. Ceci élimine la double
logique (2 formules différentes) qui existait avant.
"""
import json
import os
import time
from pathlib import Path

from app.core.rule_engine import RuleEngine
from app.core.ml_scorer import MultiModelScorer
from app.core.graph_analyzer import FraudGraphAnalyzer
from app.core.shap_explainer import SHAPExplainer
from app.core.llm_explainer import LLMExplainer
from app.schemas.fraud import FraudScoreResponse, RiskLevel, Action, ShapFactor


RULES_PATH = os.getenv("RULES_PATH", "/app/ml/rules/fraud_rules.json")

# Poids d'agrégation de l'étage 2 (Rule / ML-ensemble / Anomaly / Graph).
# Optimisés par grid-search 5-fold CV sur holdout (2025-07).
# Fallback solide quand metadata.json n'a pas de learned_weights.
WEIGHTS = {
    "rule": 0.20,
    "ml": 0.55,
    "anomaly": 0.05,
    "graph": 0.20,
}


def _load_learned_weights():
    """Charge les poids appris du metadata.json si disponibles."""
    try:
        meta_path = Path(os.getenv("MODELS_DIR", "/app/models_saved")) / "metadata.json"
        if meta_path.exists():
            with open(meta_path, "r") as f:
                data = json.load(f)
            w = data.get("learned_weights")
            if w and all(k in w for k in ("rule", "ml", "anomaly", "graph")):
                return w
    except Exception:
        pass
    return WEIGHTS


class FraudPipeline:
    def __init__(self):
        self.rule_engine = RuleEngine(RULES_PATH)
        self.ml_scorer = MultiModelScorer()
        self.graph = FraudGraphAnalyzer()
        # SHAP a besoin d'un modèle arbre unique pour l'explicabilité
        shap_base_model = self.ml_scorer.get_base_model_for_shap("random_forest")
        self.shap = SHAPExplainer(shap_base_model)
        self.llm = LLMExplainer()

    def analyze(self, features: dict) -> FraudScoreResponse:
        t0 = time.time()
        bid = features.get("beneficiary_id") or features.get("partner_id")

        # 1 — Moteur de règles
        result = self.rule_engine.evaluate_one(features)
        rule_s = float(result.rule_score)

        # 2 — Scoring ML multi-modèles (RF + XGBoost + LogReg → méta-modèle)
        ml = self.ml_scorer.score(features)
        ml_s = float(ml.get("ml_score", 0.0) or 0.0)
        anomaly_s = float(ml.get("anomaly_score", 0.0) or 0.0)

        # 3 — Analyse de réseau
        graph = self.graph.get_risk(bid)
        graph_s = float(graph.get("graph_score", 0.0) or 0.0)

        # 4 — Agrégation étage 2 (poids appris si disponibles, sinon optimisés)
        w = _load_learned_weights()
        final = round(
            w["rule"] * rule_s +
            w["ml"] * ml_s +
            w["anomaly"] * anomaly_s +
            w["graph"] * graph_s,
            3
        )

        # 5 — Niveau de risque
        if final >= 0.80:
            level, action = RiskLevel.CRITICAL, Action.BLOCK_PAYMENT
        elif final >= 0.60:
            level, action = RiskLevel.HIGH, Action.MANUAL_REVIEW
        elif final >= 0.40:
            level, action = RiskLevel.MEDIUM, Action.MONITOR
        else:
            level, action = RiskLevel.LOW, Action.CLEAR

        # 6 — SHAP (sur le modèle Random Forest, à titre indicatif)
        try:
            factors = self.shap.get_top_factors(features)
        except Exception:
            factors = []

        # 7 — Explication LLM
        try:
            explanation = self.llm.explain(
                bid, final, level.value, factors, result.triggered_flags,
            )
        except Exception:
            explanation = (
                f"Score final = {final}, niveau = {level.value}, "
                f"règles déclenchées = {result.triggered_flags}"
            )

        ms = int((time.time() - t0) * 1000)

        return FraudScoreResponse(
            beneficiary_id=bid,
            rule_score=rule_s,
            ml_score=ml_s,
            graph_score=graph_s,
            final_score=final,
            risk_level=level,
            action=action,
            rule_flags=result.triggered_flags,
            shap_factors=[ShapFactor(**f) for f in factors] if factors else [],
            explanation=explanation,
            processing_ms=ms,
            # Bonus : transparence sur les avis individuels des modèles de base
            base_model_scores=ml.get("base_model_scores", {}),
        )


_pipeline: FraudPipeline | None = None


def get_pipeline() -> FraudPipeline:
    global _pipeline
    if _pipeline is None:
        _pipeline = FraudPipeline()
    return _pipeline