"""Pipeline Orchestrator — Rule Engine + ML + Graph + XAI + LLM"""
import os
import time

from app.core.rule_engine import RuleEngine
from app.core.ml_scorer import MLScorer
from app.core.graph_analyzer import FraudGraphAnalyzer
from app.core.shap_explainer import SHAPExplainer
from app.core.llm_explainer import LLMExplainer
from app.schemas.fraud import FraudScoreResponse, RiskLevel, Action, ShapFactor


RULES_PATH = os.getenv("RULES_PATH", "/app/ml/rules/fraud_rules.json")


class FraudPipeline:
    def __init__(self):
        self.rule_engine = RuleEngine(RULES_PATH)
        self.ml_scorer = MLScorer(model_name="random_forest")
        self.graph = FraudGraphAnalyzer()
        self.shap = SHAPExplainer(
            self.ml_scorer.model if self.ml_scorer.ready else None
        )
        self.llm = LLMExplainer()

    def analyze(self, features: dict) -> FraudScoreResponse:
        t0 = time.time()
        bid = features.get("beneficiary_id") or features.get("partner_id")

        # 1 — Rule Engine
        result = self.rule_engine.evaluate_one(features)

        # 2 — ML Scoring
        ml = self.ml_scorer.score(features)

        # 3 — Graph Analysis
        graph = self.graph.get_risk(bid)
        graph_s = float(graph.get("graph_score", 0.0) or 0.0)

        # 4 — Aggregate scores
        rule_s = float(result.rule_score)
        ml_s = float(ml.get("ml_score", 0.0) or 0.0)
        anomaly_s = float(ml.get("anomaly_score", 0.0) or 0.0)

        # Tu peux ajuster plus tard cette formule
        final = round(
            0.20 * rule_s +
            0.50 * ml_s +
            0.10 * anomaly_s +
            0.20 * graph_s,
            3
        )

        # 5 — Risk level + Action
        if final >= 0.80:
            level, action = RiskLevel.CRITICAL, Action.BLOCK_PAYMENT
        elif final >= 0.60:
            level, action = RiskLevel.HIGH, Action.MANUAL_REVIEW
        elif final >= 0.40:
            level, action = RiskLevel.MEDIUM, Action.MONITOR
        else:
            level, action = RiskLevel.LOW, Action.CLEAR

        # 6 — SHAP explanations
        factors = []
        try:
            factors = self.shap.get_top_factors(features)
        except Exception:
            factors = []

        # 7 — LLM explanation
        try:
            explanation = self.llm.explain(
                bid,
                final,
                level.value,
                factors,
                result.triggered_flags,
            )
        except Exception:
            explanation = (
                f"Fraud risk analysis completed for beneficiary {bid}. "
                f"Final score={final}, level={level.value}, "
                f"flags={result.triggered_flags}"
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
        )


_pipeline: FraudPipeline | None = None


def get_pipeline() -> FraudPipeline:
    global _pipeline
    if _pipeline is None:
        _pipeline = FraudPipeline()
    return _pipeline