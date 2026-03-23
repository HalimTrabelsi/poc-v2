"""Pipeline Orchestrator — Rule Engine + ML + Graph + XAI + LLM"""
import time
import os

from app.core.rule_engine    import RuleEngine
from app.core.ml_scorer      import MLScorer
from app.core.graph_analyzer import FraudGraphAnalyzer
from app.core.shap_explainer import SHAPExplainer
from app.core.llm_explainer  import LLMExplainer
from app.schemas.fraud       import (
    FraudScoreResponse, RiskLevel, Action, ShapFactor
)

RULES_PATH  = os.getenv("RULES_PATH",  "ml/rules/fraud_rules.json")
MODELS_DIR  = os.getenv("MODELS_DIR",  "models_saved")


class FraudPipeline:
    def __init__(self):
        self.rule_engine = RuleEngine(RULES_PATH)
        self.ml_scorer   = MLScorer(MODELS_DIR)
        self.graph       = FraudGraphAnalyzer()
        self.shap        = SHAPExplainer(
            self.ml_scorer.xgb if self.ml_scorer.ready else None
        )
        self.llm         = LLMExplainer()

    def analyze(self, features: dict) -> FraudScoreResponse:
        t0  = time.time()
        bid = features["beneficiary_id"]

        # 1 — Rule Engine
        rules = self.rule_engine.evaluate(features)

        # 2 — ML Scoring
        ml = self.ml_scorer.score(features)

        # 3 — Graph Analysis
        graph = self.graph.get_risk(bid)

        # 4 — Aggregate scores (Rule 25% + ML 50% + Graph 25%)
        rule_s  = rules["rule_score"]
        ml_s    = ml["ml_score"]
        graph_s = graph["graph_score"]
        final   = round(0.25 * rule_s + 0.50 * ml_s + 0.25 * graph_s, 3)

        # 5 — Risk level + Action
        if final > 0.80:
            level, action = RiskLevel.CRITICAL, Action.BLOCK_PAYMENT
        elif final > 0.60:
            level, action = RiskLevel.HIGH,     Action.MANUAL_REVIEW
        elif final > 0.40:
            level, action = RiskLevel.MEDIUM,   Action.MONITOR
        else:
            level, action = RiskLevel.LOW,      Action.CLEAR

        # 6 — SHAP explanations
        factors = self.shap.get_top_factors(features)

        # 7 — LLM natural language explanation
        explanation = self.llm.explain(
            bid, final, level.value, factors, rules["flags"]
        )

        ms = int((time.time() - t0) * 1000)

        return FraudScoreResponse(
            beneficiary_id = bid,
            rule_score     = rule_s,
            ml_score       = ml_s,
            graph_score    = graph_s,
            final_score    = final,
            risk_level     = level,
            action         = action,
            rule_flags     = rules["flags"],
            shap_factors   = [ShapFactor(**f) for f in factors],
            explanation    = explanation,
            processing_ms  = ms,
        )


# Singleton
_pipeline: FraudPipeline | None = None


def get_pipeline() -> FraudPipeline:
    global _pipeline
    if _pipeline is None:
        _pipeline = FraudPipeline()
    return _pipeline