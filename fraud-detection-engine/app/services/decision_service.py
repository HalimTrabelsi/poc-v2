"""Orchestrate all services for a complete fraud decision."""
import logging
import time
from datetime import date
from typing import Optional

from app.config import settings
from app.data.extractors import BeneficiaryExtractor, RelationshipExtractor
from app.data.repository import FraudCaseRepository
from app.services.explainability_service import Explainer
from app.services.features_service import FeatureEngineer
from app.services.graph_service import GraphAnalyzer
from app.services.ml_service import MLScorer
from app.services.rules_service import RuleService

logger = logging.getLogger(__name__)


def _determine_risk_level(score: float) -> str:
    if score >= settings.critical_threshold:
        return "CRITICAL"
    if score >= settings.high_threshold:
        return "HIGH"
    if score >= settings.medium_threshold:
        return "MEDIUM"
    return "LOW"


def _determine_recommendation(risk_level: str) -> str:
    mapping = {
        "CRITICAL": "BLOCK_PAYMENT",
        "HIGH": "MANUAL_REVIEW",
        "MEDIUM": "MONITOR",
        "LOW": "CLEAR",
    }
    return mapping.get(risk_level, "CLEAR")


class DecisionOrchestrator:
    """Run the full fraud pipeline and persist results."""

    def __init__(self) -> None:
        beneficiary_extractor = BeneficiaryExtractor()
        relationship_extractor = RelationshipExtractor()

        self.feature_engineer = FeatureEngineer(extractor=beneficiary_extractor)
        self.rule_service = RuleService()
        self.graph_analyzer = GraphAnalyzer(relationship_extractor=relationship_extractor)
        self.ml_scorer = MLScorer()
        self.explainer = Explainer()
        self.repository = FraudCaseRepository()

    def score_beneficiary(
        self, beneficiary_id: str, snapshot_date: Optional[date] = None
    ) -> dict:
        """Execute the full pipeline and return a FraudDecision dict.

        Pipeline stages:
            1. Feature extraction
            2. Rule evaluation
            3. Graph / network analysis
            4. ML scoring
            5. Score aggregation  (0.30 * rule + 0.50 * ml + 0.20 * graph)
            6. Risk level and recommendation
            7. Explainability
            8. Persist to repository

        Returns:
            Dict compatible with the FraudDecisionResponse Pydantic schema.
        """
        t0 = time.perf_counter()

        # 1. Extract features
        from app.api.errors import BeneficiaryNotFoundError, FeatureExtractionError

        try:
            features = self.feature_engineer.get_features(beneficiary_id, snapshot_date)
        except ValueError as exc:
            raise BeneficiaryNotFoundError(beneficiary_id) from exc
        except Exception as exc:
            raise FeatureExtractionError(beneficiary_id, str(exc)) from exc

        # 2. Rule evaluation
        rule_result = self.rule_service.evaluate(features)
        rule_score: float = rule_result.get("rule_score", 0.0)
        triggered_rules: list = rule_result.get("triggered_rules", [])

        # 3. Graph analysis (fraud-aware: seed PageRank with known case scores)
        try:
            known_scores = self.repository.get_known_fraud_scores()
            graph_result = self.graph_analyzer.analyze_network(
                beneficiary_id, known_fraud_scores=known_scores
            )
        except Exception as exc:
            logger.warning("Graph analysis failed for %s: %s", beneficiary_id, exc)
            graph_result = {"network_score": 0.0, "pagerank_score": 0.0,
                            "community_risk": 0.0, "network_size": 0, "density": 0.0,
                            "fraud_connections": 0, "risk_factors": []}

        graph_score: float = graph_result.get("network_score", 0.0)

        # 4. ML scoring
        try:
            ml_result = self.ml_scorer.score(features)
        except Exception as exc:
            logger.warning("ML scoring failed for %s: %s", beneficiary_id, exc)
            ml_result = {"combined_score": 0.0, "xgboost_score": 0.0, "isolation_score": 0.0}

        ml_score: float = ml_result.get("combined_score", 0.0)

        # 5. Aggregate
        final_score = round(
            0.30 * rule_score + 0.50 * ml_score + 0.20 * graph_score, 4
        )
        final_score = max(0.0, min(1.0, final_score))

        # 6. Risk level & recommendation
        risk_level = _determine_risk_level(final_score)
        recommendation = _determine_recommendation(risk_level)

        # 7. Explainability
        decision_context = {
            "beneficiary_id": beneficiary_id,
            "final_score": final_score,
            "risk_level": risk_level,
            "rule_score": rule_score,
            "ml_score": ml_score,
            "graph_score": graph_score,
            "rules_triggered": triggered_rules,
            "features": features,
            "model": ml_result.get("model"),
        }

        try:
            explanation_result = self.explainer.explain(decision_context)
        except Exception as exc:
            logger.warning("Explainability failed: %s", exc)
            explanation_result = {
                "summary": f"Risk level: {risk_level}",
                "top_reasons": [],
                "rule_explanations": [],
                "feature_contributions": [],
            }

        processing_ms = round((time.perf_counter() - t0) * 1000, 2)

        decision = {
            "beneficiary_id": beneficiary_id,
            "final_score": final_score,
            "risk_level": risk_level,
            "recommendation": recommendation,
            "rules_triggered": triggered_rules,
            "rule_score": rule_score,
            "ml_score": ml_score,
            "graph_score": graph_score,
            "top_features": explanation_result.get("feature_contributions", []),
            "explanation": explanation_result.get("summary", ""),
            "processing_ms": processing_ms,
            "features": features,
        }

        # 8. Persist
        try:
            case_id = self.repository.save_decision(decision)
            decision["case_id"] = case_id
        except Exception as exc:
            logger.warning("Failed to persist decision for %s: %s", beneficiary_id, exc)

        # 9. Alert (fire-and-forget, never blocks pipeline)
        try:
            from app.services.alert_service import get_alert_service
            get_alert_service().send(decision)
        except Exception as exc:
            logger.debug("Alert dispatch error: %s", exc)

        return decision
