"""Unified API router for the fraud detection engine."""
import logging
import time
from datetime import date
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query

from app.api.errors import BeneficiaryNotFoundError, ModelNotReadyError
from app.api.models import (
    CaseListResponse,
    CaseStatusUpdate,
    ExplainResponse,
    FeatureContribution,
    FraudDecisionResponse,
    HealthResponse,
    Recommendation,
    RiskLevel,
    RuleResult,
)

logger = logging.getLogger(__name__)
router = APIRouter()


def _get_orchestrator():
    """Lazily import to avoid circular dependencies at module load time."""
    from app.services.decision_service import DecisionOrchestrator
    return DecisionOrchestrator()


@router.post(
    "/v1/score/beneficiary/{beneficiary_id}",
    response_model=FraudDecisionResponse,
    summary="Score a beneficiary for fraud risk",
)
async def score_beneficiary(
    beneficiary_id: str,
    snapshot_date: Optional[date] = Query(None, description="Point-in-time evaluation date"),
) -> FraudDecisionResponse:
    """Run the full fraud detection pipeline for a single beneficiary.

    Executes features → rules → graph → ML → explain → persist in sequence
    and returns a complete fraud decision.
    """
    t0 = time.perf_counter()
    try:
        orchestrator = _get_orchestrator()
        result = orchestrator.score_beneficiary(beneficiary_id, snapshot_date=snapshot_date)
    except BeneficiaryNotFoundError:
        raise
    except ModelNotReadyError:
        raise
    except Exception as exc:
        logger.exception("Pipeline failed for beneficiary %s", beneficiary_id)
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    processing_ms = round((time.perf_counter() - t0) * 1000, 2)
    result["processing_ms"] = processing_ms
    return FraudDecisionResponse(**result)


@router.get(
    "/v1/cases",
    response_model=CaseListResponse,
    summary="List fraud cases",
)
async def list_cases(
    risk_level: Optional[RiskLevel] = Query(None),
    status: Optional[str] = Query(None),
    limit: int = Query(50, ge=1, le=500),
) -> CaseListResponse:
    """Return a paginated list of fraud cases, optionally filtered by risk level or status."""
    from app.data.repository import FraudCaseRepository

    repo = FraudCaseRepository()
    risk_levels = [risk_level.value] if risk_level else None
    statuses = [status] if status else None
    cases = repo.list_cases(risk_levels=risk_levels, statuses=statuses, limit=limit)
    return CaseListResponse(total=len(cases), cases=cases)


@router.patch(
    "/v1/cases/{case_id}/status",
    summary="Update the status of a fraud case",
)
async def update_case_status(case_id: str, body: CaseStatusUpdate) -> dict:
    """Update a case status and append agent notes to the audit trail."""
    from app.data.repository import FraudCaseRepository

    repo = FraudCaseRepository()
    updated = repo.update_case_status(case_id, body.status, body.notes)
    if not updated:
        raise HTTPException(status_code=404, detail=f"Case {case_id} not found")
    return {"case_id": case_id, "status": body.status, "updated": True}


@router.get(
    "/v1/explain/{beneficiary_id}",
    response_model=ExplainResponse,
    summary="Get full explanation for a beneficiary decision",
)
async def explain_decision(beneficiary_id: str) -> ExplainResponse:
    """Return the most recent stored decision with full SHAP and rule explanations."""
    from app.data.repository import FraudCaseRepository
    from app.services.explainability_service import Explainer

    repo = FraudCaseRepository()
    history = repo.get_beneficiary_history(beneficiary_id)
    if not history:
        raise HTTPException(
            status_code=404,
            detail=f"No decision history found for beneficiary {beneficiary_id}",
        )

    latest = history[0]
    explainer = Explainer()
    explanation = explainer.explain(latest)

    contributions = [
        FeatureContribution(**fc) for fc in explanation.get("feature_contributions", [])
    ]

    return ExplainResponse(
        beneficiary_id=beneficiary_id,
        summary=explanation.get("summary", ""),
        top_reasons=explanation.get("top_reasons", []),
        rule_explanations=explanation.get("rule_explanations", []),
        feature_contributions=contributions,
        raw_scores={
            "final_score": latest.get("final_score"),
            "rule_score": latest.get("rule_score"),
            "ml_score": latest.get("ml_score"),
            "graph_score": latest.get("graph_score"),
        },
    )


@router.post(
    "/v1/score/features",
    response_model=FraudDecisionResponse,
    summary="Score pre-computed features directly (no OpenG2P DB lookup)",
)
async def score_from_features(features: dict) -> FraudDecisionResponse:
    """Accept a complete feature dict and run rules + ML + explainability.

    Used for batch scoring synthetic data without requiring an OpenG2P connection.
    The dict must contain the beneficiary_id key plus all 25 ML feature columns.
    """
    t0 = time.perf_counter()
    try:
        from app.services.rules_service import RuleService
        from app.services.ml_service import MLScorer
        from app.services.graph_service import GraphAnalyzer
        from app.services.explainability_service import Explainer
        from app.data.repository import FraudCaseRepository
        from app.config import settings

        bid = str(features.get("beneficiary_id", "unknown"))

        rule_svc = RuleService()
        rule_result = rule_svc.evaluate(features)

        ml_scorer = MLScorer()
        ml_result = ml_scorer.score(features)

        rule_score = float(rule_result.get("rule_score", 0.0))
        ml_score = float(ml_result.get("combined_score", 0.0))
        graph_score = 0.0  # no graph available for synthetic data

        final_score = round(
            0.30 * rule_score + 0.50 * ml_score + 0.20 * graph_score, 4
        )

        if final_score >= settings.critical_threshold:
            risk_level, recommendation = RiskLevel.CRITICAL, Recommendation.BLOCK_PAYMENT
        elif final_score >= settings.high_threshold:
            risk_level, recommendation = RiskLevel.HIGH, Recommendation.MANUAL_REVIEW
        elif final_score >= settings.medium_threshold:
            risk_level, recommendation = RiskLevel.MEDIUM, Recommendation.MONITOR
        else:
            risk_level, recommendation = RiskLevel.LOW, Recommendation.CLEAR

        explainer = Explainer()
        decision_data = {
            "beneficiary_id": bid,
            "final_score": final_score,
            "rule_score": rule_score,
            "ml_score": ml_score,
            "graph_score": graph_score,
            "risk_level": risk_level.value,
            "recommendation": recommendation.value,
            "rules_triggered": rule_result.get("triggered_rules", []),
            "features": features,
        }
        explanation = explainer.explain(decision_data)

        repo = FraudCaseRepository()
        case_id = repo.save_decision({**decision_data, **explanation})

        processing_ms = round((time.perf_counter() - t0) * 1000, 2)

        return FraudDecisionResponse(
            beneficiary_id=bid,
            case_id=case_id,
            final_score=final_score,
            risk_level=risk_level,
            recommendation=recommendation,
            rules_triggered=[RuleResult(**r) for r in rule_result.get("triggered_rules", [])],
            top_features=explanation.get("feature_contributions", []),
            explanation=explanation.get("summary", ""),
            processing_ms=processing_ms,
        )
    except Exception as exc:
        logger.exception("Feature scoring failed for %s", features.get("beneficiary_id"))
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post(
    "/v1/scan/now",
    summary="Trigger an immediate scan of all unscored OpenG2P beneficiaries",
)
async def trigger_scan() -> dict:
    """Scan OpenG2P for beneficiaries not yet scored and run the full pipeline on each.

    Returns a summary with counts by risk level. Safe to call repeatedly — already-scored
    beneficiaries are skipped.
    """
    try:
        from app.services.scanner_service import get_scanner
        summary = get_scanner().scan_now()
        return {"status": "ok", "summary": summary}
    except Exception as exc:
        logger.exception("Scan failed")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get(
    "/v1/scan/status",
    summary="Get scanner status and unscored beneficiary count",
)
async def scan_status() -> dict:
    """Return how many beneficiaries exist in OpenG2P and how many are already scored."""
    from app.data.connector import OpenG2PConnector
    from app.data.repository import FraudCaseRepository
    from sqlalchemy import text

    try:
        connector = OpenG2PConnector()
        with connector.get_session() as session:
            total_row = session.execute(text("""
                SELECT COUNT(*) FROM res_partner p
                WHERE COALESCE(p.active, true) = true
                  AND COALESCE(p.is_company, false) = false
                  AND COALESCE(p.is_registrant, false) = true
                  AND p.name NOT IN (
                      'My Company','Administrator','Public user',
                      'Default User Template','OdooBot'
                  )
            """)).fetchone()
        total_in_openg2p = total_row[0] if total_row else 0

        repo = FraudCaseRepository()
        scored = len(repo.get_all_scored_beneficiary_ids())

        return {
            "total_in_openg2p": total_in_openg2p,
            "already_scored": scored,
            "pending": max(0, total_in_openg2p - scored),
        }
    except Exception as exc:
        logger.warning("Scan status check failed: %s", exc)
        return {"total_in_openg2p": 0, "already_scored": 0, "pending": 0, "error": str(exc)}


@router.get(
    "/v1/beneficiaries",
    summary="List beneficiary IDs from OpenG2P",
)
async def list_beneficiaries(limit: int = Query(100, ge=1, le=5000)) -> list[dict]:
    """Return partner_id list from OpenG2P for batch scoring."""
    from app.data.extractors import BeneficiaryExtractor

    try:
        extractor = BeneficiaryExtractor()
        df = extractor.get_all_features(limit=limit)
        return [{"partner_id": int(row)} for row in df["partner_id"].tolist()]
    except Exception as exc:
        logger.exception("Failed to list beneficiaries")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get(
    "/v1/health",
    response_model=HealthResponse,
    summary="Health check",
)
async def health_check() -> HealthResponse:
    """Return service health, model readiness, and loaded rule count."""
    models_ready = False
    rules_loaded = 0

    try:
        from app.services.ml_service import MLScorer
        scorer = MLScorer()
        models_ready = scorer.is_ready
    except Exception:
        pass

    try:
        from app.services.rules_service import RuleService
        svc = RuleService()
        rules_loaded = len(svc.get_rules_summary())
    except Exception:
        pass

    return HealthResponse(
        status="ok",
        service="fraud-detection-engine",
        version="2.0.0",
        models_ready=models_ready,
        rules_loaded=rules_loaded,
    )
