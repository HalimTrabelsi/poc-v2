"""Unified API router for the fraud detection engine."""
import logging
import time
from datetime import date
from typing import Optional

from fastapi import APIRouter, BackgroundTasks, Depends, File, HTTPException, Query, UploadFile
from fastapi.responses import Response, StreamingResponse
from pydantic import BaseModel

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
    country_code: Optional[str] = Query(
        None, description="Deployment country ISO-2 for income/poverty calibration "
                          "(defaults to the configured deployment country)"),
) -> FraudDecisionResponse:
    """Run the full fraud detection pipeline for a single beneficiary.

    Executes features → rules → graph → ML → explain → persist in sequence
    and returns a complete fraud decision.
    """
    t0 = time.perf_counter()
    try:
        orchestrator = _get_orchestrator()
        result = orchestrator.score_beneficiary(
            beneficiary_id, snapshot_date=snapshot_date, country_code=country_code
        )
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


@router.get(
    "/v1/cases/{case_id}",
    summary="Get a single fraud case with full detail",
)
async def get_case_detail(case_id: str) -> dict:
    """Return full case detail including rules_triggered and LLM explanation."""
    from app.data.repository import FraudCaseRepository

    repo = FraudCaseRepository()
    case = repo.get_case(case_id)
    if not case:
        raise HTTPException(status_code=404, detail=f"Case {case_id} not found")
    # Serialize datetime fields and ensure JSON-safe response
    return {
        k: (v.isoformat() if hasattr(v, "isoformat") else v)
        for k, v in case.items()
    }


@router.post(
    "/v1/cases/{case_id}/llm_explain",
    summary="Generate an LLM-based natural-language explanation",
)
async def generate_llm_explanation(case_id: str) -> dict:
    """Generate (or refresh) a human-readable explanation via Ollama."""
    from app.data.extractors import RelationshipExtractor
    from app.data.repository import FraudCaseRepository
    from app.services.llm_explainer_service import LLMExplainer

    repo = FraudCaseRepository()
    case = repo.get_case(case_id)
    if not case:
        raise HTTPException(status_code=404, detail=f"Case {case_id} not found")

    relationship_extractor = RelationshipExtractor()
    try:
        shared_entities = relationship_extractor.get_shared_entities(case["beneficiary_id"])
    except Exception as exc:
        logger.warning("Could not resolve shared-entity names for %s: %s", case_id, exc)
        shared_entities = {}
    try:
        case["beneficiary_name"] = relationship_extractor.get_partner_name(case["beneficiary_id"])
    except Exception as exc:
        logger.warning("Could not resolve beneficiary name for %s: %s", case_id, exc)

    explainer = LLMExplainer()
    text_explanation = explainer.explain_case(case, shared_entities=shared_entities)
    if text_explanation:
        repo.update_llm_explanation(case_id, text_explanation)
    return {"case_id": case_id, "llm_explanation": text_explanation}


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
async def score_from_features(
    features: dict,
    country_code: Optional[str] = Query(
        None, description="Deployment country ISO-2 for income/poverty calibration "
                          "(defaults to the configured deployment country)"),
) -> FraudDecisionResponse:
    """Accept a complete feature dict and run rules + ML + explainability.

    Used for batch scoring synthetic data without requiring an OpenG2P connection.
    The dict must contain the beneficiary_id key plus all 25 ML feature columns.
    """
    t0 = time.perf_counter()

    # Validate beneficiary_id is present and non-empty
    bid_raw = features.get("beneficiary_id")
    if not bid_raw or not str(bid_raw).strip():
        raise HTTPException(
            status_code=422,
            detail="Field 'beneficiary_id' is required and must be a non-empty string",
        )
    bid = str(bid_raw).strip()

    try:
        from app.services.rules_service import RuleService
        from app.services.ml_service import MLScorer
        from app.services.explainability_service import Explainer
        from app.services.features_service import _DEFAULTS
        from app.data.repository import FraudCaseRepository
        from app.core.country_reference import get_country_profile
        from app.config import settings

        # Apply feature defaults so missing fields (e.g. temporal counters not
        # supplied by synthetic batch jobs) do not produce phantom rule triggers.
        complete_features = {**_DEFAULTS, **{k: v for k, v in features.items()
                                              if k != "beneficiary_id" and v is not None}}

        # Inject the deployment country's economic anchors (same mechanism as
        # DecisionOrchestrator.score_beneficiary) so SE002/SE003 calibrate to
        # the local income scale instead of a hardcoded number.
        country_profile = get_country_profile(country_code or settings.default_country_code)
        complete_features["poverty_line"] = country_profile["poverty_line"]
        complete_features["national_median_income"] = country_profile["median_income"]

        rule_svc = RuleService()
        rule_result = rule_svc.evaluate(complete_features)

        ml_scorer = MLScorer()
        ml_result = ml_scorer.score(complete_features)

        rule_score = float(rule_result.get("rule_score", 0.0))
        ml_score = float(ml_result.get("combined_score", 0.0))
        graph_score = 0.0  # no graph available for ad-hoc feature scoring

        # Use optimized weights from config (same as DecisionOrchestrator)
        final_score = round(
            settings.ensemble_rules_weight * rule_score
            + settings.ensemble_ml_weight * ml_score
            + settings.ensemble_graph_weight * graph_score,
            4,
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
            "features": complete_features,
            "model": ml_result.get("model"),  # required for SHAP
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
async def trigger_scan(
    country_code: Optional[str] = Query(
        None, description="Deployment country ISO-2 for income/poverty calibration "
                          "(defaults to the configured deployment country)"),
) -> dict:
    """Scan OpenG2P for beneficiaries not yet scored and run the full pipeline on each.

    Returns a summary with counts by risk level. Safe to call repeatedly — already-scored
    beneficiaries are skipped.
    """
    try:
        from app.services.scanner_service import get_scanner
        summary = get_scanner().scan_now(country_code=country_code)
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
    "/v1/stats/by-country",
    summary="Per-country scan counts and risk-level breakdown",
)
async def stats_by_country() -> list[dict]:
    """Return how many beneficiaries were scored under each deployment
    country, with a risk-level breakdown. Powers the dashboard's
    per-country statistics sidebar."""
    from app.data.repository import FraudCaseRepository
    return FraudCaseRepository().get_country_stats()


@router.get(
    "/v1/country-profile/{country_code}",
    summary="Preview a country's economic reference data before launching a scan",
)
async def country_profile(country_code: str) -> dict:
    """Return the World Bank-derived income/poverty profile for a country.

    Lets the dashboard show median income, poverty line, and whether the
    data came from the live World Bank API or a fallback, before the
    analyst launches a scan calibrated to that country.
    """
    from app.core.country_reference import get_country_profile as _get_profile
    return _get_profile(country_code)


@router.get(
    "/v1/beneficiaries",
    summary="List beneficiary IDs from OpenG2P",
)
async def list_beneficiaries(limit: int = Query(100, ge=1, le=5000)) -> list[dict]:
    """Return partner_id + name/age/phone/address from OpenG2P, for display and name search."""
    from app.data.extractors import BeneficiaryExtractor

    try:
        extractor = BeneficiaryExtractor()
        df = extractor.get_all_features(limit=limit)
        cols = ["partner_id"] + [c for c in ("name", "age") if c in df.columns]

        # `age` comes from get_all_features(); raw phone/street don't (only
        # shared_phone_count is used internally) — fetch them separately from
        # res_partner (same fields Odoo's own case display reads from) rather
        # than touching the large shared feature-extraction query.
        contact_info: dict[int, dict] = {}
        try:
            from sqlalchemy import text
            ids = [int(x) for x in df["partner_id"].tolist()]
            if ids:
                with extractor._connector.engine.connect() as conn:
                    rows = conn.execute(
                        text("SELECT id, phone, street FROM res_partner WHERE id = ANY(:ids)"),
                        {"ids": ids},
                    ).fetchall()
                contact_info = {r[0]: {"phone": r[1] or "", "address": r[2] or ""} for r in rows}
        except Exception:
            logger.warning("Could not fetch beneficiary contact info", exc_info=True)

        return [
            {
                "partner_id": int(row["partner_id"]),
                "name": row.get("name") or "",
                "age": int(row["age"]) if "age" in cols and row.get("age") is not None else None,
                "phone": contact_info.get(int(row["partner_id"]), {}).get("phone", ""),
                "address": contact_info.get(int(row["partner_id"]), {}).get("address", ""),
            }
            for _, row in df[cols].iterrows()
        ]
    except Exception as exc:
        logger.exception("Failed to list beneficiaries")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


# ── Geospatial endpoints ─────────────────────────────────────────────────────

@router.get("/v1/geo/heatmap", summary="Fraud score heatmap points (lat/lon + score)")
async def geo_heatmap() -> list[dict]:
    """Return one point per scored beneficiary with lat, lon, fraud_score.
    Suitable for feeding directly into a pydeck HeatmapLayer.
    """
    try:
        from app.services.geo_service import GeoService
        return GeoService().get_heatmap_data()
    except Exception as exc:
        logger.exception("Heatmap failed")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/v1/geo/hotspots", summary="DBSCAN fraud cluster hotspots")
async def geo_hotspots() -> list[dict]:
    """Cluster scored beneficiaries geographically and return fraud-density per cluster."""
    try:
        from app.services.geo_service import GeoService
        return GeoService().get_hotspots()
    except Exception as exc:
        logger.exception("Hotspots failed")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


# ── Feedback loop endpoints ───────────────────────────────────────────────────

class FeedbackPayload(BaseModel):
    verdict: str   # "confirmed_fraud" | "false_positive" | "uncertain"
    notes: str = ""
    investigator: str = "investigator"


@router.post(
    "/v1/cases/{case_id}/feedback",
    status_code=201,
    summary="Submit investigator verdict for a fraud case",
)
async def submit_feedback(case_id: str, body: FeedbackPayload) -> dict:
    """Record an investigator verdict (confirmed_fraud / false_positive / uncertain).

    Verdicts are stored in fraud_feedback and used to retrain XGBoost weekly,
    so the model learns from real investigator decisions over time.
    """
    allowed = {"confirmed_fraud", "false_positive", "uncertain"}
    if body.verdict not in allowed:
        raise HTTPException(
            status_code=422,
            detail=f"verdict must be one of {sorted(allowed)}",
        )
    try:
        from app.services.retraining_service import get_retrainer
        feedback_id = get_retrainer().save_feedback(
            case_id, body.verdict, body.notes, body.investigator
        )
        if not feedback_id:
            raise HTTPException(status_code=404, detail=f"Case {case_id} not found")
        return {
            "feedback_id": feedback_id,
            "case_id": case_id,
            "verdict": body.verdict,
            "status": "recorded",
        }
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("Feedback submission failed for case %s", case_id)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/v1/feedback/stats", summary="Investigator feedback statistics")
async def feedback_stats() -> dict:
    """Return verdict counts, estimated model precision, and retraining history."""
    try:
        from app.services.retraining_service import get_retrainer
        return get_retrainer().get_feedback_stats()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/v1/retrain", summary="Manually trigger XGBoost retraining on feedback labels")
async def trigger_retrain(background_tasks: BackgroundTasks) -> dict:
    """Start a retraining job in the background. Returns immediately with status 'started'."""
    from app.services.retraining_service import get_retrainer
    retrainer = get_retrainer()
    background_tasks.add_task(retrainer.retrain)
    return {"status": "started", "message": "Retraining running in background — check /feedback/stats for progress"}


# ── Report / export endpoints ─────────────────────────────────────────────────

@router.get(
    "/v1/cases/{case_id}/report/pdf",
    summary="Download PDF investigation report for a case",
    response_class=Response,
)
async def download_pdf_report(case_id: str) -> Response:
    """Generate and return a PDF audit report for the given fraud case."""
    from app.data.repository import FraudCaseRepository
    from app.services.report_service import generate_pdf_report

    repo = FraudCaseRepository()
    cases = repo.list_cases(limit=1000)
    case = next((c for c in cases if str(c.get("case_id", "")) == case_id), None)
    if not case:
        raise HTTPException(status_code=404, detail=f"Case {case_id} not found")
    try:
        pdf_bytes = generate_pdf_report(case)
        return Response(
            content=pdf_bytes,
            media_type="application/pdf",
            headers={"Content-Disposition": f'attachment; filename="fraud_case_{case_id[:8]}.pdf"'},
        )
    except Exception as exc:
        logger.exception("PDF generation failed for case %s", case_id)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get(
    "/v1/cases/export/csv",
    summary="Export all fraud cases as CSV",
    response_class=Response,
)
async def export_cases_csv(
    risk_level: Optional[RiskLevel] = Query(None),
    limit: int = Query(5000, ge=1, le=50000),
) -> Response:
    """Download a CSV of all (or filtered) fraud cases for compliance / audit."""
    from app.data.repository import FraudCaseRepository
    from app.services.report_service import generate_csv_report

    repo = FraudCaseRepository()
    risk_levels = [risk_level.value] if risk_level else None
    cases = repo.list_cases(risk_levels=risk_levels, limit=limit)
    csv_bytes = generate_csv_report(cases)
    return Response(
        content=csv_bytes,
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=fraud_cases_export.csv"},
    )


# ── Batch CSV upload scoring (Feature 8) ─────────────────────────────────────

@router.post(
    "/v1/score/batch",
    summary="Upload a CSV of beneficiary IDs and score all of them",
)
async def score_batch_csv(
    file: UploadFile = File(..., description="CSV with a 'beneficiary_id' column"),
    background: bool = Query(False, description="Run in background; returns job_id immediately"),
    country_code: Optional[str] = Query(
        None, description="Deployment country ISO-2 for income/poverty calibration "
                          "(defaults to the configured deployment country)"),
    background_tasks: BackgroundTasks = None,
) -> Response:
    """Accept a CSV file, score every listed beneficiary, and return a results CSV.

    CSV must have at least one column named ``beneficiary_id``.
    All other columns are ignored.  Returns a new CSV with scores appended.
    """
    import asyncio
    import csv
    import io
    import concurrent.futures

    if not file.filename or not file.filename.lower().endswith(".csv"):
        raise HTTPException(status_code=422, detail="Uploaded file must be a .csv")

    raw = await file.read()
    try:
        text = raw.decode("utf-8-sig")  # handle BOM
        reader = csv.DictReader(io.StringIO(text))
        rows = list(reader)
    except Exception as exc:
        raise HTTPException(status_code=422, detail=f"Could not parse CSV: {exc}") from exc

    if not rows:
        raise HTTPException(status_code=422, detail="CSV has no data rows")

    # Detect beneficiary_id column (case-insensitive)
    col_map = {c.lower().strip(): c for c in rows[0].keys()}
    bid_col = col_map.get("beneficiary_id") or col_map.get("partner_id") or col_map.get("id")
    if not bid_col:
        raise HTTPException(
            status_code=422,
            detail="CSV must have a column named 'beneficiary_id', 'partner_id', or 'id'",
        )

    beneficiary_ids = [str(r[bid_col]).strip() for r in rows if r.get(bid_col, "").strip()]
    if not beneficiary_ids:
        raise HTTPException(status_code=422, detail="No valid beneficiary IDs found in CSV")

    if len(beneficiary_ids) > 10_000:
        raise HTTPException(status_code=422, detail="Batch limit is 10,000 beneficiaries per request")

    def _score_one(bid: str) -> dict:
        try:
            from app.services.decision_service import DecisionOrchestrator
            orch = DecisionOrchestrator()
            result = orch.score_beneficiary(bid, country_code=country_code)
            return {
                "beneficiary_id": bid,
                "case_id": result.get("case_id", ""),
                "final_score": round(result.get("final_score", 0.0), 4),
                "rule_score": round(result.get("rule_score", 0.0), 4),
                "ml_score": round(result.get("ml_score", 0.0), 4),
                "graph_score": round(result.get("graph_score", 0.0), 4),
                "risk_level": result.get("risk_level", ""),
                "recommendation": result.get("recommendation", ""),
                "status": "scored",
                "error": "",
            }
        except Exception as exc:
            return {
                "beneficiary_id": bid,
                "case_id": "",
                "final_score": "",
                "rule_score": "",
                "ml_score": "",
                "graph_score": "",
                "risk_level": "",
                "recommendation": "",
                "status": "error",
                "error": str(exc)[:120],
            }

    max_workers = min(8, len(beneficiary_ids))
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as pool:
        results = list(pool.map(_score_one, beneficiary_ids))

    out = io.StringIO()
    fieldnames = [
        "beneficiary_id", "case_id", "final_score", "rule_score",
        "ml_score", "graph_score", "risk_level", "recommendation", "status", "error",
    ]
    writer = csv.DictWriter(out, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(results)

    return Response(
        content=out.getvalue().encode("utf-8"),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=batch_scores.csv"},
    )


# ── MLflow model versioning endpoints ────────────────────────────────────────

@router.get("/v1/models/versions", summary="List recent MLflow training runs")
async def list_model_versions() -> list[dict]:
    """Return recent retraining runs from MLflow with accuracy and sample counts."""
    try:
        from app.services.retraining_service import get_retrainer
        return get_retrainer().get_mlflow_runs()
    except Exception as exc:
        logger.warning("Could not fetch model versions: %s", exc)
        return []


@router.post("/v1/models/rollback/{run_id}", summary="Restore model artifact from an MLflow run")
async def rollback_model(run_id: str) -> dict:
    """Download the XGBoost model from the given MLflow run and make it the active model."""
    try:
        from app.services.retraining_service import get_retrainer
        result = get_retrainer().rollback_to_run(run_id)
        if result.get("status") == "error":
            raise HTTPException(status_code=500, detail=result.get("error", "Rollback failed"))
        return result
    except HTTPException:
        raise
    except Exception as exc:
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
