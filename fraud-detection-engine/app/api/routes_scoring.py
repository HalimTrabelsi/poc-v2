from typing import Any, Dict, List

from fastapi import APIRouter, HTTPException, Query
from app.schemas.fraud import BeneficiaryInput, FraudScoreResponse
from app.core.ml_scorer import MLScorer
from app.db.postgres import FraudDatabase

router = APIRouter()

ml_scorer = MLScorer(model_name="random_forest")
db = FraudDatabase()


def clamp(value: float, lo: float = 0.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, value))


def compute_rule_score(payload: Dict[str, Any]) -> float:
    score = 0.0

    shared_phone_count = float(payload.get("shared_phone_count", 0) or 0)
    shared_account_count = float(payload.get("shared_account_count", 0) or 0)
    dependency_ratio = float(payload.get("dependency_ratio", 0) or 0)
    disability_flag = int(payload.get("disability_flag", 0) or 0)

    if shared_account_count >= 2:
        score += 0.40
    if shared_phone_count >= 2:
        score += 0.35
    if dependency_ratio > 1.0:
        score += 0.15
    if disability_flag == 1:
        score += 0.05

    return clamp(score)


def compute_graph_score(payload: Dict[str, Any]) -> float:
    shared_phone_count = float(payload.get("shared_phone_count", 0) or 0)
    shared_account_count = float(payload.get("shared_account_count", 0) or 0)

    score = 0.0
    score += min(shared_phone_count / 4.0, 1.0) * 0.5
    score += min(shared_account_count / 4.0, 1.0) * 0.5
    return clamp(score)


def compute_risk_level(final_score: float, ml_prediction: int, rule_score: float, graph_score: float) -> str:
    evidence_score = max(rule_score, graph_score)

    if final_score >= 0.90 and ml_prediction == 1 and evidence_score >= 0.35:
        return "CRITICAL"
    if final_score >= 0.75:
        return "HIGH"
    if final_score >= 0.45:
        return "MEDIUM"
    return "LOW"


def compute_recommended_action(risk_level: str) -> str:
    if risk_level == "CRITICAL":
        return "Escalate for manual review"
    if risk_level == "HIGH":
        return "Review beneficiary case with priority"
    if risk_level == "MEDIUM":
        return "Review beneficiary case"
    return "No immediate action"


def normalize_gender(value: Any) -> int:
    if value is None:
        return 0
    s = str(value).strip().lower()
    if s in ["female", "f", "1"]:
        return 1
    return 0


def normalize_numeric(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except Exception:
        return default


def map_db_row_to_model_features(row: Dict[str, Any]) -> Dict[str, Any]:
    household_size = normalize_numeric(row.get("household_size"), 0.0)
    nb_children = normalize_numeric(row.get("nb_children"), 0.0)
    income = normalize_numeric(row.get("income"), 0.0)

    dependency_ratio = normalize_numeric(row.get("dependency_ratio"), 0.0)
    income_per_person = normalize_numeric(row.get("income_per_person"), 0.0)

    if dependency_ratio == 0.0 and household_size > 0:
        nb_adults = max(household_size - nb_children, 0.0)
        dependency_ratio = (nb_children / nb_adults) if nb_adults > 0 else 0.0

    if income_per_person == 0.0 and household_size > 0:
        income_per_person = income / household_size

    return {
        "gender": normalize_gender(row.get("gender")),
        "age": normalize_numeric(row.get("age"), 0.0),
        "income": income,
        "household_size": household_size,
        "nb_children": nb_children,
        "vehicles_owned": normalize_numeric(row.get("vehicles_owned"), 0.0),
        "dependency_ratio": dependency_ratio,
        "income_per_person": income_per_person,
        "disability_flag": int(normalize_numeric(row.get("disability_flag"), 0.0)),
        "immigration_flag": int(normalize_numeric(row.get("immigration_flag"), 0.0)),
        "own_home_flag": int(normalize_numeric(row.get("own_home_flag"), 0.0)),
        "shared_phone_count": normalize_numeric(row.get("shared_phone_count"), 0.0),
        "shared_account_count": normalize_numeric(row.get("shared_account_count"), 0.0),
    }


def build_response(
    beneficiary_id: int | None,
    payload: Dict[str, Any],
    ml_result: Dict[str, Any],
) -> FraudScoreResponse:
    if not ml_result.get("ready", False):
        return FraudScoreResponse(
            beneficiary_id=beneficiary_id,
            ready=False,
            model_name=ml_result.get("model_name", "unknown"),
            ml_prediction=None,
            ml_probability=0.0,
            ml_score=0.0,
            rule_score=0.0,
            graph_score=0.0,
            final_score=0.0,
            risk_level="LOW",
            explanation=None,
            recommended_action=None,
            error=ml_result.get("error", "Model not ready"),
        )

    ml_prediction = int(ml_result.get("ml_prediction", 0) or 0)
    ml_score = float(ml_result.get("ml_score", 0.0) or 0.0)

    rule_score = compute_rule_score(payload)
    graph_score = compute_graph_score(payload)

    final_score = clamp((0.70 * ml_score) + (0.20 * rule_score) + (0.10 * graph_score))
    risk_level = compute_risk_level(final_score, ml_prediction, rule_score, graph_score)
    recommended_action = compute_recommended_action(risk_level)

    explanation = (
        f"Model {ml_result.get('model_name')} predicted {ml_prediction} "
        f"with ml_score={ml_score:.3f}, rule_score={rule_score:.3f}, "
        f"graph_score={graph_score:.3f}, final_score={final_score:.3f}."
    )

    return FraudScoreResponse(
        beneficiary_id=beneficiary_id,
        ready=True,
        model_name=ml_result.get("model_name", "unknown"),
        ml_prediction=ml_prediction,
        ml_probability=ml_result.get("ml_probability", 0.0),
        ml_score=ml_score,
        rule_score=rule_score,
        graph_score=graph_score,
        final_score=final_score,
        risk_level=risk_level,
        explanation=explanation,
        recommended_action=recommended_action,
        error=None,
    )


@router.post("/score", response_model=FraudScoreResponse)
def score_manual(payload: BeneficiaryInput):
    data = payload.model_dump()
    beneficiary_id = data.get("beneficiary_id")

    model_payload = map_db_row_to_model_features(data)
    ml_result = ml_scorer.score(model_payload)
    response = build_response(beneficiary_id, model_payload, ml_result)
    return response


@router.get("/scan/{beneficiary_id}", response_model=FraudScoreResponse)
def scan_beneficiary_from_openg2p(beneficiary_id: int):
    if not hasattr(db, "get_beneficiary_features"):
        raise HTTPException(
            status_code=500,
            detail="FraudDatabase.get_beneficiary_features() is not implemented yet."
        )

    row = db.get_beneficiary_features(beneficiary_id)
    if not row:
        raise HTTPException(status_code=404, detail=f"Beneficiary {beneficiary_id} not found")

    payload = map_db_row_to_model_features(row)
    ml_result = ml_scorer.score(payload)
    response = build_response(beneficiary_id, payload, ml_result)

    if hasattr(db, "save_case_result"):
        try:
            db.save_case_result({
                "beneficiary_id": beneficiary_id,
                "model_name": response.model_name,
                "ml_prediction": response.ml_prediction,
                "ml_probability": response.ml_probability,
                "ml_score": response.ml_score,
                "rule_score": response.rule_score,
                "graph_score": response.graph_score,
                "final_score": response.final_score,
                "risk_level": response.risk_level,
                "recommended_action": response.recommended_action,
                "rule_flags": [],
                "explanation": response.explanation,
            })
        except Exception as e:
            print(f"[routes_scoring] save_case_result failed: {e}")

    return response


@router.post("/scan/all")
def scan_all_from_openg2p(limit: int = Query(default=50, ge=1, le=500)) -> Dict[str, Any]:
    if not hasattr(db, "get_all_beneficiaries_features"):
        raise HTTPException(
            status_code=500,
            detail="FraudDatabase.get_all_beneficiaries_features() is not implemented yet."
        )

    rows = db.get_all_beneficiaries_features(limit=limit)
    results: List[Dict[str, Any]] = []

    for row in rows:
        beneficiary_id = row.get("beneficiary_id") or row.get("id")
        payload = map_db_row_to_model_features(row)
        ml_result = ml_scorer.score(payload)
        response = build_response(beneficiary_id, payload, ml_result)

        results.append(response.model_dump())

        if hasattr(db, "save_case_result"):
            try:
                db.save_case_result({
                    "beneficiary_id": beneficiary_id,
                    "model_name": response.model_name,
                    "ml_prediction": response.ml_prediction,
                    "ml_probability": response.ml_probability,
                    "ml_score": response.ml_score,
                    "rule_score": response.rule_score,
                    "graph_score": response.graph_score,
                    "final_score": response.final_score,
                    "risk_level": response.risk_level,
                    "recommended_action": response.recommended_action,
                    "rule_flags": [],
                    "explanation": response.explanation,
                })
            except Exception as e:
                print(f"[routes_scoring] save_case_result failed for {beneficiary_id}: {e}")

    return {
        "ready": True,
        "model_name": ml_scorer.model_name,
        "count": len(results),
        "results": results,
    }