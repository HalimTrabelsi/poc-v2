from typing import Any, Dict, List

from fastapi import APIRouter, HTTPException, Query
from app.schemas.fraud import BeneficiaryInput, FraudScoreResponse
from app.core.ml_scorer import MLScorer
from app.db.postgres import FraudDatabase

router = APIRouter()

# Modèle principal phase 1
ml_scorer = MLScorer(model_name="random_forest")
db = FraudDatabase()


# =========================================================
# HELPERS
# =========================================================
def compute_risk_level(score: float) -> str:
    if score >= 0.85:
        return "CRITICAL"
    elif score >= 0.65:
        return "HIGH"
    elif score >= 0.35:
        return "MEDIUM"
    return "LOW"


def compute_recommended_action(risk_level: str) -> str:
    if risk_level in ["CRITICAL", "HIGH"]:
        return "Escalate for manual review"
    elif risk_level == "MEDIUM":
        return "Review beneficiary case"
    return "No immediate action"


def normalize_gender(value: Any) -> int:
    if value is None:
        return 0
    s = str(value).strip().lower()
    if s in ["female", "f", "1"]:
        return 1
    return 0


def normalize_flag(value: Any) -> int:
    if value is None:
        return 0
    s = str(value).strip().lower()
    if s in ["yes", "true", "1", "owned", "own_home", "immigrant", "migrant", "disabled", "has_disability"]:
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
    """
    Cette fonction transforme une ligne venant de postgres.py
    vers les 13 features attendues par le modèle.
    """
    household_size = normalize_numeric(row.get("household_size"), 0.0)
    nb_children = normalize_numeric(row.get("nb_children"), 0.0)
    income = normalize_numeric(row.get("income"), 0.0)

    nb_adults = max(household_size - nb_children, 0.0)

    dependency_ratio = row.get("dependency_ratio")
    if dependency_ratio is None:
        dependency_ratio = (nb_children / nb_adults) if nb_adults > 0 else 0.0

    income_per_person = row.get("income_per_person")
    if income_per_person is None:
        income_per_person = (income / household_size) if household_size > 0 else 0.0

    payload = {
        "gender": normalize_gender(row.get("gender")),
        "age": normalize_numeric(row.get("age"), 0.0),
        "income": income,
        "household_size": household_size,
        "nb_children": nb_children,
        "vehicles_owned": normalize_numeric(row.get("vehicles_owned"), 0.0),
        "dependency_ratio": normalize_numeric(dependency_ratio, 0.0),
        "income_per_person": normalize_numeric(income_per_person, 0.0),
        "disability_flag": normalize_flag(row.get("disability_flag", row.get("disability_status"))),
        "immigration_flag": normalize_flag(row.get("immigration_flag", row.get("immigration_status"))),
        "own_home_flag": normalize_flag(row.get("own_home_flag", row.get("own_home"))),
        "shared_phone_count": normalize_numeric(row.get("shared_phone_count"), 1.0),
        "shared_account_count": normalize_numeric(row.get("shared_account_count"), 1.0),
    }
    return payload


def build_response(
    beneficiary_id: int | None,
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

    ml_score = float(ml_result.get("ml_score", 0.0))
    final_score = ml_score
    risk_level = compute_risk_level(final_score)
    recommended_action = compute_recommended_action(risk_level)

    explanation = (
        f"Model {ml_result.get('model_name')} predicted "
        f"{ml_result.get('ml_prediction')} with probability {ml_score:.3f}."
    )

    return FraudScoreResponse(
        beneficiary_id=beneficiary_id,
        ready=True,
        model_name=ml_result.get("model_name", "unknown"),
        ml_prediction=ml_result.get("ml_prediction"),
        ml_probability=ml_result.get("ml_probability", 0.0),
        ml_score=ml_score,
        rule_score=0.0,
        graph_score=0.0,
        final_score=final_score,
        risk_level=risk_level,
        explanation=explanation,
        recommended_action=recommended_action,
        error=None,
    )


# =========================================================
# ROUTES
# =========================================================
@router.post("/score", response_model=FraudScoreResponse)
def score_manual(payload: BeneficiaryInput):
    """
    Scoring manuel via payload JSON.
    Utile pour Swagger, Postman, Streamlit.
    """
    data = payload.model_dump()
    beneficiary_id = data.get("beneficiary_id")

    ml_result = ml_scorer.score(data)
    response = build_response(beneficiary_id, ml_result)
    return response


@router.get("/scan/{beneficiary_id}", response_model=FraudScoreResponse)
def scan_beneficiary_from_openg2p(beneficiary_id: int):
    """
    Va chercher un bénéficiaire dans OpenG2P via postgres.py,
    transforme la ligne en features modèle, puis score.
    """
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
    response = build_response(beneficiary_id, ml_result)

    # Sauvegarde facultative dans fraud-db
    if hasattr(db, "save_case_result"):
        try:
            db.save_case_result({
                "beneficiary_id": beneficiary_id,
                "model_name": response.model_name,
                "ml_prediction": response.ml_prediction,
                "ml_probability": response.ml_probability,
                "ml_score": response.ml_score,
                "final_score": response.final_score,
                "risk_level": response.risk_level,
                "recommended_action": response.recommended_action,
                "explanation": response.explanation,
            })
        except Exception as e:
            print(f"[routes_scoring] save_case_result failed: {e}")

    return response


@router.post("/scan/all")
def scan_all_from_openg2p(limit: int = Query(default=50, ge=1, le=500)) -> Dict[str, Any]:
    """
    Scanne plusieurs bénéficiaires depuis OpenG2P.
    """
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
        response = build_response(beneficiary_id, ml_result)

        results.append(response.model_dump())

        if hasattr(db, "save_case_result"):
            try:
                db.save_case_result({
                    "beneficiary_id": beneficiary_id,
                    "model_name": response.model_name,
                    "ml_prediction": response.ml_prediction,
                    "ml_probability": response.ml_probability,
                    "ml_score": response.ml_score,
                    "final_score": response.final_score,
                    "risk_level": response.risk_level,
                    "recommended_action": response.recommended_action,
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