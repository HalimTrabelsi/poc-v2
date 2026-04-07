"""
routes_scoring.py — Endpoints de scoring fraude
================================================
Schéma des features aligné avec feature_extractor.py / train_openg2p.py.

Endpoints :
  POST /score              — scoring manuel (payload JSON)
  GET  /scan/{id}          — scoring depuis OpenG2P via feature_extractor
  POST /scan/all           — scoring batch depuis OpenG2P
  POST /score/upload       — upload CSV + scoring batch (réponse JSON ou CSV)
"""

import io
from typing import Any, Dict, List, Optional

import pandas as pd
from fastapi import APIRouter, HTTPException, Query, UploadFile, File
from fastapi.responses import StreamingResponse
from app.schemas.fraud import BeneficiaryInput, FraudScoreResponse
from app.core.ml_scorer import MLScorer
from app.db.postgres import FraudDatabase

router = APIRouter()

ml_scorer = MLScorer(model_name="random_forest")
db = FraudDatabase()

# Features officielle utilisées pour rule_score / graph_score
# (sous-ensemble du schéma ML complet)
_ML_FEATURES_DEFAULTS: Dict[str, Any] = {
    "age": 35.0, "income": 0.0, "income_per_person": 0.0,
    "household_size": 1.0, "nb_children": 0.0, "nb_elderly": 0.0,
    "dependency_ratio": 0.0, "has_disabled": 0, "single_head": 0,
    "nb_programs": 1, "nb_active_programs": 1,
    "pmt_score": 0.5, "pmt_score_min": 0.5, "avg_enrollment_days": 365.0,
    "payment_count": 1, "payment_gap_ratio": 0.0,
    "payment_success_rate": 1.0, "amount_variance": 0.0, "cycle_count": 1,
    "shared_phone_count": 0.0, "shared_account_count": 0.0, "network_risk": 0.0,
    "group_membership_count": 0, "high_amount_flag": 0,
    "income_program_inconsistency": 0,
}


def clamp(value: float, lo: float = 0.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, value))


def fill_defaults(row: Dict[str, Any]) -> Dict[str, Any]:
    """Garantit la présence de toutes les features attendues par le modèle."""
    out = {**_ML_FEATURES_DEFAULTS, **{k: v for k, v in row.items() if v is not None}}
    return out


# ─── Scores heuristiques ──────────────────────────────────────────────────────

def compute_rule_score(payload: Dict[str, Any]) -> float:
    score = 0.0
    if float(payload.get("shared_account_count", 0)) >= 2:
        score += 0.30
    if float(payload.get("shared_phone_count", 0)) >= 2:
        score += 0.25
    if float(payload.get("payment_gap_ratio", 0)) > 0.5:
        score += 0.20
    if int(payload.get("nb_programs", 0)) > 3:
        score += 0.15
    if int(payload.get("has_disabled", 0)) == 1:
        score += 0.05
    if int(payload.get("income_program_inconsistency", 0)) == 1:
        score += 0.05
    return clamp(score)


def compute_graph_score(payload: Dict[str, Any]) -> float:
    import math
    phone   = float(payload.get("shared_phone_count", 0))
    account = float(payload.get("shared_account_count", 0))
    score = math.tanh(phone / 3.0) * 0.5 + math.tanh(account / 3.0) * 0.5
    return clamp(score)


def compute_risk_level(
    final_score: float,
    ml_prediction: int,
    rule_score: float,
    graph_score: float,
) -> str:
    evidence = max(rule_score, graph_score)
    if final_score >= 0.90 and ml_prediction == 1 and evidence >= 0.35:
        return "CRITICAL"
    if final_score >= 0.75:
        return "HIGH"
    if final_score >= 0.45:
        return "MEDIUM"
    return "LOW"


def compute_recommended_action(risk_level: str) -> str:
    return {
        "CRITICAL": "Escalate for manual review",
        "HIGH":     "Review beneficiary case with priority",
        "MEDIUM":   "Review beneficiary case",
    }.get(risk_level, "No immediate action")


# ─── Construction de la réponse ───────────────────────────────────────────────

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
    ml_score      = float(ml_result.get("ml_score", 0.0) or 0.0)
    rule_score    = compute_rule_score(payload)
    graph_score   = compute_graph_score(payload)
    final_score   = clamp(0.70 * ml_score + 0.20 * rule_score + 0.10 * graph_score)
    risk_level    = compute_risk_level(final_score, ml_prediction, rule_score, graph_score)

    explanation = (
        f"Model {ml_result.get('model_name')} — ml_score={ml_score:.3f}, "
        f"rule_score={rule_score:.3f}, graph_score={graph_score:.3f}, "
        f"final_score={final_score:.3f}."
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
        recommended_action=compute_recommended_action(risk_level),
        error=None,
    )


def _persist(response: FraudScoreResponse, beneficiary_id: int | None):
    """Sauvegarde silencieuse de l'alerte dans la Fraud DB."""
    try:
        db.save_case_result({
            "beneficiary_id":    beneficiary_id,
            "final_score":       response.final_score,
            "risk_level":        response.risk_level,
            "recommended_action": response.recommended_action,
            "rule_flags":        [],
            "explanation":       response.explanation or "",
        })
    except Exception as e:
        print(f"[routes_scoring] save_case_result failed for {beneficiary_id}: {e}")


# ─── Endpoints ────────────────────────────────────────────────────────────────

@router.post("/score", response_model=FraudScoreResponse)
def score_manual(payload: BeneficiaryInput):
    """Scoring à partir d'une entrée manuelle JSON."""
    data = fill_defaults(payload.model_dump())
    beneficiary_id = data.pop("beneficiary_id", None)
    ml_result = ml_scorer.score(data)
    return build_response(beneficiary_id, data, ml_result)


@router.get("/scan/{beneficiary_id}", response_model=FraudScoreResponse)
def scan_beneficiary(beneficiary_id: int):
    """
    Scoring d'un bénéficiaire extrait depuis OpenG2P via feature_extractor.
    Les features retournées sont directement compatibles avec le modèle.
    """
    row = db.get_beneficiary_features(beneficiary_id)
    if not row:
        raise HTTPException(status_code=404, detail=f"Beneficiary {beneficiary_id} not found")

    payload = fill_defaults(row)
    ml_result = ml_scorer.score(payload)
    response = build_response(beneficiary_id, payload, ml_result)
    _persist(response, beneficiary_id)
    return response


@router.post("/scan/all")
def scan_all(limit: int = Query(default=50, ge=1, le=500)) -> Dict[str, Any]:
    """Scoring batch de tous les bénéficiaires depuis OpenG2P."""
    rows = db.get_all_beneficiaries_features(limit=limit)
    results: List[Dict[str, Any]] = []

    for row in rows:
        bid     = row.get("partner_id") or row.get("beneficiary_id")
        payload = fill_defaults(row)
        ml_result = ml_scorer.score(payload)
        response  = build_response(bid, payload, ml_result)
        results.append(response.model_dump())
        _persist(response, bid)

    return {
        "ready":      True,
        "model_name": ml_scorer.model_name,
        "count":      len(results),
        "results":    results,
    }


# ─── Upload CSV ───────────────────────────────────────────────────────────────

# Colonnes minimales requises pour que le scoring soit significatif.
# Les autres colonnes de ML_FEATURES sont complétées automatiquement par fill_defaults.
_REQUIRED_COLUMNS = {"income", "nb_programs", "pmt_score", "payment_gap_ratio"}


def _validate_csv(df: pd.DataFrame) -> Optional[str]:
    """Retourne un message d'erreur si le CSV est invalide, None sinon."""
    if df.empty:
        return "Le fichier CSV est vide."
    missing = _REQUIRED_COLUMNS - set(df.columns)
    if missing:
        return f"Colonnes requises manquantes : {sorted(missing)}"
    if len(df) > 5000:
        return f"Trop de lignes ({len(df)}). Maximum autorisé : 5 000."
    return None


def _score_dataframe(df: pd.DataFrame) -> List[Dict[str, Any]]:
    """Score toutes les lignes du DataFrame et retourne une liste de résultats."""
    results = []
    for _, row in df.iterrows():
        raw = row.where(pd.notna(row), None).to_dict()
        bid = raw.get("partner_id") or raw.get("beneficiary_id")
        payload   = fill_defaults(raw)
        ml_result = ml_scorer.score(payload)
        resp      = build_response(bid, payload, ml_result)
        results.append({
            # Identifiants
            "beneficiary_id":    bid,
            "scenario":          raw.get("scenario"),
            "is_fraud":          raw.get("is_fraud"),          # label synthétique si présent
            # Scores
            "ml_score":          resp.ml_score,
            "rule_score":        resp.rule_score,
            "graph_score":       resp.graph_score,
            "final_score":       resp.final_score,
            "ml_prediction":     resp.ml_prediction,
            "risk_level":        resp.risk_level,
            "recommended_action": resp.recommended_action,
            "model_name":        resp.model_name,
            "explanation":       resp.explanation,
        })
    return results


@router.post("/score/upload", summary="Scoring batch par upload CSV")
async def score_upload(
    file: UploadFile = File(..., description="Fichier CSV avec les features bénéficiaires"),
    format: str = Query(default="json", enum=["json", "csv"],
                        description="Format de la réponse : json ou csv"),
):
    """
    Upload un fichier CSV et score toutes les lignes avec le modèle actuel.

    **Colonnes requises** : `income`, `nb_programs`, `pmt_score`, `payment_gap_ratio`

    **Colonnes optionnelles** : toutes les autres features ML — complétées automatiquement
    si absentes (voir `ML_FEATURES` dans `feature_extractor.py`).

    **Colonnes de contexte conservées** : `partner_id`, `beneficiary_id`, `scenario`, `is_fraud`

    **Formats de réponse** :
    - `?format=json` (défaut) → liste de résultats JSON
    - `?format=csv`  → fichier CSV enrichi téléchargeable
    """
    if not file.filename or not file.filename.endswith(".csv"):
        raise HTTPException(status_code=400, detail="Le fichier doit être un CSV (.csv).")

    content = await file.read()
    try:
        df = pd.read_csv(io.BytesIO(content))
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Impossible de lire le CSV : {e}")

    error = _validate_csv(df)
    if error:
        raise HTTPException(status_code=422, detail=error)

    results = _score_dataframe(df)

    if format == "csv":
        out_df = pd.DataFrame(results)
        stream = io.StringIO()
        out_df.to_csv(stream, index=False)
        stream.seek(0)
        filename = file.filename.replace(".csv", "_scored.csv")
        return StreamingResponse(
            iter([stream.getvalue()]),
            media_type="text/csv",
            headers={"Content-Disposition": f"attachment; filename={filename}"},
        )

    return {
        "ready":      True,
        "model_name": ml_scorer.model_name,
        "filename":   file.filename,
        "rows_input": len(df),
        "count":      len(results),
        "results":    results,
    }
