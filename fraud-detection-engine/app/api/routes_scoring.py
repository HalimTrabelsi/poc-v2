"""Scoring API routes"""
from fastapi import APIRouter, HTTPException, BackgroundTasks
from prometheus_client import Counter, Histogram

from app.schemas.fraud import BeneficiaryInput, FraudScoreResponse
from app.core.pipeline import get_pipeline
from app.db.postgres   import get_openg2p_db, get_fraud_db

router = APIRouter()

fraud_counter = Counter(
    "fraud_detections_total",
    "Total fraud detections by level",
    ["level"]
)
latency_hist = Histogram(
    "fraud_scoring_seconds",
    "Time to score a beneficiary"
)


@router.post(
    "/score",
    response_model=FraudScoreResponse,
    summary="Score a single beneficiary"
)
async def score_beneficiary(data: BeneficiaryInput):
    try:
        with latency_hist.time():
            result = get_pipeline().analyze(data.model_dump())
        fraud_counter.labels(level=result.risk_level.value).inc()

        # Save to fraud DB
        get_fraud_db().save_alert(result.model_dump())
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post(
    "/score/batch",
    summary="Score multiple beneficiaries"
)
async def score_batch(items: list[BeneficiaryInput]):
    try:
        results = []
        for item in items:
            r = get_pipeline().analyze(item.model_dump())
            get_fraud_db().save_alert(r.model_dump())
            results.append(r)
        return results
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post(
    "/scan/all",
    summary="Scan ALL beneficiaries from OpenG2P database"
)
async def scan_all_beneficiaries(background_tasks: BackgroundTasks):
    """
    Lance un scan complet de tous les bénéficiaires
    du POC OpenG2P et sauvegarde les alertes
    """
    background_tasks.add_task(_run_full_scan)
    return {
        "message": "Scan lancé en arrière-plan",
        "status":  "running"
    }


@router.get(
    "/scan/status",
    summary="Get scan results summary"
)
async def scan_status():
    try:
        alerts = get_fraud_db().get_alerts(status="pending")
        critical = [a for a in alerts if a["risk_level"] == "CRITICAL"]
        high     = [a for a in alerts if a["risk_level"] == "HIGH"]
        return {
            "total_pending": len(alerts),
            "critical":      len(critical),
            "high":          len(high),
            "alerts":        alerts[:10]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get(
    "/openg2p/connection",
    summary="Test OpenG2P database connection"
)
async def test_openg2p_connection():
    ok = get_openg2p_db().test_connection()
    return {
        "connected": ok,
        "database":  "openg2p",
        "host":      "postgresql"
    }


def _run_full_scan():
    """Background task — scan all beneficiaries"""
    print("[SCAN] Starting full scan of OpenG2P beneficiaries...")
    db       = get_openg2p_db()
    fraud_db = get_fraud_db()
    pipeline = get_pipeline()

    df = db.get_all_beneficiaries()
    if df.empty:
        print("[SCAN] No beneficiaries found in OpenG2P DB")
        return

    print(f"[SCAN] Found {len(df)} beneficiaries to analyze")
    high_risk = 0

    for _, row in df.iterrows():
        try:
            features = row.to_dict()
            result   = pipeline.analyze(features)
            fraud_db.save_alert(result.model_dump())
            if result.risk_level.value in ["HIGH", "CRITICAL"]:
                high_risk += 1
                print(
                    f"[SCAN] ⚠️  {features['beneficiary_id']} — "
                    f"{result.risk_level.value} ({result.final_score:.0%})"
                )
        except Exception as e:
            print(f"[SCAN] Error on {row.get('beneficiary_id')}: {e}")

    print(f"[SCAN] Done. High risk: {high_risk}/{len(df)}")