"""Scoring API routes"""
from fastapi import APIRouter, HTTPException
from prometheus_client import Counter, Histogram

from app.schemas.fraud import BeneficiaryInput, FraudScoreResponse
from app.core.pipeline import get_pipeline

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
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post(
    "/score/batch",
    summary="Score multiple beneficiaries"
)
async def score_batch(items: list[BeneficiaryInput]):
    try:
        return [
            get_pipeline().analyze(item.model_dump())
            for item in items
        ]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))