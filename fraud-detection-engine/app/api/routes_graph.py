"""Graph analysis routes"""
from fastapi import APIRouter
from app.core.pipeline import get_pipeline

router = APIRouter()


@router.get(
    "/graph/{beneficiary_id}",
    summary="Get graph risk for a beneficiary"
)
async def graph_risk(beneficiary_id: str):
    return get_pipeline().graph.get_risk(beneficiary_id)