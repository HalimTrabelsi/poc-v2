"""Rules management routes"""
from fastapi import APIRouter
from app.core.pipeline import get_pipeline

router = APIRouter()


@router.get("/rules", summary="List all fraud detection rules")
async def list_rules():
    engine = get_pipeline().rule_engine
    return {
        "count": engine.active_rule_count,      
        "rules": engine.get_rules_summary(),     
    }