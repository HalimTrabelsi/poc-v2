"""Rules management routes"""
from fastapi import APIRouter
from app.core.pipeline import get_pipeline

router = APIRouter()


@router.get("/rules", summary="List all fraud detection rules")
async def list_rules():
    engine = get_pipeline().rule_engine
    return {
        "version": engine.version,
        "count":   len(engine.rules),
        "rules":   engine.rules,
    }