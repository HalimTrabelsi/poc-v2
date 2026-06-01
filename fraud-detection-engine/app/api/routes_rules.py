"""Rules management routes — inspect the active fraud-detection rules."""
from fastapi import APIRouter, HTTPException

from app.services.rules_service import RuleService

router = APIRouter()

_rule_service: RuleService | None = None


def _get_service() -> RuleService:
    global _rule_service
    if _rule_service is None:
        _rule_service = RuleService()
    return _rule_service


@router.get("/rules", summary="List all loaded fraud-detection rules")
async def list_rules():
    """Return all rules currently active, with id, name, weight, condition."""
    service = _get_service()
    rules = service.get_rules_summary()
    return {"count": len(rules), "rules": rules}


@router.post("/rules/reload", summary="Re-read rule YAML files from disk")
async def reload_rules():
    """Hot-reload rules without restarting the service."""
    service = _get_service()
    try:
        service.reload_rules()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Reload failed: {exc}")
    return {"status": "reloaded", "count": len(service.get_rules_summary())}
