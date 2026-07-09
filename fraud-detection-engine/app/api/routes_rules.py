"""Rules management routes — inspect, create, update, enable/disable and
delete the fraud-detection rules that back the rule-engine sub-score.

All write endpoints hot-reload the RuleService immediately after persisting
to YAML, so a change is live for the very next /score call — no restart.
"""
from typing import Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from app.rules.loader import RuleValidationError
from app.services.rules_service import RuleService

router = APIRouter()

_rule_service: RuleService | None = None


def _get_service() -> RuleService:
    global _rule_service
    if _rule_service is None:
        _rule_service = RuleService()
    return _rule_service


class RulePayload(BaseModel):
    id: str = Field(..., description="Unique rule id, e.g. 'NF007'")
    name: str
    description: Optional[str] = ""
    weight: float = Field(..., ge=0.0, le=1.0)
    alert_level: str = Field("MEDIUM", pattern="^(LOW|MEDIUM|HIGH|CRITICAL)$")
    condition: str = Field(..., description="Boolean expression evaluated against features")
    evidence_template: Optional[str] = "Rule triggered"
    enabled: bool = True
    scenario_file: str = Field(
        ..., description="Target YAML filename under app/rules/rules/, e.g. 'network_fraud.yaml'"
    )


class RuleToggle(BaseModel):
    enabled: bool


@router.get("/rules", summary="List all active fraud-detection rules")
async def list_rules():
    """Return enabled rules currently loaded in the scoring engine."""
    service = _get_service()
    rules = service.get_rules_summary()
    return {"count": len(rules), "rules": rules}


@router.get("/rules/admin", summary="List every rule (enabled and disabled) for the admin UI")
async def list_rules_admin():
    """Return all rules across scenario files, including disabled ones."""
    service = _get_service()
    rules = service.get_all_rules_admin()
    return {"count": len(rules), "rules": rules}


@router.get("/rules/scenarios", summary="List scenario YAML files available for new rules")
async def list_scenarios():
    service = _get_service()
    return {"files": service.list_scenario_files()}


@router.post("/rules/reload", summary="Re-read rule YAML files from disk")
async def reload_rules():
    """Hot-reload rules without restarting the service."""
    service = _get_service()
    try:
        service.reload_rules()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Reload failed: {exc}")
    return {"status": "reloaded", "count": len(service.get_rules_summary())}


@router.post("/rules", summary="Create a new rule or replace an existing one (matched by id)")
async def upsert_rule(payload: RulePayload):
    service = _get_service()
    rule = payload.model_dump(exclude={"scenario_file"})
    try:
        result = service.create_or_update_rule(payload.scenario_file, rule)
    except RuleValidationError as exc:
        raise HTTPException(status_code=422, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to save rule: {exc}")
    return result


@router.patch("/rules/{rule_id}/enabled", summary="Enable or disable a rule without deleting it")
async def toggle_rule(rule_id: str, payload: RuleToggle):
    service = _get_service()
    found = service.set_rule_enabled(rule_id, payload.enabled)
    if not found:
        raise HTTPException(status_code=404, detail=f"Rule '{rule_id}' not found")
    return {"status": "enabled" if payload.enabled else "disabled", "id": rule_id}


@router.delete("/rules/{rule_id}", summary="Permanently delete a rule")
async def delete_rule(rule_id: str):
    service = _get_service()
    found = service.delete_rule(rule_id)
    if not found:
        raise HTTPException(status_code=404, detail=f"Rule '{rule_id}' not found")
    return {"status": "deleted", "id": rule_id}
