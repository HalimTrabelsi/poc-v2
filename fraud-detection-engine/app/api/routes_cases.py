"""Fraud cases management"""
from fastapi import APIRouter
from pydantic import BaseModel
from typing import Optional
import datetime

router = APIRouter()

_cases: list[dict] = []


class CaseUpdate(BaseModel):
    status:     str
    resolution: Optional[str] = None
    notes:      Optional[str] = None


@router.get("/cases", summary="List fraud cases")
async def list_cases(status: str = "pending", limit: int = 50):
    filtered = [c for c in _cases if c["status"] == status]
    return {"cases": filtered[:limit], "total": len(filtered)}


@router.put("/cases/{case_id}", summary="Update a fraud case")
async def update_case(case_id: str, update: CaseUpdate):
    for c in _cases:
        if c["id"] == case_id:
            c.update(update.model_dump(exclude_none=True))
            c["updated_at"] = datetime.datetime.utcnow().isoformat()
            return c
    return {"error": "Case not found"}