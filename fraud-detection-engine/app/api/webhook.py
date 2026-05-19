"""Real-time webhook endpoint — called by OpenG2P/Odoo when a beneficiary is saved."""
import logging

from fastapi import APIRouter, BackgroundTasks, Header, HTTPException
from pydantic import BaseModel

logger = logging.getLogger(__name__)
router = APIRouter()


class BeneficiarySavedPayload(BaseModel):
    partner_id: int
    event: str = "saved"  # "created" | "updated"


@router.post(
    "/v1/webhook/beneficiary-saved",
    status_code=202,
    summary="Called by OpenG2P when a beneficiary is created or updated",
    tags=["Webhook"],
)
async def beneficiary_saved(
    payload: BeneficiarySavedPayload,
    background_tasks: BackgroundTasks,
    x_webhook_secret: str = Header(None, alias="X-Webhook-Secret"),
) -> dict:
    """Receive a partner_id from Odoo and immediately score that beneficiary in the
    background. Returns 202 instantly so Odoo is never blocked waiting for the result.

    Authentication uses the same secret as the REST API (X-Webhook-Secret header).
    """
    from app.config import settings

    if x_webhook_secret != settings.api_secret_key:
        logger.warning(
            "Webhook rejected for partner_id=%d — invalid secret", payload.partner_id
        )
        raise HTTPException(status_code=401, detail="Invalid webhook secret")

    logger.info(
        "Webhook received: partner_id=%d event=%s", payload.partner_id, payload.event
    )
    background_tasks.add_task(_score_in_background, str(payload.partner_id))
    return {"status": "accepted", "partner_id": payload.partner_id, "event": payload.event}


async def _score_in_background(beneficiary_id: str) -> None:
    """Run the full fraud pipeline for one beneficiary (non-blocking)."""
    try:
        from app.services.decision_service import DecisionOrchestrator
        result = DecisionOrchestrator().score_beneficiary(beneficiary_id)
        logger.info(
            "Webhook scoring complete: partner_id=%s risk=%s score=%.3f",
            beneficiary_id,
            result.get("risk_level"),
            result.get("final_score", 0),
        )
    except Exception as exc:
        logger.error("Webhook scoring failed for partner_id=%s: %s", beneficiary_id, exc)
