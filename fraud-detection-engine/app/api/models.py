"""Pydantic v2 request/response schemas for the fraud detection API."""
from datetime import date, datetime
from enum import Enum
from typing import Any, Optional

from pydantic import BaseModel, Field


class RiskLevel(str, Enum):
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"


class Recommendation(str, Enum):
    CLEAR = "CLEAR"
    MONITOR = "MONITOR"
    MANUAL_REVIEW = "MANUAL_REVIEW"
    BLOCK_PAYMENT = "BLOCK_PAYMENT"


class BeneficiaryScoreRequest(BaseModel):
    beneficiary_id: str = Field(..., description="OpenG2P partner_id")
    snapshot_date: Optional[date] = Field(None, description="Point-in-time evaluation date")


class RuleResult(BaseModel):
    rule_id: str
    name: str
    weight: float
    triggered: bool
    explanation: str


class FeatureContribution(BaseModel):
    feature: str
    value: float
    shap_value: float
    direction: str  # "increases_risk" | "decreases_risk"


class FraudDecisionResponse(BaseModel):
    beneficiary_id: str
    final_score: float = Field(..., ge=0.0, le=1.0)
    risk_level: RiskLevel
    recommendation: Recommendation
    rules_triggered: list[RuleResult]
    top_features: list[FeatureContribution]
    explanation: str
    processing_ms: float
    evaluated_at: datetime = Field(default_factory=datetime.utcnow)


class CaseItem(BaseModel):
    case_id: str
    beneficiary_id: str
    final_score: float
    risk_level: RiskLevel
    recommendation: Recommendation
    status: str
    created_at: datetime
    updated_at: Optional[datetime] = None
    notes: Optional[str] = None


class CaseListResponse(BaseModel):
    total: int
    cases: list[CaseItem]


class CaseStatusUpdate(BaseModel):
    status: str = Field(..., description="New status: OPEN, UNDER_REVIEW, CLOSED, FALSE_POSITIVE")
    notes: Optional[str] = Field(None, description="Agent notes for audit trail")


class HealthResponse(BaseModel):
    status: str
    service: str
    version: str
    models_ready: bool
    rules_loaded: int
    timestamp: datetime = Field(default_factory=datetime.utcnow)


class ExplainResponse(BaseModel):
    beneficiary_id: str
    summary: str
    top_reasons: list[str]
    rule_explanations: list[str]
    feature_contributions: list[FeatureContribution]
    raw_scores: dict[str, Any]
