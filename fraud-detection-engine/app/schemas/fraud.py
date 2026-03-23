from pydantic import BaseModel, Field
from typing import Optional
from enum import Enum


class RiskLevel(str, Enum):
    LOW      = "LOW"
    MEDIUM   = "MEDIUM"
    HIGH     = "HIGH"
    CRITICAL = "CRITICAL"


class Action(str, Enum):
    CLEAR          = "CLEAR"
    MONITOR        = "MONITOR"
    MANUAL_REVIEW  = "MANUAL_REVIEW"
    BLOCK_PAYMENT  = "BLOCK_PAYMENT"


class BeneficiaryInput(BaseModel):
    beneficiary_id:        str
    nb_programs:           int   = Field(ge=0, default=1)
    total_amount:          float = Field(ge=0, default=500)
    amount_ratio:          float = Field(ge=0, default=1.0)
    nb_cycles:             int   = Field(ge=0, default=3)
    days_since_enrollment: int   = Field(ge=0, default=180)
    account_changes_30d:   int   = Field(ge=0, default=0)
    household_size:        int   = Field(ge=1, default=4)
    nb_payment_failures:   int   = Field(ge=0, default=0)
    location_risk_score:   float = Field(ge=0, le=1, default=0.3)


class ShapFactor(BaseModel):
    feature:   str
    impact:    float
    direction: str


class FraudScoreResponse(BaseModel):
    beneficiary_id: str
    rule_score:     float
    ml_score:       float
    graph_score:    float
    final_score:    float
    risk_level:     RiskLevel
    action:         Action
    rule_flags:     list[str]
    shap_factors:   list[ShapFactor]
    explanation:    str
    processing_ms:  int