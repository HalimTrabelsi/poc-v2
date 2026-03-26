from enum import Enum
from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field, ConfigDict


class RiskLevel(str, Enum):
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"


class Action(str, Enum):
    NO_ACTION = "No immediate action"
    REVIEW = "Review beneficiary case"
    ESCALATE = "Escalate for manual review"
    BLOCK = "Block / suspend pending investigation"


class ShapFactor(BaseModel):
    feature: str
    value: Optional[Any] = None
    impact: float = 0.0
    direction: Optional[str] = None


class BeneficiaryInput(BaseModel):
    model_config = ConfigDict(protected_namespaces=())

    beneficiary_id: Optional[int] = Field(default=None)

    gender: int
    age: float
    income: float
    household_size: float
    nb_children: float
    vehicles_owned: float
    dependency_ratio: float
    income_per_person: float
    disability_flag: int
    immigration_flag: int
    own_home_flag: int
    shared_phone_count: float
    shared_account_count: float


class FraudScoreResponse(BaseModel):
    model_config = ConfigDict(protected_namespaces=())

    beneficiary_id: Optional[int] = None

    ready: bool
    model_name: str
    ml_prediction: Optional[int] = None
    ml_probability: float = 0.0
    ml_score: float = 0.0

    rule_score: float = 0.0
    graph_score: float = 0.0
    final_score: float = 0.0
    risk_level: RiskLevel = RiskLevel.LOW

    explanation: Optional[str] = None
    recommended_action: Optional[str] = None
    error: Optional[str] = None


# Compatibilité avec l’ancien pipeline
class FraudCase(BaseModel):
    beneficiary_id: Optional[int] = None
    score: float = 0.0
    risk_level: RiskLevel = RiskLevel.LOW
    action: Action = Action.NO_ACTION
    explanation: Optional[str] = None
    rule_flags: List[str] = []
    shap_factors: List[ShapFactor] = []


class CaseExplanation(BaseModel):
    summary: Optional[str] = None
    reasons: List[str] = []
    top_features: Dict[str, Any] = {}