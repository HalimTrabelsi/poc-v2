from pydantic import BaseModel, Field
from typing import Optional


class BeneficiaryInput(BaseModel):
    beneficiary_id: Optional[int] = Field(default=None, description="Optional beneficiary identifier")

    gender: int = Field(..., description="0 = male, 1 = female")
    age: float = Field(..., description="Age in years")
    income: float = Field(..., description="Annual household income")
    household_size: float = Field(..., description="Number of household members")
    nb_children: float = Field(..., description="Number of children in household")
    vehicles_owned: float = Field(..., description="Encoded number/type of vehicles")
    dependency_ratio: float = Field(..., description="Children to adults ratio")
    income_per_person: float = Field(..., description="Income divided by household size")
    disability_flag: int = Field(..., description="1 if disability present, else 0")
    immigration_flag: int = Field(..., description="1 if immigrant profile, else 0")
    own_home_flag: int = Field(..., description="1 if owns home, else 0")
    shared_phone_count: float = Field(..., description="How many beneficiaries share the same phone")
    shared_account_count: float = Field(..., description="How many beneficiaries share the same bank account")


class FraudScoreResponse(BaseModel):
    beneficiary_id: Optional[int] = None

    ready: bool
    model_name: str
    ml_prediction: Optional[int] = None
    ml_probability: float = 0.0
    ml_score: float = 0.0

    rule_score: float = 0.0
    graph_score: float = 0.0
    final_score: float = 0.0
    risk_level: str = "LOW"

    explanation: Optional[str] = None
    recommended_action: Optional[str] = None
    error: Optional[str] = None