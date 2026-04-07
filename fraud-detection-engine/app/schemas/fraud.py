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
    """
    Entrée manuelle pour POST /score.
    Schéma aligné avec ML_FEATURES de feature_extractor.py.
    Toutes les colonnes ont une valeur par défaut pour faciliter les tests partiels.
    """
    model_config = ConfigDict(protected_namespaces=())

    beneficiary_id: Optional[int] = Field(default=None)

    # Démographie
    age: float = 35.0
    income: float = 0.0
    income_per_person: float = 0.0
    household_size: float = 1.0
    nb_children: float = 0.0
    nb_elderly: float = 0.0
    dependency_ratio: float = 0.0
    has_disabled: int = 0
    single_head: int = 0

    # Programmes
    nb_programs: int = 1
    nb_active_programs: int = 1
    pmt_score: float = 0.5
    pmt_score_min: float = 0.5
    avg_enrollment_days: float = 365.0

    # Paiements
    payment_count: int = 1
    payment_gap_ratio: float = 0.0
    payment_success_rate: float = 1.0
    amount_variance: float = 0.0
    cycle_count: int = 1

    # Réseau
    shared_phone_count: float = 0.0
    shared_account_count: float = 0.0
    network_risk: float = 0.0

    # Groupes
    group_membership_count: int = 0

    # Flags dérivés
    high_amount_flag: int = 0
    income_program_inconsistency: int = 0


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