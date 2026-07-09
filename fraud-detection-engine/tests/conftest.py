"""Shared pytest fixtures."""
import pytest
from fastapi.testclient import TestClient

from app.main import app


@pytest.fixture
def client() -> TestClient:
    """Return a synchronous FastAPI test client with auth bypassed."""
    # Inject the dev API key so middleware passes
    return TestClient(app, headers={"X-API-Key": "dev-secret-change-in-prod"})


@pytest.fixture
def decision_orchestrator():
    """Return a DecisionOrchestrator instance (monkeypatched to skip DB)."""
    from unittest.mock import MagicMock
    from app.services.decision_service import DecisionOrchestrator
    orch = DecisionOrchestrator()
    orch.repository = MagicMock()
    orch.repository.get_known_fraud_scores.return_value = {}
    return orch


@pytest.fixture
def sample_features() -> dict:
    """Return a complete feature dict matching the 25 ML_FEATURES."""
    return {
        "age": 35.0,
        "income": 400.0,
        "income_per_person": 100.0,
        "household_size": 4.0,
        "nb_children": 2.0,
        "nb_elderly": 0.0,
        "dependency_ratio": 0.5,
        "has_disabled": 0.0,
        "single_head": 0.0,
        "nb_programs": 2.0,
        "nb_active_programs": 1.0,
        "pmt_score": 0.6,
        "pmt_score_min": 0.55,
        "avg_enrollment_days": 365.0,
        "payment_count": 8.0,
        "payment_gap_ratio": 0.05,
        "payment_success_rate": 0.95,
        "amount_variance": 150.0,
        "cycle_count": 4.0,
        "shared_phone_count": 0.0,
        "shared_account_count": 0.0,
        "network_risk": 0.0,
        "group_membership_count": 1.0,
        "high_amount_flag": 0.0,
        "income_program_inconsistency": 0.0,
    }


@pytest.fixture
def fraud_features() -> dict:
    """Return a feature dict that should trigger multiple fraud rules."""
    return {
        "age": 28.0,
        "income": 80.0,
        "income_per_person": 10.0,
        "household_size": 8.0,
        "nb_children": 5.0,
        "nb_elderly": 1.0,
        "dependency_ratio": 3.0,
        "has_disabled": 0.0,
        "single_head": 1.0,
        "nb_programs": 5.0,
        "nb_active_programs": 4.0,
        "pmt_score": 0.10,
        "pmt_score_min": 0.08,
        "avg_enrollment_days": 90.0,
        "payment_count": 10.0,
        "payment_gap_ratio": 0.65,
        "payment_success_rate": 0.35,
        "amount_variance": 5000.0,
        "cycle_count": 5.0,
        "shared_phone_count": 4.0,
        "shared_account_count": 3.0,
        "network_risk": 0.82,
        "group_membership_count": 2.0,
        "high_amount_flag": 1.0,
        "income_program_inconsistency": 1.0,
    }


@pytest.fixture
def mock_decision() -> dict:
    """Return a complete FraudDecision dict for unit testing explainability."""
    return {
        "beneficiary_id": "999",
        "final_score": 0.82,
        "risk_level": "CRITICAL",
        "recommendation": "BLOCK_PAYMENT",
        "rule_score": 0.75,
        "ml_score": 0.85,
        "graph_score": 0.60,
        "rules_triggered": [
            {
                "rule_id": "NF003",
                "name": "Phone and Account Network",
                "weight": 0.40,
                "triggered": True,
                "explanation": "Shares both phone and bank account with multiple beneficiaries",
            },
            {
                "rule_id": "ME001",
                "name": "Multiple Program Enrollment",
                "weight": 0.30,
                "triggered": True,
                "explanation": "Enrolled in 5 programs simultaneously (threshold: 4)",
            },
        ],
        "top_features": [],
        "explanation": "CRITICAL risk level detected.",
        "processing_ms": 125.4,
        "features": {},
        "model": None,
    }
