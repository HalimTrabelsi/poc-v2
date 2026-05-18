"""Integration tests for the v1 API endpoints.

These tests use FastAPI's TestClient and mock the DecisionOrchestrator and
repository so that no live database connection is required.
"""
from unittest.mock import MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Health endpoint — no mocking required
# ---------------------------------------------------------------------------

def test_health_endpoint_returns_200(client):
    """GET /health must always return 200."""
    response = client.get("/health")
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "ok"


# ---------------------------------------------------------------------------
# POST /api/v1/score/beneficiary/{id}
# ---------------------------------------------------------------------------

def _mock_decision(beneficiary_id: str = "42") -> dict:
    return {
        "beneficiary_id": beneficiary_id,
        "final_score": 0.75,
        "risk_level": "HIGH",
        "recommendation": "MANUAL_REVIEW",
        "rules_triggered": [
            {
                "rule_id": "NF001",
                "name": "Shared Bank Account",
                "weight": 0.35,
                "triggered": True,
                "explanation": "Bank account shared with 2 others",
            }
        ],
        "top_features": [],
        "explanation": "High risk detected due to shared bank account.",
        "processing_ms": 50.0,
        "rule_score": 0.35,
        "ml_score": 0.80,
        "graph_score": 0.50,
        "features": {},
        "model": None,
    }


def test_score_beneficiary_returns_200(client):
    """POST /api/v1/score/beneficiary/{id} must return 200 with a valid decision."""
    with patch("app.api.routes._get_orchestrator") as mock_factory:
        mock_orch = MagicMock()
        mock_orch.score_beneficiary.return_value = _mock_decision("42")
        mock_factory.return_value = mock_orch

        response = client.post("/api/v1/score/beneficiary/42")

    assert response.status_code == 200
    body = response.json()
    assert body["beneficiary_id"] == "42"
    assert 0.0 <= body["final_score"] <= 1.0
    assert body["risk_level"] in ["LOW", "MEDIUM", "HIGH", "CRITICAL"]
    assert "recommendation" in body


def test_score_beneficiary_not_found_returns_404(client):
    """A BeneficiaryNotFoundError must translate to HTTP 404."""
    from app.api.errors import BeneficiaryNotFoundError

    with patch("app.api.routes._get_orchestrator") as mock_factory:
        mock_orch = MagicMock()
        mock_orch.score_beneficiary.side_effect = BeneficiaryNotFoundError("999")
        mock_factory.return_value = mock_orch

        response = client.post("/api/v1/score/beneficiary/999")

    assert response.status_code == 404


# ---------------------------------------------------------------------------
# GET /api/v1/cases
# ---------------------------------------------------------------------------

def test_list_cases_returns_200(client):
    """GET /api/v1/cases must return 200 with total and cases list."""
    with patch("app.api.routes.FraudCaseRepository") as MockRepo:
        instance = MockRepo.return_value
        instance.list_cases.return_value = []

        response = client.get("/api/v1/cases")

    assert response.status_code == 200
    body = response.json()
    assert "total" in body
    assert "cases" in body
    assert isinstance(body["cases"], list)


def test_list_cases_respects_limit(client):
    """limit query param must be passed through to the repository."""
    with patch("app.api.routes.FraudCaseRepository") as MockRepo:
        instance = MockRepo.return_value
        instance.list_cases.return_value = []

        client.get("/api/v1/cases?limit=10")
        call_kwargs = instance.list_cases.call_args[1]
        assert call_kwargs.get("limit") == 10


# ---------------------------------------------------------------------------
# PATCH /api/v1/cases/{id}/status
# ---------------------------------------------------------------------------

def test_update_case_status_returns_200(client):
    """PATCH /api/v1/cases/{id}/status must return 200 when case exists."""
    with patch("app.api.routes.FraudCaseRepository") as MockRepo:
        instance = MockRepo.return_value
        instance.update_case_status.return_value = True

        response = client.patch(
            "/api/v1/cases/some-case-id/status",
            json={"status": "CLOSED", "notes": "Verified clean"},
        )

    assert response.status_code == 200
    body = response.json()
    assert body["updated"] is True


def test_update_case_status_not_found_returns_404(client):
    """PATCH must return 404 when the repository reports the case is missing."""
    with patch("app.api.routes.FraudCaseRepository") as MockRepo:
        instance = MockRepo.return_value
        instance.update_case_status.return_value = False

        response = client.patch(
            "/api/v1/cases/nonexistent/status",
            json={"status": "CLOSED"},
        )

    assert response.status_code == 404
