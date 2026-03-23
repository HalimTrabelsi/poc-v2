"""Tests for Rule Engine"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest
from app.core.rule_engine import RuleEngine

RULES_PATH = "ml/rules/fraud_rules.json"


@pytest.fixture
def engine():
    return RuleEngine(RULES_PATH)


def test_clean_beneficiary(engine):
    f = {"nb_programs": 1, "amount_ratio": 1.0,
         "account_changes_30d": 0, "household_size": 4,
         "nb_payment_failures": 1, "location_risk_score": 0.1}
    r = engine.evaluate(f)
    assert r["rule_score"] == 0.0
    assert r["flags"] == []


def test_multi_program_flag(engine):
    f = {"nb_programs": 5, "amount_ratio": 1.0,
         "account_changes_30d": 0, "household_size": 4,
         "nb_payment_failures": 0, "location_risk_score": 0.1}
    r = engine.evaluate(f)
    assert "MULTI_PROGRAM_ENROLLMENT" in r["flags"]
    assert r["rule_score"] > 0


def test_multiple_flags(engine):
    f = {"nb_programs": 5, "amount_ratio": 4.0,
         "account_changes_30d": 6, "household_size": 15,
         "nb_payment_failures": 8, "location_risk_score": 0.9}
    r = engine.evaluate(f)
    assert r["rule_score"] >= 0.8
    assert len(r["flags"]) >= 3


def test_score_capped_at_1(engine):
    f = {"nb_programs": 10, "amount_ratio": 10.0,
         "account_changes_30d": 20, "household_size": 20,
         "nb_payment_failures": 20, "location_risk_score": 1.0}
    r = engine.evaluate(f)
    assert r["rule_score"] <= 1.0