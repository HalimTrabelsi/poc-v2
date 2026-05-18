"""Generate realistic beneficiary profiles for ML training."""
import logging
from typing import Optional

import numpy as np
import pandas as pd

from synthetic_data.generators.fraud_generator import FraudScenarioInjector

logger = logging.getLogger(__name__)

_RNG = np.random.default_rng(42)


def _base_beneficiary(n: int) -> pd.DataFrame:
    """Generate n legitimate-looking beneficiary rows."""
    age = _RNG.integers(18, 70, size=n).astype(float)
    household_size = _RNG.integers(1, 9, size=n).astype(float)
    nb_children = np.minimum(_RNG.integers(0, 5, size=n), household_size - 1).astype(float)
    nb_elderly = np.minimum(_RNG.integers(0, 3, size=n), household_size - nb_children - 1).clip(min=0).astype(float)
    has_disabled = _RNG.choice([0, 1], size=n, p=[0.85, 0.15]).astype(float)
    single_head = _RNG.choice([0, 1], size=n, p=[0.75, 0.25]).astype(float)

    income = np.abs(_RNG.normal(loc=350, scale=200, size=n)).clip(min=0)
    income_per_person = income / household_size.clip(min=1)

    adults = (household_size - nb_children - nb_elderly).clip(min=1)
    dependency_ratio = (nb_children + nb_elderly) / adults

    nb_programs = _RNG.integers(1, 4, size=n).astype(float)
    nb_active_programs = np.minimum(_RNG.integers(0, 3, size=n), nb_programs).astype(float)
    pmt_score = _RNG.uniform(0.2, 0.9, size=n)
    pmt_score_min = pmt_score - _RNG.uniform(0, 0.1, size=n)
    avg_enrollment_days = _RNG.integers(30, 1000, size=n).astype(float)

    payment_count = _RNG.integers(0, 20, size=n).astype(float)
    payment_gap_ratio = _RNG.beta(1, 9, size=n)
    payment_success_rate = 1.0 - payment_gap_ratio
    amount_variance = _RNG.exponential(scale=200, size=n)
    cycle_count = _RNG.integers(1, 12, size=n).astype(float)

    shared_phone_count = _RNG.choice([0, 0, 0, 1, 2], size=n, p=[0.7, 0.15, 0.08, 0.04, 0.03]).astype(float)
    shared_account_count = _RNG.choice([0, 0, 0, 1], size=n, p=[0.8, 0.1, 0.07, 0.03]).astype(float)
    group_membership_count = _RNG.integers(0, 4, size=n).astype(float)

    network_risk = (
        shared_phone_count.clip(max=5) * 0.4 + shared_account_count.clip(max=5) * 0.6
    ).clip(max=1.0)

    total_issued = income * cycle_count * 0.3
    q95 = np.percentile(total_issued, 95)
    high_amount_flag = (total_issued > q95).astype(float)

    q15 = np.percentile(income_per_person, 15)
    income_program_inconsistency = ((income_per_person < q15) & (nb_programs >= 3)).astype(float)

    return pd.DataFrame({
        "age": age, "income": income, "income_per_person": income_per_person,
        "household_size": household_size, "nb_children": nb_children,
        "nb_elderly": nb_elderly, "dependency_ratio": dependency_ratio,
        "has_disabled": has_disabled, "single_head": single_head,
        "nb_programs": nb_programs, "nb_active_programs": nb_active_programs,
        "pmt_score": pmt_score, "pmt_score_min": pmt_score_min,
        "avg_enrollment_days": avg_enrollment_days,
        "payment_count": payment_count, "payment_gap_ratio": payment_gap_ratio,
        "payment_success_rate": payment_success_rate, "amount_variance": amount_variance,
        "cycle_count": cycle_count,
        "shared_phone_count": shared_phone_count, "shared_account_count": shared_account_count,
        "network_risk": network_risk, "group_membership_count": group_membership_count,
        "high_amount_flag": high_amount_flag,
        "income_program_inconsistency": income_program_inconsistency,
        "is_fraud": 0,
    })


class BeneficiaryGenerator:
    """Generate a labelled beneficiary dataset for ML training."""

    def generate(self, n: int, fraud_rate: float = 0.05) -> pd.DataFrame:
        """Generate n beneficiaries with the given fraud rate.

        Fraud is deliberately made ambiguous: not every fraud case triggers
        every rule, and some legitimate cases have mildly suspicious values.
        Target model accuracy: 85–92%.

        Args:
            n: Total number of beneficiaries to generate.
            fraud_rate: Proportion that are fraudulent (default 5%).

        Returns:
            DataFrame with all 25 ML features + is_fraud label.
        """
        n_fraud = int(n * fraud_rate)
        n_legit = n - n_fraud

        df = _base_beneficiary(n)
        injector = FraudScenarioInjector()

        # Each scenario gets a roughly equal share of the fraud quota
        per_scenario = max(1, n_fraud // 4)
        fraud_indices = _RNG.choice(n_fraud * 2, size=n_fraud, replace=False)
        # We will inject in-place on the first n_fraud rows, then shuffle
        df_fraud = df.iloc[:n_fraud].copy()
        df_legit = df.iloc[n_fraud:].copy()

        scenarios = [
            injector.inject_multi_enrollment,
            injector.inject_network_fraud,
            injector.inject_socio_economic,
            injector.inject_payment_anomalies,
        ]
        start = 0
        for scenario_fn in scenarios:
            end = min(start + per_scenario, n_fraud)
            df_fraud = scenario_fn(df_fraud, list(range(start, end)))
            start = end

        df_fraud["is_fraud"] = 1

        combined = pd.concat([df_legit, df_fraud], ignore_index=True)
        # Shuffle so fraud rows are not contiguous
        combined = combined.sample(frac=1, random_state=42).reset_index(drop=True)
        logger.info("Generated %d beneficiaries (%d fraud, %d legit)", n, n_fraud, n_legit)
        return combined
