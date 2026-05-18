"""Inject realistic, ambiguous fraud patterns into a beneficiary DataFrame."""
import logging

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

_RNG = np.random.default_rng(99)

SCENARIOS = ["multi_enrollment", "network_fraud", "socio_economic", "payment_anomalies"]


class FraudScenarioInjector:
    """Mutate selected rows of a DataFrame to exhibit specific fraud patterns.

    Fraud is deliberately imperfect: noise is added so that a trained model
    cannot simply memorise the injection rules.
    """

    def inject_multi_enrollment(self, df: pd.DataFrame, indices: list[int]) -> pd.DataFrame:
        """Inflate program enrollment counts for the selected rows."""
        idx = [i for i in indices if i < len(df)]
        if not idx:
            return df
        df = df.copy()
        df.loc[idx, "nb_programs"] = _RNG.integers(4, 8, size=len(idx)).astype(float)
        df.loc[idx, "nb_active_programs"] = (
            df.loc[idx, "nb_programs"] * _RNG.uniform(0.6, 1.0, size=len(idx))
        ).astype(int).astype(float)
        # Some with suspiciously recent enrollment to match ME002
        short_tenure = _RNG.choice([True, False], size=len(idx), p=[0.6, 0.4])
        df.loc[[idx[i] for i, s in enumerate(short_tenure) if s], "avg_enrollment_days"] = (
            _RNG.integers(30, 150, size=int(short_tenure.sum()))
        ).astype(float)
        return df

    def inject_network_fraud(self, df: pd.DataFrame, indices: list[int]) -> pd.DataFrame:
        """Raise shared contact/account counts for the selected rows."""
        idx = [i for i in indices if i < len(df)]
        if not idx:
            return df
        df = df.copy()
        df.loc[idx, "shared_phone_count"] = _RNG.integers(2, 6, size=len(idx)).astype(float)
        df.loc[idx, "shared_account_count"] = _RNG.integers(1, 4, size=len(idx)).astype(float)
        # Recompute network_risk for consistency
        df.loc[idx, "network_risk"] = (
            df.loc[idx, "shared_phone_count"].clip(upper=5) * 0.4
            + df.loc[idx, "shared_account_count"].clip(upper=5) * 0.6
        ).clip(upper=1.0)
        return df

    def inject_socio_economic(self, df: pd.DataFrame, indices: list[int]) -> pd.DataFrame:
        """Introduce implausible socio-economic profiles."""
        idx = [i for i in indices if i < len(df)]
        if not idx:
            return df
        df = df.copy()
        # Very low PMT score
        df.loc[idx, "pmt_score"] = _RNG.uniform(0.05, 0.20, size=len(idx))
        df.loc[idx, "pmt_score_min"] = df.loc[idx, "pmt_score"] * 0.9
        # Large household with tiny income per person (ghost household pattern)
        half = len(idx) // 2
        ghost_idx = idx[:half]
        if ghost_idx:
            df.loc[ghost_idx, "household_size"] = _RNG.integers(8, 15, size=len(ghost_idx)).astype(float)
            df.loc[ghost_idx, "income"] = _RNG.uniform(50, 200, size=len(ghost_idx))
            df.loc[ghost_idx, "income_per_person"] = (
                df.loc[ghost_idx, "income"] / df.loc[ghost_idx, "household_size"]
            )
        # Low income while enrolled in multiple programs
        rest_idx = idx[half:]
        if rest_idx:
            df.loc[rest_idx, "income_per_person"] = _RNG.uniform(10, 45, size=len(rest_idx))
            df.loc[rest_idx, "nb_programs"] = _RNG.integers(3, 6, size=len(rest_idx)).astype(float)
        return df

    def inject_payment_anomalies(self, df: pd.DataFrame, indices: list[int]) -> pd.DataFrame:
        """Create payment patterns inconsistent with legitimate behaviour."""
        idx = [i for i in indices if i < len(df)]
        if not idx:
            return df
        df = df.copy()
        df.loc[idx, "payment_gap_ratio"] = _RNG.uniform(0.45, 0.95, size=len(idx))
        df.loc[idx, "payment_success_rate"] = 1.0 - df.loc[idx, "payment_gap_ratio"]
        # High amount anomaly for half the group
        half = len(idx) // 2
        df.loc[idx[:half], "high_amount_flag"] = 1.0
        df.loc[idx[:half], "amount_variance"] = _RNG.exponential(2000, size=half)
        # Combine with network indicators for PA002 rule
        df.loc[idx, "shared_phone_count"] = np.maximum(
            df.loc[idx, "shared_phone_count"], _RNG.integers(1, 3, size=len(idx)).astype(float)
        )
        df.loc[idx, "income_program_inconsistency"] = _RNG.choice(
            [0, 1], size=len(idx), p=[0.3, 0.7]
        ).astype(float)
        return df
