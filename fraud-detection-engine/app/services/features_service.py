"""Stateless feature calculation service."""
import logging
from datetime import date
from typing import Optional

from app.data.extractors import BeneficiaryExtractor

logger = logging.getLogger(__name__)

FEATURE_NAMES = [
    "age",
    "income",
    "income_per_person",
    "household_size",
    "nb_children",
    "nb_elderly",
    "dependency_ratio",
    "has_disabled",
    "single_head",
    "nb_programs",
    "nb_active_programs",
    "pmt_score",
    "pmt_score_min",
    "avg_enrollment_days",
    "payment_count",
    "payment_gap_ratio",
    "payment_success_rate",
    "amount_variance",
    "cycle_count",
    "shared_phone_count",
    "shared_account_count",
    "identity_cluster_count",
    "network_risk",
    "group_membership_count",
    "high_amount_flag",
    "income_program_inconsistency",
    "income_ratio_to_national",
    "household_size_deviation",
    # Used by the rule engine (IDC003 "Duplicate National ID") AND now part
    # of ML_FEATURES in ml/scripts/train_openg2p.py. Still NOT computed by
    # the live OpenG2P SQL extractor (extractors.py has no g2p_reg_id join
    # yet) — always defaults to 0 in production until that's wired up. Only
    # the synthetic training data (generate_dataset.py) has real values.
    "duplicate_national_id_count",
    # Temporal features (referenced by rules TA001-TA005)
    "days_reg_to_first_payment",
    "enroll_last_30d",
    "enroll_last_90d",
    "enrollment_velocity",
    "payments_within_7d_of_enroll",
    "registration_age_days",
]

# Sentinel: 999 days = "no payment yet" so the < 7 ghost-payment rule (TA002)
# does NOT trigger on missing data. Same idea for the other temporal counters.
_DEFAULTS: dict = {
    "age": 35,
    "income": 0.0,
    "income_per_person": 0.0,
    "household_size": 1,
    "nb_children": 0,
    "nb_elderly": 0,
    "dependency_ratio": 0.0,
    "has_disabled": 0,
    "single_head": 0,
    "nb_programs": 0,
    "nb_active_programs": 0,
    "pmt_score": 0.5,
    "pmt_score_min": 0.5,
    "avg_enrollment_days": 0,
    "payment_count": 0,
    "payment_gap_ratio": 0.0,
    "payment_success_rate": 0.0,
    "amount_variance": 0.0,
    "cycle_count": 0,
    "shared_phone_count": 0,
    "shared_account_count": 0,
    "identity_cluster_count": 0,
    "network_risk": 0.0,
    "group_membership_count": 0,
    "high_amount_flag": 0,
    "income_program_inconsistency": 0,
    # Country-relative features: the model is trained on multi-scale data
    # (see ml/scripts/generate_dataset.py add_country_scale_diversity), but no
    # national-median source exists at serving time yet, so both stay at their
    # neutral training-distribution centers (ratio-to-median=1.0, deviation=0.0).
    # Wiring a real national median here is the follow-up that activates them.
    "income_ratio_to_national": 1.0,
    "household_size_deviation": 0.0,
    "duplicate_national_id_count": 0,
    # Temporal defaults (sentinels chosen so missing data does NOT trigger rules)
    "days_reg_to_first_payment": 999,
    "enroll_last_30d": 0,
    "enroll_last_90d": 0,
    "enrollment_velocity": 0.0,
    "payments_within_7d_of_enroll": 0,
    "registration_age_days": 365,
}


class FeatureEngineer:
    """Orchestrate raw extraction and derived-feature computation for one beneficiary."""

    FEATURE_NAMES = FEATURE_NAMES

    def __init__(self, extractor: Optional[BeneficiaryExtractor] = None) -> None:
        self._extractor = extractor or BeneficiaryExtractor()

    def get_features(
        self, beneficiary_id: str, snapshot_date: Optional[date] = None
    ) -> dict:
        """Extract + engineer features for one beneficiary.

        The SQL query already computes most derived columns; this method adds
        any remaining transformations and ensures the feature dict is complete.
        """
        raw = self._extractor.get_beneficiary_features(beneficiary_id, snapshot_date)

        # Re-derive in case the SQL layer returned nulls for computed columns
        income = float(raw.get("income", 0.0) or 0.0)
        household_size = max(int(raw.get("household_size", 1) or 1), 1)
        nb_children = int(raw.get("nb_children", 0) or 0)
        nb_elderly = int(raw.get("nb_elderly", 0) or 0)
        shared_phone = int(raw.get("shared_phone_count", 0) or 0)
        shared_account = int(raw.get("shared_account_count", 0) or 0)

        raw["income_per_person"] = round(income / household_size, 2)

        adults = max(household_size - nb_children - nb_elderly, 1)
        raw["dependency_ratio"] = round((nb_children + nb_elderly) / adults, 3)

        raw["network_risk"] = round(
            min(1.0, min(shared_phone, 5) * 0.4 + min(shared_account, 5) * 0.6), 4
        )

        # high_amount_flag and income_program_inconsistency need population quantiles,
        # so we preserve whatever the extractor computed; default to 0 if absent.
        raw.setdefault("high_amount_flag", 0)
        raw.setdefault("income_program_inconsistency", 0)

        return self.validate_features(raw)

    def validate_features(self, features: dict) -> dict:
        """Fill missing feature keys with sensible defaults and return cleaned dict."""
        validated = _DEFAULTS.copy()
        for key in FEATURE_NAMES:
            val = features.get(key)
            if val is not None:
                try:
                    validated[key] = float(val)
                except (TypeError, ValueError):
                    pass  # keep default
        return validated
