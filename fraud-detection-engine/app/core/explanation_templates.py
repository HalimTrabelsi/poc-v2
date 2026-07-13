"""Deterministic translation of technical flags into human-readable sentences.

NEVER let a raw rule_id, snake_case feature name, or numeric threshold reach
the screen — everything shown to a non-technical fraud officer must go
through this module first. The LLM (see app.services.llm_explainer_service)
is only allowed to rephrase the plain-language sentences produced here into
fluent prose; it must never be asked to interpret raw numbers itself.

Keyed by the real rule_id values defined in app/rules/rules/*.yaml (NF*,
ME*, PA*, SE*, TA*, IDC*) — these are the stable identifiers already present
on every triggered-rule dict returned by RuleService.evaluate().
"""

RULE_EXPLANATIONS = {
    # network_fraud.yaml
    "NF001": "shares a bank account with {shared_account_count} other beneficiary(ies)",
    "NF002": "shares a phone number with {shared_phone_count} other beneficiary(ies)",
    "NF003": "shares both a phone number AND a bank account with other beneficiaries — a strong signal of an organized network",
    "NF004": "is strongly connected to other at-risk beneficiaries in the network",
    "NF005": "was flagged as a duplicate by OpenG2P's own deduplication check",
    "NF006": "appears in multiple formal duplicate records across programs",
    "IDC001": "shares the same birthdate and family name as {identity_cluster_count} other beneficiaries",
    "IDC002": "shares the same birthdate and family name as another beneficiary",
    # multi_enrollment.yaml
    "ME001": "is enrolled in {nb_programs} programs at the same time, which is unusual",
    "ME002": "has {nb_active_programs} active programs and enrolled very recently",
    # payment_anomalies.yaml
    "PA001": "failed {payment_gap_pct}% of recent payments",
    "PA002": "combines failed payments with shared resources with other beneficiaries",
    "PA003": "received payment amounts far above the normal range",
    # socio_economic.yaml
    "SE001": "has an eligibility score abnormally low for the declared profile",
    "SE002": "declares very low income while enrolled in multiple programs",
    "SE003": "declares a household of {household_size} people with very low income per person, a rare profile that needs review",
    # temporal_anomalies.yaml
    "TA001": "enrolled in several programs within the last 30 days — abnormal pace",
    "TA002": "received a first payment only days after registering — possible ghost identity",
    "TA003": "is enrolling in new programs far faster than a typical beneficiary",
    "TA004": "received a payment within days of enrolling, bypassing the normal approval timeline",
    "TA005": "had a burst of enrollments shortly after registering",
}

FEATURE_EXPLANATIONS = {
    "shared_account_count": {
        "high": "shares a bank account with other people",
        "low": "has no shared bank account",
    },
    "shared_phone_count": {
        "high": "shares a phone number with other people",
        "low": "has a phone number unique to them",
    },
    "nb_programs": {
        "high": "is enrolled in an unusual number of programs",
        "low": "is enrolled in a normal number of programs",
    },
    "nb_active_programs": {
        "high": "has a high number of simultaneously active programs",
        "low": "has a normal number of active programs",
    },
    "income_per_person": {
        "high": "declares an above-average income per person",
        "low": "declares a very low income per person",
    },
    "payment_gap_ratio": {
        "high": "has a high rate of failed payments",
        "low": "has a normal payment history",
    },
    "pmt_score": {
        "high": "has an eligibility score consistent with their profile",
        "low": "has an abnormally low eligibility score",
    },
    "pmt_score_min": {
        "high": "has a consistent minimum eligibility score",
        "low": "has a very low minimum eligibility score",
    },
    "network_risk": {
        "high": "is connected to a network of at-risk beneficiaries",
        "low": "has no suspicious connection to other beneficiaries",
    },
    "network_score": {
        "high": "is connected to a network of at-risk beneficiaries",
        "low": "has no suspicious connection to other beneficiaries",
    },
    "household_size": {
        "high": "declares an unusually large household",
        "low": "declares a normal household size",
    },
    "income_program_inconsistency": {
        "high": "has declared income inconsistent with their number of programs",
        "low": "has income consistent with their profile",
    },
    "high_amount_flag": {
        "high": "received unusually high payment amounts",
        "low": "received amounts within normal range",
    },
}


def translate_shap_factor(feature_name: str, value: float, direction: str) -> str | None:
    """direction: 'increases_risk' or 'decreases_risk' (matches explainability_service.py)."""
    template = FEATURE_EXPLANATIONS.get(feature_name)
    if not template:
        return None
    key = "high" if direction == "increases_risk" else "low"
    return template.get(key)


def translate_rule_flags(triggered_rules: list, context: dict) -> list[str]:
    """Turn triggered-rule dicts (rule_id/name/explanation) into human sentences.

    Args:
        triggered_rules: the list returned by RuleService.evaluate()
            (each item: {"rule_id": ..., "name": ..., "explanation": ..., ...}).
        context: the features dict used to fill in real values (shared_account_count,
            nb_programs, etc.) — never the raw rule 'explanation' string, which
            contains thresholds/percentages not meant for this audience.
    """
    sentences = []
    for rule in triggered_rules:
        if not isinstance(rule, dict):
            continue
        rule_id = rule.get("rule_id")
        template = RULE_EXPLANATIONS.get(rule_id)
        if not template:
            continue
        try:
            sentence = template.format(
                shared_account_count=int(context.get("shared_account_count", 0) or 0),
                shared_phone_count=int(context.get("shared_phone_count", 0) or 0),
                identity_cluster_count=int(context.get("identity_cluster_count", 0) or 0),
                nb_programs=int(context.get("nb_programs", 0) or 0),
                nb_active_programs=int(context.get("nb_active_programs", 0) or 0),
                household_size=int(context.get("household_size", 0) or 0),
                payment_gap_pct=int(round(float(context.get("payment_gap_ratio", 0) or 0) * 100)),
            )
            sentences.append(sentence)
        except (KeyError, ValueError, TypeError):
            continue
    return sentences
