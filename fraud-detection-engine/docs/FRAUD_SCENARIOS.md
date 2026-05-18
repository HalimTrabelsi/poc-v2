# Fraud Scenarios

## Overview

The engine detects four primary fraud patterns, each captured by a dedicated YAML rule file and ML feature set.

---

## 1. Multi-Enrollment (ME001–ME002)

**Definition**: A single beneficiary is enrolled in multiple social programs simultaneously, collecting benefits they may not be entitled to under each program's eligibility rules.

**Key Indicators**
- `nb_programs >= 4` — actively registered in four or more programs
- `nb_active_programs >= 3` with `avg_enrollment_days < 180` — rapid sequential enrollment across programs

**Rules**

| ID | Condition | Weight | Alert |
|----|-----------|--------|-------|
| ME001 | `nb_programs >= 4` | 0.30 | HIGH |
| ME002 | `nb_active_programs >= 3 and avg_enrollment_days < 180` | 0.35 | HIGH |

**Example Case**: A beneficiary enrolled in cash transfer, food subsidy, disability support, and housing programs within six months, with an average enrollment age of 120 days.

---

## 2. Network Fraud (NF001–NF004)

**Definition**: A cluster of beneficiaries share contact information or bank accounts, suggesting coordinated registration by an agent or family member submitting fictitious applications.

**Key Indicators**
- `shared_account_count >= 2` — same bank account registered by multiple beneficiaries
- `shared_phone_count >= 3` — same phone number across three or more applications
- Combined phone + account sharing (strongest signal)
- Elevated `network_risk` score (derived: `phone×0.4 + account×0.6`, capped at 1.0)

**Rules**

| ID | Condition | Weight | Alert |
|----|-----------|--------|-------|
| NF001 | `shared_account_count >= 2` | 0.35 | HIGH |
| NF002 | `shared_phone_count >= 3` | 0.30 | HIGH |
| NF003 | `shared_phone_count >= 2 and shared_account_count >= 2` | 0.40 | CRITICAL |
| NF004 | `network_risk >= 0.60` | 0.30 | HIGH |

**Example Case**: Five beneficiaries share the same mobile number and two share a bank account. NetworkX graph density = 0.8. graph_score = 0.72.

---

## 3. Socio-Economic Mismatch (SE001–SE003)

**Definition**: A beneficiary's declared socio-economic profile is internally inconsistent or implausibly extreme, suggesting falsified intake data.

**Key Indicators**
- Very low PMT (Proxy Means Test) score (`pmt_score <= 0.20`) which is statistically anomalous
- Low income per person (`< 50`) while enrolled in many programs (`>= 3`) — income underreporting
- Large household (`>= 8 members`) with extremely low income per person (`< 70`) — ghost household

**Rules**

| ID | Condition | Weight | Alert |
|----|-----------|--------|-------|
| SE001 | `pmt_score <= 0.20` | 0.20 | MEDIUM |
| SE002 | `income_per_person < 50 and nb_programs >= 3` | 0.20 | MEDIUM |
| SE003 | `household_size >= 8 and income_per_person < 70` | 0.25 | HIGH |

**Example Case**: Declared household of 12 members with total income of 120 (income_per_person = 10), enrolled in 4 programs. PMT score = 0.08.

---

## 4. Payment Anomalies (PA001–PA003)

**Definition**: Payment history shows patterns inconsistent with legitimate benefit receipt — high failure rates, outlier amounts, or payment failures combined with network fraud signals.

**Key Indicators**
- `payment_gap_ratio >= 0.50` — more than half of issued payments were not actually paid
- High gap combined with network fraud (`shared_phone >= 2` or `shared_account >= 2`)
- `high_amount_flag = 1` — total issued amount above the 95th percentile

**Rules**

| ID | Condition | Weight | Alert |
|----|-----------|--------|-------|
| PA001 | `payment_gap_ratio >= 0.50` | 0.30 | HIGH |
| PA002 | `payment_gap_ratio >= 0.30 and (shared_phone_count >= 2 or shared_account_count >= 2)` | 0.35 | CRITICAL |
| PA003 | `high_amount_flag >= 1` | 0.20 | MEDIUM |

**Example Case**: A beneficiary has 65% of issued payments unreconciled, while also sharing a phone number with three other beneficiaries. PA002 fires at weight 0.35.

---

## Scoring Combination

Each scenario contributes to the final risk score through three channels:

| Channel | Weight | Source |
|---------|--------|--------|
| Rule score | 0.30 | Sum of triggered rule weights |
| ML score | 0.50 | XGBoost probability (+ IsoForest) |
| Graph score | 0.20 | NetworkX centrality + density |

Final score = `0.30 × rule_score + 0.50 × ml_score + 0.20 × graph_score`

Risk thresholds: CRITICAL ≥ 0.80, HIGH ≥ 0.60, MEDIUM ≥ 0.40, LOW < 0.40
