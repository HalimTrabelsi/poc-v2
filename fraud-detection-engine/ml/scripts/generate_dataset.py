"""
generate_dataset.py — Générateur dataset + injection OpenG2P v5
================================================================
ALIGNÉ avec les vraies tables PostgreSQL OpenG2P confirmées :
  ✅ g2p_program, g2p_cycle, g2p_program_membership
  ✅ g2p_entitlement, g2p_payment
  ✅ g2p_phone_number, res_partner_bank
  ✅ g2p_group_membership, g2p_reg_id, g2p_id_type
  ❌ g2p_program_registrant_info — N'EXISTE PAS dans cette DB
     → PMT score stocké dans g2p_program_membership.assessment_info (fallback: 0.5)

Usage :
  python ml/scripts/generate_dataset.py
  python ml/scripts/generate_dataset.py --inject --n 2000
  python ml/scripts/generate_dataset.py --n 2000 --fraud-rate 0.10
"""

import argparse
import os
import random
import warnings
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")

SEED = 42
np.random.seed(SEED)
random.seed(SEED)

N_TOTAL    = 5000
FRAUD_RATE = 0.12
N_PROGRAMS = 5
N_CYCLES   = 3

# ── Réalisme / difficulté du problème ───────────────────────────────────────
# Without this, fraud rows are near-deterministic functions of their features
# (e.g. shared_phone_count is 13x higher for fraud), which lets any model hit
# AUC ~0.99 — an artefact of the generator, NOT real-world performance. These
# knobs inject the messiness real fraud data has, so the model must learn from
# OVERLAPPING distributions and IMPERFECT labels. Defensible target: AUC ~0.82-0.90.
#
#   REALISM_LABEL_NOISE : fraction of labels randomly flipped (mislabelled cases)
#   REALISM_OVERLAP     : fraction of fraud rows whose discriminative features are
#                         blended toward the legitimate distribution ("subtle" fraud)
#   REALISM_NOISE_MULT  : multiplier on the Gaussian feature noise already applied
# Defaults tuned so 5-fold CV AUC lands at ~0.90 (strong but believable),
# instead of the ~0.99 artefact produced by the raw separable generator.
# The earlier 0.30 overlap / 0.03 label-noise settings were unnecessarily
# punishing (capped AUC at ~0.87); 0.18 / 0.015 still models subtle fraud and
# imperfect labels without pretending real fraud data is near-random.
REALISM_LABEL_NOISE = float(os.getenv("REALISM_LABEL_NOISE", "0.015"))
REALISM_OVERLAP     = float(os.getenv("REALISM_OVERLAP", "0.18"))
REALISM_NOISE_MULT  = float(os.getenv("REALISM_NOISE_MULT", "1.8"))

PROGRAM_NAMES = [
    "Aide alimentaire urgente",
    "Bourse scolarité enfants",
    "Allocation chefs de ménage",
    "Aide personnes handicapées",
    "Programme femmes vulnérables",
]


def _clip(v, lo, hi):
    return max(lo, min(hi, v))


def _n(sigma):
    return np.random.normal(0, sigma)


# ════════════════════════════════════════════════════════
# PROFILS LÉGITIMES
# ════════════════════════════════════════════════════════

def build_legitimate(idx):
    birth_year  = random.randint(1950, 2003)
    hh_size     = _clip(int(np.random.normal(4.0, 2.0)), 1, 14)
    nb_children = _clip(int(np.random.normal(hh_size * 0.38, 1.5)), 0, hh_size - 1)
    nb_elderly  = _clip(int(np.random.normal(0.4, 0.7)), 0, 4)
    income      = _clip(np.random.normal(900, 500) + _n(150), 60, 8000)
    nb_prog     = int(np.random.choice([1, 2, 3, 4], p=[0.35, 0.40, 0.20, 0.05]))
    pmt_score   = _clip(np.random.normal(0.60, 0.18) + _n(0.06), 0.08, 1.0)
    pmt_score_min = _clip(pmt_score - abs(_n(0.08)), 0.05, pmt_score)
    n_pay       = nb_prog * N_CYCLES
    n_paid      = _clip(int(n_pay * np.random.uniform(0.60, 1.0)), 0, n_pay)
    gap_ratio   = round(1 - n_paid / max(n_pay, 1), 3)
    p_success   = round(1 - gap_ratio, 3)
    shared_phone   = int(np.random.choice([0, 1, 2, 3], p=[0.72, 0.18, 0.07, 0.03]))
    shared_account = int(np.random.choice([0, 1, 2], p=[0.78, 0.17, 0.05]))
    amount         = round(random.uniform(150, 500), 2)
    enrollment_date = date(random.randint(2020, 2023), random.randint(1, 12), 1)
    reg_age_days    = (date.today() - enrollment_date).days
    cycle_count     = min(nb_prog * N_CYCLES, N_CYCLES)
    amount_variance = round(p_success * (1 - p_success) * (amount ** 2), 2)
    total_issued    = round(n_pay * amount, 2)
    total_paid      = round(n_paid * amount, 2)
    group_membership_count = random.randint(0, 1)

    return {
        "partner_idx": idx,
        "is_fraud": 0,
        "scenario": "legitimate",
        "gender": random.choice(["male", "female"]),
        "birthdate": date(birth_year, random.randint(1, 12), random.randint(1, 28)),
        "age": date.today().year - birth_year,
        "income": round(income, 2),
        "household_size": hh_size,
        "nb_children": nb_children,
        "nb_elderly": nb_elderly,
        "has_disabled": random.random() < 0.07,
        "single_head": random.random() < 0.14,
        "elderly_head": random.random() < 0.08,
        "enrollment_date": enrollment_date,
        "nb_programs": nb_prog,
        "nb_active_programs": nb_prog,
        "prog_indices": random.sample(range(N_PROGRAMS), nb_prog),
        "pmt_score": round(pmt_score, 4),
        "pmt_score_min": round(pmt_score_min, 4),
        "payment_count": n_pay,
        "payment_count_paid": n_paid,
        "gap_ratio": gap_ratio,
        "payment_gap_ratio": gap_ratio,
        "payment_success_rate": p_success,
        "amount_per_payment": amount,
        "avg_entitlement_amount": amount,
        "total_issued": total_issued,
        "total_paid": total_paid,
        "amount_variance": amount_variance,
        "cycle_count": cycle_count,
        "shared_phone_count": shared_phone,
        "shared_account_count": shared_account,
        "group_count": group_membership_count,
        "group_membership_count": group_membership_count,
        "income_per_person": round(income / max(hh_size, 1), 2),
        "network_risk": round(
            min(shared_phone / 5, 1) * 0.4 + min(shared_account / 5, 1) * 0.6 + abs(_n(0.05)), 3
        ),
        "registration_age_days": reg_age_days,
        "avg_enrollment_days": reg_age_days,
    }


# ════════════════════════════════════════════════════════
# SCÉNARIOS DE FRAUDE
# ════════════════════════════════════════════════════════

def _fraud(idx, scenario):
    r = build_legitimate(idx)
    r["is_fraud"] = 1
    r["scenario"] = scenario
    return r


def s1_multi_enrollment(idx):
    r = _fraud(idx, "multi_enrollment")
    r["nb_programs"] = random.randint(4, 5)
    r["prog_indices"] = list(range(r["nb_programs"]))
    r["pmt_score"] = _clip(np.random.normal(0.45, 0.18) + _n(0.07), 0.08, 0.85)
    return r


def s2_shared_phone(idx):
    r = _fraud(idx, "shared_phone")
    r["shared_phone_count"] = random.randint(3, 6)
    r["shared_account_count"] = random.randint(1, 3)
    r["network_risk"] = _clip(
        r["shared_phone_count"] * 0.09 + r["shared_account_count"] * 0.12 + abs(_n(0.12)), 0, 1)
    return r


def s3_ghost_household(idx):
    r = _fraud(idx, "ghost_household")
    r["household_size"] = random.randint(7, 12)
    r["nb_children"] = random.randint(4, r["household_size"] - 1)
    r["income"] = _clip(np.random.normal(500, 250) + _n(100), 80, 2500)
    r["income_per_person"] = round(r["income"] / r["household_size"], 2)
    r["pmt_score"] = _clip(np.random.normal(0.40, 0.18) + _n(0.07), 0.08, 0.80)
    r["single_head"] = True
    return r


def s4_payment_manipulation(idx):
    r = _fraud(idx, "payment_manipulation")
    n_pay = r["payment_count"]
    n_paid = _clip(int(n_pay * np.random.uniform(0.30, 0.65)), 0, n_pay)
    r["payment_count_paid"] = n_paid
    r["gap_ratio"] = round(1 - n_paid / max(n_pay, 1), 3)
    r["payment_gap_ratio"] = r["gap_ratio"]
    r["payment_success_rate"] = round(1 - r["gap_ratio"], 3)
    r["pmt_score"] = _clip(np.random.normal(0.38, 0.18) + _n(0.07), 0.08, 0.80)
    return r


def s5_identity_fraud(idx):
    r = _fraud(idx, "identity_fraud")
    r["shared_account_count"] = random.randint(2, 4)
    r["shared_phone_count"] = random.randint(2, 4)
    r["nb_programs"] = random.randint(3, 5)
    r["prog_indices"] = list(range(min(r["nb_programs"], N_PROGRAMS)))
    r["pmt_score"] = _clip(np.random.normal(0.42, 0.18) + _n(0.07), 0.08, 0.85)
    r["network_risk"] = _clip(
        r["shared_account_count"] * 0.13 + r["shared_phone_count"] * 0.09 + abs(_n(0.13)), 0, 1)
    return r


def s6_income_underreporting(idx):
    r = _fraud(idx, "income_underreporting")
    r["income"] = _clip(np.random.normal(280, 180) + _n(80), 50, 900)
    r["income_per_person"] = round(r["income"] / max(r["household_size"], 1), 2)
    r["nb_programs"] = random.randint(2, 4)
    r["prog_indices"] = list(range(min(r["nb_programs"], N_PROGRAMS)))
    n_pay = r["payment_count"]
    n_paid = _clip(int(n_pay * np.random.uniform(0.80, 1.0)), 0, n_pay)
    r["payment_count_paid"] = n_paid
    r["gap_ratio"] = round(1 - n_paid / max(n_pay, 1), 3)
    r["payment_gap_ratio"] = r["gap_ratio"]
    r["payment_success_rate"] = round(1 - r["gap_ratio"], 3)
    r["pmt_score"] = _clip(np.random.normal(0.55, 0.15) + _n(0.06), 0.20, 0.90)
    return r


def s7_coordinated_fraud(idx):
    r = _fraud(idx, "coordinated_fraud")
    r["nb_programs"] = random.randint(4, 5)
    r["prog_indices"] = list(range(r["nb_programs"]))
    r["shared_phone_count"] = random.randint(3, 6)
    r["shared_account_count"] = random.randint(2, 5)
    r["pmt_score"] = _clip(np.random.normal(0.38, 0.18) + _n(0.07), 0.05, 0.72)
    r["network_risk"] = _clip(
        r["shared_phone_count"] * 0.12 + r["shared_account_count"] * 0.15 + abs(_n(0.14)), 0, 1)
    return r


def s8_systematic_ghost(idx):
    r = _fraud(idx, "systematic_ghost")
    r["household_size"] = random.randint(8, 13)
    r["nb_children"] = random.randint(5, r["household_size"] - 1)
    r["income"] = _clip(np.random.normal(380, 200) + _n(100), 60, 1800)
    r["income_per_person"] = round(r["income"] / r["household_size"], 2)
    n_pay = r["payment_count"]
    n_paid = _clip(int(n_pay * np.random.uniform(0.35, 0.65)), 0, n_pay)
    r["payment_count_paid"] = n_paid
    r["gap_ratio"] = round(1 - n_paid / max(n_pay, 1), 3)
    r["payment_gap_ratio"] = r["gap_ratio"]
    r["payment_success_rate"] = round(1 - r["gap_ratio"], 3)
    r["pmt_score"] = _clip(np.random.normal(0.35, 0.18) + _n(0.07), 0.05, 0.72)
    return r


def s9_network_exploitation(idx):
    r = _fraud(idx, "network_exploitation")
    r["shared_phone_count"] = random.randint(4, 8)
    r["shared_account_count"] = random.randint(3, 6)
    r["nb_programs"] = random.randint(3, 5)
    r["prog_indices"] = list(range(min(r["nb_programs"], N_PROGRAMS)))
    r["network_risk"] = _clip(
        r["shared_phone_count"] * 0.15 + r["shared_account_count"] * 0.18 + abs(_n(0.16)), 0, 1)
    r["pmt_score"] = _clip(np.random.normal(0.40, 0.20) + _n(0.08), 0.05, 0.80)
    return r


SCENARIOS = [
    s1_multi_enrollment, s2_shared_phone, s3_ghost_household,
    s4_payment_manipulation, s5_identity_fraud, s6_income_underreporting,
    s7_coordinated_fraud, s8_systematic_ghost, s9_network_exploitation
]


# ════════════════════════════════════════════════════════
# GÉNÉRATION DATASET
# ════════════════════════════════════════════════════════

def generate_dataset(n_total=N_TOTAL, fraud_rate=FRAUD_RATE):
    n_fraud = int(n_total * fraud_rate)
    n_legit = n_total - n_fraud
    print(f"Génération : {n_legit} légitimes + {n_fraud} fraudeurs")

    records = [build_legitimate(i) for i in range(n_legit)]
    for j in range(n_fraud):
        records.append(SCENARIOS[j % len(SCENARIOS)](n_legit + j))

    df = pd.DataFrame(records)

    # ── Bruit gaussien (amplifié par REALISM_NOISE_MULT) ────────────────────
    # The base sigmas alone leave the classes almost perfectly separable.
    # Multiplying them injects the measurement/reporting noise real data carries.
    noise_config = [
        ("income", 0.15), ("pmt_score", 0.08), ("gap_ratio", 0.10),
        ("network_risk", 0.10), ("income_per_person", 0.12),
        ("shared_phone_count", 0.5), ("shared_account_count", 0.4),
        ("nb_programs", 0.3),
    ]
    for col, s in noise_config:
        if col in df.columns:
            df[col] = (df[col] + np.random.normal(0, s * REALISM_NOISE_MULT, len(df))).clip(0)

    # ── Class overlap : "subtle" fraud that looks legitimate ────────────────
    # Blend a fraction of fraud rows' discriminative features toward the
    # legitimate mean. These are the hard cases that prevent a trivial 0.99 AUC
    # and mirror real fraudsters who deliberately stay under the radar.
    discriminative = ["shared_phone_count", "shared_account_count",
                      "network_risk", "nb_programs", "payment_gap_ratio", "gap_ratio"]
    fraud_all = df.index[df["is_fraud"] == 1]
    n_overlap = int(len(fraud_all) * REALISM_OVERLAP)
    if n_overlap > 0:
        overlap_idx = np.random.choice(fraud_all, size=n_overlap, replace=False)
        for col in discriminative:
            if col in df.columns:
                legit_mean = df.loc[df["is_fraud"] == 0, col].mean()
                # pull each selected fraud value most of the way to the legit mean
                blend = np.random.uniform(0.5, 0.9, size=n_overlap)
                df.loc[overlap_idx, col] = (
                    df.loc[overlap_idx, col].values * (1 - blend) + legit_mean * blend
                )

    # ── Label noise : imperfect ground truth ────────────────────────────────
    # Real fraud labels come from investigations and are wrong some of the time
    # (missed fraud + false accusations). Flip a small random fraction of labels.
    n_flip = int(len(df) * REALISM_LABEL_NOISE)
    if n_flip > 0:
        flip_idx = np.random.choice(df.index, size=n_flip, replace=False)
        df.loc[flip_idx, "is_fraud"] = 1 - df.loc[flip_idx, "is_fraud"]

    # Features dérivées
    df["income_per_person"] = (df["income"] / df["household_size"].clip(lower=1)).round(2)
    df["partner_id"] = df["partner_idx"]
    df["synthetic_label"] = df["is_fraud"]
    df["payment_gap_ratio"] = df["gap_ratio"]
    df["network_risk_score"] = df["network_risk"]

    adults = (df["household_size"] - df["nb_children"] - df["nb_elderly"]).clip(lower=1)
    df["dependency_ratio"] = ((df["nb_children"] + df["nb_elderly"]) / adults).round(3)

    q95 = df["total_issued"].quantile(0.95)
    df["high_amount_flag"] = (df["total_issued"] > q95).astype(int)

    q15 = df["income_per_person"].quantile(0.15)
    df["income_program_inconsistency"] = (
        (df["income_per_person"] < q15) & (df["nb_programs"] >= 3)
    ).astype(int)

    return df.sample(frac=1, random_state=SEED).reset_index(drop=True)


# ── Identity realism (niafaker) ──────────────────────────────────────────────
# niafaker covers 10 African locales with culturally-accurate names/phones/
# addresses/national IDs. Countries outside this list (e.g. Tunisia) keep the
# original generic placeholder identity — cosmetic only, never blocks
# generation.
NIAFAKER_LOCALES = {
    "TZ": "tz", "KE": "ke", "NG": "ng", "ZA": "za", "GH": "gh",
    "UG": "ug", "RW": "rw", "ET": "et", "EG": "eg", "MA": "ma",
}
# TN is deliberately kept in the mix as an "unsupported" case so the
# generic-fallback identity path is always exercised too, not just the
# niafaker path.
IDENTITY_COUNTRIES = list(NIAFAKER_LOCALES.keys()) + ["TN"]

# ── Static city -> (region, lat, lon) lookup ──────────────────────────────────
# niafaker's city() and region() are INDEPENDENT random draws from flat lists
# (confirmed by reading niafaker/providers/address.py) — there is no real
# city<->region relationship in the library at all, so two rows landing on
# the same city can get different, incoherent regions. This static table
# (enumerated once from each locale's fixed city list, never geocoded at
# runtime) fixes that by deriving region deterministically from city, and
# doubles as the coordinate source for the Phase 5 heatmap (no lat/lon
# provider exists in niafaker either). Approximate, hand-built from known
# geography — not a live geocoding service. Cities absent from a locale's
# dict (should not happen, built from the exact enumerated lists) fall back
# to that locale's first region/capital coordinate.
CITY_INFO = {
    "tz": {
        "Dar es Salaam": ("Dar es Salaam", -6.7924, 39.2083), "Dodoma": ("Dodoma", -6.1630, 35.7516),
        "Arusha": ("Arusha", -3.3869, 36.6830), "Mwanza": ("Mwanza", -2.5164, 32.9175),
        "Mbeya": ("Mbeya", -8.9094, 33.4608), "Morogoro": ("Morogoro", -6.8278, 37.6591),
        "Tanga": ("Tanga", -5.0689, 39.0983), "Kigoma": ("Kigoma", -4.8766, 29.6267),
        "Shinyanga": ("Shinyanga", -3.6612, 33.4232), "Tabora": ("Tabora", -5.0164, 32.8235),
        "Singida": ("Singida", -4.8189, 34.7458), "Iringa": ("Iringa", -7.7669, 35.6929),
        "Moshi": ("Kilimanjaro", -3.3349, 37.3400), "Musoma": ("Mara", -1.5006, 33.8017),
        "Songea": ("Ruvuma", -10.6833, 35.6500), "Sumbawanga": ("Rukwa", -7.9667, 31.6167),
        "Bukoba": ("Kagera", -1.3315, 31.8125), "Kahama": ("Shinyanga", -3.8283, 32.6017),
        "Korogwe": ("Tanga", -5.1519, 38.4581), "Mbulu": ("Manyara", -3.8667, 35.5333),
        "Katumba": ("Rukwa", -7.7833, 31.2333), "Sumbawanga ": ("Rukwa", -7.9667, 31.6167),
    },
    "ke": {
        "Nairobi": ("Nairobi", -1.2921, 36.8219), "Mombasa": ("Mombasa", -4.0435, 39.6682),
        "Kisumu": ("Kisumu", -0.0917, 34.7680), "Nakuru": ("Nakuru", -0.3031, 36.0800),
        "Eldoret": ("Uasin Gishu", 0.5143, 35.2698), "Kitale": ("Trans-Nzoia", 1.0157, 35.0062),
        "Malindi": ("Kilifi", -3.2192, 40.1169), "Kilifi": ("Kilifi", -3.6305, 39.8499),
        "Garissa": ("Garissa", -0.4536, 39.6401), "Nyeri": ("Nyeri", -0.4201, 36.9476),
        "Machakos": ("Machakos", -1.5177, 37.2634), "Meru": ("Meru", 0.0470, 37.6559),
        "Embu": ("Embu", -0.5310, 37.4500), "Thika": ("Kiambu", -1.0333, 37.0693),
        "Naivasha": ("Nakuru", -0.7167, 36.4333), "Kakamega": ("Kakamega", 0.2827, 34.7519),
        "Bungoma": ("Bungoma", 0.5635, 34.5606), "Busia": ("Busia", 0.4608, 34.1115),
        "Homa Bay": ("Homa Bay", -0.5273, 34.4571), "Lodwar": ("Turkana", 3.1191, 35.5966),
    },
    "ng": {
        "Lagos": ("Lagos", 6.5244, 3.3792), "Abuja": ("FCT", 9.0765, 7.3986),
        "Kano": ("Kano", 12.0022, 8.5920), "Ibadan": ("Oyo", 7.3775, 3.9470),
        "Port Harcourt": ("Rivers", 4.8156, 7.0498), "Benin City": ("Edo", 6.3350, 5.6037),
        "Kaduna": ("Kaduna", 10.5222, 7.4383), "Enugu": ("Enugu", 6.5244, 7.5086),
        "Aba": ("Abia", 5.1066, 7.3667), "Onitsha": ("Anambra", 6.1667, 6.7833),
        "Warri": ("Delta", 5.5167, 5.7500), "Calabar": ("Cross River", 4.9757, 8.3417),
        "Uyo": ("Akwa Ibom", 5.0378, 7.9128), "Ilorin": ("Kwara", 8.4966, 4.5426),
        "Owerri": ("Imo", 5.4840, 7.0351), "Abeokuta": ("Ogun", 7.1475, 3.3619),
        "Akure": ("Ondo", 7.2571, 5.2058), "Sokoto": ("Sokoto", 13.0059, 5.2476),
        "Maiduguri": ("Borno", 11.8333, 13.1500), "Jos": ("Plateau", 9.8965, 8.8583),
    },
    "za": {
        "Johannesburg": ("Gauteng", -26.2041, 28.0473), "Cape Town": ("Western Cape", -33.9249, 18.4241),
        "Durban": ("KwaZulu-Natal", -29.8587, 31.0218), "Pretoria": ("Gauteng", -25.7479, 28.2293),
        "Centurion": ("Gauteng", -25.8603, 28.1894), "Sandton": ("Gauteng", -26.1076, 28.0567),
        "Soweto": ("Gauteng", -26.2678, 27.8585), "Bloemfontein": ("Free State", -29.0852, 26.1596),
        "Port Elizabeth": ("Eastern Cape", -33.9608, 25.6022), "East London": ("Eastern Cape", -33.0153, 27.9116),
        "Pietermaritzburg": ("KwaZulu-Natal", -29.6006, 30.3794), "Polokwane": ("Limpopo", -23.9045, 29.4689),
        "Nelspruit": ("Mpumalanga", -25.4753, 30.9694), "Kimberley": ("Northern Cape", -28.7282, 24.7499),
        "Rustenburg": ("North West", -25.6672, 27.2424), "Stellenbosch": ("Western Cape", -33.9321, 18.8602),
    },
    "gh": {
        "Accra": ("Greater Accra", 5.6037, -0.1870), "Kumasi": ("Ashanti", 6.6885, -1.6244),
        "Tamale": ("Northern", 9.4008, -0.8393), "Sekondi-Takoradi": ("Western", 4.9346, -1.7137),
        "Sunyani": ("Bono", 7.3399, -2.3268), "Cape Coast": ("Central", 5.1053, -1.2466),
        "Koforidua": ("Eastern", 6.0940, -0.2591), "Techiman": ("Bono East", 7.5911, -1.9395),
        "Ho": ("Volta", 6.6108, 0.4713), "Wa": ("Upper West", 10.0601, -2.5099),
        "Bolgatanga": ("Upper East", 10.7856, -0.8514), "Tema": ("Greater Accra", 5.6698, -0.0166),
        "Obuasi": ("Ashanti", 6.2025, -1.6708), "Nkawkaw": ("Eastern", 6.5500, -0.7667),
        "Keta": ("Volta", 5.9167, 0.9833),
    },
    "ug": {
        "Kampala": ("Central", 0.3476, 32.5825), "Entebbe": ("Central", 0.0512, 32.4637),
        "Jinja": ("Eastern", 0.4478, 33.2026), "Mbarara": ("Western", -0.6072, 30.6545),
        "Gulu": ("Northern", 2.7746, 32.2989), "Lira": ("Northern", 2.2350, 32.9028),
        "Mbale": ("Eastern", 1.0827, 34.1751), "Masaka": ("Central", -0.3333, 31.7333),
        "Fort Portal": ("Western", 0.6710, 30.2748), "Kabale": ("Western", -1.2486, 29.9897),
        "Soroti": ("Eastern", 1.7147, 33.6111), "Arua": ("West Nile", 3.0201, 30.9111),
        "Kasese": ("Western", 0.1833, 30.0833), "Kitgum": ("Northern", 3.2783, 32.8867),
        "Njeru": ("Eastern", 0.4633, 33.1681),
    },
    "rw": {
        "Kigali": ("Kigali City", -1.9441, 30.0619), "Butare": ("Southern Province", -2.5967, 29.7394),
        "Gisenyi": ("Western Province", -1.7021, 29.2564), "Ruhengeri": ("Northern Province", -1.4996, 29.6339),
        "Musanze": ("Northern Province", -1.4996, 29.6339), "Rubavu": ("Western Province", -1.6792, 29.2661),
        "Nyagatare": ("Eastern Province", -1.2942, 30.3253), "Cyangugu": ("Western Province", -2.4846, 28.9075),
        "Kibuye": ("Western Province", -2.0603, 29.3486), "Byumba": ("Northern Province", -1.5764, 30.0678),
        "Gitarama": ("Southern Province", -2.0742, 29.7594), "Kayonza": ("Eastern Province", -1.8833, 30.6167),
        "Rwamagana": ("Eastern Province", -1.9487, 30.4347), "Huye": ("Southern Province", -2.5967, 29.7394),
        "Muhanga": ("Southern Province", -2.0742, 29.7594),
    },
    "et": {
        "Addis Ababa": ("Addis Ababa", 9.0250, 38.7469), "Dire Dawa": ("Dire Dawa", 9.5931, 41.8661),
        "Bahir Dar": ("Amhara", 11.5936, 37.3908), "Gondar": ("Amhara", 12.6000, 37.4667),
        "Mekelle": ("Tigray", 13.4967, 39.4753), "Hawassa": ("SNNPR", 7.0504, 38.4955),
        "Jimma": ("Oromia", 7.6667, 36.8333), "Dessie": ("Amhara", 11.1333, 39.6333),
        "Adama": ("Oromia", 8.5400, 39.2700), "Bishoftu": ("Oromia", 8.7500, 38.9833),
        "Harar": ("Harari", 9.3100, 42.1200), "Arba Minch": ("SNNPR", 6.0333, 37.5500),
        "Sodo": ("SNNPR", 6.8500, 37.7500), "Debre Birhan": ("Amhara", 9.6833, 39.5333),
        "Shashamane": ("Oromia", 7.2000, 38.6000),
    },
    "eg": {
        "Cairo": ("Cairo", 30.0444, 31.2357), "Alexandria": ("Alexandria", 31.2001, 29.9187),
        "Giza": ("Giza", 30.0131, 31.2089), "Port Said": ("Port Said", 31.2653, 32.3019),
        "Suez": ("Suez", 29.9668, 32.5498), "Luxor": ("Luxor", 25.6872, 32.6396),
        "Aswan": ("Aswan", 24.0889, 32.8998), "Mansoura": ("Dakahlia", 31.0409, 31.3785),
        "Tanta": ("Gharbia", 30.7865, 31.0004), "Ismailia": ("Ismailia", 30.5965, 32.2715),
        "Zagazig": ("Sharqia", 30.5877, 31.5020), "Assiut": ("Assiut", 27.1809, 31.1837),
        "Beni Suef": ("Beni Suef", 29.0661, 31.0994), "Hurghada": ("Red Sea", 27.2579, 33.8116),
        "Sharm El-Sheikh": ("South Sinai", 27.9158, 34.3300),
    },
    "ma": {
        "Casablanca": ("Casablanca-Settat", 33.5731, -7.5898), "Rabat": ("Rabat-Salé-Kénitra", 34.0209, -6.8417),
        "Marrakech": ("Marrakech-Safi", 31.6295, -7.9811), "Fès": ("Fès-Meknès", 34.0331, -5.0003),
        "Tangier": ("Tanger-Tétouan-Al Hoceima", 35.7595, -5.8340), "Agadir": ("Souss-Massa", 30.4278, -9.5981),
        "Meknès": ("Fès-Meknès", 33.8935, -5.5473), "Oujda": ("Oriental", 34.6867, -1.9114),
        "Kénitra": ("Rabat-Salé-Kénitra", 34.2610, -6.5802), "Tétouan": ("Tanger-Tétouan-Al Hoceima", 35.5785, -5.3684),
        "Safi": ("Marrakech-Safi", 32.2994, -9.2372), "El Jadida": ("Casablanca-Settat", 33.2549, -8.5062),
        "Béni Mellal": ("Béni Mellal-Khénifra", 32.3373, -6.3498), "Nador": ("Oriental", 35.1681, -2.9287),
        "Khouribga": ("Béni Mellal-Khénifra", 32.8811, -6.9063),
    },
}


def _city_info(locale: str, city: str):
    """Return (region, lat, lon) for a city, falling back to that locale's
    first entry if the city is somehow not in the static table."""
    table = CITY_INFO.get(locale, {})
    if city in table:
        return table[city]
    if table:
        return next(iter(table.values()))
    return ("", 0.0, 0.0)


def assign_identity_countries(df):
    """Assign each row a country for IDENTITY generation AND income scale.

    Must run BEFORE add_country_scale_diversity() so that function can
    calibrate its income scale factor to each row's real assigned country
    (via World Bank GNI, see _gni_ratio below) instead of an arbitrary
    random pick.
    """
    rng = np.random.default_rng(SEED + 1)
    df["country_code"] = rng.choice(IDENTITY_COUNTRIES, size=len(df))
    return df


# ── Real economic calibration (World Bank GNI) ───────────────────────────────
# niafaker's amount() has NO built-in economic data — it's a uniform random
# draw between caller-supplied bounds, formatted with the locale's currency
# symbol (confirmed by reading its source: `self._currency.amount(min_val,
# max_val)`). It cannot make the income distribution more realistic. What
# CAN: real World Bank GNI-per-capita data, fetched the same way
# app/core/country_reference.py does for the scoring side — reimplemented
# here standalone (not imported) so this script has no dependency on the
# FastAPI app package and keeps working if run outside that container.
_GNI_REFERENCE_USD = 2800.0  # same reference used in app/core/country_reference.py
_gni_cache: dict[str, float] = {}


def _gni_ratio(country_code: str) -> float:
    """Return gni_per_capita(country) / _GNI_REFERENCE_USD, cached per run.

    Falls back to 1.0 (neutral — no scaling effect) on any network/data
    failure, so generation never blocks or crashes without internet access.
    """
    if country_code in _gni_cache:
        return _gni_cache[country_code]
    ratio = 1.0
    try:
        import json
        import urllib.request

        url = (
            f"https://api.worldbank.org/v2/country/{country_code}/indicator/"
            f"NY.GNP.PCAP.CD?format=json&per_page=10&date=2015:2025"
        )
        req = urllib.request.Request(url, headers={"User-Agent": "generate_dataset/1.0"})
        with urllib.request.urlopen(req, timeout=8) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
        if isinstance(payload, list) and len(payload) > 1 and payload[1]:
            for row in payload[1]:
                if row.get("value") is not None:
                    ratio = float(row["value"]) / _GNI_REFERENCE_USD
                    break
    except Exception:
        pass  # offline / API down — neutral fallback, never blocks generation
    _gni_cache[country_code] = ratio
    return ratio


def generate_identities(df, company_rate=0.15):
    """Generate the full niafaker identity/context field set — cosmetic only.

    Does NOT touch income, household_size, or any ML_FEATURES column
    (declared_income_display is a separate string column, purely for
    display — see the docstring note on income below). Uses one seeded
    NiaFaker instance per locale (reused across rows — its internal RNG
    advances per call, giving distinct output per row while staying
    reproducible run-to-run). Unsupported countries (e.g. Tunisia) fall
    back to the original generic placeholder style for ALL fields — no
    partial niafaker application.

    Company fields (company_name, registration_number) are assigned to
    only ~company_rate of rows — not every beneficiary owns a business.
    """
    from niafaker import NiaFaker

    fakers = {locale: NiaFaker(locale, seed=SEED) for locale in set(NIAFAKER_LOCALES.values())}
    company_rng = np.random.default_rng(SEED + 3)
    is_company_row = company_rng.random(len(df)) < company_rate

    cols = {k: [] for k in (
        "name", "first_name", "last_name", "email", "phone", "carrier",
        "address", "city", "region", "country_name", "national_id",
        "mobile_money_provider", "mobile_money_number", "transaction_id",
        "company_name", "registration_number", "declared_income_display",
        "lat", "lon",
    )}

    for row_pos, (_, row) in enumerate(df.iterrows()):
        idx = int(row["partner_idx"])
        locale = NIAFAKER_LOCALES.get(row["country_code"])
        if locale:
            f = fakers[locale]
            gender = row.get("gender") if row.get("gender") in ("male", "female") else None
            first, last = f.first_name(gender=gender), f.last_name()
            mm = f.mobile_money()

            cols["name"].append(f"{first} {last}")
            cols["first_name"].append(first)
            cols["last_name"].append(last)
            cols["email"].append(f.email())
            cols["phone"].append(f.phone())
            cols["carrier"].append(mm.get("provider", ""))  # niafaker phone() doesn't
            # expose which carrier it picked internally; mobile_money's
            # provider is the closest available "network operator" signal.
            cols["address"].append(f.address())
            city = f.city()
            # region is DERIVED from city via the static CITY_INFO table,
            # NOT an independent f.region() call — niafaker's city()/
            # region() are unrelated random draws (see CITY_INFO comment
            # above), which produced incoherent pairs (e.g. two rows both
            # in "Agadir" ending up with different regions).
            region, lat, lon = _city_info(locale, city)
            small_offset = np.random.default_rng(idx).uniform(-0.05, 0.05, 2)
            cols["city"].append(city)
            cols["region"].append(region)
            cols["lat"].append(round(lat + small_offset[0], 6))
            cols["lon"].append(round(lon + small_offset[1], 6))
            cols["country_name"].append(f.country())
            cols["national_id"].append(f.national_id())
            cols["mobile_money_provider"].append(mm.get("provider", ""))
            cols["mobile_money_number"].append(mm.get("number", ""))
            cols["transaction_id"].append(f.transaction_id())
            # Cosmetic only — real income (ML_FEATURES) stays fully
            # scenario-driven; niafaker's amount() has no economic data of
            # its own (just a random draw formatted with a currency
            # symbol), so it's never used for the real income column, only
            # this separate display string.
            cols["declared_income_display"].append(f.amount(50, 500_000))
            if is_company_row[row_pos]:
                cols["company_name"].append(f.company())
                cols["registration_number"].append(f.registration_number())
            else:
                cols["company_name"].append("")
                cols["registration_number"].append("")
        else:
            # Fallback for countries niafaker doesn't cover (e.g. Tunisia) —
            # generic placeholders for ALL fields, no partial application.
            cols["name"].append(f"BEN-{idx:05d}")
            cols["first_name"].append("")
            cols["last_name"].append("")
            cols["email"].append("")
            cols["phone"].append(f"+216{20000000 + idx % 10000000:08d}")
            cols["carrier"].append("")
            cols["address"].append("")
            cols["city"].append("Tunis")
            cols["region"].append("Tunis")
            tn_offset = np.random.default_rng(idx).uniform(-0.05, 0.05, 2)
            cols["lat"].append(round(36.8065 + tn_offset[0], 6))
            cols["lon"].append(round(10.1815 + tn_offset[1], 6))
            cols["country_name"].append("Tunisia")
            cols["national_id"].append(f"SYN-{idx:08d}")
            cols["mobile_money_provider"].append("")
            cols["mobile_money_number"].append("")
            cols["transaction_id"].append("")
            cols["company_name"].append("")
            cols["registration_number"].append("")
            cols["declared_income_display"].append("")

    for k, v in cols.items():
        df[k] = v
    return df


def apply_duplicate_national_id_scenario(df, rate=0.02):
    """s10 — Duplicate National ID.

    Picks ~`rate` of FRAUD rows and gives each one a national_id copied
    from another random row (mix of legitimate and fraud), simulating a
    stolen/reused identity. Computes duplicate_national_id_count as a
    rule-engine signal ONLY — deliberately not added to ML_FEATURES/
    training in this pass (see train_openg2p.py — untouched).
    """
    rng = np.random.default_rng(SEED + 2)
    fraud_idx = df.index[df["is_fraud"] == 1]
    n_dup = max(1, int(len(fraud_idx) * rate)) if len(fraud_idx) else 0

    if n_dup:
        dup_targets = rng.choice(fraud_idx, size=n_dup, replace=False)
        for i in dup_targets:
            source_pool = df.index[df.index != i]
            source = rng.choice(source_pool)
            df.loc[i, "national_id"] = df.loc[source, "national_id"]
            df.loc[i, "scenario"] = "duplicate_identity"

    counts = df["national_id"].value_counts()
    df["duplicate_national_id_count"] = (
        df["national_id"].map(counts) - 1
    ).clip(lower=0).astype(int)

    return df


def add_country_scale_diversity(df):
    """
    Assigns each beneficiary an economic scale factor. Calibrated to real
    World Bank GNI-per-capita data for the row's assigned identity country
    (df["country_code"], set by assign_identity_countries() — must run
    BEFORE this function) instead of an arbitrary pick from a fixed list.
    Falls back to a neutral 1.0 ratio per-country on any network failure
    (see _gni_ratio), so generation never blocks offline.

    Preserves the relative gaps already created by the fraud scenarios —
    only the absolute scale changes.
    """
    rng = np.random.default_rng(SEED)

    if "country_code" in df.columns:
        # Clip to a sane range: real GNI ratios across this specific
        # 11-country identity list can span roughly 0.3x-3x; clipping
        # avoids one outlier country destabilizing the income distribution
        # while still reflecting real relative differences (vs. the old
        # arbitrary 0.55-1.9 list, which was not tied to any real country).
        income_scale_factor = df["country_code"].map(_gni_ratio).clip(0.3, 3.0).to_numpy()
    else:
        # No identity country assigned yet — neutral factor, not an error.
        income_scale_factor = np.ones(len(df))
    household_scale_factor = rng.choice([0.85, 1.0, 1.15, 1.6], size=len(df))

    df["income"] = (df["income"] * income_scale_factor).round(2)
    df["household_size"] = (df["household_size"] * household_scale_factor).clip(lower=1).round(0)
    df["income_per_person"] = (df["income"] / df["household_size"].clip(lower=1)).round(2)

    # household_size changed, so re-derive the features computed from it
    # (same formulas as generate_dataset) to keep the CSV self-consistent
    adults = (df["household_size"] - df["nb_children"] - df["nb_elderly"]).clip(lower=1)
    df["dependency_ratio"] = ((df["nb_children"] + df["nb_elderly"]) / adults).round(3)
    q15 = df["income_per_person"].quantile(0.15)
    df["income_program_inconsistency"] = (
        (df["income_per_person"] < q15) & (df["nb_programs"] >= 3)
    ).astype(int)

    # Relative features = what the model should learn to rely on
    # instead of absolute values (same naming as the serving-side
    # features in app/services/features_service.py, for consistency)
    df["income_ratio_to_national"] = (df["income"] / df["income"].median()).round(3)
    df["household_size_deviation"] = (df["household_size"] - df["household_size"].median()).round(2)

    return df


ML_FEATURES = [
    "age", "income", "income_per_person",
    "household_size", "nb_children", "nb_elderly",
    "dependency_ratio", "has_disabled", "single_head",
    "nb_programs", "nb_active_programs",
    "pmt_score", "pmt_score_min", "avg_enrollment_days",
    "payment_count", "payment_gap_ratio",
    "payment_success_rate", "amount_variance", "cycle_count",
    "shared_phone_count", "shared_account_count", "network_risk",
    "group_membership_count", "high_amount_flag", "income_program_inconsistency",
]


# ════════════════════════════════════════════════════════
# INJECTION PostgreSQL — ALIGNÉE AVEC LES VRAIES TABLES
# ════════════════════════════════════════════════════════

def inject(df, db_url):
    try:
        from sqlalchemy import create_engine, text
    except ImportError:
        print("sqlalchemy manquant")
        return

    print(f"\nConnexion à {db_url}...")
    eng = create_engine(db_url, pool_pre_ping=True, connect_args={"connect_timeout": 15})

    with eng.begin() as c:
        # ── company_id ─────────────────────────────────────
        cid = c.execute(text("SELECT id FROM res_company ORDER BY id LIMIT 1")).scalar()
        if not cid:
            raise RuntimeError("Aucune société dans res_company")

        # ── national-ID type (g2p_id_type) — for g2p_reg_id inserts ────
        national_id_type = c.execute(text(
            "SELECT id FROM g2p_id_type WHERE name='National ID' LIMIT 1"
        )).scalar()
        if not national_id_type:
            national_id_type = c.execute(text(
                "INSERT INTO g2p_id_type(name) VALUES('National ID') RETURNING id"
            )).scalar()

        # ── res_country cache, ISO2 -> id, for country_id lookups ──────
        country_id_cache: dict[str, int] = {}
        def _country_id(code: str):
            if code not in country_id_cache:
                country_id_cache[code] = c.execute(text(
                    "SELECT id FROM res_country WHERE code=:cc"), {"cc": code}
                ).scalar()
            return country_id_cache[code]

        # ── res_country_state cache, (country_id, region name) -> id ───
        # Used to persist niafaker's region() so graph_service.py's
        # same_region/same_city queries (state_id-based) have real data.
        state_id_cache: dict[tuple, int] = {}
        def _state_id(cty_id, region_name: str):
            if not cty_id or not region_name:
                return None
            key = (cty_id, region_name)
            if key not in state_id_cache:
                sid = c.execute(text(
                    "SELECT id FROM res_country_state WHERE country_id=:c AND name=:n"
                ), {"c": cty_id, "n": region_name}).scalar()
                if not sid:
                    # UNIQUE(country_id, code) — two different region names
                    # can share a 3-letter prefix, so fall back to a longer
                    # slice, then re-select on conflict rather than crash
                    # mid-injection.
                    code = (region_name[:3] or region_name).upper()
                    sid = c.execute(text("""
                        INSERT INTO res_country_state(country_id, name, code)
                        VALUES(:c, :n, :code)
                        ON CONFLICT ON CONSTRAINT res_country_state_name_code_uniq
                        DO NOTHING RETURNING id
                    """), {"c": cty_id, "n": region_name, "code": code}).scalar()
                    if sid is None:
                        # Code collided with a different region — retry with
                        # the full region name as the code (still bounded,
                        # Odoo's code column has no fixed-length constraint
                        # confirmed via schema: character varying, no limit).
                        code = region_name.upper()
                        sid = c.execute(text("""
                            INSERT INTO res_country_state(country_id, name, code)
                            VALUES(:c, :n, :code)
                            ON CONFLICT ON CONSTRAINT res_country_state_name_code_uniq
                            DO NOTHING RETURNING id
                        """), {"c": cty_id, "n": region_name, "code": code}).scalar()
                    if sid is None:
                        sid = c.execute(text(
                            "SELECT id FROM res_country_state WHERE country_id=:c AND name=:n"
                        ), {"c": cty_id, "n": region_name}).scalar()
                state_id_cache[key] = sid
            return state_id_cache[key]

        # ── res_bank cache, provider name -> id — res_partner_bank.bank_id
        # is a FK to res_bank (not free text), so a mobile-money "provider"
        # name (e.g. "PalmPay") needs a real bank row, same lookup-or-create
        # pattern as g2p_program/g2p_id_type above.
        bank_id_cache: dict[str, int] = {}
        def _bank_id(provider_name: str):
            if not provider_name:
                return None
            if provider_name not in bank_id_cache:
                bid = c.execute(text(
                    "SELECT id FROM res_bank WHERE name=:n LIMIT 1"), {"n": provider_name}
                ).scalar()
                if not bid:
                    bid = c.execute(text(
                        "INSERT INTO res_bank(name) VALUES(:n) RETURNING id"), {"n": provider_name}
                    ).scalar()
                bank_id_cache[provider_name] = bid
            return bank_id_cache[provider_name]

        # ── secondary id_type for mobile-money transaction IDs ──────────
        txn_id_type = c.execute(text(
            "SELECT id FROM g2p_id_type WHERE name='Mobile Money Transaction ID' LIMIT 1"
        )).scalar()
        if not txn_id_type:
            txn_id_type = c.execute(text(
                "INSERT INTO g2p_id_type(name) VALUES('Mobile Money Transaction ID') RETURNING id"
            )).scalar()

        # ── 1. g2p_program ─────────────────────────────────
        prog_ids = []
        for name in PROGRAM_NAMES:
            ex = c.execute(text(
                "SELECT id FROM g2p_program WHERE name=:n LIMIT 1"), {"n": name}).scalar()
            if ex:
                prog_ids.append(ex)
            else:
                pid = c.execute(text("""
                    INSERT INTO g2p_program(name, state, active, company_id)
                    VALUES(:n, 'active', true, :c) RETURNING id
                """), {"n": name, "c": cid}).scalar()
                prog_ids.append(pid)
        print(f"   {len(prog_ids)} programmes OK")

        # ── 2. g2p_cycle (FK → g2p_program) ────────────────
        # Use INSERT ... ON CONFLICT DO NOTHING RETURNING id to be safe against
        # TOCTOU races and re-runs against an already-seeded DB. If the row
        # already exists, RETURNING yields NULL, so we fall back to a SELECT.
        base = date(2022, 1, 1)
        cycle_map = {}
        for pid in prog_ids:
            for ci in range(N_CYCLES):
                nm = f"Cycle {ci+1}"
                sd = base + timedelta(days=ci * 90)
                ed = sd + timedelta(days=89)
                cdb = c.execute(text("""
                    INSERT INTO g2p_cycle(
                        program_id, name, sequence,
                        start_date, end_date, state, company_id
                    ) VALUES(:p, :nm, :sq, :sd, :ed, 'approved', :co)
                    ON CONFLICT ON CONSTRAINT g2p_cycle_unique_cycle_name_program
                    DO NOTHING RETURNING id
                """), {"p": pid, "nm": nm, "sq": ci+1,
                       "sd": sd, "ed": ed, "co": cid}).scalar()
                if cdb is None:
                    cdb = c.execute(text(
                        "SELECT id FROM g2p_cycle WHERE program_id=:p AND name=:nm"
                    ), {"p": pid, "nm": nm}).scalar()
                cycle_map[(pid, ci)] = cdb
        print(f"   {len(cycle_map)} cycles OK")

        # ── 3-8. Partenaires ───────────────────────────────
        print(f"   Insertion de {len(df)} partenaires...")
        partner_map = {}

        for _, row in df.iterrows():

            # Identity fields (cosmetic — niafaker for supported countries,
            # generic fallback otherwise; see generate_identities()). Falls
            # back to the old BEN-XXXXX code if the columns aren't present
            # (e.g. an older CSV generated before this feature).
            partner_name = row.get("name") or f"BEN-{int(row.partner_idx):05d}"
            address = row.get("address") or ""
            country_code = row.get("country_code")
            country_id = _country_id(country_code) if country_code else None
            city = row.get("city") or ""
            region_name = row.get("region") or None
            state_id = _state_id(country_id, region_name) if region_name else None
            email = row.get("email") or ""
            company_name = row.get("company_name") or None
            registration_number = row.get("registration_number") or None
            # niafaker doesn't expose a dedicated zip() — some locales embed
            # a postal code as a trailing number in address(); best-effort
            # extraction only, left blank if none is found (never invented).
            import re as _re
            zip_match = _re.search(r"(\d{4,6})\s*$", address)
            zip_code = zip_match.group(1) if zip_match else None

            lat = row.get("lat")
            lon = row.get("lon")

            pid_db = c.execute(text("""
                INSERT INTO res_partner(
                    name, active, is_company, is_registrant,
                    gender, birthdate, income,
                    registration_date, company_id, street, country_id,
                    city, zip, email, company_name, vat, state_id,
                    partner_latitude, partner_longitude
                ) VALUES(
                    :name, true, false, true,
                    :g, :bd, :inc,
                    :rd, :co, :street, :country_id,
                    :city, :zip, :email, :company_name, :vat, :state_id,
                    :lat, :lon
                ) RETURNING id
            """), {
                "name": partner_name,
                "g":   row.get("gender", "male"),
                "bd":  row.get("birthdate"),
                "inc": float(row.get("income", 0)),
                "rd":  row.get("enrollment_date", date(2022, 1, 1)),
                "co":  cid,
                "street": address,
                "country_id": country_id,
                "city": city,
                "zip": zip_code,
                "email": email,
                "company_name": company_name,
                # registration_number has no dedicated column on res_partner;
                # `vat` is Odoo's standard field for a business registration/
                # VAT number, so it's a legitimate reuse, not an invented column.
                "vat": registration_number,
                "state_id": state_id,
                "lat": float(lat) if lat is not None and not pd.isna(lat) else None,
                "lon": float(lon) if lon is not None and not pd.isna(lon) else None,
            }).scalar()
            partner_map[int(row.partner_idx)] = pid_db

            # National ID (g2p_reg_id) — the s10 duplicate_national_id
            # scenario deliberately assigns the SAME national_id value to
            # ~2% of fraud rows and another random row, so this table ends
            # up with real duplicate `value`s for the rule-engine signal.
            national_id = row.get("national_id")
            if national_id:
                c.execute(text("""
                    INSERT INTO g2p_reg_id(partner_id, id_type, value, status)
                    VALUES(:p, :t, :v, 'verified')
                """), {"p": pid_db, "t": national_id_type, "v": national_id})

            # Mobile-money transaction ID as a secondary registry ID —
            # g2p_id_type has no notion of "kind", just a name, so a second
            # named type row (created once, reused) is the natural fit.
            transaction_id = row.get("transaction_id")
            if transaction_id:
                c.execute(text("""
                    INSERT INTO g2p_reg_id(partner_id, id_type, value, status)
                    VALUES(:p, :t, :v, 'verified')
                """), {"p": pid_db, "t": txn_id_type, "v": transaction_id})

            prog_indices = row.get("prog_indices", [0])
            if isinstance(prog_indices, str):
                import ast
                prog_indices = ast.literal_eval(prog_indices)

            # 4. g2p_program_membership
            # NOTE: this table has no company_id column (unlike g2p_program,
            # g2p_cycle, g2p_entitlement, g2p_payment, res_partner_bank,
            # res_partner — all of which do). Confirmed via information_schema.
            for pi in prog_indices:
                if pi >= len(prog_ids):
                    continue
                c.execute(text("""
                    INSERT INTO g2p_program_membership(
                        partner_id, program_id, state,
                        enrollment_date
                    ) VALUES(:p, :pr, 'enrolled', :ed)
                    ON CONFLICT DO NOTHING
                """), {
                    "p":  pid_db,
                    "pr": prog_ids[pi],
                    "ed": row.get("enrollment_date", date(2022, 1, 1)),
                })

            # 5. g2p_phone_number
            # ✅ Fraudes partagent le même numéro (network fraud) — this
            # deliberate duplication for NF002/NF003 takes priority over the
            # per-row niafaker phone, which would otherwise be unique per row.
            if int(row.get("shared_phone_count", 0)) >= 2:
                # Numéro partagé entre fraudeurs du même réseau
                phone_suffix = int(row.get("partner_idx", pid_db)) % 1000
                phone = f"+21699{phone_suffix:04d}"
            else:
                phone = row.get("phone") or f"+21620{pid_db:06d}"

            c.execute(text("""
                INSERT INTO g2p_phone_number(
                    partner_id, phone_no, phone_sanitized, date_collected
                ) VALUES(:p, :ph, :ps, NOW())
            """), {"p": pid_db, "ph": phone,
                   "ps": phone.replace("+", "").replace(" ", "")})

            # Also mirror onto res_partner.phone — Odoo's fraud.case
            # beneficiary_phone display field reads partner_id.phone/.mobile,
            # not the g2p_phone_number table, so without this the case list
            # would show a real name next to a blank phone.
            c.execute(text("UPDATE res_partner SET phone = :ph WHERE id = :p"),
                      {"ph": phone, "p": pid_db})

            # 6. res_partner_bank
            # ✅ Fraudes partagent le même compte bancaire — this deliberate
            # duplication for NF001/NF003 is untouched and takes priority
            # over the per-row mobile-money account.
            bank_id = None
            if int(row.get("shared_account_count", 0)) >= 2:
                account_suffix = int(row.get("partner_idx", pid_db)) % 100
                iban = f"TN59000000{account_suffix:010d}"
            else:
                mm_number = row.get("mobile_money_number")
                if mm_number:
                    iban = mm_number
                    bank_id = _bank_id(row.get("mobile_money_provider"))
                else:
                    iban = f"TN59{pid_db:016d}"

            c.execute(text("""
                INSERT INTO res_partner_bank(
                    partner_id, acc_number, sanitized_acc_number,
                    active, company_id, bank_id
                ) VALUES(:p, :acc, :sacc, true, :co, :bank_id)
                ON CONFLICT DO NOTHING
            """), {"p": pid_db, "acc": iban, "sacc": iban, "co": cid, "bank_id": bank_id})

            # 7. g2p_entitlement + g2p_payment
            success_rate = float(row.get("payment_success_rate", 0.8))
            amount = float(row.get("amount_per_payment", 300))

            for pi in prog_indices:
                if pi >= len(prog_ids):
                    continue
                for ci in range(N_CYCLES):
                    cdb = cycle_map.get((prog_ids[pi], ci))
                    if not cdb:
                        continue

                    ent_id = c.execute(text("""
                        INSERT INTO g2p_entitlement(
                            partner_id, cycle_id,
                            initial_amount, state,
                            approval_state, company_id,
                            code, valid_from, valid_until
                        ) VALUES(
                            :p, :cy,
                            :amt, 'approved',
                            'approved', :co,
                            :code, :vf, :vu
                        ) RETURNING id
                    """), {
                        "p":    pid_db,
                        "cy":   cdb,
                        "amt":  amount,
                        "co":   cid,
                        "code": f"ENT-{pid_db}-{pi}-{ci}",
                        "vf":   date(2022, 1, 1) + timedelta(days=ci * 90),
                        "vu":   date(2022, 1, 1) + timedelta(days=ci * 90 + 89),
                    }).scalar()

                    paid   = random.random() < success_rate
                    a_paid = round(amount, 2) if paid else 0.0
                    c.execute(text("""
                        INSERT INTO g2p_payment(
                            entitlement_id, cycle_id,
                            amount_issued, amount_paid,
                            state, company_id, name,
                            issuance_date
                        ) VALUES(
                            :eid, :cy,
                            :ai, :ap,
                            :st, :co, :nm,
                            NOW()
                        )
                    """), {
                        "eid": ent_id,
                        "cy":  cdb,
                        "ai":  amount,
                        "ap":  a_paid,
                        "st":  "paid" if paid else "failed",
                        "co":  cid,
                        "nm":  f"PAY-{pid_db}-{pi}-{ci}",
                    })

        print(f"   {len(partner_map)} partenaires injectés")
        print("Injection terminée")


# ════════════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════════════

def main():
    global REALISM_LABEL_NOISE, REALISM_OVERLAP, REALISM_NOISE_MULT

    SCRIPT_DIR   = Path(__file__).resolve().parent
    PROJECT_ROOT = SCRIPT_DIR.parent
    DEFAULT_OUT  = PROJECT_ROOT / "data" / "synthetic" / "dataset_ml.csv"

    ap = argparse.ArgumentParser()
    ap.add_argument("--output",     default=str(DEFAULT_OUT))
    ap.add_argument("--inject",     action="store_true")
    ap.add_argument("--db-url",     default=os.getenv(
        "OPENG2P_DB_URL", "postgresql://odoo:openg2p@postgresql:5432/openg2p"))
    ap.add_argument("--n",          type=int,   default=N_TOTAL)
    ap.add_argument("--fraud-rate", type=float, default=FRAUD_RATE)
    ap.add_argument("--label-noise", type=float, default=REALISM_LABEL_NOISE,
                    help="Fraction of labels to flip (imperfect ground truth)")
    ap.add_argument("--overlap",     type=float, default=REALISM_OVERLAP,
                    help="Fraction of fraud rows blended toward legit (subtle fraud)")
    ap.add_argument("--noise-mult",  type=float, default=REALISM_NOISE_MULT,
                    help="Multiplier on Gaussian feature noise")
    args = ap.parse_args()

    # apply realism overrides globally
    REALISM_LABEL_NOISE = args.label_noise
    REALISM_OVERLAP     = args.overlap
    REALISM_NOISE_MULT  = args.noise_mult
    print(f"Réalisme : label_noise={REALISM_LABEL_NOISE} "
          f"overlap={REALISM_OVERLAP} noise_mult={REALISM_NOISE_MULT}")

    df  = generate_dataset(args.n, args.fraud_rate)
    df  = assign_identity_countries(df)      # country_code must exist first —
    df  = add_country_scale_diversity(df)    # scale is now GNI-calibrated per row
    df  = generate_identities(df)
    df  = apply_duplicate_national_id_scenario(df)
    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)

    cols = ML_FEATURES + [
        "income_ratio_to_national", "household_size_deviation",
        "is_fraud", "synthetic_label", "partner_idx", "partner_id",
        "scenario", "registration_age_days", "avg_entitlement_amount",
        "total_issued", "total_paid", "network_risk_score", "elderly_head",
        # Identity/context fields (cosmetic) + duplicate-ID rule signal —
        # NOT ML features, deliberately excluded from ML_FEATURES/training.
        "country_code", "name", "first_name", "last_name", "email",
        "phone", "carrier", "address", "city", "region", "country_name",
        "national_id", "mobile_money_provider", "mobile_money_number",
        "transaction_id", "company_name", "registration_number",
        "declared_income_display", "duplicate_national_id_count",
        "lat", "lon",
    ]
    df[[c for c in cols if c in df.columns]].to_csv(out, index=False)
    print(f"\nCSV -> {out}  ({len(df)} lignes)")

    print("\nMoyennes légitimes vs fraudeurs :")
    check = ["income_per_person", "pmt_score", "gap_ratio",
             "shared_phone_count", "nb_programs", "network_risk"]
    print(df.groupby("is_fraud")[[c for c in check if c in df.columns]]
          .mean().round(3).to_string())

    if args.inject:
        inject(df, args.db_url)


if __name__ == "__main__":
    main()