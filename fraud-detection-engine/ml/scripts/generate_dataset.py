"""
generate_dataset.py — Générateur dataset + injection OpenG2P v4 (final)
========================================================================
Schéma 100% vérifié. Toutes les FK respectées.

Chaîne d'injection dans l'ordre des FK :
  1. g2p_program           (pas de FK vers les autres)
  2. g2p_cycle             (FK → g2p_program)
  3. res_partner           (pas de FK critique ici)
  4. g2p_program_membership (FK → res_partner + g2p_program)
  5. g2p_program_registrant_info (FK → res_partner + g2p_program)
  6. g2p_phone_number      (FK → res_partner)
  7. res_partner_bank      (FK → res_partner)
  8. g2p_entitlement       (FK → res_partner + g2p_cycle)
  9. g2p_payment           (FK → g2p_entitlement)

Usage :
  python ml/generate_dataset.py                  # CSV seulement
  python ml/generate_dataset.py --inject         # + injection PostgreSQL
  python ml/generate_dataset.py --n 2000 --fraud-rate 0.08
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

N_TOTAL     = 10000
FRAUD_RATE  = 0.12
N_PROGRAMS  = 5
N_CYCLES    = 3

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
# PROFILS
# ════════════════════════════════════════════════════════

def build_legitimate(idx):
    birth_year  = random.randint(1950, 2003)
    hh_size     = _clip(int(np.random.normal(4.0, 2.0)), 1, 14)
    nb_children = _clip(int(np.random.normal(hh_size * 0.38, 1.5)), 0, hh_size - 1)
    nb_elderly  = _clip(int(np.random.normal(0.4, 0.7)), 0, 4)
    income      = _clip(np.random.normal(900, 500) + _n(150), 60, 8000)
    # Légitimes peuvent parfois s'inscrire à 3-4 programmes (ex: cumul autorisé)
    nb_prog     = int(np.random.choice([1,2,3,4], p=[0.35, 0.40, 0.20, 0.05]))
    # pmt_score légitimes : distribution plus large, overlap avec fraudes
    pmt_score   = _clip(np.random.normal(0.60, 0.18) + _n(0.06), 0.08, 1.0)
    n_pay       = nb_prog * N_CYCLES
    n_paid      = _clip(int(n_pay * np.random.uniform(0.60, 1.0)), 0, n_pay)
    gap_ratio   = round(1 - n_paid / max(n_pay, 1), 3)
    # Légitimes peuvent partager des téléphones (famille, rural)
    shared_phone   = int(np.random.choice([0,1,2,3], p=[0.72, 0.18, 0.07, 0.03]))
    shared_account = int(np.random.choice([0,1,2], p=[0.78, 0.17, 0.05]))
    return {
        "partner_idx": idx, "is_fraud": 0, "scenario": "legitimate",
        "gender": random.choice(["male","female"]),
        "birthdate": date(birth_year, random.randint(1,12), random.randint(1,28)),
        "age": date.today().year - birth_year,
        "income": round(income, 2),
        "household_size": hh_size,
        "nb_children": nb_children,
        "nb_elderly": nb_elderly,
        "has_disabled": random.random() < 0.07,
        "single_head": random.random() < 0.14,
        "elderly_head": random.random() < 0.08,
        "enrollment_date": date(random.randint(2020, 2023), random.randint(1,12), 1),
        "nb_programs": nb_prog,
        "prog_indices": random.sample(range(N_PROGRAMS), nb_prog),
        "pmt_score": round(pmt_score, 4),
        "payment_count": n_pay,
        "payment_count_paid": n_paid,
        "gap_ratio": gap_ratio,
        "payment_success_rate": round(1 - gap_ratio, 3),
        "amount_per_payment": round(random.uniform(150, 500), 2),
        "shared_phone_count": shared_phone,
        "shared_account_count": shared_account,
        "group_count": random.randint(0, 1),
        "income_per_person": round(income / max(hh_size, 1), 2),
        "network_risk": round(
            min(shared_phone/5, 1)*0.4 + min(shared_account/5, 1)*0.6 + abs(_n(0.05)), 3
        ),
    }


def _fraud(idx, scenario):
    r = build_legitimate(idx)
    r["is_fraud"] = 1
    r["scenario"] = scenario
    return r

def s1_multi_enrollment(idx):
    r = _fraud(idx, "multi_enrollment")
    # Overlap: certains légitimes ont aussi 3-4 programmes → on commence à 4 seulement
    r["nb_programs"] = random.randint(4, 5)
    r["prog_indices"] = list(range(r["nb_programs"]))
    # pmt_score plus large → overlap avec légitimes
    r["pmt_score"] = _clip(np.random.normal(0.45, 0.18) + _n(0.07), 0.08, 0.85)
    return r

def s2_shared_phone(idx):
    r = _fraud(idx, "shared_phone")
    # Réduction de l'écart : légitimes peuvent avoir jusqu'à 3 → fraudes 3-6
    r["shared_phone_count"]   = random.randint(3, 6)
    r["shared_account_count"] = random.randint(1, 3)
    r["network_risk"] = _clip(
        r["shared_phone_count"]*0.09 + r["shared_account_count"]*0.12 + abs(_n(0.12)), 0, 1)
    return r

def s3_ghost_household(idx):
    r = _fraud(idx, "ghost_household")
    # Taille ménage moins extrême pour overlap avec légitimes à grande famille
    r["household_size"] = random.randint(7, 12)
    r["nb_children"]    = random.randint(4, r["household_size"] - 1)
    r["income"]         = _clip(np.random.normal(500, 250) + _n(100), 80, 2500)
    r["income_per_person"] = round(r["income"] / r["household_size"], 2)
    r["pmt_score"]      = _clip(np.random.normal(0.40, 0.18) + _n(0.07), 0.08, 0.80)
    r["single_head"]    = True
    return r

def s4_payment_manipulation(idx):
    r = _fraud(idx, "payment_manipulation")
    n_pay = r["payment_count"]
    # Taux d'échec moins extrême : 30-65% (vs 68-92% avant)
    n_paid = _clip(int(n_pay * np.random.uniform(0.30, 0.65)), 0, n_pay)
    r["payment_count_paid"]    = n_paid
    r["gap_ratio"]             = round(1 - n_paid / max(n_pay, 1), 3)
    r["payment_success_rate"]  = round(1 - r["gap_ratio"], 3)
    r["pmt_score"]             = _clip(np.random.normal(0.38, 0.18) + _n(0.07), 0.08, 0.80)
    return r

def s5_identity_fraud(idx):
    r = _fraud(idx, "identity_fraud")
    r["shared_account_count"] = random.randint(2, 4)
    r["shared_phone_count"]   = random.randint(2, 4)
    r["nb_programs"]          = random.randint(3, 5)
    r["prog_indices"]         = list(range(min(r["nb_programs"], N_PROGRAMS)))
    r["pmt_score"]  = _clip(np.random.normal(0.42, 0.18) + _n(0.07), 0.08, 0.85)
    r["network_risk"] = _clip(
        r["shared_account_count"]*0.13 + r["shared_phone_count"]*0.09 + abs(_n(0.13)), 0, 1)
    return r

def s6_income_underreporting(idx):
    r = _fraud(idx, "income_underreporting")
    # Revenus déclarés moins extrêmes : overlap avec vraies personnes pauvres
    r["income"]         = _clip(np.random.normal(280, 180) + _n(80), 50, 900)
    r["income_per_person"] = round(r["income"] / max(r["household_size"], 1), 2)
    r["nb_programs"]    = random.randint(2, 4)
    r["prog_indices"]   = list(range(min(r["nb_programs"], N_PROGRAMS)))
    n_pay = r["payment_count"]
    # Parfois quelques paiements échoués pour ne pas être parfait
    n_paid = _clip(int(n_pay * np.random.uniform(0.80, 1.0)), 0, n_pay)
    r["payment_count_paid"]   = n_paid
    r["gap_ratio"]            = round(1 - n_paid / max(n_pay, 1), 3)
    r["payment_success_rate"] = round(1 - r["gap_ratio"], 3)
    r["pmt_score"] = _clip(np.random.normal(0.55, 0.15) + _n(0.06), 0.20, 0.90)
    return r

def s7_coordinated_fraud(idx):
    """Fraude coordonnee : multi-enrollment + shared resources"""
    r = _fraud(idx, "coordinated_fraud")
    r["nb_programs"] = random.randint(4, 5)
    r["prog_indices"] = list(range(r["nb_programs"]))
    r["shared_phone_count"] = random.randint(3, 6)
    r["shared_account_count"] = random.randint(2, 5)
    r["pmt_score"] = _clip(np.random.normal(0.38, 0.18) + _n(0.07), 0.05, 0.72)
    r["network_risk"] = _clip(
        r["shared_phone_count"]*0.12 + r["shared_account_count"]*0.15 + abs(_n(0.14)), 0, 1)
    return r

def s8_systematic_ghost(idx):
    """Ghost household systematique avec manipulation de paiements"""
    r = _fraud(idx, "systematic_ghost")
    r["household_size"] = random.randint(8, 13)
    r["nb_children"] = random.randint(5, r["household_size"] - 1)
    r["income"] = _clip(np.random.normal(380, 200) + _n(100), 60, 1800)
    r["income_per_person"] = round(r["income"] / r["household_size"], 2)
    n_pay = r["payment_count"]
    n_paid = _clip(int(n_pay * np.random.uniform(0.35, 0.65)), 0, n_pay)
    r["payment_count_paid"] = n_paid
    r["gap_ratio"] = round(1 - n_paid / max(n_pay, 1), 3)
    r["payment_success_rate"] = round(1 - r["gap_ratio"], 3)
    r["pmt_score"] = _clip(np.random.normal(0.35, 0.18) + _n(0.07), 0.05, 0.72)
    return r

def s9_network_exploitation(idx):
    """Exploitation de reseau avec identites multiples"""
    r = _fraud(idx, "network_exploitation")
    r["shared_phone_count"] = random.randint(4, 8)
    r["shared_account_count"] = random.randint(3, 6)
    r["nb_programs"] = random.randint(3, 5)
    r["prog_indices"] = list(range(min(r["nb_programs"], N_PROGRAMS)))
    r["network_risk"] = _clip(
        r["shared_phone_count"]*0.15 + r["shared_account_count"]*0.18 + abs(_n(0.16)), 0, 1)
    r["pmt_score"] = _clip(np.random.normal(0.40, 0.20) + _n(0.08), 0.05, 0.80)
    return r

SCENARIOS = [s1_multi_enrollment, s2_shared_phone, s3_ghost_household,
             s4_payment_manipulation, s5_identity_fraud, s6_income_underreporting, s7_coordinated_fraud, s8_systematic_ghost, s9_network_exploitation]


def generate_dataset(n_total=N_TOTAL, fraud_rate=FRAUD_RATE):
    n_fraud = int(n_total * fraud_rate)
    n_legit = n_total - n_fraud
    print(f"📊 {n_legit} légitimes + {n_fraud} fraudeurs")
    records = [build_legitimate(i) for i in range(n_legit)]
    for j in range(n_fraud):
        records.append(SCENARIOS[j % len(SCENARIOS)](n_legit + j))
    df = pd.DataFrame(records)
    # Bruit gaussien realiste — suffisant pour casser la separation parfaite
    noise_config = [
        ("income", 0.15),           # +/-15% de variabilite relative
        ("pmt_score", 0.08),        # overlap significant
        ("gap_ratio", 0.10),
        ("network_risk", 0.10),
        ("income_per_person", 0.12),
        ("shared_phone_count", 0.5),
        ("shared_account_count", 0.4),
        ("nb_programs", 0.3),
    ]
    for col, s in noise_config:
        if col in df.columns:
            df[col] = (df[col] + np.random.normal(0, s, len(df))).clip(0)

    # Cas borderline : 8% des fraudeurs recoivent des features proches des legitimes
    n_borderline = int(len(df[df["is_fraud"]==1]) * 0.08)
    fraud_idx = df[df["is_fraud"]==1].index[:n_borderline]
    legit_sample = df[df["is_fraud"]==0].sample(n_borderline, random_state=SEED+1)
    for col in ["pmt_score", "shared_phone_count", "nb_programs", "network_risk"]:
        if col in df.columns:
            df.loc[fraud_idx, col] = legit_sample[col].values
    df["income_per_person"] = (df["income"] / df["household_size"].clip(lower=1)).round(2)
    df["partner_id"] = df["partner_idx"]
    df["synthetic_label"] = df["is_fraud"]

    df["payment_gap_ratio"] = df["gap_ratio"]
    df["network_risk_score"] = df["network_risk"]
    
    
    return df.sample(frac=1, random_state=SEED).reset_index(drop=True)


ML_FEATURES = [
    "age","income","income_per_person","household_size","nb_children",
    "nb_elderly","has_disabled","single_head",
    "nb_programs","pmt_score","gap_ratio","payment_success_rate",
    "payment_count","shared_phone_count","shared_account_count",
    "network_risk","group_count",
]


# ════════════════════════════════════════════════════════
# INJECTION — ordre FK strict
# ════════════════════════════════════════════════════════

def inject(df, db_url):
    try:
        from sqlalchemy import create_engine, text
    except ImportError:
        print("❌ sqlalchemy manquant"); return

    print(f"\n🗄️  Connexion...")
    eng = create_engine(db_url, pool_pre_ping=True,
                        connect_args={"connect_timeout": 15})

    with eng.begin() as c:
        # company_id
        cid = c.execute(text(
            "SELECT id FROM res_company ORDER BY id LIMIT 1")).scalar()
        if not cid:
            raise RuntimeError("Aucune société dans res_company")

        # ── 1. Programmes ─────────────────────────────────
        prog_ids = []
        for name in PROGRAM_NAMES:
            ex = c.execute(text(
                "SELECT id FROM g2p_program WHERE name=:n LIMIT 1"), {"n":name}).scalar()
            if ex:
                prog_ids.append(ex)
            else:
                pid = c.execute(text("""
                    INSERT INTO g2p_program(name,state,active,company_id)
                    VALUES(:n,'active',true,:c) RETURNING id
                """), {"n":name,"c":cid}).scalar()
                prog_ids.append(pid)
        print(f"   ✅ {len(prog_ids)} programmes")

        # ── 2. Cycles (FK → g2p_program) ──────────────────
        base = date(2022, 1, 1)
        cycle_map = {}   # (prog_id, cycle_idx) → cycle_db_id
        for pid in prog_ids:
            for ci in range(N_CYCLES):
                sd = base + timedelta(days=ci*90)
                ed = sd + timedelta(days=89)
                cdb = c.execute(text("""
                    INSERT INTO g2p_cycle(program_id,name,sequence,
                                         start_date,end_date,state,company_id)
                    VALUES(:p,:nm,:sq,:sd,:ed,'approved',:co) RETURNING id
                """), {"p":pid,"nm":f"Cycle {ci+1}","sq":ci+1,
                       "sd":sd,"ed":ed,"co":cid}).scalar()
                cycle_map[(pid, ci)] = cdb
        print(f"   ✅ {len(cycle_map)} cycles")

        # ── 3-9. Partenaires + toutes les tables liées ────
        print(f"   Insertion de {len(df)} partenaires...")
        partner_map = {}

        for _, row in df.iterrows():
            # 3. res_partner
            pid_db = c.execute(text("""
                INSERT INTO res_partner(
                    name, active, is_company, is_registrant,
                    gender, birthdate, income,
                    z_ind_grp_num_individuals,
                    z_ind_grp_num_children,
                    z_ind_grp_num_elderly,
                    z_ind_grp_is_hh_with_disabled,
                    z_ind_grp_is_single_head_hh,
                    z_ind_grp_is_elderly_head_hh,
                    registration_date, company_id
                ) VALUES(
                    :name,true,false,true,
                    :g,:bd,:inc,
                    :hh,:nc,:ne,:dis,:sh,:eh,
                    :rd,:co
                ) RETURNING id
            """), {
                "name": f"BEN-{int(row.partner_idx):04d}",
                "g":    row.get("gender","male"),
                "bd":   row.get("birthdate"),
                "inc":  float(row.get("income",0)),
                "hh":   int(row.get("household_size",1)),
                "nc":   int(row.get("nb_children",0)),
                "ne":   int(row.get("nb_elderly",0)),
                "dis":  bool(row.get("has_disabled",False)),
                "sh":   bool(row.get("single_head",False)),
                "eh":   bool(row.get("elderly_head",False)),
                "rd":   row.get("enrollment_date", date(2022,1,1)),
                "co":   cid,
            }).scalar()
            partner_map[int(row.partner_idx)] = pid_db

            prog_indices = row.get("prog_indices", [0])

            # 4. g2p_program_membership (FK → res_partner + g2p_program)
            for pi in prog_indices:
                if pi >= len(prog_ids): continue
                c.execute(text("""
                    INSERT INTO g2p_program_membership(
                        partner_id, program_id, state,
                        enrollment_date, company_id
                    ) VALUES(:p,:pr,'enrolled',:ed,:co)
                    ON CONFLICT DO NOTHING
                """), {
                    "p": pid_db,
                    "pr": prog_ids[pi],
                    "ed": row.get("enrollment_date", date(2022,1,1)),
                    "co": cid,
                })

            # 5. g2p_program_registrant_info (FK → res_partner + g2p_program)
            for pi in prog_indices:
                if pi >= len(prog_ids): continue
                c.execute(text("""
                    INSERT INTO g2p_program_registrant_info(
                        registrant_id, program_id,
                        pmt_score, latest_pmt_score, state
                    ) VALUES(:r,:p,:pmt,:lpmt,'enrolled')
                    ON CONFLICT DO NOTHING
                """), {
                    "r":   pid_db,
                    "p":   prog_ids[pi],
                    "pmt": float(row.get("pmt_score",0.5)),
                    "lpmt":float(row.get("pmt_score",0.5)),
                })

            # 6. g2p_phone_number (FK → res_partner)
            phone = f"+21620{pid_db:06d}"
            c.execute(text("""
                INSERT INTO g2p_phone_number(
                    partner_id, phone_no, phone_sanitized, date_collected
                ) VALUES(:p,:ph,:ps,NOW())
            """), {"p":pid_db, "ph":phone,
                   "ps":phone.replace("+","").replace(" ","")})

            # 7. res_partner_bank (FK → res_partner)
            iban = f"TN59{pid_db:016d}"
            c.execute(text("""
                INSERT INTO res_partner_bank(
                    partner_id, acc_number, sanitized_acc_number,
                    active, company_id
                ) VALUES(:p,:acc,:sacc,true,:co)
                ON CONFLICT DO NOTHING
            """), {"p":pid_db,"acc":iban,"sacc":iban,"co":cid})

            # 8+9. g2p_entitlement + g2p_payment
            # (FK entitlement → partner + cycle ; payment → entitlement)
            success_rate = float(row.get("payment_success_rate", 0.8))
            amount       = float(row.get("amount_per_payment", 300))
            for pi in prog_indices:
                if pi >= len(prog_ids): continue
                for ci in range(N_CYCLES):
                    cdb = cycle_map.get((prog_ids[pi], ci))
                    if not cdb: continue
                    # 8. g2p_entitlement
                    ent_id = c.execute(text("""
                        INSERT INTO g2p_entitlement(
                            partner_id, cycle_id,
                            initial_amount, state,
                            approval_state, company_id,
                            code, valid_from, valid_until
                        ) VALUES(
                            :p,:cy,
                            :amt,'approved',
                            'approved',:co,
                            :code,:vf,:vu
                        ) RETURNING id
                    """), {
                        "p":    pid_db,
                        "cy":   cdb,
                        "amt":  amount,
                        "co":   cid,
                        "code": f"ENT-{pid_db}-{pi}-{ci}",
                        "vf":   date(2022,1,1) + timedelta(days=ci*90),
                        "vu":   date(2022,1,1) + timedelta(days=ci*90+89),
                    }).scalar()
                    # 9. g2p_payment (FK → g2p_entitlement)
                    paid   = random.random() < success_rate
                    a_paid = round(amount, 2) if paid else 0.0
                    c.execute(text("""
                        INSERT INTO g2p_payment(
                            entitlement_id, cycle_id,
                            amount_issued, amount_paid,
                            state, company_id, name,
                            issuance_date
                        ) VALUES(
                            :eid,:cy,
                            :ai,:ap,
                            :st,:co,:nm,
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

        print(f"   ✅ {len(partner_map)} partenaires + toutes FK injectées")


# ════════════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════════════

def main():
    SCRIPT_DIR = Path(__file__).resolve().parent
    PROJECT_ROOT = SCRIPT_DIR.parent.parent
    DEFAULT_OUTPUT = PROJECT_ROOT / "ml" / "data" / "synthetic" / "dataset_ml.csv"
        # DEBUG - À retirer après test
    print(f"📂 SCRIPT_DIR: {SCRIPT_DIR}")
    print(f"📂 PROJECT_ROOT: {PROJECT_ROOT}")
    print(f"📂 DEFAULT_OUTPUT: {DEFAULT_OUTPUT}")
    print()

    ap = argparse.ArgumentParser()
    ap.add_argument("--output",     default=str(DEFAULT_OUTPUT))
    ap.add_argument("--inject",     action="store_true")
    ap.add_argument("--db-url",     default=os.getenv(
        "OPENG2P_DB_URL","postgresql://odoo:openg2p@postgresql:5432/openg2p"))
    ap.add_argument("--n",          type=int,   default=N_TOTAL)
    ap.add_argument("--fraud-rate", type=float, default=FRAUD_RATE)
    args = ap.parse_args()

    df    = generate_dataset(args.n, args.fraud_rate)
    out   = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)
    cols = ML_FEATURES + [
    "is_fraud",
    "synthetic_label",
    "partner_idx",
    "partner_id",
    "scenario",
    "payment_gap_ratio",
    "network_risk_score"
    ]    
    df[[c for c in cols if c in df.columns]].to_csv(out, index=False)
    print(f"\n✅ CSV → {out}  ({len(df)} lignes)")

    # Stats
    print("\n📈 Moyennes légitimes vs fraudeurs :")
    check = ["income_per_person","pmt_score","gap_ratio",
             "shared_phone_count","nb_programs","network_risk"]
    print(df.groupby("is_fraud")[[c for c in check if c in df.columns]]
            .mean().round(3).to_string())
    print(df.columns)
    
    if args.inject:
        inject(df, args.db_url)


if __name__ == "__main__":
    main()
