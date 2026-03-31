"""
=============================================================
generate_and_inject.py — Solution complète au problème données
=============================================================
Ce script fait TOUT en une seule commande :
1. Génère 2000 bénéficiaires réalistes
2. Crée les programmes et cycles
3. Injecte les données dans TOUTES les tables OpenG2P
4. Simule 6 scénarios de fraude réels
5. Exporte le dataset ML prêt à l'entraînement

Usage dans le container :
    python ml/generate_and_inject.py

Résultat :
    - PostgreSQL OpenG2P rempli avec données réalistes
    - ml/data/dataset_ml.csv prêt pour train.py
    - AUC attendu : 0.80 - 0.90
=============================================================
"""

import os, sys, random, json
import numpy as np
import pandas as pd
from datetime import datetime, timedelta, date
from sqlalchemy import create_engine, text
from faker import Faker
import warnings
warnings.filterwarnings("ignore")

np.random.seed(42)
random.seed(42)
fake = Faker(['fr_FR', 'ar_AA'])

# ── Config ────────────────────────────────────────────────────
DB_URL = os.getenv("OPENG2P_DB_URL",
                   "postgresql://odoo:openg2p@postgresql:5432/openg2p")
N_BENEFICIAIRES = 2000
FRAUD_RATE       = 0.08   # 8% de fraudeurs
N_PROGRAMS       = 5
N_CYCLES         = 3

REGIONS   = ["Nord", "Sud", "Est", "Ouest", "Centre"]
PROGRAMS  = [
    "Aide alimentaire urgente",
    "Bourse scolarité enfants",
    "Allocation chefs de ménage",
    "Aide personnes handicapées",
    "Programme femmes vulnérables",
]


def get_engine():
    return create_engine(DB_URL, pool_pre_ping=True,
                         connect_args={"connect_timeout": 15})


def check_tables(engine) -> list:
    """Vérifie quelles tables existent"""
    query = text("""
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = 'public'
        ORDER BY table_name
    """)
    with engine.connect() as conn:
        tables = [r[0] for r in conn.execute(query)]
    return tables


def count_partners(engine) -> int:
    try:
        with engine.connect() as conn:
            return conn.execute(
                text("SELECT COUNT(*) FROM res_partner WHERE active=true AND is_company=false")
            ).scalar()
    except Exception:
        return 0
    
def get_default_company_id(engine) -> int:
    """Récupère un company_id valide depuis res_company"""
    with engine.connect() as conn:
        company_id = conn.execute(text("""
            SELECT id
            FROM res_company
            ORDER BY id
            LIMIT 1
        """)).scalar()

    if not company_id:
        raise RuntimeError("Aucune société trouvée dans res_company")
    return int(company_id)

# ══════════════════════════════════════════════════════════════
# GÉNÉRATION DES PROFILS BÉNÉFICIAIRES
# ══════════════════════════════════════════════════════════════

def generate_profiles(n: int, fraud_rate: float) -> pd.DataFrame:
    """Génère des profils réalistes de bénéficiaires"""
    print(f"\n👤 Génération de {n} profils bénéficiaires...")

    n_fraud = int(n * fraud_rate)
    n_legit = n - n_fraud

    profiles = []

    # ── Bénéficiaires légitimes ───────────────────────────────
    for i in range(n_legit):
        age = int(np.random.choice(
            range(18, 75),
            p=np.ones(57)/57
        ))
        gender = random.choice(["male", "female"])

        # Revenu réaliste selon profil
        base_income = np.random.lognormal(5.8, 0.7)  # ~330 FCFA médiane
        income = max(50, min(base_income, 3000))

        household = int(np.random.choice(
            [1,2,3,4,5,6,7,8],
            p=[0.05,0.10,0.20,0.25,0.20,0.10,0.07,0.03]
        ))

        profiles.append({
            "name":           fake.name(),
            "gender":         gender,
            "birthdate":      date.today() - timedelta(days=age*365 + random.randint(0,364)),
            "income":         round(income, 2),
            "household_size": household,
            "nb_children":    max(0, household - random.randint(1, 2)),
            "nb_elderly":     random.choice([0, 0, 0, 1]),
            "has_disabled":   random.random() < 0.05,
            "single_head":    random.random() < 0.15,
            "region":         random.choice(REGIONS),
            "is_fraud":       False,
            "fraud_type":     None,
        })

    # ── Fraudeurs — 6 types de fraude ────────────────────────
    fraud_types = {
        "multi_program":    int(n_fraud * 0.30),
        "shared_account":   int(n_fraud * 0.25),
        "shared_phone":     int(n_fraud * 0.20),
        "high_amount":      int(n_fraud * 0.10),
        "ghost_household":  int(n_fraud * 0.10),
        "recent_fraud":     n_fraud - int(n_fraud*0.95),
    }

    for fraud_type, count in fraud_types.items():
        for i in range(count):
            age = int(np.random.normal(38, 8))
            age = max(20, min(age, 65))

            profiles.append({
                "name":           fake.name(),
                "gender":         random.choice(["male", "female"]),
                "birthdate":      date.today() - timedelta(days=age*365),
                "income":         round(np.random.uniform(100, 500), 2),
                "household_size": random.randint(3, 8),
                "nb_children":    random.randint(1, 4),
                "nb_elderly":     0,
                "has_disabled":   False,
                "single_head":    False,
                "region":         random.choice(REGIONS),
                "is_fraud":       True,
                "fraud_type":     fraud_type,
            })

    df = pd.DataFrame(profiles).sample(frac=1, random_state=42).reset_index(drop=True)
    print(f"   Légitimes : {(~df['is_fraud']).sum()}")
    print(f"   Fraudeurs : {df['is_fraud'].sum()} ({df['is_fraud'].mean():.1%})")
    print(f"   Types : {df[df['is_fraud']]['fraud_type'].value_counts().to_dict()}")
    return df


# ══════════════════════════════════════════════════════════════
# INJECTION DANS POSTGRESQL
# ══════════════════════════════════════════════════════════════

def inject_partners(engine, profiles_df: pd.DataFrame) -> dict:
    """Injecte les bénéficiaires dans res_partner"""
    print("\n📥 Injection dans res_partner...")
    partner_ids = {}

    company_id = get_default_company_id(engine)
    print(f"   company_id utilisé : {company_id}")

    with engine.begin() as conn:
        for idx, row in profiles_df.iterrows():
            result = conn.execute(text("""
                INSERT INTO res_partner (
                    company_id,
                    name,
                    active,
                    is_company,
                    gender,
                    birthdate,
                    income,
                    z_ind_grp_num_individuals,
                    z_ind_grp_num_children,
                    z_ind_grp_num_elderly,
                    z_ind_grp_is_hh_with_disabled,
                    z_ind_grp_is_single_head_hh,
                    create_date,
                    write_date
                ) VALUES (
                    :company_id,
                    :name,
                    true,
                    false,
                    :gender,
                    :birthdate,
                    :income,
                    :household_size,
                    :nb_children,
                    :nb_elderly,
                    :has_disabled,
                    :single_head,
                    NOW(),
                    NOW()
                )
                RETURNING id
            """), {
                "company_id":     company_id,
                "name":           row["name"],
                "gender":         row["gender"],
                "birthdate":      row["birthdate"],
                "income":         row["income"],
                "household_size": row["household_size"],
                "nb_children":    row["nb_children"],
                "nb_elderly":     row["nb_elderly"],
                "has_disabled":   bool(row["has_disabled"]),
                "single_head":    bool(row["single_head"]),
            })

            pid = result.scalar()
            partner_ids[idx] = {
                "id":         pid,
                "is_fraud":   row["is_fraud"],
                "fraud_type": row["fraud_type"],
                "income":     row["income"],
            }

    print(f"   {len(partner_ids)} partenaires injectés")
    return partner_ids

def inject_programs_cycles(engine) -> tuple:
    """Crée les programmes et cycles"""
    print("\n📋 Création des programmes et cycles...")

    prog_ids = []
    with engine.begin() as conn:
        for name in PROGRAMS:
            r = conn.execute(text("""
                INSERT INTO g2p_program (name, active, create_date, write_date)
                VALUES (:name, true, NOW(), NOW())
                ON CONFLICT DO NOTHING
                RETURNING id
            """), {"name": name})
            row = r.fetchone()
            if row:
                prog_ids.append(row[0])

    if not prog_ids:
        with engine.connect() as conn:
            prog_ids = [r[0] for r in conn.execute(
                text("SELECT id FROM g2p_program ORDER BY id LIMIT 5")
            )]

    cycle_ids = []
    base = datetime(2023, 1, 1)
    with engine.begin() as conn:
        for pid in prog_ids:
            for c in range(N_CYCLES):
                start = base + timedelta(days=c * 90)
                end   = start + timedelta(days=89)
                r = conn.execute(text("""
                    INSERT INTO g2p_cycle
                        (name, program_id, start_date, end_date, state, create_date, write_date)
                    VALUES (:name, :pid, :start, :end, 'ended', NOW(), NOW())
                    RETURNING id
                """), {"name": f"Cycle {c+1}", "pid": pid,
                       "start": start, "end": end})
                cycle_ids.append(r.scalar())

    print(f"   {len(prog_ids)} programmes, {len(cycle_ids)} cycles")
    return prog_ids, cycle_ids


def inject_registrations(engine, partner_ids: dict, prog_ids: list):
    """Inscrit les bénéficiaires aux programmes"""
    print("\n📝 Inscription aux programmes...")
    count = 0
    with engine.begin() as conn:
        for idx, info in partner_ids.items():
            pid    = info["id"]
            fraud  = info["is_fraud"]
            ftype  = info["fraud_type"]

            # Fraudeurs multi-programme : 4-5 programmes
            if fraud and ftype == "multi_program":
                selected = prog_ids
            elif fraud:
                selected = random.sample(prog_ids, random.randint(2, 3))
            else:
                n = np.random.choice([1,2,3], p=[0.6, 0.3, 0.1])
                selected = random.sample(prog_ids, min(n, len(prog_ids)))

            for prog_id in selected:
                pmt = np.random.uniform(0.1, 0.35) if fraud else \
                      np.random.uniform(0.35, 0.85)
                try:
                    conn.execute(text("""
                        INSERT INTO g2p_program_registrant_info
                            (registrant_id, program_id, pmt_score,
                             latest_pmt_score, create_date, write_date)
                        VALUES (:rid, :pid, :pmt, :pmt, NOW(), NOW())
                        ON CONFLICT DO NOTHING
                    """), {"rid": pid, "pid": prog_id, "pmt": round(pmt, 4)})
                    count += 1
                except Exception:
                    pass

    print(f"   {count} inscriptions créées")


def inject_phones(engine, partner_ids: dict):
    """Injecte les numéros de téléphone avec partage pour fraudes"""
    print("\n📱 Injection des téléphones...")

    # Pool de numéros partagés
    shared_pool = [f"+2126{random.randint(10000000,99999999)}" for _ in range(20)]

    # Grouper les fraudeurs shared_phone
    shared_phone_fraudeurs = [
        info for info in partner_ids.values()
        if info["is_fraud"] and info["fraud_type"] == "shared_phone"
    ]

    # Assigner des numéros partagés par groupes de 2-4
    phone_map = {}
    i = 0
    while i < len(shared_phone_fraudeurs):
        group_size = random.randint(2, 4)
        phone = random.choice(shared_pool)
        for member in shared_phone_fraudeurs[i:i+group_size]:
            phone_map[member["id"]] = phone
        i += group_size

    count = 0
    with engine.begin() as conn:
        for idx, info in partner_ids.items():
            pid = info["id"]
            phone = phone_map.get(pid, f"+2126{random.randint(10000000,99999999)}")
            try:
                conn.execute(text("""
                    INSERT INTO g2p_phone_number
                        (partner_id, phone_no, date_collected, create_date, write_date)
                    VALUES (:pid, :phone, NOW(), NOW(), NOW())
                    ON CONFLICT DO NOTHING
                """), {"pid": pid, "phone": phone})
                count += 1
            except Exception:
                pass

    print(f"   {count} téléphones injectés")


def inject_bank_accounts(engine, partner_ids: dict):
    """Injecte les comptes bancaires avec partage pour fraudes"""
    print("\n🏦 Injection des comptes bancaires...")

    shared_pool = [f"MA{random.randint(10**11, 10**12-1)}" for _ in range(15)]

    shared_account_fraudeurs = [
        info for info in partner_ids.values()
        if info["is_fraud"] and info["fraud_type"] == "shared_account"
    ]

    account_map = {}
    i = 0
    while i < len(shared_account_fraudeurs):
        group_size = random.randint(2, 5)
        acct = random.choice(shared_pool)
        for member in shared_account_fraudeurs[i:i+group_size]:
            account_map[member["id"]] = acct
        i += group_size

    count = 0
    with engine.begin() as conn:
        for idx, info in partner_ids.items():
            pid  = info["id"]
            acct = account_map.get(pid, f"MA{random.randint(10**11, 10**12-1)}")
            try:
                conn.execute(text("""
                    INSERT INTO res_partner_bank
                        (partner_id, account_number, create_date, write_date)
                    VALUES (:pid, :acct, NOW(), NOW())
                    ON CONFLICT DO NOTHING
                """), {"pid": pid, "acct": acct})
                count += 1
            except Exception:
                pass

    print(f"   {count} comptes bancaires injectés")


def inject_payments(engine, partner_ids: dict, cycle_ids: list):
    """Injecte les paiements avec scénarios de fraude"""
    print("\n💰 Injection des paiements...")
    rows = []

    for idx, info in partner_ids.items():
        pid   = info["id"]
        fraud = info["is_fraud"]
        ftype = info["fraud_type"]

        # Cycles assignés aléatoirement
        assigned_cycles = random.sample(cycle_ids, min(
            random.randint(2, N_CYCLES), len(cycle_ids)
        ))

        for cycle_id in assigned_cycles:
            if fraud and ftype == "high_amount":
                issued = round(np.random.uniform(5000, 20000), 2)
                paid   = round(issued * np.random.uniform(0.95, 1.0), 2)

            elif fraud and ftype == "ghost_household":
                # Paiement partiel suspect (gap élevé)
                issued = round(np.random.uniform(1000, 5000), 2)
                paid   = round(issued * np.random.uniform(0.2, 0.5), 2)

            elif fraud:
                # Paiements multiples dans le même cycle
                for _ in range(random.randint(2, 4)):
                    issued = round(np.random.uniform(300, 800), 2)
                    paid   = round(issued * np.random.uniform(0.9, 1.0), 2)
                    rows.append({
                        "partner_id":      pid,
                        "cycle_id":        cycle_id,
                        "amount_issued":   issued,
                        "amount_paid":     paid,
                        "status":          "paid",
                        "payment_datetime":datetime.now() - timedelta(
                            days=random.randint(1, 365)),
                    })
                continue
            else:
                # Paiement normal
                issued = round(max(50, np.random.lognormal(5.8, 0.5)), 2)
                paid   = round(issued * np.random.uniform(0.97, 1.0), 2)

            rows.append({
                "partner_id":      pid,
                "cycle_id":        cycle_id,
                "amount_issued":   issued,
                "amount_paid":     paid,
                "status":          "paid",
                "payment_datetime":datetime.now() - timedelta(
                    days=random.randint(1, 365)),
            })

    count = 0
    with engine.begin() as conn:
        for row in rows:
            try:
                conn.execute(text("""
                    INSERT INTO g2p_payment
                        (partner_id, cycle_id, amount_issued, amount_paid,
                         status, payment_datetime, create_date, write_date)
                    VALUES
                        (:partner_id, :cycle_id, :amount_issued, :amount_paid,
                         :status, :payment_datetime, NOW(), NOW())
                """), row)
                count += 1
            except Exception:
                pass

    print(f"   {count} paiements injectés")


# ══════════════════════════════════════════════════════════════
# EXTRACTION DES FEATURES POUR ML
# ══════════════════════════════════════════════════════════════

def extract_ml_dataset(engine, partner_ids: dict) -> pd.DataFrame:
    """Extrait les features depuis PostgreSQL pour l'entraînement ML"""
    print("\n🔧 Extraction du dataset ML depuis PostgreSQL...")

    query = text("""
    WITH partner_base AS (
        SELECT
            p.id,
            p.gender,
            EXTRACT(YEAR FROM AGE(p.birthdate))::int          AS age,
            COALESCE(p.income, 0)                             AS income,
            COALESCE(p.z_ind_grp_num_individuals, 1)          AS household_size,
            COALESCE(p.z_ind_grp_num_children, 0)             AS nb_children,
            COALESCE(p.z_ind_grp_num_elderly, 0)              AS nb_elderly,
            CASE WHEN p.z_ind_grp_is_hh_with_disabled
                 THEN 1 ELSE 0 END                            AS has_disabled,
            CASE WHEN p.z_ind_grp_is_single_head_hh
                 THEN 1 ELSE 0 END                            AS single_head
        FROM res_partner p
        WHERE p.active = true AND p.is_company = false
    ),
    prog_agg AS (
        SELECT
            registrant_id,
            COUNT(DISTINCT program_id)                        AS nb_programs,
            AVG(pmt_score)                                    AS avg_pmt_score
        FROM g2p_program_registrant_info
        GROUP BY registrant_id
    ),
    pay_agg AS (
        SELECT
            partner_id,
            SUM(amount_issued)                                AS total_issued,
            SUM(amount_paid)                                  AS total_paid,
            SUM(amount_issued - amount_paid)                  AS total_gap,
            CASE WHEN SUM(amount_issued) > 0
                 THEN SUM(amount_issued-amount_paid)/SUM(amount_issued)
                 ELSE 0 END                                   AS gap_ratio,
            COUNT(id)                                         AS payment_count,
            COUNT(DISTINCT cycle_id)                          AS cycle_count
        FROM g2p_payment
        GROUP BY partner_id
    ),
    phone_shared AS (
        SELECT ph.partner_id,
               COUNT(DISTINCT ph2.partner_id) - 1            AS shared_phone_count
        FROM g2p_phone_number ph
        JOIN g2p_phone_number ph2
          ON ph.phone_no = ph2.phone_no
         AND ph.partner_id != ph2.partner_id
        GROUP BY ph.partner_id
    ),
    bank_shared AS (
        SELECT rb.partner_id,
               COUNT(DISTINCT rb2.partner_id) - 1            AS shared_account_count
        FROM res_partner_bank rb
        JOIN res_partner_bank rb2
          ON rb.account_number = rb2.account_number
         AND rb.partner_id != rb2.partner_id
        GROUP BY rb.partner_id
    )
    SELECT
        pb.id                                                 AS partner_id,
        COALESCE(pb.age, 35)                                  AS age,
        CASE WHEN pb.gender = 'male' THEN 1 ELSE 0 END        AS gender_m,
        pb.household_size,
        pb.nb_children,
        pb.nb_elderly,
        pb.has_disabled,
        pb.single_head,
        COALESCE(pa.nb_programs, 0)                           AS nb_programs,
        COALESCE(pa.avg_pmt_score, 0.5)                       AS pmt_score,
        COALESCE(py.total_issued, 0)                          AS total_issued,
        COALESCE(py.total_paid, 0)                            AS total_paid,
        COALESCE(py.gap_ratio, 0)                             AS gap_ratio,
        COALESCE(py.payment_count, 0)                         AS payment_count,
        COALESCE(py.cycle_count, 0)                           AS cycle_count,
        COALESCE(ps.shared_phone_count, 0)                    AS shared_phone,
        COALESCE(bs.shared_account_count, 0)                  AS shared_account
    FROM partner_base pb
    LEFT JOIN prog_agg  pa ON pb.id = pa.registrant_id
    LEFT JOIN pay_agg   py ON pb.id = py.partner_id
    LEFT JOIN phone_shared ps ON pb.id = ps.partner_id
    LEFT JOIN bank_shared  bs ON pb.id = bs.partner_id
    WHERE pa.nb_programs > 0
    ORDER BY pb.id
    """)

    df = pd.read_sql(query, engine)

    # Ajouter les labels depuis notre map
    fraud_map = {info["id"]: info["is_fraud"] for info in partner_ids.values()}
    df["is_fraud"] = df["partner_id"].map(fraud_map).fillna(False).astype(int)

    # Features dérivées
    df["income_per_person"] = df["total_issued"] / df["household_size"].clip(lower=1)
    df["income_log"]        = np.log1p(df["total_issued"])
    df["high_amount_flag"]  = (df["total_issued"] > df["total_issued"].quantile(0.95)).astype(int)
    df["high_gap_flag"]     = (df["gap_ratio"] > 0.5).astype(int)
    df["multi_prog_flag"]   = (df["nb_programs"] > 3).astype(int)
    df["network_risk"]      = (
        df["shared_phone"].clip(upper=5) * 0.4 +
        df["shared_account"].clip(upper=5) * 0.6
    ).clip(upper=1)

    print(f"   Dataset : {len(df)} lignes × {len(df.columns)} colonnes")
    print(f"   Fraudes : {df['is_fraud'].sum()} ({df['is_fraud'].mean():.1%})")
    return df


# ══════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════

def main():
    print("\n" + "="*60)
    print("  OpenG2P — Génération & Injection complète des données")
    print("="*60)

    # ── Connexion ────────────────────────────────────────────
    engine = get_engine()
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("✅ Connexion PostgreSQL OK")
    except Exception as e:
        print(f"❌ Connexion échouée : {e}")
        sys.exit(1)

    # ── Vérifier tables disponibles ──────────────────────────
    tables = check_tables(engine)
    print(f"✅ {len(tables)} tables trouvées dans OpenG2P")

    # ── Vérifier si données existent déjà ────────────────────
    n_existing = count_partners(engine)
    if n_existing > 500:
        print(f"\n⚠️  {n_existing} bénéficiaires déjà présents.")
        rep = input("Continuer quand même ? (o/n) : ").strip().lower()
        if rep != "o":
            print("Annulé.")
            sys.exit(0)

    # ── 1. Générer les profils ───────────────────────────────
    profiles = generate_profiles(N_BENEFICIAIRES, FRAUD_RATE)

    # ── 2. Injecter les partenaires ──────────────────────────
    partner_ids = inject_partners(engine, profiles)

    # ── 3. Programmes et cycles ──────────────────────────────
    prog_ids, cycle_ids = inject_programs_cycles(engine)

    # ── 4. Inscriptions ──────────────────────────────────────
    inject_registrations(engine, partner_ids, prog_ids)

    # ── 5. Téléphones ────────────────────────────────────────
    inject_phones(engine, partner_ids)

    # ── 6. Comptes bancaires ─────────────────────────────────
    inject_bank_accounts(engine, partner_ids)

    # ── 7. Paiements ─────────────────────────────────────────
    inject_payments(engine, partner_ids, cycle_ids)

    # ── 8. Extraire dataset ML ───────────────────────────────
    df_ml = extract_ml_dataset(engine, partner_ids)

    # ── 9. Sauvegarder ───────────────────────────────────────
    os.makedirs("ml/data", exist_ok=True)
    df_ml.to_csv("ml/data/dataset_ml.csv", index=False)

    print("\n" + "="*60)
    print("  RÉSUMÉ FINAL")
    print("="*60)
    print(f"  Bénéficiaires injectés : {len(partner_ids)}")
    print(f"  Fraudeurs              : {sum(1 for i in partner_ids.values() if i['is_fraud'])}")
    print(f"  Dataset ML             : ml/data/dataset_ml.csv")
    print(f"  Features               : {len(df_ml.columns) - 2} variables")
    print(f"\n  Prochaine étape :")
    print(f"  python ml/train_openg2p.py")
    print("="*60)


if __name__ == "__main__":
    try:
        from faker import Faker
    except ImportError:
        print("Installation de faker...")
        os.system("pip install faker -q")
        from faker import Faker

    main()
