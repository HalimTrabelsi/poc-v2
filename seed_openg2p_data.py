"""
=============================================================
Seed Script — Injection des données synthétiques OpenG2P
=============================================================
Ce script :
1. Lit les res_partner existants
2. Génère des données réalistes pour toutes les tables vides
3. Injecte des scénarios de fraude contrôlés
4. Prépare les données pour l'entraînement ML

Usage (dans le container fraud-engine) :
    python ml/seed_openg2p_data.py
=============================================================
"""

import os
import sys
import random
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
import warnings
warnings.filterwarnings("ignore")

# ── Configuration ─────────────────────────────────────────────
OPENG2P_DB_URL = os.getenv(
    "OPENG2P_DB_URL",
    "postgresql://odoo:openg2p@postgresql:5432/openg2p"
)

np.random.seed(42)
random.seed(42)

FRAUD_RATE      = 0.05   # 5% de fraudeurs
N_PROGRAMS      = 5      # Nombre de programmes sociaux
N_CYCLES        = 3      # Cycles de paiement par programme


# ══════════════════════════════════════════════════════════════
# ÉTAPE 1 — Connexion et lecture des partenaires existants
# ══════════════════════════════════════════════════════════════

def get_engine():
    return create_engine(OPENG2P_DB_URL, pool_pre_ping=True)


def get_table_columns(engine, table_name: str) -> set:
    """Retourne l'ensemble des colonnes d'une table PostgreSQL."""
    query = text("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = :table_name
    """)
    df = pd.read_sql(query, engine, params={"table_name": table_name})
    return set(df["column_name"].tolist())


def load_partners(engine) -> pd.DataFrame:
    """Charge tous les res_partner existants"""
    query = text("""
        SELECT
            id,
            name,
            gender,
            birthdate,
            active
        FROM res_partner
        WHERE active = true
          AND is_company = false
          AND name IS NOT NULL
        ORDER BY id
        LIMIT 2000
    """)
    df = pd.read_sql(query, engine)
    print(f"✅ {len(df)} partenaires chargés depuis res_partner")
    return df


# ══════════════════════════════════════════════════════════════
# ÉTAPE 2 — Génération des programmes et cycles
# ══════════════════════════════════════════════════════════════

def generate_programs(engine):
    """Crée des programmes sociaux réalistes"""
    programs = [
        {"name": "Programme Aide Alimentaire",       "code": "PAA"},
        {"name": "Programme Scolarité Enfants",      "code": "PSE"},
        {"name": "Programme Santé Maternelle",       "code": "PSM"},
        {"name": "Programme Handicap",               "code": "PHC"},
        {"name": "Programme Chefs de Ménage Seuls",  "code": "PMS"},
    ]

    with engine.begin() as conn:
        # Vérifier si des programmes existent déjà
        result = conn.execute(text("SELECT COUNT(*) FROM g2p_program")).scalar()
        if result > 0:
            print(f"⏭️  {result} programmes déjà présents")
            return pd.read_sql("SELECT id, name FROM g2p_program", engine)

        for p in programs:
            conn.execute(text("""
                INSERT INTO g2p_program (name, active, create_date, write_date)
                VALUES (:name, true, NOW(), NOW())
                ON CONFLICT DO NOTHING
            """), {"name": p["name"]})

    df = pd.read_sql("SELECT id, name FROM g2p_program ORDER BY id", engine)
    print(f"✅ {len(df)} programmes créés")
    return df


def generate_cycles(engine, programs_df: pd.DataFrame) -> pd.DataFrame:
    """Crée des cycles de paiement pour chaque programme"""
    with engine.begin() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM g2p_cycle")).scalar()
        if result > 0:
            print(f"⏭️  {result} cycles déjà présents")
            return pd.read_sql("SELECT id, program_id FROM g2p_cycle", engine)

        base_date = datetime(2023, 1, 1)
        for _, prog in programs_df.iterrows():
            for c in range(N_CYCLES):
                start = base_date + timedelta(days=c * 90)
                end   = start + timedelta(days=89)
                conn.execute(text("""
                    INSERT INTO g2p_cycle
                        (name, program_id, sequence, start_date, end_date, state,
                         create_date, write_date)
                    VALUES
                        (:name, :program_id, :sequence, :start, :end, 'ended',
                         NOW(), NOW())
                """), {
                    "name":       f"Cycle {c+1} - {prog['name'][:20]}",
                    "program_id": int(prog["id"]),
                    "sequence":   c + 1,
                    "start":      start,
                    "end":        end,
                })

    df = pd.read_sql("SELECT id, program_id FROM g2p_cycle ORDER BY id", engine)
    print(f"✅ {len(df)} cycles créés")
    return df


# ══════════════════════════════════════════════════════════════
# ÉTAPE 3 — Inscription bénéficiaires aux programmes
# ══════════════════════════════════════════════════════════════

def generate_registrations(engine, partners_df, programs_df):
    """Inscrit les bénéficiaires aux programmes avec scores PMT"""

    with engine.begin() as conn:
        result = conn.execute(
            text("SELECT COUNT(*) FROM g2p_program_registrant_info")
        ).scalar()
        if result > 0:
            print(f"⏭️  {result} inscriptions déjà présentes")
            return

    n_partners = len(partners_df)
    fraud_mask = np.zeros(n_partners, dtype=bool)
    fraud_idx  = np.random.choice(
        n_partners, int(n_partners * FRAUD_RATE), replace=False
    )
    fraud_mask[fraud_idx] = True

    rows = []
    for i, (_, partner) in enumerate(partners_df.iterrows()):
        is_fraud = fraud_mask[i]

        # Fraudeurs : inscrits à plus de programmes
        if is_fraud:
            n_prog = np.random.randint(4, len(programs_df) + 1)
        else:
            n_prog = np.random.choice([1, 2, 3], p=[0.60, 0.30, 0.10])

        selected = programs_df.sample(min(n_prog, len(programs_df)))

        for _, prog in selected.iterrows():
            pmt = np.random.uniform(0.1, 0.4) if is_fraud else np.random.uniform(0.3, 0.9)
            rows.append({
                "registrant_id":   int(partner["id"]),
                "program_id":      int(prog["id"]),
                "pmt_score":       round(pmt, 4),
                "latest_pmt_score": round(pmt, 4),
            })

    df_rows = pd.DataFrame(rows)
    with engine.begin() as conn:
        for _, row in df_rows.iterrows():
            conn.execute(text("""
                INSERT INTO g2p_program_registrant_info
                    (registrant_id, program_id, pmt_score,
                     latest_pmt_score, create_date, write_date)
                VALUES
                    (:registrant_id, :program_id, :pmt_score,
                     :latest_pmt_score, NOW(), NOW())
                ON CONFLICT DO NOTHING
            """), row.to_dict())

    print(f"✅ {len(df_rows)} inscriptions créées ({sum(fraud_mask)} fraudeurs)")
    return fraud_mask


# ══════════════════════════════════════════════════════════════
# ÉTAPE 4 — Numéros de téléphone (scénario fraude : partage)
# ══════════════════════════════════════════════════════════════

def generate_phones(engine, partners_df, fraud_mask):
    """
    Fraude simulée : plusieurs bénéficiaires partagent
    le même numéro de téléphone
    """
    with engine.begin() as conn:
        result = conn.execute(
            text("SELECT COUNT(*) FROM g2p_phone_number")
        ).scalar()
        if result > 0:
            print(f"⏭️  {result} téléphones déjà présents")
            return

    # Pool de numéros partagés pour fraudeurs
    shared_phones = [
        f"+2126{np.random.randint(10000000,99999999)}"
        for _ in range(20)
    ]

    rows = []
    shared_count = 0

    for i, (_, partner) in enumerate(partners_df.iterrows()):
        is_fraud = fraud_mask[i] if fraud_mask is not None else False

        if is_fraud and np.random.random() < 0.6:
            # 60% des fraudeurs partagent un numéro
            phone = random.choice(shared_phones)
            shared_count += 1
        else:
            phone = f"+2126{np.random.randint(10000000, 99999999)}"

        rows.append({
            "partner_id": int(partner["id"]),
            "phone_no":   phone,
        })

    with engine.begin() as conn:
        for row in rows:
            conn.execute(text("""
                INSERT INTO g2p_phone_number
                    (partner_id, phone_no, date_collected,
                     create_date, write_date)
                VALUES
                    (:partner_id, :phone_no, NOW(), NOW(), NOW())
                ON CONFLICT DO NOTHING
            """), row)

    print(f"✅ {len(rows)} téléphones créés ({shared_count} partagés = fraude)")


# ══════════════════════════════════════════════════════════════
# ÉTAPE 5 — Comptes bancaires (scénario fraude : partage)
# ═════════════════════════════════════════════════════════════

def generate_bank_accounts(engine, partners_df, fraud_mask):
    """
    Fraude simulée : plusieurs bénéficiaires partagent
    le même compte bancaire
    """
    with engine.begin() as conn:
        result = conn.execute(
            text("SELECT COUNT(*) FROM res_partner_bank")
        ).scalar()
        if result > 0:
            print(f"⏭️  {result} comptes bancaires déjà présents")
            return

    bank_cols = get_table_columns(engine, "res_partner_bank")
    account_col = "account_number" if "account_number" in bank_cols else "acc_number" if "acc_number" in bank_cols else None
    if account_col is None or "partner_id" not in bank_cols:
        print("⚠️  res_partner_bank: colonnes attendues absentes, seed des comptes ignoré")
        return

    # Pool de comptes partagés pour fraudeurs
    shared_accounts = [
        f"MA{np.random.randint(100000000000, 999999999999)}"
        for _ in range(15)
    ]

    rows = []
    shared_count = 0

    for i, (_, partner) in enumerate(partners_df.iterrows()):
        is_fraud = fraud_mask[i] if fraud_mask is not None else False

        if is_fraud and np.random.random() < 0.7:
            # 70% des fraudeurs partagent un compte
            account = random.choice(shared_accounts)
            shared_count += 1
        else:
            account = f"MA{np.random.randint(100000000000, 999999999999)}"

        rows.append({
            "partner_id": int(partner["id"]),
            "account_value": account,
        })

    with engine.begin() as conn:
        for row in rows:
            conn.execute(
                text(f"""
                    INSERT INTO res_partner_bank
                        (partner_id, {account_col}, create_date, write_date)
                    VALUES
                        (:partner_id, :account_value, NOW(), NOW())
                    ON CONFLICT DO NOTHING
                """),
                row,
            )

    print(f"✅ {len(rows)} comptes créés ({shared_count} partagés = fraude)")


# ══════════════════════════════════════════════════════════════
# ÉTAPE 6 — Paiements (scénarios fraude multiples)
# ══════════════════════════════════════════════════════════════

def generate_payments(engine, partners_df, cycles_df, fraud_mask):
    """
    Scénarios de fraude dans les paiements :
    1. Montants anormalement élevés
    2. Écart important issued vs paid (gap élevé)
    3. Paiements multiples dans un même cycle
    """
    with engine.begin() as conn:
        result = conn.execute(
            text("SELECT COUNT(*) FROM g2p_payment")
        ).scalar()
        if result > 0:
            print(f"⏭️  {result} paiements déjà présents")
            return

    payment_cols = get_table_columns(engine, "g2p_payment")
    required_cols = {"partner_id", "cycle_id", "amount_issued", "amount_paid"}
    if not required_cols.issubset(payment_cols):
        print("⚠️  g2p_payment: schéma incompatible avec le seed simplifié, paiements ignorés")
        return

    # Charger les inscriptions pour savoir quel cycle → quel partenaire
    registrations = pd.read_sql("""
        SELECT registrant_id, program_id
        FROM g2p_program_registrant_info
    """, engine)

    rows = []
    for i, (_, partner) in enumerate(partners_df.iterrows()):
        is_fraud = fraud_mask[i] if fraud_mask is not None else False

        # Trouver les cycles correspondant aux programmes du partenaire
        partner_progs = registrations[
            registrations["registrant_id"] == partner["id"]
        ]["program_id"].tolist()

        if not partner_progs:
            continue

        for cycle_id in cycles_df[
            cycles_df["program_id"].isin(partner_progs)
        ]["id"].tolist():

            if is_fraud:
                # Fraude type 1 : montant élevé
                if np.random.random() < 0.5:
                    amount_issued = np.random.uniform(5000, 15000)
                    amount_paid   = amount_issued * np.random.uniform(0.9, 1.0)

                # Fraude type 2 : gap élevé (issued >> paid)
                elif np.random.random() < 0.5:
                    amount_issued = np.random.uniform(2000, 8000)
                    amount_paid   = amount_issued * np.random.uniform(0.2, 0.5)

                # Fraude type 3 : paiements multiples
                else:
                    for _ in range(np.random.randint(2, 5)):
                        amount_issued = np.random.uniform(500, 2000)
                        amount_paid   = amount_issued
                        rows.append({
                            "partner_id":      int(partner["id"]),
                            "cycle_id":        int(cycle_id),
                            "amount_issued":   round(amount_issued, 2),
                            "amount_paid":     round(amount_paid, 2),
                            "status":          "paid",
                            "payment_datetime": datetime.now() - timedelta(
                                days=np.random.randint(1, 365)),
                        })
                    continue
            else:
                # Paiement normal
                amount_issued = np.random.normal(500, 150)
                amount_issued = max(amount_issued, 50)
                amount_paid   = amount_issued * np.random.uniform(0.95, 1.0)

            rows.append({
                "partner_id":      int(partner["id"]),
                "cycle_id":        int(cycle_id),
                "amount_issued":   round(amount_issued, 2),
                "amount_paid":     round(amount_paid, 2),
                "status":          "paid",
                "payment_datetime": datetime.now() - timedelta(
                    days=np.random.randint(1, 365)),
            })

    with engine.begin() as conn:
        for row in rows:
            conn.execute(
                text("""
                    INSERT INTO g2p_payment
                        (partner_id, cycle_id, amount_issued, amount_paid,
                         status, payment_datetime, create_date, write_date)
                    VALUES
                        (:partner_id, :cycle_id, :amount_issued, :amount_paid,
                         :status, :payment_datetime, NOW(), NOW())
                """),
                row,
            )

    print(f"✅ {len(rows)} paiements créés")


# ══════════════════════════════════════════════════════════════
# ÉTAPE 7 — Extraction features + export CSV pour ML
# ══════════════════════════════════════════════════════════════

def extract_features_for_ml(engine) -> pd.DataFrame:
    """
    Extrait toutes les features depuis les tables OpenG2P
    pour entraîner le modèle ML
    """
    print("\n🔧 Extraction des features ML depuis OpenG2P...")

    bank_cols = get_table_columns(engine, "res_partner_bank")
    bank_account_col = "account_number" if "account_number" in bank_cols else "acc_number" if "acc_number" in bank_cols else None
    if bank_account_col and "partner_id" in bank_cols:
        bank_cte = f"""
    bank_info AS (
        SELECT
            rb.partner_id,
            COUNT(DISTINCT rb2.partner_id) - 1                AS shared_account_count
        FROM res_partner_bank rb
        JOIN res_partner_bank rb2 ON rb.{bank_account_col} = rb2.{bank_account_col}
            AND rb.partner_id != rb2.partner_id
        GROUP BY rb.partner_id
    )
    """
    else:
        bank_cte = """
    bank_info AS (
        SELECT NULL::int AS partner_id, 0::int AS shared_account_count
        WHERE FALSE
    )
    """

    payment_cols = get_table_columns(engine, "g2p_payment")
    if "partner_id" in payment_cols:
        payment_partner_expr = "pay.partner_id"
        payment_join_sql = ""
    elif "entitlement_id" in payment_cols and "id" in get_table_columns(engine, "g2p_entitlement"):
        payment_partner_expr = "ent.partner_id"
        payment_join_sql = "JOIN g2p_entitlement ent ON ent.id = pay.entitlement_id"
    else:
        payment_partner_expr = None
        payment_join_sql = ""

    if payment_partner_expr:
        payment_cte = f"""
    payment_info AS (
        SELECT
            {payment_partner_expr}                            AS partner_id,
            SUM(pay.amount_issued)                            AS total_amount_issued,
            SUM(pay.amount_paid)                              AS total_amount_paid,
            SUM(pay.amount_issued - pay.amount_paid)          AS payment_gap,
            CASE WHEN SUM(pay.amount_issued) > 0
                 THEN SUM(pay.amount_issued - pay.amount_paid)
                      / SUM(pay.amount_issued)
                 ELSE 0 END                                   AS payment_gap_ratio,
            COUNT(pay.id)                                     AS payment_count,
            COUNT(DISTINCT pay.cycle_id)                      AS payment_count_in_cycle
        FROM g2p_payment pay
        {payment_join_sql}
        GROUP BY {payment_partner_expr}
    )
    """
    else:
        payment_cte = """
    payment_info AS (
        SELECT NULL::int AS partner_id,
               0::numeric AS total_amount_issued,
               0::numeric AS total_amount_paid,
               0::numeric AS payment_gap,
               0::numeric AS payment_gap_ratio,
               0::int AS payment_count,
               0::int AS payment_count_in_cycle
        WHERE FALSE
    )
    """

    query = text(f"""
    WITH
    -- Profil de base
    partner_base AS (
        SELECT
            p.id                                              AS partner_id,
            p.gender,
            EXTRACT(YEAR FROM AGE(p.birthdate))::int          AS age,
            COALESCE(p.income, 0)                             AS income,
            COALESCE(p.z_ind_grp_num_individuals, 1)          AS household_size,
            COALESCE(p.z_ind_grp_num_children, 0)             AS nb_children,
            COALESCE(p.z_ind_grp_num_elderly, 0)              AS nb_elderly,
            CASE WHEN p.z_ind_grp_is_hh_with_disabled THEN 1
                 ELSE 0 END                                   AS has_disabled,
            CASE WHEN p.z_ind_grp_is_single_head_hh THEN 1
                 ELSE 0 END                                   AS single_head
        FROM res_partner p
        WHERE p.active = true AND p.is_company = false
    ),

    -- Programmes
    prog_info AS (
        SELECT
            pri.registrant_id                                 AS partner_id,
            COUNT(DISTINCT pri.program_id)                    AS nb_programs,
            CASE WHEN COUNT(DISTINCT pri.program_id) > 3
                 THEN 1 ELSE 0 END                            AS program_overlap_flag,
            AVG(pri.pmt_score)                                AS pmt_score
        FROM g2p_program_registrant_info pri
        GROUP BY pri.registrant_id
    ),

    -- Paiements
    {payment_cte},

    -- Réseau téléphonique (partage de numéro = fraude)
    phone_info AS (
        SELECT
            ph.partner_id,
            COUNT(DISTINCT ph2.partner_id) - 1                AS shared_phone_count
        FROM g2p_phone_number ph
        JOIN g2p_phone_number ph2 ON ph.phone_no = ph2.phone_no
            AND ph.partner_id != ph2.partner_id
        GROUP BY ph.partner_id
    ),

    -- Réseau bancaire (partage de compte = fraude)
    {bank_cte},

    -- Groupes
    group_info AS (
        SELECT
            gm.individual                                     AS partner_id,
            COUNT(DISTINCT gm."group")                        AS active_group_memberships
        FROM g2p_group_membership gm
        GROUP BY gm.individual
    )

    SELECT
        pb.partner_id,

        -- Profil
        COALESCE(pb.age, 35)                                  AS age,
        pb.gender,
        COALESCE(pb.income, 0)                                AS income,
        COALESCE(pb.household_size, 1)                        AS household_size,
        COALESCE(pb.nb_children + pb.nb_elderly, 0)::float
            / NULLIF(pb.household_size, 0)                    AS dependency_ratio,
        COALESCE(pb.income, 0)
            / NULLIF(pb.household_size, 1)                    AS income_per_person,
        pb.has_disabled,
        pb.single_head,

        -- Programmes
        COALESCE(pi.nb_programs, 0)                           AS nb_programs,
        COALESCE(pi.program_overlap_flag, 0)                  AS program_overlap_flag,
        COALESCE(pi.pmt_score, 0.5)                           AS pmt_score,

        -- Paiements
        COALESCE(pai.total_amount_issued, 0)                  AS total_amount_issued,
        COALESCE(pai.total_amount_paid, 0)                    AS total_amount_paid,
        COALESCE(pai.payment_gap, 0)                          AS payment_gap,
        COALESCE(pai.payment_gap_ratio, 0)                    AS payment_gap_ratio,
        COALESCE(pai.payment_count, 0)                        AS payment_count,
        COALESCE(pai.payment_count_in_cycle, 0)               AS payment_count_in_cycle,

        -- Réseau
        COALESCE(phi.shared_phone_count, 0)                   AS shared_phone_count,
        COALESCE(bi.shared_account_count, 0)                  AS shared_account_count,

        -- Groupes
        COALESCE(gi.active_group_memberships, 0)              AS active_group_memberships

    FROM partner_base pb
    LEFT JOIN prog_info    pi  ON pb.partner_id = pi.partner_id
    LEFT JOIN payment_info pai ON pb.partner_id = pai.partner_id
    LEFT JOIN phone_info   phi ON pb.partner_id = phi.partner_id
    LEFT JOIN bank_info    bi  ON pb.partner_id = bi.partner_id
    LEFT JOIN group_info   gi  ON pb.partner_id = gi.partner_id

    WHERE pi.nb_programs > 0   -- Seulement les bénéficiaires inscrits
    ORDER BY pb.partner_id
    """)

    df = pd.read_sql(query, engine)
    print(f"✅ {len(df)} bénéficiaires avec features extraites")
    return df


def add_fraud_labels(df: pd.DataFrame) -> pd.DataFrame:
    """
    Génère les labels de fraude basés sur les patterns détectés.
    Règles de labeling :
    - shared_account_count > 0 → forte probabilité fraude
    - shared_phone_count > 0 → forte probabilité fraude
    - payment_gap_ratio > 0.5 → fraude paiement
    - nb_programs > 3 + total_amount_issued élevé → fraude inscription
    """
    df = df.copy()

    # Score de fraude basé sur les règles
    fraud_score = np.zeros(len(df))

    fraud_score += (df["shared_account_count"] > 0).astype(float) * 0.5
    fraud_score += (df["shared_phone_count"] > 0).astype(float) * 0.4
    fraud_score += (df["payment_gap_ratio"] > 0.5).astype(float) * 0.3
    fraud_score += (df["nb_programs"] > 3).astype(float) * 0.3
    fraud_score += (df["total_amount_issued"] >
                    df["total_amount_issued"].quantile(0.95)).astype(float) * 0.3
    fraud_score += (df["payment_count_in_cycle"] >
                    df["payment_count_in_cycle"].quantile(0.90)).astype(float) * 0.2

    # Label final
    df["is_fraud"] = (fraud_score >= 0.5).astype(int)

    print(f"\n📊 Distribution des labels :")
    print(f"  Légitimes : {(df['is_fraud']==0).sum()} ({(df['is_fraud']==0).mean():.1%})")
    print(f"  Fraudeurs : {(df['is_fraud']==1).sum()} ({df['is_fraud'].mean():.1%})")
    return df


# ══════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════

def main():
    print("\n" + "="*60)
    print("  OpenG2P — Seed des données + Feature Extraction")
    print("="*60)

    engine = get_engine()

    # Test connexion
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("✅ Connexion DB OK")
    except Exception as e:
        print(f"❌ Connexion DB échouée : {e}")
        sys.exit(1)

    # ── 1. Charger les partenaires ───────────────────────────
    partners = load_partners(engine)
    if partners.empty:
        print("❌ Aucun partenaire trouvé dans res_partner")
        sys.exit(1)

    # ── 2. Créer programmes et cycles ───────────────────────
    programs = generate_programs(engine)
    cycles   = generate_cycles(engine, programs)

    # ── 3. Marquer les fraudeurs ─────────────────────────────
    n = len(partners)
    fraud_idx  = np.random.choice(n, int(n * FRAUD_RATE), replace=False)
    fraud_mask = np.zeros(n, dtype=bool)
    fraud_mask[fraud_idx] = True
    print(f"\n👤 {fraud_mask.sum()} bénéficiaires marqués comme fraudeurs ({FRAUD_RATE:.0%})")

    # ── 4. Générer les données des tables vides ──────────────
    generate_registrations(engine, partners, programs)
    generate_phones(engine, partners, fraud_mask)
    generate_bank_accounts(engine, partners, fraud_mask)
    generate_payments(engine, partners, cycles, fraud_mask)

    # ── 5. Extraire les features pour ML ────────────────────
    df_features = extract_features_for_ml(engine)
    df_features = add_fraud_labels(df_features)

    # ── 6. Sauvegarder pour entraînement ────────────────────
    os.makedirs("ml/data", exist_ok=True)
    output_path = "ml/data/openg2p_features.csv"
    df_features.to_csv(output_path, index=False)
    print(f"\n✅ Features sauvegardées : {output_path}")
    print(f"   Shape : {df_features.shape}")
    print(f"\n📊 Aperçu des features :")
    print(df_features.describe().round(2).to_string())

    print("\n" + "="*60)
    print("  ✅ Seed terminé — Lance maintenant :")
    print("     python ml/train_openg2p.py")
    print("="*60)


if __name__ == "__main__":
    main()