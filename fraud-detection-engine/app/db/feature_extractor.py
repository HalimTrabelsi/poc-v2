"""
feature_extractor.py — Extraction features anti-fraude depuis OpenG2P
======================================================================
Schéma 100% vérifié sur les vraies tables (avril 2025).

Jointures réelles confirmées :
  g2p_payment.entitlement_id  → g2p_entitlement.id
  g2p_entitlement.partner_id  → res_partner.id       ← clé principale
  g2p_entitlement.cycle_id    → g2p_cycle.id
  g2p_program_membership.partner_id → res_partner.id
  g2p_group_membership.individual   → res_partner.id


"""

from typing import Optional
import pandas as pd
from sqlalchemy import text
from app.db.postgres import get_openg2p_db


def extract_features(limit: Optional[int] = None) -> pd.DataFrame:
    """
    Extrait toutes les features ML depuis OpenG2P PostgreSQL.

    Returns:
        DataFrame — 1 ligne par bénéficiaire, toutes features incluses.
    """
    db     = get_openg2p_db()
    engine = db.engine
    params = {}
    limit_sql = ""
    if limit is not None:
        limit_sql = "LIMIT :limit"
        params["limit"] = limit

    query = text(f"""
    /* ═══════════════════════════════════════════════════
       CTE 1 — Démographie (res_partner)
       Toutes les colonnes z_ind_grp_* confirmées
    ═══════════════════════════════════════════════════ */
    WITH partner_base AS (
        SELECT
            p.id                                                    AS partner_id,
            COALESCE(EXTRACT(YEAR FROM AGE(p.birthdate))::int, 35) AS age,
            LOWER(COALESCE(p.gender, 'unknown'))                    AS gender,
            COALESCE(p.income, 0.0)                                 AS income,
            GREATEST(COALESCE(p.z_ind_grp_num_individuals, 1), 1)  AS household_size,
            COALESCE(p.z_ind_grp_num_children, 0)                  AS nb_children,
            COALESCE(p.z_ind_grp_num_elderly,  0)                  AS nb_elderly,
            CASE WHEN p.z_ind_grp_is_hh_with_disabled THEN 1 ELSE 0 END  AS has_disabled,
            CASE WHEN p.z_ind_grp_is_single_head_hh   THEN 1 ELSE 0 END  AS single_head,
            CASE WHEN p.z_ind_grp_is_elderly_head_hh  THEN 1 ELSE 0 END  AS elderly_head,
            -- Ancienneté (jours depuis inscription ou création)
            COALESCE(
                EXTRACT(DAY FROM NOW() - p.registration_date::timestamp)::int,
                EXTRACT(DAY FROM NOW() - p.create_date)::int,
                365
            )                                                       AS registration_age_days
        FROM res_partner p
        WHERE p.active                       = true
          AND COALESCE(p.is_company,    false) = false
          AND COALESCE(p.is_registrant, false) = true
          AND p.name NOT IN (
              'My Company', 'Administrator', 'Public user',
              'Default User Template', 'OdooBot'
          )
    ),

    /* ═══════════════════════════════════════════════════
       CTE 2 — Programmes et PMT
       Via g2p_program_membership (partner_id direct)
       + g2p_program_registrant_info pour pmt_score
    ═══════════════════════════════════════════════════ */
    prog_agg AS (
        SELECT
            pm.partner_id,
            COUNT(DISTINCT pm.program_id)                           AS nb_programs,
            -- Nb de programmes actifs (pas encore sortis)
            COUNT(DISTINCT CASE WHEN pm.state = 'enrolled'
                  AND (pm.exit_date IS NULL OR pm.exit_date > NOW()::date)
                  THEN pm.program_id END)                           AS nb_active_programs,
            -- Ancienneté moyenne dans les programmes (jours)
            AVG(
                EXTRACT(DAY FROM NOW() - COALESCE(pm.enrollment_date, pm.create_date))
            )::int                                                  AS avg_enrollment_days
        FROM g2p_program_membership pm
        GROUP BY pm.partner_id
    ),

    pmt_agg AS (
        SELECT
            pri.registrant_id                                       AS partner_id,
            AVG(COALESCE(pri.pmt_score, pri.latest_pmt_score, 0.5)) AS pmt_score,
            MIN(COALESCE(pri.pmt_score, pri.latest_pmt_score, 0.5)) AS pmt_score_min
        FROM g2p_program_registrant_info pri
        GROUP BY pri.registrant_id
    ),

    /* ═══════════════════════════════════════════════════
       CTE 3 — Paiements
       Jointure réelle : g2p_payment → g2p_entitlement → partner_id
       g2p_payment n'a PAS de partner_id direct
    ═══════════════════════════════════════════════════ */
    pay_agg AS (
        SELECT
            ent.partner_id,
            COUNT(py.id)                                            AS payment_count,
            COALESCE(SUM(py.amount_issued), 0.0)                   AS total_issued,
            COALESCE(SUM(py.amount_paid),   0.0)                   AS total_paid,
            -- payment_gap_ratio : part non payée
            CASE
                WHEN COALESCE(SUM(py.amount_issued), 0) > 0
                THEN (SUM(py.amount_issued) - SUM(py.amount_paid))
                     / SUM(py.amount_issued)
                ELSE 0.0
            END                                                     AS payment_gap_ratio,
            -- payment_success_rate : part effectivement payée
            CASE
                WHEN COALESCE(SUM(py.amount_issued), 0) > 0
                THEN SUM(py.amount_paid) / SUM(py.amount_issued)
                ELSE 0.0
            END                                                     AS payment_success_rate,
            -- Variance des montants payés (erratique = suspect)
            COALESCE(VAR_POP(py.amount_paid), 0.0)                 AS amount_variance,
            COUNT(DISTINCT ent.cycle_id)                            AS cycle_count,
            -- Montant initial moyen par entitlement
            AVG(ent.initial_amount)                                 AS avg_entitlement_amount
        FROM g2p_payment py
        JOIN g2p_entitlement ent ON py.entitlement_id = ent.id
        GROUP BY ent.partner_id
    ),

    /* ═══════════════════════════════════════════════════
       CTE 4 — Téléphones partagés (g2p_phone_number)
       phone_sanitized prioritaire, fallback phone_no
       Les deux colonnes existent dans le vrai schéma
    ═══════════════════════════════════════════════════ */
    phone_shared AS (
        SELECT
            ph.partner_id,
            COUNT(DISTINCT ph2.partner_id) - 1                     AS shared_phone_count
        FROM g2p_phone_number ph
        JOIN g2p_phone_number ph2
          ON COALESCE(ph.phone_sanitized, ph.phone_no)
           = COALESCE(ph2.phone_sanitized, ph2.phone_no)
         AND ph.partner_id != ph2.partner_id
        GROUP BY ph.partner_id
    ),

    /* ═══════════════════════════════════════════════════
       CTE 5 — Comptes bancaires partagés (res_partner_bank)
       Colonnes réelles : acc_number + sanitized_acc_number
       PAS de colonne account_number dans ce schéma
    ═══════════════════════════════════════════════════ */
    bank_shared AS (
        SELECT
            rb.partner_id,
            COUNT(DISTINCT rb2.partner_id) - 1                     AS shared_account_count
        FROM res_partner_bank rb
        JOIN res_partner_bank rb2
          ON COALESCE(NULLIF(rb.sanitized_acc_number,  ''), rb.acc_number)
           = COALESCE(NULLIF(rb2.sanitized_acc_number, ''), rb2.acc_number)
         AND rb.partner_id  != rb2.partner_id
         AND rb.active  = true
         AND rb2.active = true
        GROUP BY rb.partner_id
    ),

    /* ═══════════════════════════════════════════════════
       CTE 6 — Appartenance aux groupes/ménages
       g2p_group_membership.individual = res_partner.id
    ═══════════════════════════════════════════════════ */
    group_agg AS (
        SELECT
            gm.individual                                          AS partner_id,
            COUNT(*)                                               AS group_membership_count
        FROM g2p_group_membership gm
        WHERE COALESCE(gm.is_ended, false) = false
        GROUP BY gm.individual
    )

    /* ═══════════════════════════════════════════════════
       SELECT FINAL
    ═══════════════════════════════════════════════════ */
    SELECT
        pb.partner_id,
        -- Démographie
        pb.age,
        pb.gender,
        pb.income,
        pb.household_size,
        pb.nb_children,
        pb.nb_elderly,
        pb.has_disabled,
        pb.single_head,
        pb.elderly_head,
        pb.registration_age_days,
        -- Programmes
        COALESCE(pa.nb_programs,         0)     AS nb_programs,
        COALESCE(pa.nb_active_programs,  0)     AS nb_active_programs,
        COALESCE(pa.avg_enrollment_days, 0)     AS avg_enrollment_days,
        COALESCE(pm.pmt_score,           0.5)   AS pmt_score,
        COALESCE(pm.pmt_score_min,       0.5)   AS pmt_score_min,
        -- Paiements
        COALESCE(py.payment_count,            0)   AS payment_count,
        COALESCE(py.total_issued,             0.0) AS total_issued,
        COALESCE(py.total_paid,               0.0) AS total_paid,
        COALESCE(py.payment_gap_ratio,        0.0) AS payment_gap_ratio,
        COALESCE(py.payment_success_rate,     0.0) AS payment_success_rate,
        COALESCE(py.amount_variance,          0.0) AS amount_variance,
        COALESCE(py.cycle_count,              0)   AS cycle_count,
        COALESCE(py.avg_entitlement_amount,   0.0) AS avg_entitlement_amount,
        -- Réseau
        COALESCE(ps.shared_phone_count,   0)    AS shared_phone_count,
        COALESCE(bs.shared_account_count, 0)    AS shared_account_count,
        -- Groupes
        COALESCE(ga.group_membership_count, 0)  AS group_membership_count
    FROM partner_base pb
    LEFT JOIN prog_agg   pa ON pb.partner_id = pa.partner_id
    LEFT JOIN pmt_agg    pm ON pb.partner_id = pm.partner_id
    LEFT JOIN pay_agg    py ON pb.partner_id = py.partner_id
    LEFT JOIN phone_shared ps ON pb.partner_id = ps.partner_id
    LEFT JOIN bank_shared  bs ON pb.partner_id = bs.partner_id
    LEFT JOIN group_agg    ga ON pb.partner_id = ga.partner_id
    {limit_sql}
    """)

    df = pd.read_sql(query, engine, params=params)

    # ── Features dérivées ──────────────────────────────────────────────

    # Revenu par personne
    df["income_per_person"] = (
        df["income"] / df["household_size"].clip(lower=1)
    ).round(2)

    # Ratio de dépendance : (enfants + âgés) / adultes
    adults = (
        df["household_size"] - df["nb_children"] - df["nb_elderly"]
    ).clip(lower=1)
    df["dependency_ratio"] = (
        (df["nb_children"] + df["nb_elderly"]) / adults
    ).round(3)

    # Score réseau combiné phone + compte
    df["network_risk"] = (
        df["shared_phone_count"].clip(upper=5)   * 0.4
        + df["shared_account_count"].clip(upper=5) * 0.6
    ).clip(upper=1.0).round(4)

    # Flag montant anormalement élevé (> 95e percentile)
    q95 = df["total_issued"].quantile(0.95)
    df["high_amount_flag"] = (df["total_issued"] > q95).astype(int)

    # Incohérence revenu-programme : revenu très faible + beaucoup de programmes
    q15 = df["income_per_person"].quantile(0.15)
    df["income_program_inconsistency"] = (
        (df["income_per_person"] < q15) & (df["nb_programs"] >= 3)
    ).astype(int)

    return df


# ── Liste des features utilisées pour l'entraînement ML ───────────────
ML_FEATURES = [
    # Démographie
    "age", "income", "income_per_person",
    "household_size", "nb_children", "nb_elderly",
    "dependency_ratio",
    "has_disabled", "single_head",
    # Programmes
    "nb_programs", "nb_active_programs",
    "pmt_score", "pmt_score_min",
    "avg_enrollment_days",
    # Paiements
    "payment_count", "payment_gap_ratio",
    "payment_success_rate", "amount_variance",
    "cycle_count",
    # Réseau
    "shared_phone_count", "shared_account_count",
    "network_risk",
    # Groupes
    "group_membership_count",
    # Flags dérivés
    "high_amount_flag", "income_program_inconsistency",
]
