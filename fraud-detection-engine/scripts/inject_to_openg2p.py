"""Inject synthetic beneficiaries directly into OpenG2P PostgreSQL tables.

This creates realistic beneficiaries in the same tables the fraud engine reads from,
so they appear in the OpenG2P UI and can be scored by the fraud detection pipeline.

Usage:
    python scripts/inject_to_openg2p.py --n 200 --fraud-rate 0.10
    python scripts/inject_to_openg2p.py --n 50 --fraud-rate 0.20 --truncate
"""
import argparse
import logging
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text

sys.path.insert(0, str(Path(__file__).parent.parent))

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

OPENG2P_DB_URL = "postgresql://odoo:openg2p@localhost:5432/openg2p"
RNG = np.random.default_rng(42)

FIRST_NAMES = [
    "Amara", "Fatou", "Mamadou", "Ibrahim", "Aissatou", "Oumar", "Mariama",
    "Abdoulaye", "Fatoumata", "Moussa", "Kadiatou", "Sekou", "Aminata",
    "Boubacar", "Hawa", "Lansana", "Djénéba", "Thierno", "Mariam", "Saliou",
    "Binta", "Alpha", "Ramata", "Cheikh", "Coumba", "Aliou", "Ndeye",
    "Mohamed", "Rokhaya", "Ousmane", "Adama", "Safiatou", "Ibrahima",
]
LAST_NAMES = [
    "Diallo", "Camara", "Bah", "Barry", "Sylla", "Kouyaté", "Baldé",
    "Sow", "Touré", "Condé", "Keita", "Traoré", "Konaté", "Coulibaly",
    "Diabaté", "Sidibé", "Sanogo", "Dembélé", "Koné", "Cissé",
]
GENDERS = ["male", "female"]

# Fraud scenario phone pools — shared across multiple beneficiaries
FRAUD_PHONE_POOL = [f"+224 6{i:02d} 00 00 00" for i in range(10)]
FRAUD_ACCOUNT_POOL = [f"GN{i:010d}" for i in range(10)]


# ── helpers ──────────────────────────────────────────────────

def _random_name() -> str:
    fn = RNG.choice(FIRST_NAMES)
    ln = RNG.choice(LAST_NAMES)
    return f"{fn} {ln}"


def _random_birthdate(age_min: int = 18, age_max: int = 70) -> str:
    days = int(RNG.integers(age_min * 365, age_max * 365))
    dob = datetime.now(timezone.utc) - timedelta(days=days)
    return dob.strftime("%Y-%m-%d")


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


# ── get existing program/cycle IDs ───────────────────────────

def _get_program_ids(conn) -> list[int]:
    rows = conn.execute(text("SELECT id FROM g2p_program LIMIT 5")).fetchall()
    return [r[0] for r in rows] if rows else []


def _get_or_create_cycle(conn, program_id: int) -> int | None:
    row = conn.execute(
        text("SELECT id FROM g2p_cycle WHERE program_id = :pid LIMIT 1"),
        {"pid": program_id},
    ).fetchone()
    return row[0] if row else None


def _get_admin_uid(conn) -> int:
    row = conn.execute(text("SELECT id FROM res_users WHERE login='admin' LIMIT 1")).fetchone()
    return row[0] if row else 1


def _get_main_company_id(conn) -> int:
    row = conn.execute(text("SELECT id FROM res_company ORDER BY id LIMIT 1")).fetchone()
    return row[0] if row else 1


# ── core insertion ────────────────────────────────────────────

def _insert_partner(conn, name: str, birthdate: str, gender: str,
                     income: float, admin_uid: int, company_id: int) -> int:
    """Insert a res_partner row and return its new id."""
    row = conn.execute(text("""
        INSERT INTO res_partner
            (name, birthdate, gender, income, active, is_company, is_registrant,
             company_id, create_uid, write_uid, create_date, write_date)
        VALUES
            (:name, :birthdate, :gender, :income, true, false, true,
             :company_id, :uid, :uid, NOW(), NOW())
        RETURNING id
    """), {
        "name": name, "birthdate": birthdate, "gender": str(gender),
        "income": float(income), "uid": admin_uid, "company_id": company_id,
    })
    return row.fetchone()[0]


def _insert_phone(conn, partner_id: int, phone: str) -> None:
    conn.execute(text("""
        INSERT INTO g2p_phone_number (partner_id, phone_no, phone_sanitized, date_collected)
        VALUES (:pid, :phone, :phone, NOW())
        ON CONFLICT DO NOTHING
    """), {"pid": partner_id, "phone": phone})


def _insert_bank(conn, partner_id: int, account: str) -> None:
    conn.execute(text("""
        INSERT INTO res_partner_bank (partner_id, acc_number, sanitized_acc_number, active,
                                      create_uid, write_uid, create_date, write_date)
        VALUES (:pid, :acc, :acc, true, 1, 1, NOW(), NOW())
        ON CONFLICT DO NOTHING
    """), {"pid": partner_id, "acc": account})


def _insert_membership(conn, partner_id: int, program_id: int,
                        n_extra_programs: int = 0,
                        all_program_ids: list[int] = None) -> None:
    """Enroll beneficiary in program_id plus optional extra programs."""
    conn.execute(text("""
        INSERT INTO g2p_program_membership
            (partner_id, program_id, state, enrollment_date,
             create_uid, write_uid, create_date, write_date)
        VALUES (:pid, :prog, 'enrolled', NOW() - INTERVAL '90 days',
                1, 1, NOW(), NOW())
        ON CONFLICT DO NOTHING
    """), {"pid": partner_id, "prog": program_id})

    if n_extra_programs and all_program_ids:
        extras = [p for p in all_program_ids if p != program_id][:n_extra_programs]
        for extra_pid in extras:
            conn.execute(text("""
                INSERT INTO g2p_program_membership
                    (partner_id, program_id, state, enrollment_date,
                     create_uid, write_uid, create_date, write_date)
                VALUES (:pid, :prog, 'enrolled', NOW() - INTERVAL '30 days',
                        1, 1, NOW(), NOW())
                ON CONFLICT DO NOTHING
            """), {"pid": partner_id, "prog": extra_pid})


def _insert_payment(conn, partner_id: int, cycle_id: int,
                     amount_issued: float, gap_ratio: float) -> None:
    amount_paid = round(amount_issued * (1 - gap_ratio), 2)
    import uuid
    ent_row = conn.execute(text("""
        INSERT INTO g2p_entitlement
            (partner_id, cycle_id, initial_amount, is_cash_entitlement, state, code,
             create_uid, write_uid, create_date, write_date)
        VALUES (:pid, :cid, :amt, true, 'approved', :code, 1, 1, NOW(), NOW())
        RETURNING id
    """), {"pid": partner_id, "cid": cycle_id, "amt": amount_issued, "code": str(uuid.uuid4())[:16]})
    ent_id = ent_row.fetchone()[0]

    conn.execute(text("""
        INSERT INTO g2p_payment
            (entitlement_id, amount_issued, amount_paid, payment_datetime, state, status,
             create_uid, write_uid, create_date, write_date)
        VALUES (:eid, :issued, :paid, NOW() - INTERVAL '15 days', 'posted', 'paid',
                1, 1, NOW(), NOW())
    """), {"eid": ent_id, "issued": amount_issued, "paid": amount_paid})


# ── scenario builders ─────────────────────────────────────────

def _inject_legit(conn, admin_uid: int, company_id: int, program_ids: list[int],
                  cycle_map: dict) -> int:
    name = _random_name()
    income = float(RNG.uniform(100, 800))
    pid = _insert_partner(conn, name, _random_birthdate(), RNG.choice(GENDERS), income, admin_uid, company_id)

    if program_ids:
        prog = int(RNG.choice(program_ids))
        _insert_membership(conn, pid, prog)
        cyc = cycle_map.get(prog)
        if cyc:
            _insert_payment(conn, pid, cyc, round(income * 0.3, 2), float(RNG.beta(1, 9)))

    phone = f"+224 6{RNG.integers(10,99):02d} {RNG.integers(10,99):02d} {RNG.integers(10,99):02d} {RNG.integers(10,99):02d}"
    _insert_phone(conn, pid, phone)
    return pid


def _inject_multi_enrollment(conn, admin_uid: int, company_id: int, program_ids: list[int],
                              cycle_map: dict) -> int:
    """Beneficiary enrolled in 4+ programs — triggers ME001."""
    name = _random_name()
    income = float(RNG.uniform(80, 300))
    pid = _insert_partner(conn, name, _random_birthdate(20, 50), RNG.choice(GENDERS), income, admin_uid, company_id)

    for prog in program_ids[:min(4, len(program_ids))]:
        _insert_membership(conn, pid, prog)
        cyc = cycle_map.get(prog)
        if cyc:
            _insert_payment(conn, pid, cyc, round(income * 0.3, 2), float(RNG.beta(1, 4)))

    phone = f"+224 6{RNG.integers(10,99):02d} {RNG.integers(10,99):02d} {RNG.integers(10,99):02d} {RNG.integers(10,99):02d}"
    _insert_phone(conn, pid, phone)
    logger.debug("MULTI-ENROLLMENT fraud injected: partner_id=%d, programs=%d", pid, min(4, len(program_ids)))
    return pid


def _inject_network_fraud(conn, admin_uid: int, company_id: int, program_ids: list[int],
                           cycle_map: dict, shared_phone: str, shared_account: str) -> int:
    """Beneficiary sharing phone + bank account — triggers NF001, NF002, NF003."""
    name = _random_name()
    income = float(RNG.uniform(50, 250))
    pid = _insert_partner(conn, name, _random_birthdate(18, 45), RNG.choice(GENDERS), income, admin_uid, company_id)

    if program_ids:
        prog = int(RNG.choice(program_ids))
        _insert_membership(conn, pid, prog)
        cyc = cycle_map.get(prog)
        if cyc:
            _insert_payment(conn, pid, cyc, round(income * 0.4, 2), float(RNG.beta(1, 3)))

    _insert_phone(conn, pid, shared_phone)
    _insert_bank(conn, pid, shared_account)
    logger.debug("NETWORK fraud injected: partner_id=%d", pid)
    return pid


def _inject_payment_anomaly(conn, admin_uid: int, company_id: int, program_ids: list[int],
                             cycle_map: dict) -> int:
    """High payment gap ratio — triggers PA001."""
    name = _random_name()
    income = float(RNG.uniform(100, 500))
    pid = _insert_partner(conn, name, _random_birthdate(25, 60), RNG.choice(GENDERS), income, admin_uid, company_id)

    if program_ids:
        prog = int(RNG.choice(program_ids))
        _insert_membership(conn, pid, prog)
        cyc = cycle_map.get(prog)
        if cyc:
            _insert_payment(conn, pid, cyc, round(income * 0.5, 2), float(RNG.uniform(0.55, 0.85)))

    phone = f"+224 6{RNG.integers(10,99):02d} {RNG.integers(10,99):02d} {RNG.integers(10,99):02d} {RNG.integers(10,99):02d}"
    _insert_phone(conn, pid, phone)
    logger.debug("PAYMENT ANOMALY fraud injected: partner_id=%d", pid)
    return pid


def _inject_socio_economic(conn, admin_uid: int, company_id: int, program_ids: list[int],
                            cycle_map: dict) -> int:
    """Very low income + many programs — triggers SE002, SE003."""
    name = _random_name()
    income = float(RNG.uniform(10, 60))   # abnormally low
    pid = _insert_partner(conn, name, _random_birthdate(30, 65), RNG.choice(GENDERS), income, admin_uid, company_id)

    for prog in program_ids[:min(3, len(program_ids))]:
        _insert_membership(conn, pid, prog)
        cyc = cycle_map.get(prog)
        if cyc:
            _insert_payment(conn, pid, cyc, round(income * 0.3, 2), float(RNG.beta(2, 5)))

    phone = f"+224 6{RNG.integers(10,99):02d} {RNG.integers(10,99):02d} {RNG.integers(10,99):02d} {RNG.integers(10,99):02d}"
    _insert_phone(conn, pid, phone)
    logger.debug("SOCIO-ECONOMIC fraud injected: partner_id=%d", pid)
    return pid


# ── main injection orchestrator ───────────────────────────────

def inject(n: int, fraud_rate: float, db_url: str) -> list[int]:
    engine = create_engine(db_url)

    with engine.begin() as conn:
        admin_uid = _get_admin_uid(conn)
        company_id = _get_main_company_id(conn)
        program_ids = _get_program_ids(conn)
        cycle_map = {pid: _get_or_create_cycle(conn, pid) for pid in program_ids}
        cycle_map = {k: v for k, v in cycle_map.items() if v is not None}

        if not program_ids:
            logger.warning(
                "No g2p_program found in OpenG2P. "
                "Create at least one program in Odoo first, then re-run this script."
            )

        n_fraud = int(n * fraud_rate)
        n_legit = n - n_fraud

        per_scenario = max(1, n_fraud // 4)
        counts = {
            "multi_enrollment": per_scenario,
            "network_fraud": per_scenario,
            "payment_anomaly": per_scenario,
            "socio_economic": n_fraud - 3 * per_scenario,
        }

        inserted_ids: list[int] = []

        logger.info("Inserting %d legitimate beneficiaries…", n_legit)
        for _ in range(n_legit):
            pid = _inject_legit(conn, admin_uid, company_id, program_ids, cycle_map)
            inserted_ids.append(pid)

        logger.info("Injecting %d fraud beneficiaries across 4 scenarios…", n_fraud)

        phone_idx = account_idx = 0
        for _ in range(counts["multi_enrollment"]):
            pid = _inject_multi_enrollment(conn, admin_uid, company_id, program_ids, cycle_map)
            inserted_ids.append(pid)

        for _ in range(counts["network_fraud"]):
            shared_phone = FRAUD_PHONE_POOL[phone_idx % len(FRAUD_PHONE_POOL)]
            shared_account = FRAUD_ACCOUNT_POOL[account_idx % len(FRAUD_ACCOUNT_POOL)]
            if _ % 3 == 2:
                phone_idx += 1
                account_idx += 1
            pid = _inject_network_fraud(conn, admin_uid, company_id, program_ids, cycle_map,
                                         shared_phone, shared_account)
            inserted_ids.append(pid)

        for _ in range(counts["payment_anomaly"]):
            pid = _inject_payment_anomaly(conn, admin_uid, company_id, program_ids, cycle_map)
            inserted_ids.append(pid)

        for _ in range(max(0, counts["socio_economic"])):
            pid = _inject_socio_economic(conn, admin_uid, company_id, program_ids, cycle_map)
            inserted_ids.append(pid)

    logger.info(
        "Injected %d beneficiaries (%d legit + %d fraud) into OpenG2P. "
        "IDs range: %d – %d",
        len(inserted_ids), n_legit, n_fraud,
        min(inserted_ids), max(inserted_ids),
    )
    return inserted_ids


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Inject synthetic beneficiaries into OpenG2P")
    p.add_argument("--n", type=int, default=100, help="Total beneficiaries to inject")
    p.add_argument("--fraud-rate", type=float, default=0.10, help="Fraction that are fraudulent")
    p.add_argument("--db-url", default=OPENG2P_DB_URL)
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    ids = inject(args.n, args.fraud_rate, args.db_url)
    print(f"\nDone. {len(ids)} beneficiaries inserted.")
    print(f"Go to http://localhost:8069/web#action=345&model=res.partner&view_type=list to see them.")
    print(f"Then score them at http://localhost:8501 → Score / Scan → Scan All.")
