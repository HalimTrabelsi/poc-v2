"""Create 4 obvious fraud cases in OpenG2P and score them immediately.

Run this to get a guaranteed working demo with visible results in the dashboard.

Usage:
    python scripts/demo_fraud_scenarios.py
"""
import sys
import json
import logging
from pathlib import Path

import requests
from sqlalchemy import create_engine, text

sys.path.insert(0, str(Path(__file__).parent.parent))

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

import os
OPENG2P_DB_URL = os.getenv("OPENG2P_DB_URL", "postgresql://odoo:openg2p@localhost:5432/openg2p")
API_BASE = os.getenv("FRAUD_API_BASE", "http://localhost:8000/api/v1")
HEADERS = {"X-API-Key": "dev-secret-change-in-prod", "Content-Type": "application/json"}


def get_setup(conn):
    # Get any active internal user (not public)
    row = conn.execute(text("""
        SELECT id FROM res_users
        WHERE active = true AND share = false
        ORDER BY id LIMIT 1
    """)).fetchone()
    uid = row[0] if row else 1

    cid = conn.execute(text("SELECT id FROM res_company ORDER BY id LIMIT 1")).fetchone()[0]
    programs = [r[0] for r in conn.execute(text("SELECT id FROM g2p_program LIMIT 6")).fetchall()]
    cycles = {}
    for pid in programs:
        row = conn.execute(text("SELECT id FROM g2p_cycle WHERE program_id=:p LIMIT 1"), {"p": pid}).fetchone()
        if row:
            cycles[pid] = row[0]

    logger.info("Setup: uid=%d, company=%d, programs=%s, cycles=%s", uid, cid, programs, cycles)
    return uid, cid, programs, cycles


def insert_partner(conn, name, birthdate, gender, income, uid, cid):
    row = conn.execute(text("""
        INSERT INTO res_partner (name, birthdate, gender, income, active, is_company,
            is_registrant, company_id, create_uid, write_uid, create_date, write_date)
        VALUES (:name, :bd, :gender, :income, true, false, true,
            :cid, :uid, :uid, NOW(), NOW())
        RETURNING id
    """), {"name": name, "bd": birthdate, "gender": gender, "income": float(income),
           "cid": cid, "uid": uid})
    return row.fetchone()[0]


def add_phone(conn, pid, phone):
    conn.execute(text("""
        INSERT INTO g2p_phone_number (partner_id, phone_no, phone_sanitized, date_collected)
        VALUES (:pid, :phone, :phone, NOW()) ON CONFLICT DO NOTHING
    """), {"pid": pid, "phone": phone})


def add_bank(conn, pid, account):
    conn.execute(text("""
        INSERT INTO res_partner_bank (partner_id, acc_number, sanitized_acc_number,
            active, create_uid, write_uid, create_date, write_date)
        VALUES (:pid, :acc, :acc, true, 1, 1, NOW(), NOW()) ON CONFLICT DO NOTHING
    """), {"pid": pid, "acc": account})


def enroll(conn, pid, prog_id, uid):
    conn.execute(text("""
        INSERT INTO g2p_program_membership (partner_id, program_id, state, enrollment_date,
            create_uid, write_uid, create_date, write_date)
        VALUES (:pid, :prog, 'enrolled', NOW()-INTERVAL '60 days', :uid, :uid, NOW(), NOW())
        ON CONFLICT DO NOTHING
    """), {"pid": pid, "prog": prog_id, "uid": uid})


def add_payment(conn, pid, cycle_id, amount_issued, amount_paid, uid):
    import uuid
    eid = conn.execute(text("""
        INSERT INTO g2p_entitlement (partner_id, cycle_id, initial_amount,
            is_cash_entitlement, state, code, create_uid, write_uid, create_date, write_date)
        VALUES (:pid, :cid, :amt, true, 'approved', :code, :uid, :uid, NOW(), NOW())
        RETURNING id
    """), {"pid": pid, "cid": cycle_id, "amt": float(amount_issued),
           "code": str(uuid.uuid4())[:16], "uid": uid}).fetchone()[0]
    conn.execute(text("""
        INSERT INTO g2p_payment (entitlement_id, amount_issued, amount_paid,
            payment_datetime, state, status, create_uid, write_uid, create_date, write_date)
        VALUES (:eid, :issued, :paid, NOW()-INTERVAL '10 days', 'posted', 'paid', :uid, :uid, NOW(), NOW())
    """), {"eid": eid, "issued": float(amount_issued), "paid": float(amount_paid), "uid": uid})


def score_beneficiary(partner_id):
    r = requests.post(f"{API_BASE}/score/beneficiary/{partner_id}", headers=HEADERS, timeout=30)
    if r.status_code == 200:
        return r.json()
    return {"error": r.text}


def print_result(label, partner_id, result):
    print(f"\n{'='*60}")
    print(f"  {label}")
    print(f"  Partner ID : {partner_id}")
    if "error" in result:
        print(f"  ERROR: {result['error']}")
        return
    score = result.get("final_score", 0)
    risk  = result.get("risk_level", "?")
    rec   = result.get("recommendation", "?")
    rules = result.get("rules_triggered", [])
    emoji = {"CRITICAL": "[CRITICAL]", "HIGH": "[HIGH]", "MEDIUM": "[MEDIUM]", "LOW": "[LOW]"}.get(risk, "[?]")
    print(f"  Score      : {score:.1%}")
    print(f"  Risk Level : {emoji} {risk}")
    print(f"  Action     : {rec}")
    if rules:
        print(f"  Rules fired:")
        for r in rules:
            print(f"    - [{r.get('rule_id')}] {r.get('name')} (weight={r.get('weight')})")
    print(f"  Explanation: {result.get('explanation','')[:120]}")
    print(f"{'='*60}")


def main():
    engine = create_engine(OPENG2P_DB_URL)
    injected = []

    with engine.begin() as conn:
        uid, cid, programs, cycles = get_setup(conn)

        if not programs:
            logger.error("No programs in OpenG2P. Create at least 1 program first.")
            sys.exit(1)

        # ── Scenario 1: MULTI-ENROLLMENT ────────────────────────
        # Enrolled in ALL available programs simultaneously
        logger.info("Creating Scenario 1: Multi-Enrollment fraud...")
        p1 = insert_partner(conn, "Moussa FRAUD-MultiEnroll", "1988-03-15", "male", 120.0, uid, cid)
        for prog in programs[:min(5, len(programs))]:
            enroll(conn, p1, prog, uid)
            if prog in cycles:
                add_payment(conn, p1, cycles[prog], 150.0, 148.0, uid)
        add_phone(conn, p1, "+224 601 00 00 01")
        injected.append((p1, "SCENARIO 1 — Multi-Enrollment (enrolled in all programs)"))

        # ── Scenario 2: NETWORK FRAUD ────────────────────────────
        # 3 people share the same phone AND the same bank account
        SHARED_PHONE   = "+224 666 SHARED 99"
        SHARED_ACCOUNT = "GN-SHARED-ACCOUNT-001"
        logger.info("Creating Scenario 2: Network fraud (3 people, shared phone+account)...")
        for i, name in enumerate(["Aminata FRAUD-Net1", "Ibrahim FRAUD-Net2", "Fatou FRAUD-Net3"]):
            px = insert_partner(conn, name, "1990-06-20", "female" if i != 1 else "male", 200.0, uid, cid)
            add_phone(conn, px, SHARED_PHONE)
            add_bank(conn, px, SHARED_ACCOUNT)
            if programs:
                enroll(conn, px, programs[0], uid)
                if programs[0] in cycles:
                    add_payment(conn, px, cycles[programs[0]], 200.0, 195.0, uid)
            if i == 0:
                injected.append((px, "SCENARIO 2 — Network Fraud (shared phone+bank with 2 others)"))

        # ── Scenario 3: PAYMENT ANOMALY ─────────────────────────
        # 80% of payments are never received
        logger.info("Creating Scenario 3: Payment anomaly (80% gap)...")
        p3 = insert_partner(conn, "Cheikh FRAUD-PayGap", "1975-11-02", "male", 450.0, uid, cid)
        if programs:
            enroll(conn, p3, programs[0], uid)
            if programs[0] in cycles:
                add_payment(conn, p3, cycles[programs[0]], 500.0, 100.0, uid)  # 80% gap
        add_phone(conn, p3, "+224 602 00 00 03")
        injected.append((p3, "SCENARIO 3 — Payment Anomaly (80% payment gap ratio)"))

        # ── Scenario 4: SOCIO-ECONOMIC MISMATCH ─────────────────
        # Income = 15 (almost zero) but enrolled in 3 programs
        logger.info("Creating Scenario 4: Socio-economic mismatch (very low income + multi-program)...")
        p4 = insert_partner(conn, "Hawa FRAUD-SocioEco", "1982-07-30", "female", 15.0, uid, cid)
        for prog in programs[:min(3, len(programs))]:
            enroll(conn, p4, prog, uid)
            if prog in cycles:
                add_payment(conn, p4, cycles[prog], 50.0, 49.0, uid)
        add_phone(conn, p4, "+224 603 00 00 04")
        injected.append((p4, "SCENARIO 4 — Socio-Economic Mismatch (income=15, enrolled in 3 programs)"))

    logger.info("Injected %d demo beneficiaries. Scoring them now...", len(injected))

    print("\n" + "="*60)
    print("  FRAUD DETECTION ENGINE — DEMO RESULTS")
    print("="*60)

    for partner_id, label in injected:
        result = score_beneficiary(partner_id)
        print_result(label, partner_id, result)

    print(f"\nDone. View all cases at http://localhost:8501 -> Cases tab")
    print(f"   Or explore via Swagger at http://localhost:8002/docs")


if __name__ == "__main__":
    main()
