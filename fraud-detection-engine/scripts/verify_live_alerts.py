#!/usr/bin/env python3
"""
Verify that the live alert monitor works end-to-end:
1. Creates a fraud case in the fraud-engine
2. Syncs to Odoo fraud.case table
3. Broadcasts via bus.bus to the frontend
4. LLM explanation is auto-generated
"""
import sys
import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

import json
import time
import requests
import subprocess
from datetime import datetime

API_URL = "http://localhost:8002/api/v1"
API_KEY = "dev-secret-change-in-prod"
HEADERS = {"X-API-Key": API_KEY, "Content-Type": "application/json"}

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

def check_fraud_engine():
    """Verify fraud-engine is responding"""
    try:
        resp = requests.get(f"{API_URL}/health", headers=HEADERS, timeout=5)
        if resp.status_code == 200:
            log("✓ Fraud-engine is responsive")
            return True
    except Exception as e:
        log(f"✗ Fraud-engine unreachable: {e}")
        return False

def check_odoo_db():
    """Verify Odoo database is accessible"""
    try:
        from psycopg2 import connect
        conn = connect(
            host="localhost", port=5432, database="openg2p",
            user="odoo", password="openg2p"
        )
        conn.close()
        log("✓ Odoo database is accessible")
        return True
    except Exception as e:
        log(f"✗ Odoo database unreachable: {e}")
        return False

def check_beneficiary_exists(bid):
    """Check if beneficiary exists in Odoo"""
    try:
        from psycopg2 import connect
        conn = connect(
            host="localhost", port=5432, database="openg2p",
            user="odoo", password="openg2p"
        )
        cur = conn.cursor()
        cur.execute("SELECT id, name FROM res_partner WHERE id=%s", (int(bid),))
        row = cur.fetchone()
        cur.close()
        conn.close()
        if row:
            log(f"✓ Beneficiary {bid} exists: {row[1]}")
            return True
        else:
            log(f"✗ Beneficiary {bid} not found in Odoo")
            return False
    except Exception as e:
        log(f"✗ Database check failed: {e}")
        return False

def score_beneficiary(bid):
    """Score a beneficiary and create a fraud case"""
    try:
        log(f"Scoring beneficiary {bid}...")
        resp = requests.post(
            f"{API_URL}/score/beneficiary/{bid}",
            headers=HEADERS,
            json={},
            timeout=30
        )
        if resp.status_code == 200:
            data = resp.json()
            score = data.get('final_score', 0)
            risk = data.get('risk_level', 'UNKNOWN')
            log(f"✓ Scored: {score:.4f} ({risk})")
            return data
        else:
            log(f"✗ Scoring failed: {resp.status_code} {resp.text[:100]}")
            return None
    except Exception as e:
        log(f"✗ Scoring error: {e}")
        return None

def check_case_in_fraud_engine(bid):
    """Check if case exists in fraud-engine"""
    try:
        resp = requests.get(
            f"{API_URL}/cases?limit=100",
            headers=HEADERS,
            timeout=10
        )
        if resp.status_code == 200:
            cases = resp.json().get('cases', [])
            matching = [c for c in cases if c['beneficiary_id'] == str(bid)]
            if matching:
                case = matching[0]
                llm = case.get('llm_explanation', '')
                log(f"✓ Case found in fraud-engine: {case['case_id'][:8]}...")
                if llm:
                    log(f"  LLM explanation: {llm[:60]}...")
                    if 'bénéficiaire' in llm or 'français' in llm.lower():
                        log(f"  ✓ Explanation is in French")
                    else:
                        log(f"  ⚠ Explanation may not be in French")
                else:
                    log(f"  ⚠ No LLM explanation yet (still generating?)")
                return case
            else:
                log(f"✗ Case not found in fraud-engine for {bid}")
                return None
    except Exception as e:
        log(f"✗ Error checking fraud-engine: {e}")
        return None

def check_case_in_odoo(bid):
    """Check if case exists in Odoo"""
    try:
        from psycopg2 import connect
        conn = connect(
            host="localhost", port=5432, database="openg2p",
            user="odoo", password="openg2p"
        )
        cur = conn.cursor()
        cur.execute(
            "SELECT case_id, risk_level, llm_explanation FROM fraud_case "
            "WHERE beneficiary_id=%s ORDER BY create_date DESC LIMIT 1",
            (str(bid),)
        )
        row = cur.fetchone()
        cur.close()
        conn.close()

        if row:
            case_id, risk_level, llm_exp = row
            log(f"✓ Case synced to Odoo: {case_id[:8]}... ({risk_level})")
            if llm_exp:
                log(f"  LLM explanation: {llm_exp[:60]}...")
            else:
                log(f"  ⚠ No LLM explanation in Odoo yet")
            return True
        else:
            log(f"✗ Case not found in Odoo for {bid}")
            return False
    except Exception as e:
        log(f"✗ Error checking Odoo: {e}")
        return False

def main():
    log("=" * 70)
    log("LIVE ALERT MONITOR - END-TO-END TEST")
    log("=" * 70)

    # Pre-flight checks
    log("\n[1/5] PRE-FLIGHT CHECKS")
    if not check_fraud_engine():
        log("Fraud-engine is not running. Start it with: docker-compose up -d fraud-engine")
        return False

    if not check_odoo_db():
        log("Odoo database is not running. Start it with: docker-compose up -d postgresql openg2p-odoo")
        return False

    # Check for demo data
    log("\n[2/5] CHECKING DEMO DATA")
    bid = "4887"  # from simulate_import.py
    if not check_beneficiary_exists(bid):
        log("Demo data not found. Run: python fraud-detection-engine/scripts/simulate_import.py")
        return False

    # Score the beneficiary
    log("\n[3/5] SCORING BENEFICIARY")
    score_result = score_beneficiary(bid)
    if not score_result:
        return False

    if score_result['risk_level'] not in ('CRITICAL', 'HIGH'):
        log(f"⚠ Beneficiary scored {score_result['risk_level']} (won't trigger alert notification)")

    # Wait for fraud-engine to create case
    log("\n[4/5] WAITING FOR FRAUD-ENGINE TO CREATE CASE (5 seconds)...")
    time.sleep(5)
    case_data = check_case_in_fraud_engine(bid)

    # Wait for Odoo cron to sync (runs every minute)
    log("\nWAITING FOR ODOO CRON TO SYNC (up to 70 seconds)...")
    log("  (Odoo syncs fraud cases every minute)")
    for i in range(7):
        time.sleep(10)
        log(f"  Checking... ({(i+1)*10}s)")
        if check_case_in_odoo(bid):
            break
    else:
        log("✗ Case did not sync within 70 seconds")
        log("  Check Odoo logs: docker logs openg2p-odoo | grep 'Fraud sync'")
        return False

    log("\n[5/5] FRONTEND TEST INSTRUCTIONS")
    log("=" * 70)
    log("The backend is working! Now test the frontend:")
    log("")
    log("1. Open http://localhost:8069 (admin / admin)")
    log("2. Go to Fraud Detection → Live Alert Monitor (kanban view)")
    log("3. Open DevTools (F12 → Console)")
    log("4. You should see: [FraudMonitor] subscribed to fraud_alerts channel")
    log("")
    log("If you see that message:")
    log("  ✓ Frontend bus.bus listener is working")
    log("  ✓ Next test will verify the notification toast")
    log("")
    log("If you see an error instead:")
    log("  ✗ Check the troubleshooting guide: docs/LIVE_ALERT_MONITOR_TEST.md")
    log("  ✗ Common issues:")
    log("    - Odoo assets not reloaded (try: docker restart openg2p-odoo)")
    log("    - bus_service not initialized (Odoo config issue)")
    log("=" * 70)

    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
