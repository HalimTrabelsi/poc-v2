"""Score all newly-imported beneficiaries through the fraud-detection-engine.

After importing the demo CSV via the OpenG2P dashboard, run this script to:
  1. Find all res_partner registrants with ref starting 'DEMO-'
  2. POST each one to /api/v1/score/features
  3. Print a summary table of results

This produces fraud cases in fraud-db, which the Odoo addon's 1-min cron will
then pull into fraud.case for display in the Live Alert Monitor.

To speed up the demo, this script also triggers an immediate Odoo sync at the
end via the /fraud/sync_now endpoint (requires an Odoo session cookie — for
the demo, we just wait the 60s for the cron).
"""
import json
import time
import urllib.request
import urllib.error

API_BASE = "http://localhost:8002/api/v1"
API_KEY = "dev-secret-change-in-prod"
ODOO_DB = "openg2p"

import subprocess

def query_demo_partners():
    """Pull the imported demo beneficiaries straight from OpenG2P PostgreSQL."""
    sql = (
        "SELECT id, name, ref FROM res_partner "
        "WHERE is_registrant=true AND ref LIKE 'DEMO-%' "
        "ORDER BY id"
    )
    result = subprocess.run(
        ["docker", "exec", "openg2p-postgresql", "psql", "-U", "odoo",
         "-d", ODOO_DB, "-tAc", sql],
        capture_output=True, text=True, check=True,
    )
    rows = [line.strip() for line in result.stdout.strip().split("\n") if line.strip()]
    partners = []
    for row in rows:
        parts = row.split("|")
        if len(parts) >= 3:
            partners.append({"id": int(parts[0]), "name": parts[1], "ref": parts[2]})
    return partners


def score_one(beneficiary_id):
    # Use /score/beneficiary/{id} — it extracts features from the OpenG2P
    # database (phones, shared accounts, programs, etc.) rather than
    # accepting a pre-built dict. This is what the demo needs to see the
    # planted fraud patterns surface as actual risk signals.
    req = urllib.request.Request(
        f"{API_BASE}/score/beneficiary/{beneficiary_id}",
        method="POST",
        data=b"",
        headers={"X-API-Key": API_KEY, "Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        return {"error": f"HTTP {e.code}", "detail": e.read().decode("utf-8", errors="replace")[:200]}
    except urllib.error.URLError as e:
        return {"error": "URLError", "detail": str(e)}


def main():
    partners = query_demo_partners()
    if not partners:
        print("No DEMO-* beneficiaries found in res_partner.")
        print("Import the CSV first via Odoo dashboard → Registry → Individuals → Import.")
        return

    print(f"Scoring {len(partners)} imported beneficiaries...\n")
    print(f"{'Ref':<10} {'ID':<6} {'Name':<28} {'Score':<8} {'Risk':<10} {'Recommendation':<18}")
    print("-" * 86)

    results = []
    for p in partners:
        result = score_one(p["id"])
        if "error" in result:
            print(f"{p['ref']:<10} {p['id']:<6} {p['name'][:27]:<28} ERROR — {result['error']}")
            continue
        score = result.get("final_score", 0)
        risk = result.get("risk_level", "?")
        rec = result.get("recommendation", "?")
        rules = result.get("rules_triggered", []) or []
        results.append({"ref": p["ref"], "name": p["name"], "score": score,
                        "risk": risk, "rec": rec, "n_rules": len(rules)})
        print(f"{p['ref']:<10} {p['id']:<6} {p['name'][:27]:<28} {score:<8.4f} {risk:<10} {rec:<18}")
        time.sleep(0.1)  # don't hammer the API

    print()
    print("Summary:")
    by_risk = {}
    for r in results:
        by_risk[r["risk"]] = by_risk.get(r["risk"], 0) + 1
    for risk in ("CRITICAL", "HIGH", "MEDIUM", "LOW"):
        if risk in by_risk:
            print(f"  {risk:<10} {by_risk[risk]:>3}")

    print()
    print("Fraud cases are now in fraud-db.")
    print("They will appear in the Odoo Live Alert Monitor within 60 seconds")
    print("(or restart the Odoo cron from Settings > Technical > Scheduled Actions).")


if __name__ == "__main__":
    main()
