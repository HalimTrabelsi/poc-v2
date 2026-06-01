"""Post-import setup for the demo workflow.

The Odoo CSV-import wizard creates res_partner rows, but it does not
populate the satellite tables that OpenG2P uses for fraud signals:

  • g2p_phone_number   — separate phone records used for shared-phone detection
  • res_partner_bank   — bank accounts used for shared-account detection
  • g2p_program_membership — program enrollment (without this, no rules fire)

This script reads the DEMO-* partners just imported via Odoo, copies
their phone into g2p_phone_number, extracts the bank account from the
'comment' field into res_partner_bank, and enrols them in the Social
Aid Program (id=16).

Run this immediately after the dashboard import is complete.
"""
import subprocess
import sys

PROGRAM_ID = 16  # 'Social Aid Program'

def sql(query: str) -> str:
    """Run a SQL statement via docker exec, return stdout."""
    result = subprocess.run(
        ["docker", "exec", "openg2p-postgresql", "psql", "-U", "odoo",
         "-d", "openg2p", "-tAc", query],
        capture_output=True, text=True, check=True,
    )
    return result.stdout.strip()


def main():
    # Pull all DEMO-* partners just imported
    rows = sql("""
        SELECT id, name, phone, comment, ref
        FROM res_partner
        WHERE is_registrant=true AND ref LIKE 'DEMO-%'
        ORDER BY id
    """)
    if not rows:
        print("No DEMO-* beneficiaries found. Import the CSV via Odoo first.")
        print("  Odoo → Registry → Individuals → Actions → Import records")
        sys.exit(1)

    partners = []
    for row in rows.split("\n"):
        parts = row.split("|")
        if len(parts) >= 4:
            partners.append({
                "id": int(parts[0]),
                "name": parts[1],
                "phone": parts[2],
                "comment": parts[3],
                "ref": parts[4] if len(parts) > 4 else "",
            })

    print(f"Found {len(partners)} DEMO-* beneficiaries.")

    phone_count = 0
    bank_count = 0
    enroll_count = 0
    for p in partners:
        pid = p["id"]
        phone = p["phone"]

        # 1) g2p_phone_number record (skip if already present)
        exists = sql(f"SELECT 1 FROM g2p_phone_number WHERE partner_id={pid} LIMIT 1")
        if not exists and phone:
            sql(f"""
                INSERT INTO g2p_phone_number
                    (partner_id, phone_no, phone_sanitized, date_collected, create_date)
                VALUES ({pid}, '{phone}', '{phone}', CURRENT_DATE, NOW())
            """)
            phone_count += 1

        # 2) Bank account from comment field (format: '...account=NNNNN...')
        account = ""
        for chunk in (p["comment"] or "").split(";"):
            chunk = chunk.strip()
            if chunk.startswith("account="):
                account = chunk.split("=", 1)[1].strip()
                break
        if account:
            exists = sql(f"SELECT 1 FROM res_partner_bank WHERE partner_id={pid} LIMIT 1")
            if not exists:
                sql(f"""
                    INSERT INTO res_partner_bank
                        (partner_id, acc_number, sanitized_acc_number, active, create_date)
                    VALUES ({pid}, '{account}', '{account}', true, NOW())
                """)
                bank_count += 1

        # 3) Program enrolment
        exists = sql(
            f"SELECT 1 FROM g2p_program_membership "
            f"WHERE partner_id={pid} AND program_id={PROGRAM_ID} LIMIT 1"
        )
        if not exists:
            sql(f"""
                INSERT INTO g2p_program_membership
                    (partner_id, program_id, state, enrollment_date, create_date)
                VALUES ({pid}, {PROGRAM_ID}, 'enrolled', NOW(), NOW())
            """)
            enroll_count += 1

    print(f"  Phone records added:  {phone_count}")
    print(f"  Bank accounts added:  {bank_count}")
    print(f"  Enrolled in program:  {enroll_count}")
    print()
    print("Next step: run fraud scoring")
    print("  python fraud-detection-engine/scripts/score_imported_beneficiaries.py")


if __name__ == "__main__":
    main()
