"""Simulate the Odoo CSV import directly via SQL — used to validate the
demo workflow end-to-end without needing manual dashboard clicks.

This mirrors what the Odoo Import wizard does when uploading the CSV:
creates res_partner rows, attaches phone numbers, joins the program, etc.

Only intended for testing the demo. The real demo uses the Odoo dashboard
import wizard so the manager sees the OpenG2P UX.
"""
import csv
import subprocess
from pathlib import Path

CSV = Path(r"C:\Users\Mega Pc\Desktop\poc-v2\poc-v2\fraud-detection-engine\data\demo\demo_beneficiaries.csv")
PROGRAM_ID = 16  # 'Social Aid Program'

def sql(query: str) -> str:
    return subprocess.run(
        ["docker", "exec", "openg2p-postgresql", "psql", "-U", "odoo",
         "-d", "openg2p", "-tAc", query],
        capture_output=True, text=True, check=True,
    ).stdout.strip()


with CSV.open(encoding="utf-8") as f:
    rows = list(csv.DictReader(f))

print(f"Importing {len(rows)} beneficiaries...")
ids = []
for r in rows:
    name = r["name"].replace("'", "''")
    dob = r["birthdate"]
    gender = r["gender"]
    phone = r["phone"]
    ref = r["ref"]
    comment = r["comment"].replace("'", "''")

    pid = sql(f"""
        INSERT INTO res_partner
            (name, birthdate, gender, phone, mobile,
             is_registrant, is_group, active, ref, comment, create_date, company_id)
        VALUES
            ('{name}', '{dob}', '{gender}', '{phone}', '{phone}',
             true, false, true, '{ref}', '{comment}', NOW(), 1)
        RETURNING id;
    """)
    pid = pid.split("\n")[0].strip()
    ids.append(int(pid))
    # Attach a phone record
    sql(f"""
        INSERT INTO g2p_phone_number (partner_id, phone_no, phone_sanitized, date_collected, create_date)
        VALUES ({pid}, '{phone}', '{phone}', CURRENT_DATE, NOW());
    """)
    # Extract account number from the comment field (CSV format)
    account = ""
    for chunk in r["comment"].split(";"):
        if "account=" in chunk:
            account = chunk.split("account=", 1)[1].strip()
            break
    if account:
        sql(f"""
            INSERT INTO res_partner_bank (partner_id, acc_number, sanitized_acc_number, active, create_date)
            VALUES ({pid}, '{account}', '{account}', true, NOW());
        """)
    # Join the program
    sql(f"""
        INSERT INTO g2p_program_membership
            (partner_id, program_id, state, enrollment_date, create_date)
        VALUES ({pid}, {PROGRAM_ID}, 'enrolled', NOW(), NOW());
    """)

print(f"Created partner IDs: {min(ids)} .. {max(ids)}")
print()
print("Verification:")
print(sql("""
    SELECT
      (SELECT COUNT(*) FROM res_partner WHERE ref LIKE 'DEMO-%') AS partners,
      (SELECT COUNT(*) FROM g2p_phone_number p JOIN res_partner rp ON p.partner_id = rp.id WHERE rp.ref LIKE 'DEMO-%') AS phones,
      (SELECT COUNT(*) FROM g2p_program_membership m JOIN res_partner rp ON m.partner_id = rp.id WHERE rp.ref LIKE 'DEMO-%') AS memberships
"""))
