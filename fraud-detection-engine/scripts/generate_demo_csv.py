"""Generate a demo CSV in native Odoo res.partner import format.

20 beneficiaries with 3 deliberate fraud patterns the demo will showcase:

  1. Shared phone (cluster of 3 beneficiaries share +224 600 11 11 11)
  2. Shared bank account (cluster of 2 share account 1000000099)
  3. Identity cluster (3 beneficiaries with same DOB + same last name)

The other 12 are clean. The fraud engine should produce:
  • ~8 CRITICAL/HIGH cases (the 5 colluding + 3 cluster members)
  • ~12 LOW cases (the clean ones)

Output format matches Odoo's standard res.partner import schema, so it
can be uploaded directly via Odoo dashboard → Contacts/Registrants → Import.
"""
import csv
from pathlib import Path

OUT = Path(r"C:\Users\Mega Pc\Desktop\poc-v2\poc-v2\fraud-detection-engine\data\demo")
OUT.mkdir(parents=True, exist_ok=True)

# === Shared resources for fraud patterns ===================================
SHARED_PHONE = "+224 600 11 11 11"      # 3 beneficiaries
SHARED_ACCOUNT = "1000000099"           # 2 beneficiaries
CLUSTER_DOB = "1985-06-15"              # 3 cluster members
CLUSTER_LASTNAME = "Diallo"

# === The 20 beneficiaries ==================================================
# Format: (name, dob, gender, phone, account, region, employment, fraud_pattern)
BENEFICIARIES = [
    # Clean baseline (8)
    ("Mamadou Bah",        "1972-03-14", "male",   "+224 622 31 45 67", "1000000001", "GN-C", "employed_fulltime", ""),
    ("Aissatou Camara",    "1980-11-02", "female", "+224 655 78 12 34", "1000000002", "GN-D", "employed_parttime", ""),
    ("Ibrahim Toure",      "1965-07-21", "male",   "+224 666 90 23 45", "1000000003", "GN-K", "self_employed",     ""),
    ("Fatoumata Sylla",    "1990-01-09", "female", "+224 611 22 34 56", "1000000004", "GN-L", "employed_fulltime", ""),
    ("Sekou Conde",        "1955-12-30", "male",   "+224 677 33 45 67", "1000000005", "GN-M", "retired",           ""),
    ("Mariama Barry",      "1988-04-17", "female", "+224 633 44 56 78", "1000000006", "GN-N", "unemployed",        ""),
    ("Alpha Sow",          "1975-09-25", "male",   "+224 688 55 67 89", "1000000007", "GN-C", "employed_fulltime", ""),
    ("Hadja Conte",        "1982-06-11", "female", "+224 644 66 78 90", "1000000008", "GN-D", "employed_parttime", ""),

    # Pattern 1: Shared phone (3 partners)
    ("Lansana Keita",      "1978-02-08", "male",   SHARED_PHONE,        "1000000010", "GN-C", "employed_fulltime", "shared_phone"),
    ("Bintou Doumbouya",   "1992-08-19", "female", SHARED_PHONE,        "1000000011", "GN-D", "employed_parttime", "shared_phone"),
    ("Aminata Soumah",     "1986-05-03", "female", SHARED_PHONE,        "1000000012", "GN-K", "unemployed",        "shared_phone"),

    # Pattern 2: Shared bank account (2 partners)
    ("Saran Diakite",      "1971-10-28", "female", "+224 699 77 89 01", SHARED_ACCOUNT, "GN-L", "self_employed",   "shared_account"),
    ("Mabinty Cisse",      "1983-12-14", "female", "+224 600 88 90 12", SHARED_ACCOUNT, "GN-M", "employed_fulltime", "shared_account"),

    # Pattern 3: Identity cluster (3 partners with same DOB + last name)
    ("Mamadou Diallo",     CLUSTER_DOB, "male",   "+224 611 11 21 31", "1000000020", "GN-C", "employed_fulltime", "identity_cluster"),
    ("Ibrahim Diallo",     CLUSTER_DOB, "male",   "+224 622 12 22 32", "1000000021", "GN-C", "employed_parttime", "identity_cluster"),
    ("Alpha Diallo",       CLUSTER_DOB, "male",   "+224 633 13 23 33", "1000000022", "GN-C", "unemployed",        "identity_cluster"),

    # More clean
    ("Thierno Kone",       "1968-04-22", "male",   "+224 644 14 24 34", "1000000030", "GN-N", "employed_fulltime", ""),
    ("Kadiatou Conte",     "1995-07-05", "female", "+224 655 15 25 35", "1000000031", "GN-D", "employed_parttime", ""),
    ("Ousmane Camara",     "1962-11-19", "male",   "+224 666 16 26 36", "1000000032", "GN-K", "retired",           ""),
    ("Fanta Bah",          "1989-09-08", "female", "+224 677 17 27 37", "1000000033", "GN-L", "self_employed",     ""),
]

# === Write the CSV in standard Odoo res.partner import columns =============
out_path = OUT / "demo_beneficiaries.csv"
with out_path.open("w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow([
        "name", "birthdate", "gender", "phone", "mobile",
        "is_registrant", "is_group", "active",
        "ref", "comment",  # ref = optional external ID, comment = note
    ])
    for i, (name, dob, gender, phone, account, region, employment, pattern) in enumerate(BENEFICIARIES):
        # comment captures the planted pattern for demo traceability — strip
        # this before showing to the manager if you want to keep the surprise
        comment = f"region={region}; employment={employment}; account={account}"
        if pattern:
            comment += f"; planted_pattern={pattern}"
        writer.writerow([
            name,
            dob,
            gender,
            phone,
            phone,                              # mobile = same as phone for demo
            "TRUE",
            "FALSE",
            "TRUE",
            f"DEMO-{i+1:03d}",                  # external ref
            comment,
        ])

print(f"Wrote {len(BENEFICIARIES)} beneficiaries to:")
print(f"  {out_path}")
print()
print("Fraud patterns embedded:")
print(f"  • 3 beneficiaries share phone {SHARED_PHONE}")
print(f"  • 2 beneficiaries share account {SHARED_ACCOUNT}")
print(f"  • 3 beneficiaries share DOB {CLUSTER_DOB} + last name '{CLUSTER_LASTNAME}'")
print(f"  • 12 clean beneficiaries")
print()
print("Expected fraud-engine output: ~8 HIGH/CRITICAL, ~12 LOW")
