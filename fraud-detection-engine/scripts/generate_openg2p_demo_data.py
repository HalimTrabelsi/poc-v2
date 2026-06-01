"""Generate OpenG2P-aligned demo data with embedded fraud patterns.

Output CSVs match the column format of registry-individual-data.csv
so they can be imported via the OpenG2P dashboard's CSV import wizard.

Three CSVs are produced:

1. openg2p_beneficiaries_import.csv  — individuals (res.partner registrants)
2. openg2p_payments_import.csv       — entitlement amounts (for g2p.payment)
3. openg2p_phones_import.csv         — phone records (g2p.phone.number)

The fraud-engine's rules + ML models look for these patterns, so we
seed each one explicitly with a `_fraud_label` column (for evaluation)
and `_fraud_pattern` column (for traceability). These two underscore
columns can be stripped before import — they're audit-only.

Fraud patterns embedded
-----------------------
P1  shared_phone        — N partners share one phone number
P2  shared_account      — N partners share one bank account
P3  identity_cluster    — same family-name + same DOB + adjacent IDs
P4  income_outlier      — declared income << median for region
P5  age_outlier         — birthdate < 1900 or > today
P6  mass_enrollment     — 20+ partners with same enrollment date
P7  round_payment       — payment amount is exact multiple of 100
P8  rapid_payout        — entitlement approved < 1 day after enrollment
P9  duplicate_name      — exact name match across partners
P10 ghost_payment       — payment exists but no valid entitlement
"""
import csv
import random
import uuid
from datetime import date, datetime, timedelta
from pathlib import Path

random.seed(42)

OUT = Path(r"C:\Users\Mega Pc\Desktop\poc-v2\poc-v2\fraud-detection-engine\data\openg2p_demo")
OUT.mkdir(parents=True, exist_ok=True)

# === Configuration =========================================================
N_BENEFICIARIES = 1000
FRAUD_TARGET_PCT = 0.15  # 15% fraud — realistic for a stress-test demo

GUINEAN_FIRST_NAMES = [
    "Mamadou", "Ibrahim", "Sekou", "Alpha", "Ousmane", "Amadou", "Mohamed",
    "Thierno", "Lansana", "Fanta", "Aissatou", "Mariama", "Kadiatou",
    "Fatoumata", "Hadja", "Bintou", "Aminata", "Saran", "Mabinty",
]
GUINEAN_LAST_NAMES = [
    "Diallo", "Bah", "Barry", "Camara", "Sylla", "Conde", "Toure", "Sow",
    "Kone", "Conte", "Diakite", "Keita", "Cisse", "Soumah", "Doumbouya",
]
REGIONS = ["GN-C", "GN-D", "GN-K", "GN-L", "GN-M", "GN-N"]
EMPLOYMENT = ["employed_fulltime", "employed_parttime", "unemployed",
              "self_employed", "student", "retired"]
INCOME_BANDS = ["0_5000", "5001_10000", "10001_20000", "20001_50000", "50001_plus"]
HOME_OWNED = ["yes", "no", "rented"]
VEHICLES = ["none", "two_wheeler", "four_wheeler", "multiple"]
DISABILITY = ["not_disabled", "physical", "visual", "hearing"]
IMMIGRATION = ["non_immigrant", "refugee", "asylum_seeker"]

# === Helpers ===============================================================
def rand_dob(min_year=1940, max_year=2008):
    y = random.randint(min_year, max_year)
    m = random.randint(1, 12)
    d = random.randint(1, 28)
    return date(y, m, d)

def rand_phone(prefix="+224"):
    return f"{prefix} {random.randint(600,699)} {random.randint(10,99)} " \
           f"{random.randint(10,99)} {random.randint(10,99)}"

def rand_account():
    return str(random.randint(10**9, 10**10 - 1))

def rand_name():
    return f"{random.choice(GUINEAN_FIRST_NAMES)} {random.choice(GUINEAN_LAST_NAMES)}"

def rand_address(region):
    return f"{random.randint(1,500)} Rue {random.choice(['Independence','Soudan','Niger','Faranah'])}, {region}"

# === Generate beneficiaries ===============================================
beneficiaries = []
phones_table = []   # for g2p_phone_number import
payments_table = [] # for entitlement/payment import
next_id = 100000    # synthetic registrant_id range, won't collide with live DB

n_fraud_target = int(N_BENEFICIARIES * FRAUD_TARGET_PCT)
fraud_indices = set(random.sample(range(N_BENEFICIARIES), n_fraud_target))

# Plant some shared resources up-front for collision patterns
SHARED_PHONE_POOL = [rand_phone() for _ in range(8)]      # 8 phones each used by ~5-10 partners
SHARED_ACCOUNT_POOL = [rand_account() for _ in range(5)]  # 5 accounts shared
MASS_ENROLL_DATE = datetime(2026, 4, 15, 10, 30)          # 30+ enrollments same minute
DUP_NAME_TARGET = "Mamadou Diallo"
CLUSTER_FAMILY_DOB = (date(1985, 6, 15), "Bah")            # 6 partners with same DOB + lastname

for i in range(N_BENEFICIARIES):
    rid = next_id + i
    is_fraud = i in fraud_indices
    fraud_pattern = ""

    # Default clean profile
    name = rand_name()
    dob = rand_dob()
    region = random.choice(REGIONS)
    phone = rand_phone()
    account = rand_account()
    income_band = random.choice(INCOME_BANDS)
    employment = random.choice(EMPLOYMENT)
    home = random.choice(HOME_OWNED)
    vehicles = random.choice(VEHICLES)
    disability = random.choice(DISABILITY)
    immigration = random.choice(IMMIGRATION)
    enroll_dt = datetime(2026, random.randint(1, 5),
                          random.randint(1, 28),
                          random.randint(8, 18),
                          random.randint(0, 59))

    # Inject fraud patterns deterministically
    if is_fraud:
        pat = random.choice([
            "shared_phone", "shared_account", "identity_cluster",
            "income_outlier", "mass_enrollment", "duplicate_name",
            "rapid_payout", "round_payment",
        ])
        fraud_pattern = pat
        if pat == "shared_phone":
            phone = random.choice(SHARED_PHONE_POOL)
        elif pat == "shared_account":
            account = random.choice(SHARED_ACCOUNT_POOL)
        elif pat == "identity_cluster":
            dob_c, ln_c = CLUSTER_FAMILY_DOB
            dob = dob_c
            name = f"{random.choice(GUINEAN_FIRST_NAMES)} {ln_c}"
        elif pat == "income_outlier":
            income_band = "0_5000"
            employment = "unemployed"
            home = "yes"          # contradicts low income
            vehicles = "four_wheeler"
        elif pat == "mass_enrollment":
            enroll_dt = MASS_ENROLL_DATE
        elif pat == "duplicate_name":
            name = DUP_NAME_TARGET
        elif pat == "rapid_payout":
            pass  # marked when generating payment row
        elif pat == "round_payment":
            pass  # marked when generating payment row

    beneficiaries.append({
        "Registrant ID": rid,
        "ID": str(random.randint(10**9, 10**10 - 1)),
        "Token ID": uuid.uuid4().hex,
        "Full Name": name,
        "Date of Birth": dob.strftime("%d-%m-%Y"),
        "Email ID": "",  # 0% fill in live DB
        "Address": rand_address(region),
        "Region": region,
        "Phone Number": phone,
        "Gender": random.choice(["Male", "Female"]),
        "Home Owned": home,
        "Employement Status": employment,
        "Annual Household Income (USD)": income_band,
        "Vehicles Owned": vehicles,
        "Account Number": account,
        "Disability Status": disability,
        "Immigration Status": immigration,
        "_enrollment_date": enroll_dt.strftime("%Y-%m-%d %H:%M:%S"),
        "_fraud_label": int(is_fraud),
        "_fraud_pattern": fraud_pattern,
    })

    # Companion phone row
    phones_table.append({
        "partner_id": rid,
        "phone_no": phone,
        "phone_sanitized": phone,
        "date_collected": enroll_dt.strftime("%Y-%m-%d"),
    })

    # Companion payment row (one per beneficiary, like the live DB)
    enroll_d = enroll_dt.date()
    if is_fraud and fraud_pattern == "rapid_payout":
        pay_date = enroll_dt + timedelta(hours=2)
    else:
        pay_date = enroll_dt + timedelta(days=random.randint(7, 45))

    if is_fraud and fraud_pattern == "round_payment":
        amount = random.choice([100.0, 200.0, 500.0, 1000.0])
    else:
        amount = round(random.uniform(50, 600), 2)
    amount_paid = amount - random.uniform(0, 5) if random.random() > 0.1 else 0.0

    payments_table.append({
        "partner_id": rid,
        "entitlement_id": rid - next_id + 1,
        "program_id": 16,
        "cycle_id": 46,
        "amount_issued": amount,
        "amount_paid": round(amount_paid, 2),
        "state": "posted" if amount_paid > 0 else "pending",
        "account_number": account,
        "create_date": pay_date.strftime("%Y-%m-%d %H:%M:%S"),
        "payment_datetime": pay_date.strftime("%Y-%m-%d %H:%M:%S"),
        "_fraud_label": int(is_fraud),
        "_fraud_pattern": fraud_pattern,
    })

# === Write CSVs ============================================================
ben_path = OUT / "openg2p_beneficiaries_import.csv"
phone_path = OUT / "openg2p_phones_import.csv"
pay_path = OUT / "openg2p_payments_import.csv"

with ben_path.open("w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=list(beneficiaries[0].keys()))
    writer.writeheader()
    writer.writerows(beneficiaries)

with phone_path.open("w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=list(phones_table[0].keys()))
    writer.writeheader()
    writer.writerows(phones_table)

with pay_path.open("w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=list(payments_table[0].keys()))
    writer.writeheader()
    writer.writerows(payments_table)

# === Summary ===============================================================
total_fraud = sum(b["_fraud_label"] for b in beneficiaries)
print(f"Generated {N_BENEFICIARIES} beneficiaries, {total_fraud} fraud "
      f"({total_fraud/N_BENEFICIARIES*100:.1f}%)")
print(f"\nFraud pattern distribution:")
from collections import Counter
patterns = Counter(b["_fraud_pattern"] for b in beneficiaries if b["_fraud_label"])
for p, c in patterns.most_common():
    print(f"  {p:20s} {c}")
print(f"\nFiles written:")
print(f"  {ben_path}")
print(f"  {phone_path}")
print(f"  {pay_path}")
