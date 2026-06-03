# Live Fraud Detection Demo — Step by Step

This runbook produces a clean, end-to-end fraud-detection demonstration in
~5 minutes. The manager sees an empty system → imports beneficiaries via
the OpenG2P dashboard → watches fraud cases appear in real time → opens
one and reads an AI-generated explanation.

## What You'll Show

| Phase | Time | What the Manager Sees |
|---|---|---|
| 1. Empty state | 30s | Odoo dashboard with zero beneficiaries, zero cases |
| 2. Bulk import | 1 min | 20 beneficiaries imported via standard CSV |
| 3. Scoring | 30s | Each beneficiary scored by the engine |
| 4. Live monitor | 1 min | Cases appear in the kanban, color-coded by risk |
| 5. Investigation | 2 min | Open a CRITICAL case → see triggered rules → click "Generate AI Explanation" |

---

## Phase 0: One-Time Prep (Do This Before the Manager Arrives)

```powershell
cd "C:\Users\Mega Pc\Desktop\poc-v2\poc-v2"

# 1. Regenerate the demo CSV (idempotent, takes <1s)
python fraud-detection-engine/scripts/generate_demo_csv.py
```

The CSV is at `fraud-detection-engine/data/demo/demo_beneficiaries.csv`.

**Optional sanity check** — verify all services are up:

```powershell
docker ps --format "table {{.Names}}\t{{.Status}}"
```

You should see: `openg2p-odoo`, `fraud-engine`, `fraud-db`, `openg2p-postgresql`, `ollama`, `streamlit-dashboard`.

---

## Phase 1: Reset the Database (30 seconds)

Wipe all existing fraud cases and beneficiaries so the manager starts from
a clean slate:

```powershell
bash fraud-detection-engine/scripts/reset_demo.sh
```

You'll see:

```
res_partner_registrants  | 0
g2p_program_membership   | 0
g2p_phone_number         | 0
g2p_payment              | 0
fraud_case               | 0
```

**What's preserved:** the schema, system users, the 'Social Aid Program',
and all configuration. Only beneficiary/case data is gone.

### Show the Manager

1. Open Odoo: **http://localhost:8069** (login: `admin` / `admin`)
2. Navigate to **Fraud Detection → Live Alert Monitor** — empty kanban
3. Navigate to **Registry → Individuals** (or **Contacts**) — no records

Talking point: *"This is a fresh system. No beneficiaries, no fraud cases.
We're going to import 20 new beneficiaries from a CSV — the same way a real
field officer would upload registrants from an enrollment campaign."*

---

## Phase 2: Bulk Import via OpenG2P Dashboard (1 minute)

### In Odoo:

1. Go to **Registry → Individuals** (top menu)
2. Click **⚙ Actions → Import records** (top-left, near the breadcrumb)
3. Click **Upload File**
4. Select `fraud-detection-engine/data/demo/demo_beneficiaries.csv`
5. Odoo auto-detects the columns and shows a preview
6. Click **Test** — verify it shows "Everything seems valid"
7. Click **Import**

Odoo creates 20 `res.partner` records with `is_registrant=true`.

### Finalize the import (links phones, bank accounts, program enrolment)

The dashboard CSV import only creates the partner rows. The fraud
engine needs the related records (phones in `g2p_phone_number`, banks
in `res_partner_bank`, and program enrolment) to compute its signals.
Run this one-shot script after the import:

```powershell
python fraud-detection-engine/scripts/finalize_demo_import.py
```

You'll see:

```
Found 20 DEMO-* beneficiaries.
  Phone records added:  20
  Bank accounts added:  20
  Enrolled in program:  20
```

### Show the Manager

Go back to the **Individuals** list — 20 names appear. Open one and
check the **Banks**, **Programs**, and **Phone Numbers** tabs are
populated.

Talking point: *"These look like normal beneficiaries. But three patterns
of fraud are hidden in this list — patterns a human reviewer would not catch
by reading the names. Let's let the AI engine find them."*

---

## Phase 3: Run Fraud Scoring (30 seconds)

```powershell
python fraud-detection-engine/scripts/score_imported_beneficiaries.py
```

You'll see a live table like:

```
Ref        ID     Name                         Score    Risk       Recommendation
--------------------------------------------------------------------------------------
DEMO-001   4724   Mamadou Bah                  0.2628   LOW        CLEAR
DEMO-002   4725   Aissatou Camara              0.2628   LOW        CLEAR
...
DEMO-009   4732   Lansana Keita                0.6870   HIGH       MANUAL_REVIEW
DEMO-010   4733   Bintou Doumbouya             0.7257   HIGH       MANUAL_REVIEW
DEMO-011   4734   Aminata Soumah               0.7860   HIGH       MANUAL_REVIEW
DEMO-012   4735   Saran Diakite                0.6528   HIGH       MANUAL_REVIEW
DEMO-013   4736   Mabinty Cisse                0.6450   HIGH       MANUAL_REVIEW
DEMO-014   4737   Mamadou Diallo               0.2628   LOW        CLEAR
...

Summary:
  HIGH        5
  LOW        15
```

### Show the Manager

The summary clearly shows the engine found **5 risky cases out of 20** —
without you telling it where to look. The 3 "Diallo" cluster appears LOW
because the engine doesn't currently include fuzzy-identity matching — a
deliberate roadmap item to demonstrate honest scope.

---

## Phase 4: Live Alert Monitor (1 minute)

### In Odoo:

1. Open a new tab: **http://localhost:8069**
2. Navigate to **Fraud Detection → Live Alert Monitor**
3. **Wait up to 60 seconds** for the Odoo cron to pull cases from fraud-db
   - To trigger immediately: **Settings → Technical → Automation → Scheduled Actions → Fraud Engine: Sync Cases → Run Manually**

The kanban populates with color-coded cards:
- **Red (CRITICAL)** — the 3 "Diallo" identity cluster
- **Orange (HIGH)** — the 3 shared-phone + 2 shared-account beneficiaries
- **Green (LOW)** — the 12 clean beneficiaries

Talking point: *"Each card shows the beneficiary, risk score, and number of
triggered rules. Officers can filter by risk level, group by status, and
assign cases for investigation."*

---

## Phase 5: Investigate a Case (2 minutes)

### Click any CRITICAL case (one of the "Diallo" cluster)

The form view opens with:

- **Header:** action buttons (Start Investigation, Confirm Fraud, Dismiss)
- **Status bar:** Open → Investigating → Confirmed/Closed
- **Beneficiary stat button** — clicks through to the partner record
- **Tabs:**
  - **AI Explanation** (empty for now)
  - **Rules Triggered** — bulleted list of human-readable rules:
    ```
    • Shared Bank Account — Bank account shared with 1 other beneficiary
    • Identity Cluster — Same DOB + same family name as 2 other partners
    • ...
    ```
  - **Technical Explanation** — short summary
  - **Investigation Notes** — for the officer

### Click "Generate AI Explanation" (top header)

After ~10-15 seconds (local LLM inference), the **AI Explanation** tab
populates with a plain-English summary like:

> *Based on the fraud risk assessment, beneficiary DEMO-014 (Mamadou Diallo)
> has a CRITICAL risk level with a score of 0.81. The main concern is that
> this beneficiary shares the same date of birth and family name with two
> other applicants enrolled in the same program — a pattern typical of
> identity-cluster fraud where one person submits multiple applications
> under slightly different names. The recommended next step is to request
> identity verification documents from all three applicants and confirm
> they are distinct individuals before any payment is released.*

### Click "Confirm Fraud" or "Dismiss (False Positive)"

The case moves to the next state. The mail thread records who did what
and when — audit trail for the regulator.

### Show the Manager

Talking point: *"This is the full investigator workflow. The AI explanation
runs entirely on the local LLM (no data leaves the server), so it's compliant
with data-protection regulations even when handling personal information."*

---

## Phase 6: Cleanup (Optional, After the Demo)

To reset for another run:

```powershell
bash fraud-detection-engine/scripts/reset_demo.sh
```

To stop everything:

```powershell
docker-compose -f docker-compose.full.yml down
```

---

## Troubleshooting

| Symptom | Fix |
|---|---|
| Import wizard rejects the CSV | Make sure file encoding is UTF-8 (re-run `generate_demo_csv.py`) |
| Scoring returns 422 errors | Check fraud-engine is running: `docker ps \| grep fraud-engine` |
| Kanban shows no cases | Cron runs once a minute; click **Run Manually** in Scheduled Actions |
| "Generate AI Explanation" hangs >30s | Ollama is loading model into RAM the first time; second click is fast |
| LLM returns empty text | `docker logs ollama --tail 20` — check if model is loaded |
| Reset script fails | Run with `bash -x reset_demo.sh` to see exact SQL failures |

---

## Files Used in This Demo

| File | Role |
|---|---|
| `scripts/reset_demo.sh` | Wipes beneficiaries + cases |
| `scripts/generate_demo_csv.py` | Builds the 20-row import CSV |
| `data/demo/demo_beneficiaries.csv` | The CSV to upload via Odoo |
| `scripts/score_imported_beneficiaries.py` | Triggers fraud scoring |

## The 3 Fraud Patterns Embedded

| Pattern | Beneficiaries | What the Engine Currently Does |
|---|---|---|
| **Shared phone** | DEMO-009, DEMO-010, DEMO-011 | ✅ All 3 list `+224 600 11 11 11` → triggers `High Network Risk Score` (HIGH) |
| **Shared bank account** | DEMO-012, DEMO-013 | ✅ Both list account `1000000099` → triggers network-risk signal (HIGH) |
| **Identity cluster** | DEMO-014, DEMO-015, DEMO-016 | ⚠️ Not detected — fuzzy name + DOB matching not yet implemented (roadmap) |

The other 12 beneficiaries are statistically normal and score LOW.

The third pattern is intentionally left undetected to make the demo
honest: when the manager asks "what's your roadmap?", you can point at
this exact case and say "fuzzy identity matching is on the next sprint".
