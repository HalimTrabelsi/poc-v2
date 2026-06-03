# OpenG2P Beneficiary Portal & Capabilities — Research Summary

## Overview

OpenG2P provides **staff-facing portals** (for agents, supervisors, officers) but does **NOT include a native beneficiary self-service portal** in the current codebase.

---

## What EXISTS in OpenG2P

### 1. **Agent/Staff Portals** ✓ (Available)

#### a) Registration Portal (`g2p_registration_portal_base`)
- **Purpose**: Allows agents/staff to register individuals and households
- **URL**: `/portal/registration/`
- **Functions**:
  - Create new individual registrants
  - Update registrant details (name, DOB, gender, email)
  - Search and list individuals
  - View registration history
- **Who can access**: Registered agents/field officers (not beneficiaries)

#### b) Agent Portal Base (`g2p_agent_portal_base`)
- **Purpose**: Base portal framework for staff
- **Features**:
  - Dashboard for agents
  - Role-based access (field officer, supervisor, admin)
  - Portal authentication (`g2p_portal_auth`)

#### c) Reimbursement Portal (`g2p_reimbursement_portal`)
- **Purpose**: For reimbursement management
- **Likely functions**: Track disbursement requests, view payment status (staff view)

#### d) SPAR Beneficiary Portal API (`openg2p-spar-bene-portal-api`)
- **Purpose**: FastAPI service for identity-to-financial-address mapping
- **Functions**: Link ID to bank account, resolve identity, update FA info
- **NOT a beneficiary self-service portal** — it's a backend API used by payment systems
- **Endpoints**:
  - `POST /mapper/link` — link identity to financial address
  - `POST /mapper/resolve` — resolve financial address from identity
  - `POST /mapper/unlink` — unlink identity
  - `POST /mapper/update` — update financial info

---

## What is MISSING for Beneficiaries

### **NO Native Beneficiary Self-Service Portal**

OpenG2P does **NOT include a public-facing beneficiary portal** where individuals can:
- ❌ Log in with their ID/credentials
- ❌ View their enrollment status
- ❌ Check their payment/benefit status
- ❌ View payment history
- ❌ Update their profile
- ❌ File appeals or complaints
- ❌ Check fraud status (obviously, not relevant before our fraud detection addon)

---

## How Payments Are Handled (Current Architecture)

The SPAR Beneficiary Portal API is designed for **system-to-system payment integration**, not beneficiary access:

```
Beneficiary Registry ──→ SPAR Mapper ──→ DFSP (Bank)
      (ID)                (Link to FA)      (Payment)
      
FLOW:
1. Beneficiary is registered in OpenG2P with ID + demographics
2. System administrator maps the ID to a financial address (FA)
   - FA = "bank_account_12345" or "mobile_wallet_+224621234567"
3. When benefit is due, system calls SPAR API to resolve FA
4. Payment system (DFSP) receives FA and processes disbursement
5. Beneficiary receives payment (to their bank/mobile money)

Beneficiary involvement: ZERO — they don't use the portal
```

---

## What Beneficiaries CAN Access (Currently)

**Very Limited:**

1. **SMS Notifications** (if configured)
   - "You have been enrolled in program X"
   - "Your payment of $50 was processed on DATE"
   - "Your account has been flagged" (with our fraud detection addon)

2. **USSD (Unstructured Supplementary Service Data)** (if implemented)
   - Menu-based phone interface
   - Not part of standard OpenG2P — must be custom-built

3. **Mobile Money Wallet**
   - If payment goes to mobile money, beneficiary checks balance in their app
   - Not OpenG2P—beneficiary's mobile network provider's service

---

## What Documentation Says

### From OpenG2P Code

**g2p_registration_portal_base README:**
```
Refer to https://docs.openg2p.org
```

**Key finding:** Registration portal is for **agents registering beneficiaries**, not for beneficiaries self-serving.

### SPAR Bene Portal API Purpose

```python
# From openg2p-spar-bene-portal-api/main.py
"SPAR Beneficiary Portal API"
"Link ID to Financial Address for payment processing"
```

**Purpose**: Payment system integration, not beneficiary access.

---

## What We COULD Build (Options)

### Option A: Lightweight Beneficiary Dashboard (Fastest)
- Uses OpenG2P data (name, ID, enrollment status)
- Reads fraud status from our fraud_case table
- Simple login (ID + OTP)
- Shows:
  - Enrollment status
  - Last payment date/amount
  - **Fraud status** (from our fraud detection addon)
  - Contact support button
- **Build time**: 1-2 weeks
- **Where**: Odoo portal or separate FastAPI service

### Option B: Full Self-Service Portal (Comprehensive)
- Includes Option A + more
- Additional features:
  - Complete payment history
  - Appeal workflow for fraud cases
  - Document upload (appeal evidence)
  - Two-way messaging with support
  - Multi-language support
- **Build time**: 4-8 weeks
- **Where**: Separate frontend (React/Angular) + Odoo/FastAPI backend

### Option C: Use Existing SPAR Portal (Not Recommended)
- The SPAR API exists but was designed for payment systems
- Would require wrapping in a UI beneficiaries can use
- Complex because it's focused on identity mapping, not status views
- **Not practical** for self-service benefits viewing

---

## Summary: What Exists vs. What's Missing

| Feature | Exists? | Details |
|---------|---------|---------|
| **Agent Registration Portal** | ✓ Yes | Agents register beneficiaries, update records |
| **Agent Dashboard** | ✓ Yes | Staff view, manage cases/enrollments |
| **Payment Integration API** | ✓ Yes | SPAR links ID to bank account for DFSP |
| **Beneficiary Self-Service Portal** | ❌ No | **This needs to be built** |
| **Beneficiary Payment History View** | ❌ No | Would need custom build |
| **Beneficiary Status Check** | ❌ No | Would need custom build |
| **Fraud Status for Beneficiaries** | ❌ No | **Our fraud addon + portal needed** |
| **Appeal Mechanism** | ❌ No | Would need custom build |
| **SMS/USSD Notifications** | ⚠️ Partial | SMS possible, USSD needs custom build |

---

## Our Fraud Detection Addon — Integration Points

Now that we've added fraud detection, here's where beneficiaries would see it:

### 1. **Staff View** (Already works)
- Odoo: `http://localhost:8069/web#model=fraud.case&view_type=kanban`
- Shows fraud cases with color coding, rules triggered, AI explanation

### 2. **Beneficiary View** (Needs to be built)
- **Option**: Build a simple portal using our API
  - Endpoint: `GET /api/v1/beneficiary/<id>/fraud-status` (if we implement it)
  - Returns: risk level, explanation, recommendation
- **Or**: Integrate into a future beneficiary self-service portal

### 3. **Notifications** (Already implemented)
- Fraud.case chatter messages can trigger SMS/email (if configured)
- Beneficiary receives: "Your account has been flagged"

---

## Recommendations

### Short Term (For Demo)
Document the fraud detection as something **staff can use**:
- Staff accesses Odoo fraud.case kanban
- Sees flagged beneficiary
- Clicks case to read AI explanation
- Marks case as confirmed/dismissed
- **No beneficiary portal needed** for staff-only demo

### Medium Term (For Pilot)
Build a **lightweight beneficiary status API**:
- Public REST endpoint: `GET /beneficiary/<id>/status`
- Returns: enrollment, last payment, **fraud status**
- No UI needed — just API for future mobile app teams

### Long Term (For Production)
Build a **full beneficiary portal** (web + mobile):
- Self-service: view status, payments, fraud appeals
- SMS/USSD fallback for low-literacy users
- Integration with fraud detection for flagged individuals

---

## Files Reviewed

- `/openg2p-registry/g2p_registration_portal_base/` — Agent registration portal
- `/openg2p-registry/g2p_agent_portal_base/` — Agent portal framework
- `/openg2p-spar/openg2p-spar-bene-portal-api/` — Payment system API (not beneficiary portal)
- `/openg2p-program/g2p_reimbursement_portal/` — Staff reimbursement view

---

## Conclusion

**OpenG2P is designed as a backend system for social protection programs, not a beneficiary-facing application.**

- **Agents/staff** can register and manage beneficiaries via portals
- **Payment systems** can integrate via SPAR API to send money
- **Beneficiaries themselves** have no way to access their own data via OpenG2P

**For our fraud detection addon**: We've added fraud flags, explanations, and rules. But beneficiaries need **a separate portal to view them** — OpenG2P doesn't provide one out-of-the-box.

**What we CAN do (quick)**: Create a simple REST API endpoint that returns fraud status for a beneficiary, and document how to build a UI on top of it. The endpoint is straightforward; it's the authentication, UI, and beneficiary workflow that require custom development.
