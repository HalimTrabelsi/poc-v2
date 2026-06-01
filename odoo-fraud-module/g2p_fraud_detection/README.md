# G2P Fraud Detection — Odoo Addon

Native Odoo integration for the fraud-detection-engine. Replaces the
standalone HTML alert monitor with a fully-integrated kanban dashboard.

## Features

- **Live Alert Monitor** kanban with auto-refresh via `bus.bus`
- **Case management** with mail thread + activities
- **Role-based access**: Fraud Officer (read/write open cases) and
  Fraud Supervisor (full access)
- **Beneficiary navigation** — one click from a case to the partner record
- **Periodic sync** with the fraud-engine REST API (cron, every minute)
- **Configurable** via `ir.config_parameter`:
  - `fraud_detection.api_url`
  - `fraud_detection.api_key`
  - `fraud_detection.sync_limit`

## Installation

The addon is mounted into the Odoo container via `Dockerfile.odoo`.

```bash
docker-compose -f docker-compose.full.yml build openg2p-odoo
docker-compose -f docker-compose.full.yml up -d
```

After Odoo restarts, navigate to **Apps**, click **Update Apps List**,
search for **G2P Fraud Detection**, and click **Install**.

## Usage

After installation, a new top-level menu **Fraud Detection** appears with:

- **Live Alert Monitor** — kanban view, auto-refreshes when new
  CRITICAL/HIGH cases arrive (toast notifications)
- **All Cases** — full case management with filters by risk level / state
- **Configuration → System Parameters** — adjust API URL / key

## Workflow

1. The fraud-engine scores beneficiaries via `/api/v1/score/features`
2. New cases are stored in `fraud-db`
3. The Odoo cron `Fraud Engine: Sync Cases` pulls them every minute
4. When created with `risk_level` ∈ {CRITICAL, HIGH}, a `bus.bus`
   notification triggers UI refresh + toast
5. Fraud officers investigate → mark as confirmed / dismissed
