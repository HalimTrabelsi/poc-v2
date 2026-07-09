# Project Change Summary — OpenG2P Fraud Detection Engine

## 1. Model / ML Pipeline Changes

**Architecture — stacking ensemble activated:**
- Base models: XGBoost + Random Forest + Isolation Forest (anomaly) feeding into a meta-model (logistic regression stacker)
- Previously XGBoost was dead weight (old `metadata.json` was missing `base_models`/`meta_model_input_order` keys, so `MultiModelScorer._load()` silently rejected it — the ensemble was running mono-model)
- **Retrained on a fresh 20K-row synthetic dataset** → XGBoost now genuinely active: **AUC 0.875, Precision 0.96, Recall 0.76** (previous recall was 0.486)

**Training script (`ml/scripts/train_openg2p.py`):**
- Added 5-fold `StratifiedKFold` cross-validation for reporting
- Added grid-search "weight optimization" across rule/ml/anomaly/graph ensemble weights, saved as `learned_weights` in `metadata.json`
- ⚠️ **Known issue, not yet fixed:** the grid search optimizes against `np.random.uniform()` placeholder data standing in for rule-engine and graph scores — the resulting learned weights aren't meaningfully learned. Recommend disabling `learned_weights` loading or wiring in real rule/graph scores before trusting this for the report.
- Isolation Forest contamination lowered `0.12 → 0.05` (fewer false anomaly flags)

**Scoring pipeline unified:**
- `app/services/ml_service.py` rewritten to delegate to `MultiModelScorer` (the stacking ensemble) instead of a legacy mono-model loader
- `app/core/rule_engine.py` thresholds standardized: CRITICAL/HIGH/MEDIUM/LOW cutoffs → 0.80 / 0.60 / 0.40 (previously inconsistent 0.80/0.55/0.30 across files)
- `app/config.py` ensemble weights updated to rules=0.20, ml=0.55, graph=0.25

## 2. Stability Fixes (crash root causes)

**Concurrent model-loading heap corruption** (`free(): invalid next size` crashes during batch scoring):
- Root cause: `MultiModelScorer()` was constructed fresh on every request across 7 call sites, so an 8-worker thread pool triggered concurrent `joblib.load()` on the same files → native heap corruption
- Fix: singleton pattern (`get_scorer()`/`replace_scorer()` with `threading.Lock`) in `app/core/ml_scorer.py`; all callers updated to reuse the cached instance; retrain now atomically swaps the singleton instead of racing

**SHAP explainability crashes:**
- Removed an unsafe `LinearExplainer` fallback that corrupted native heap when applied to tree models (XGBoost/RF don't have linear coefficients)
- Fixed feature-name lookup order (check Pipeline/ColumnTransformer first, then unwrapped estimator) — this also fixed SHAP being silently disabled
- Added a guard: if feature names can't be resolved, return `[]` instead of crashing or leaking irrelevant features

## 3. Dead Code Removal

- Deleted `app/api/routes_scoring.py` — never wired into `app/main.py`, was a leftover parallel scoring path
- Deleted `rules/feedback_processor.py` — had a broken 3-parent path bug; the real, correct version lives at `app/core/feedback_processor.py`
- ⚠️ `app/core/pipeline.py` still exists as **reachable-but-unmounted** dead code (imported only by `routes_graph.py`, which itself is never mounted in `main.py`) — it still constructs `MultiModelScorer()` directly rather than via the singleton, so it's a latent bug if anyone reconnects it later

## 4. Security Hardening (partial)

- Removed hardcoded DB passwords/API keys from `app/config.py` source — now default to empty string, must come from environment
- `docker-compose.full.yml`: OpenG2P Postgres and Odoo passwords now use hard-fail `${VAR:?set in .env}` (no weak fallback)
- ⚠️ **Still weak:** `fraud-db` password (`fraud123`), Grafana admin password (`admin`), and dashboard API key (`change-me`) still ship as silent fallback defaults if `.env` doesn't override them — recommend tightening before any public demo

## 5. Odoo / OpenG2P Container Fixes

- **Restart-loop bug:** wrong Bitnami env var was set initially (`BITNAMI_SKIP_UPDATE`, which doesn't exist); the real one is `ODOO_SKIP_MODULES_UPDATE=yes` — fixed, container now starts reliably instead of restarting every ~30s
- **Circular import crash:** `odoo-fraud-module/g2p_fraud_detection/controllers/__init__.py` imported a non-existent `beneficiary_status` module — removed, module now loads cleanly
- **Missing DB column:** `fraud_case.top_features` field existed in the Python model but never got migrated into the actual Postgres table (because module auto-update was disabled) — ran a one-time targeted `--update=g2p_fraud_detection` to sync schema; "AI Explanation" feature now works
- **Odoo → fraud-engine 401 Unauthorized:** the security hardening above removed fraud-engine's hardcoded API key but nobody wired a replacement into the `fraud-engine` service's environment in `docker-compose.full.yml` (it was only wired to the `dashboard` service). Odoo was sending `X-API-Key: dev-secret-change-in-prod` (its own fallback) while fraud-engine expected `""`. Fixed by adding `API_SECRET_KEY: ${API_SECRET_KEY:?set in .env}` to fraud-engine's environment and setting the matching value in `.env`

## 6. Tests

- Added `tests/unit/test_edge_cases.py` — 7 tests covering empty features, None values, negative/extreme values, high-risk profiles, feature consistency, base-model score presence
- Added `decision_orchestrator` fixture to `tests/conftest.py`
- ⚠️ Minor inconsistency: these new tests construct `MultiModelScorer()` directly rather than via `get_scorer()`, bypassing the singleton discipline used everywhere else (low risk in sequential test runs, but worth aligning)

## Current State (verified live)

| Service | Status |
|---|---|
| fraud-engine | Up, healthy, API key auth working |
| openg2p-odoo | Up, HTTP 200, fraud module loaded cleanly |
| Odoo ↔ fraud-engine sync | Working (confirmed via cron logs, no more 401s) |
| PostgreSQL (both) | Healthy |

**Outstanding items to decide on:** the fake-data grid search in training, the three remaining weak fallback passwords, and the dead `pipeline.py` singleton bypass — none are blocking, but worth addressing before the report/demo if time permits.
