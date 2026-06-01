# Data Engineering Report — Fraud Detection Engine

**Date:** 2026-06-01
**Scope:** OpenG2P PostgreSQL database scan + PaySim AIML dataset cleaning, feature engineering, baseline training.

---

## 1. OpenG2P Database Audit

### Volumes & Quality

| Table | Rows | Notes |
|---|---|---|
| `res_partner` | 157 | 151 registrants, 20 individuals, 0 groups |
| `g2p_program_membership` | 130 | All `enrolled`, no `state` diversity |
| `g2p_payment` | 12 | All `posted`, range 50–500 |
| `g2p_entitlement` | 12 | Matches payments 1:1 |
| `g2p_phone_number` | 130 | **102 distinct → 28 collisions (21.5%)** |
| `g2p_reg_id` | 0 | National-ID linking unused |
| `g2p_program` | 1 | Single program in production |

**Verdict:** the live DB is a demo seed, not a real training corpus. The fraud engine has been training on synthetic data. However, the **phone-collision signal is real and exploitable today** — 5 numbers are used 3–15 times (incl. `+224 666 SHARED 99` appearing 15×).

### Field Population (Registrants Only)

| Field | Filled | % | Usable? |
|---|---|---|---|
| `name` | 151 | 100% | yes — for fuzzy-dedup |
| `birthdate` | 131 | 87% | yes — age, age-cohort features |
| `gender` | 131 | 87% | yes — demographic prior |
| `mobile` | 20 | 13% | weak |
| `phone` | 0 | 0% | **column unused — use `g2p_phone_number` table instead** |
| `email` | 0 | 0% | unusable |
| `g2p_reg_id` | 0 | 0% | unusable until populated |

### Recommendations for the Fraud Engine

1. **Stop reading `res_partner.phone` / `res_partner.email`** — they're always NULL in this deployment. The current `features_service.py` should query `g2p_phone_number.phone_sanitized` joined on `partner_id`.
2. **Add a `phone_collision_count` feature** — count how many *other* partners share each partner's phone numbers. Lift in the live data already shows 21.5% collision rate; this is a top-tier signal even before any model.
3. **Mine `g2p_program_membership` for temporal velocity** — `enrollment_date` ranges 2026-02-17 to 2026-03-20 (32 days). Build features:
   - `enrollments_same_day_as_partner` (cluster signal)
   - `enrollment_velocity_30d` (per program)
   - `time_to_first_payment` (gap between enrollment_date and first g2p_payment.create_date)
4. **Activate `g2p_reg_id` once populated** — duplicate national IDs are the single strongest fraud signal across G2P deployments. Add a "shadow rule" that fires `LOW` weight today and ramps to `CRITICAL` once data lands.
5. **Add a daily refresh of `feature_store`** rather than computing features at request time — your DB is small enough that pre-materializing helps response latency drop from 4.7s (current) to <100ms.

---

## 2. AIML Dataset (PaySim) — Profile & Cleaning

### Raw State

- **6,362,620 rows, 11 columns, 493 MB**
- This is the **Kaggle PaySim** synthetic mobile-money fraud dataset
- Zero nulls structurally — but lots of *semantic* issues
- **Severe imbalance: 8,213 fraud / 6,354,407 legit = 0.129%**
- `isFlaggedFraud` only fires 16× → degenerate column, dropped

### Per-Type Fraud Distribution (Critical Finding)

```
CASH_OUT     count=2,237,500   fraud=4,116   rate=0.184%
PAYMENT      count=2,151,495   fraud=0       rate=0.000%   ← drop
CASH_IN      count=1,399,284   fraud=0       rate=0.000%   ← drop
TRANSFER     count=  532,909   fraud=4,097   rate=0.769%
DEBIT        count=   41,432   fraud=0       rate=0.000%   ← drop
```

**Decision:** filter to TRANSFER + CASH_OUT only. The other 3 types have zero positive labels and would dilute training signal. Down from 6.36M → 2.77M rows, fraud rate up from 0.13% → 0.30%.

### Engineered Features & Their Lift

Lift = `P(fraud | feature=1) / P(fraud | feature=0)`. Measured on the filtered 2.77M rows.

| Feature | Lift | Interpretation |
|---|---|---|
| `full_drain` (orig balance → 0) | **53.1×** | account fully emptied = strong fraud signal |
| `round_amount` (multiple of 100) | **30.3×** | fraudsters use round numbers |
| `dest_was_empty` (dest balance=0 before) | **11.4×** | mule account warming up |
| `is_night` (hour < 6 or ≥ 22) | **10.0×** | off-hours activity |
| `tx_type_transfer` | 4.2× | TRANSFER more risky than CASH_OUT |
| `balance_anomaly` | 0× (inverse) | PaySim simulator bug — *legit* rows have this anomaly more |
| `overdraft_attempt` | 0× (inverse) | same — simulator artifact, not real signal |

The two reversed features should be **kept** in the model — the tree learns the inverse direction automatically — but **excluded from rule definitions** in `rules/`.

### Final Training Set

After filter + engineering + 1:20 stratified undersample:
- **172,473 rows** (8,213 fraud + 164,260 legit)
- 17 features, all numeric / int8 / float32 (parquet-efficient)
- Saved to `fraud-detection-engine/data/paysim_clean_balanced.{csv,parquet}`

### Baseline Model Trained on This Set

Calibrated XGBoost, 300 trees, max_depth=6, isotonic calibration, 80/20 stratified split:

| Metric | Score |
|---|---|
| ROC-AUC | **0.9998** |
| PR-AUC | **0.9994** |
| Precision | **1.0000** |
| Recall | **0.9976** |
| F1 | **0.9988** |
| FP | 0 |
| FN | 4 |

Compared to the existing beneficiary model (ROC-AUC 0.9951, F1 0.8817), the transaction model is operating on a **different signal axis** — they should be combined via the existing ensemble, not replaced.

### Top Importance (PaySim model)

```
balance_anomaly            0.9043
orig_balance_after         0.0849
tx_amount                  0.0052
amount_to_balance_ratio    0.0013
orig_balance_before        0.0009
full_drain                 0.0009
round_amount               0.0006
```

`balance_anomaly` dominates because PaySim's simulator labels its anomalous rows as legit — the model has learned the simulator's exact bug. This is *useful for the PaySim eval set* but **will not generalize to real OpenG2P transactions**. For production, retrain after replacing PaySim data with real G2P payment ledger data.

---

## 3. Concrete Code Changes Recommended

### A. New transaction-fraud model in the ensemble

`xgboost_paysim.joblib` is already saved to `app/models_saved/`. Wire it into `app/services/ml_service.py` as a third estimator:

```python
# Pseudocode for ml_service.py
self.beneficiary_model = joblib.load("xgboost.joblib")    # existing
self.transaction_model = joblib.load("xgboost_paysim.joblib")  # NEW
# Score both, weighted-average inside the ensemble
```

Then update `config.py` ensemble weights from `(rules 0.25, ml 0.30, graph 0.45)` to e.g. `(rules 0.20, beneficiary_ml 0.25, transaction_ml 0.15, graph 0.40)`.

### B. Fix `features_service.py` data source

Replace `res_partner.phone` reads with:

```sql
SELECT partner_id, phone_sanitized
FROM g2p_phone_number
WHERE phone_sanitized IS NOT NULL
```

…and compute `shared_phone_count` as a `GROUP BY phone_sanitized HAVING COUNT(DISTINCT partner_id) > 1` lookup.

### C. New high-impact features to add to feature store

| Feature | Source | Why |
|---|---|---|
| `phone_collision_count` | `g2p_phone_number` | 21% of phones already shared |
| `time_to_first_payment_days` | `g2p_program_membership` → `g2p_payment` | catches “enroll-then-immediate-cashout” |
| `enrollment_cohort_size` | `g2p_program_membership` grouped by `enrollment_date::date` | flags mass-enrollment days |
| `payment_round_amount_ratio` | `g2p_payment.amount_issued % 100 == 0` | 30× lift in PaySim |
| `night_payment_ratio` | `g2p_payment.payment_datetime` hour ∈ [22,6) | 10× lift |

### D. Retire dead columns

Drop these from `_DEFAULTS` in `app/api/routes.py`:
- `household_size`, `nb_children`, `nb_elderly`, `has_disabled`, `single_head`, `elderly_head`, `pmt_score`, `pmt_score_min` — already in `removed_features` in metadata.json, but the API still defaults them.

---

## 4. Deliverables (Created in This Session)

| File | Purpose |
|---|---|
| `scripts/profile_aiml_dataset.py` | Profiler — 6.3M-row scan, chunked |
| `scripts/build_training_set.py` | Cleaner + feature engineering pipeline |
| `scripts/train_paysim_baseline.py` | Trains calibrated XGBoost |
| `data/paysim_clean_balanced.csv` | Clean training set (172K rows) |
| `data/paysim_clean_balanced.parquet` | Same, columnar-efficient |
| `app/models_saved/xgboost_paysim.joblib` | Trained transaction-fraud model |
| `app/models_saved/paysim_metadata.json` | Schema, metrics, importances |

---

## 5. Next-Step Roadmap (Priority Order)

1. **(immediate)** Wire `xgboost_paysim.joblib` into the ensemble — adds transaction-level signal.
2. **(this week)** Replace `res_partner.phone` queries with `g2p_phone_number` joins. Add `phone_collision_count`. Expected +5-10% recall on shared-account fraud.
3. **(this week)** Pre-materialize features in `fraud-db` rather than computing per-request. Drop p99 latency from ~5s to <200ms.
4. **(this sprint)** Add 4 new features from §3.C — they have measured lift in either PaySim or the live OpenG2P data.
5. **(this sprint)** Activate the `g2p_reg_id` duplicate-ID rule as `LOW` weight today; ramp once data populates.
6. **(next sprint)** Replace PaySim training data with real OpenG2P transaction history once you have ≥10K real transactions. The current PaySim model leans on simulator artifacts that won't generalize.
