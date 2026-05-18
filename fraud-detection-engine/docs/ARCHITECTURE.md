# Architecture

## System Overview

The Fraud Detection Engine is a layered Python service that reads beneficiary data from OpenG2P (PostgreSQL, read-only) and writes fraud decisions to its own internal feature store.

```
OpenG2P DB (read-only)          Fraud Store DB
        │                               ▲
        ▼                               │
  Data Extractors          FraudCaseRepository
  (BeneficiaryExtractor,   (SQLAlchemy Core,
   PaymentExtractor,        fraud_cases table)
   RelationshipExtractor)
        │                               │
        ▼                               │
  FeatureEngineer ──► 25 features ──►  │
                                        │
  RuleService ─── YAML rules ──►  rule_score (0.30 weight)
  GraphAnalyzer ─ NetworkX ────►  graph_score (0.20 weight)
  MLScorer ─────  XGBoost ─────►  ml_score (0.50 weight)
                                        │
                   final_score = weighted sum
                                        │
  Explainer ──── SHAP TreeExplainer ───►
                                        │
                            FraudDecisionResponse
                             (persisted + returned)
```

## Service Layer Design

Each service is a standalone class injected with its dependencies:

| Service | Responsibility | Key Dependency |
|---------|---------------|----------------|
| `FeatureEngineer` | Extract + derive 25 ML features | `BeneficiaryExtractor` |
| `RuleService` | Evaluate YAML rules | `RuleLoader`, `RuleEngine` |
| `GraphAnalyzer` | Network centrality + density | `RelationshipExtractor`, NetworkX |
| `MLScorer` | XGBoost + IsolationForest scoring | joblib artifacts |
| `Explainer` | SHAP values + natural language | SHAP TreeExplainer |
| `DecisionOrchestrator` | Coordinate all services | All of the above |

## Data Flow

```
POST /v1/score/beneficiary/{id}
  │
  ▼
DecisionOrchestrator.score_beneficiary()
  │
  ├─ 1. FeatureEngineer.get_features()
  │       └─ SQL query → 25 feature columns
  │
  ├─ 2. RuleService.evaluate(features)
  │       └─ SafeExpressionEvaluator on each YAML rule
  │           → rule_score (sum of weights, capped at 1.0)
  │
  ├─ 3. GraphAnalyzer.analyze_network()
  │       └─ shared phone/bank edges → NetworkX graph
  │           → degree centrality + density → graph_score
  │
  ├─ 4. MLScorer.score(features)
  │       └─ XGBoost predict_proba + IsoForest score_samples
  │           → combined_score = 0.70×xgb + 0.30×iso
  │
  ├─ 5. Aggregate: 0.30×rule + 0.50×ml + 0.20×graph
  │
  ├─ 6. Risk level + recommendation from thresholds
  │
  ├─ 7. Explainer.explain() → SHAP + rule text
  │
  └─ 8. FraudCaseRepository.save_decision() → case_id
```

## Database Schema

### fraud_store.fraud_cases

| Column | Type | Description |
|--------|------|-------------|
| case_id | TEXT PK | UUID |
| beneficiary_id | TEXT | OpenG2P partner_id |
| final_score | REAL | 0–1 aggregated score |
| rule_score | REAL | Rule engine contribution |
| ml_score | REAL | ML model contribution |
| graph_score | REAL | Network analysis contribution |
| risk_level | TEXT | LOW / MEDIUM / HIGH / CRITICAL |
| recommendation | TEXT | CLEAR / MONITOR / MANUAL_REVIEW / BLOCK_PAYMENT |
| status | TEXT | OPEN / UNDER_REVIEW / CLOSED / FALSE_POSITIVE |
| rules_triggered | JSONB | List of triggered rule objects |
| top_features | JSONB | SHAP feature contributions |
| explanation | TEXT | Human-readable summary |
| notes | TEXT | Agent audit notes |
| created_at | TIMESTAMPTZ | Decision timestamp |
| updated_at | TIMESTAMPTZ | Last status change |

## Rule Engine Design

Rules are loaded from YAML files in `app/rules/rules/`. Each file represents one fraud scenario. The `SafeExpressionEvaluator` (AST-based) evaluates conditions using a whitelist of allowed Python AST node types, preventing code injection.

Scoring: `rule_score = min(sum(weight for each triggered rule), 1.0)`

## ML Pipeline Design

1. `scripts/generate_synthetic_data.py` → 5000 beneficiary rows with 5% fraud
2. `scripts/train_ml_models.py` → 60/20/20 train/val/test split
3. XGBoost trains with `scale_pos_weight` to handle class imbalance
4. IsolationForest trains unsupervised (contamination=0.05)
5. Both models saved as joblib artifacts + `metadata.json` with feature list

Target metrics: ROC-AUC ≥ 0.88, F1 ≥ 0.78 on held-out test set.
