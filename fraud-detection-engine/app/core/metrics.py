"""Custom Prometheus metrics for the fraud detection pipeline.

Exposed on /metrics alongside the default prometheus_client process/platform
metrics. Scraped by Prometheus and visualised in the "Fraud Detection"
Grafana dashboard (monitoring/grafana/dashboards/fraud_engine_dashboard.json).
"""
from prometheus_client import Counter, Histogram, Gauge

fraud_scores_total = Counter(
    "fraud_scores_total",
    "Number of beneficiaries scored, by resulting risk level",
    ["risk_level"],
)

fraud_score_value = Histogram(
    "fraud_score_value",
    "Distribution of final fraud scores (0-1)",
    buckets=(0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.75, 0.8, 0.9, 1.0),
)

fraud_scoring_duration_seconds = Histogram(
    "fraud_scoring_duration_seconds",
    "End-to-end latency of the scoring pipeline (score_beneficiary)",
    buckets=(0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 20),
)

fraud_rule_triggered_total = Counter(
    "fraud_rule_triggered_total",
    "Number of times each rule has fired",
    ["rule_id"],
)

fraud_recommendation_total = Counter(
    "fraud_recommendation_total",
    "Number of scoring decisions, by recommended action",
    ["recommendation"],
)

fraud_pipeline_errors_total = Counter(
    "fraud_pipeline_errors_total",
    "Number of pipeline sub-stage failures (caught, non-fatal)",
    ["stage"],
)
