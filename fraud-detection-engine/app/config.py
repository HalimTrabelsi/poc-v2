"""Central configuration using pydantic-settings."""
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # Database — NO hardcoded passwords. Must be set via .env or env vars.
    openg2p_db_url: str = ""
    feature_store_url: str = ""

    # Models
    models_dir: Path = Path("app/models_saved")
    rules_dir: Path = Path("app/rules/rules")

    # ML (XGBoost / Isolation Forest internal mix)
    xgboost_weight: float = 0.70
    isolation_forest_weight: float = 0.30

    # Ensemble weights (rules / ML / graph) — optimised by grid search (2025-07)
    # Reflects that ML (stacking ensemble) is the strongest signal.
    ensemble_rules_weight: float = 0.20
    ensemble_ml_weight: float = 0.55
    ensemble_graph_weight: float = 0.25

    # Scoring thresholds
    critical_threshold: float = 0.80
    high_threshold: float = 0.60
    medium_threshold: float = 0.40

    # Default deployment country (ISO-2). Used to calibrate income/poverty
    # rule thresholds when a scan doesn't specify a country_code, so existing
    # callers keep working unchanged.
    default_country_code: str = "TN"

    # API — set API_SECRET_KEY in .env (no default in code)
    api_secret_key: str = ""

    # Alert system  (set ALERT_WEBHOOK_URL in .env to enable)
    alert_webhook_url: str = ""          # Slack / Teams / generic webhook URL
    alert_min_risk_level: str = "CRITICAL"   # CRITICAL | HIGH
    alert_odoo_enabled: bool = True      # also write mail.message into Odoo

    # MLflow tracking (local SQLite by default, no server needed)
    mlflow_tracking_uri: str = "sqlite:///app/models_saved/mlflow.db"
    mlflow_experiment_name: str = "fraud-detection-engine"

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")


settings = Settings()
