"""Central configuration using pydantic-settings."""
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # Database
    openg2p_db_url: str = "postgresql://odoo:odoo@localhost:5432/openg2p"
    feature_store_url: str = "postgresql://fraud:fraud@localhost:5432/fraud_store"

    # Models
    models_dir: Path = Path("app/models_saved")
    rules_dir: Path = Path("app/rules/rules")

    # ML
    xgboost_weight: float = 0.70
    isolation_forest_weight: float = 0.30

    # Scoring thresholds
    critical_threshold: float = 0.80
    high_threshold: float = 0.60
    medium_threshold: float = 0.40

    # API
    api_secret_key: str = "dev-secret-change-in-prod"

    # Alert system  (set ALERT_WEBHOOK_URL in .env to enable)
    alert_webhook_url: str = ""          # Slack / Teams / generic webhook URL
    alert_min_risk_level: str = "CRITICAL"   # CRITICAL | HIGH
    alert_odoo_enabled: bool = True      # also write mail.message into Odoo

    # MLflow tracking (local SQLite by default, no server needed)
    mlflow_tracking_uri: str = "sqlite:///app/models_saved/mlflow.db"
    mlflow_experiment_name: str = "fraud-detection-engine"

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")


settings = Settings()
