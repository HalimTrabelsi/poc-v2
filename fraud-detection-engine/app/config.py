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

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")


settings = Settings()
