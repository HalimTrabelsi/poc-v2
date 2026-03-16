from openg2p_fastapi_common.config import Settings as BaseSettings
from pydantic_settings import SettingsConfigDict

from . import __version__


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="spar_bene_portal_api_", env_file=".env", extra="allow"
    )

    openapi_title: str = "OpenG2P SPAR Bene Portal API"
    openapi_description: str = """
        FastAPI Service for OpenG2P SPAR Bene Portal API
        ***********************************
        Further details goes here
        ***********************************
        """
    openapi_version: str = __version__

    # SPAR Database
    db_username: str = "postgres"
    db_password: str = "password"
    db_hostname: str = "localhost"
    db_port: int = 5432
    db_dbname: str = "spardb"
