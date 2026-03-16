from openg2p_g2pconnect_common_lib.config import Settings as BaseSettings
from pydantic_settings import SettingsConfigDict

from . import __version__


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="g2p_bridge_", env_file=".env", extra="allow")

    openapi_title: str = "OpenG2P G2P Bridge API"
    openapi_description: str = """
        This module enables cash transfers from PBMS
        ***********************************
        Further details goes here
        ***********************************
        """
    openapi_version: str = __version__

    db_dbname: str = "openg2p_g2p_bridge_db"
    max_upload_file_size: int = 10485760  # 10 MB
    supported_file_types: list[str] = [
        "application/x-iso8583",
        "application/x-iso20022",
        "application/vnd.swift.mt940",
        "text/plain",
    ]

    jwt_validate_keymanager_app_id: str = "G2PBRIDGE"
    keymanager_auth_enabled: bool = False
