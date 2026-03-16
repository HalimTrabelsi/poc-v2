from sqlalchemy import create_engine

from .config import Settings

_config = Settings.get_config()


def get_engine():
    if _config.db_datasource:
        db_engine = create_engine(_config.db_datasource)
        return db_engine
