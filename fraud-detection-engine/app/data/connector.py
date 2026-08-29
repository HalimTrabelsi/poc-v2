"""OpenG2P read-only database connector."""
import logging
from contextlib import contextmanager
from typing import Generator

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from app.config import settings

logger = logging.getLogger(__name__)


class OpenG2PConnector:
    """Read-only connection to OpenG2P PostgreSQL.

    Uses a connection pool sized for typical API workloads. All sessions
    are opened in read-only autocommit mode to prevent accidental writes.
    """

    def __init__(self) -> None:
        self._engine: Engine = create_engine(
            settings.openg2p_db_url,
            pool_size=5,
            max_overflow=10,
            pool_pre_ping=True,
            connect_args={"options": "-c default_transaction_read_only=on"},
        )
        self._Session = sessionmaker(bind=self._engine, autocommit=False, autoflush=False)

    @contextmanager
    def get_session(self) -> Generator[Session, None, None]:
        """Yield a SQLAlchemy session and ensure it is closed on exit."""
        session: Session = self._Session()
        try:
            yield session
        finally:
            session.close()

    def test_connection(self) -> bool:
        """Return True if the database is reachable, False otherwise."""
        try:
            with self._engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return True
        except Exception as exc:
            logger.warning("OpenG2P DB connection test failed: %s", exc)
            return False

    @property
    def engine(self) -> Engine:
        """Expose the underlying SQLAlchemy engine for pd.read_sql calls."""
        return self._engine
