from enum import Enum
from typing import Optional

from openg2p_fastapi_common.context import dbengine
from openg2p_fastapi_common.models import BaseORMModelWithTimes
from sqlalchemy import String, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Mapped, mapped_column


class StrategyType(str, Enum):
    """Enum for Strategy types"""

    ID = "ID"
    FA = "FA"


class Strategy(BaseORMModelWithTimes):
    """
    Strategy model for ID and FA construction/deconstruction.

    This model stores regex patterns and format strings used to construct
    and deconstruct ID and FA values using the StrategyHelper service.

    Attributes:
        description: Human-readable description of the strategy
        strategy_type: Type of strategy (ID or FA)
        construct_strategy: Format string for constructing ID/FA (e.g., "{key1}-{key2}")
        deconstruct_strategy: Regex pattern for deconstructing ID/FA (e.g., r"(?P<key1>.*)-(?P<key2>.*)")
    """

    __tablename__ = "strategy"

    description: Mapped[Optional[str]] = mapped_column(String(), nullable=True)
    strategy_type: Mapped[str] = mapped_column(
        String(10), nullable=False, default=StrategyType.ID.value
    )
    construct_strategy: Mapped[str] = mapped_column(String(), nullable=False)
    deconstruct_strategy: Mapped[str] = mapped_column(String(), nullable=False)

    @classmethod
    async def get_strategy(cls, **kwargs):
        """
        Get a strategy by its attributes.

        Args:
            **kwargs: Filter criteria (e.g., id=1, strategy_type="ID")

        Returns:
            Strategy object or None if not found
        """
        session = AsyncSession(dbengine.get())
        try:
            stmt = select(cls)
            for key, value in kwargs.items():
                if value is not None:
                    stmt = stmt.where(getattr(cls, key) == value)

            result = await session.execute(stmt)
            return result.scalars().first()
        finally:
            await session.aclose()
