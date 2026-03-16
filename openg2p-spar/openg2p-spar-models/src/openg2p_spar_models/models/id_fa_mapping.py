from typing import Any, Dict, List, Optional

from openg2p_fastapi_common.context import dbengine
from openg2p_fastapi_common.models import BaseORMModelWithTimes
from sqlalchemy import JSON, String, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Mapped, mapped_column


class IdFaMapping(BaseORMModelWithTimes):
    __tablename__ = "id_fa_mappings"

    id_value: Mapped[str] = mapped_column(String(), index=True, unique=True)
    fa_value: Mapped[str] = mapped_column(String(), index=True)

    name: Mapped[Optional[str]] = mapped_column(String())
    phone: Mapped[Optional[str]] = mapped_column(String())

    additional_info: Mapped[Optional[List[Dict[str, Any]]]] = mapped_column(
        JSON(), default=None
    )

    @classmethod
    async def get_all_by_query(cls, session=None, **kwargs):
        if "active" not in kwargs:
            kwargs["active"] = True
        response = []

        original_session = False
        if not session:
            original_session = True
            session = AsyncSession(dbengine.get())

        stmt = select(cls)
        for key, value in kwargs.items():
            if value is not None:
                stmt = stmt.where(getattr(cls, key) == value)

        stmt = stmt.order_by(cls.id.asc())

        result = await session.execute(stmt)

        response = list(result.scalars())

        if not original_session:
            await session.aclose()

        return response
