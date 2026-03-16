import uuid
from datetime import datetime
from typing import Optional

from openg2p_fastapi_common.models import BaseORMModel
from sqlalchemy import DateTime, String
from sqlalchemy.orm import Mapped, mapped_column


class BaseORMModelWithId(BaseORMModel):
    __abstract__ = True

    id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    created_at: Mapped[datetime] = mapped_column(DateTime(), default=datetime.now)
    updated_at: Mapped[Optional[datetime]] = mapped_column(DateTime(), default=datetime.now)
