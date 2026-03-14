"""
TicketData ORM model — stores raw StubHub extraction snapshots.
"""
from sqlalchemy import Integer, String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from database.models.shared.base_entity import BaseEntity


class TicketData(BaseEntity):
    __tablename__ = "ticket_data"

    page_number: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    source_url: Mapped[str] = mapped_column(String(1000), nullable=False, index=True)
    data: Mapped[dict] = mapped_column(JSONB, nullable=False)
