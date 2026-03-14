"""
Venue ORM model — ticketing_venues table.
"""
from sqlalchemy import String, Text, UniqueConstraint, JSON
from sqlalchemy.orm import Mapped, mapped_column, relationship

from database.models.shared.base_entity import BaseEntity


class Venue(BaseEntity):
    __tablename__ = "ticketing_venues"
    __table_args__ = (UniqueConstraint("name", name="uq_ticketing_venues_name"),)

    name: Mapped[str] = mapped_column(String, nullable=False, index=True)
    stubhub_url: Mapped[str] = mapped_column(String(1000), nullable=False, index=True)
    location: Mapped[str | None] = mapped_column(String, nullable=True)
    handler: Mapped[str] = mapped_column(String, nullable=False)
    proxy: Mapped[str | None] = mapped_column(String, nullable=True)
    user_agent: Mapped[str | None] = mapped_column(Text, nullable=True)
    cookies: Mapped[dict | None] = mapped_column(JSON, nullable=True)  # JSON storage

    # Relationship back-ref
    events: Mapped[list["Event"]] = relationship("Event", back_populates="venue", lazy="select")  # noqa: F821
