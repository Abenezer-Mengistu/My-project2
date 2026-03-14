"""
Event ORM model — ticketing_events table.
"""
import datetime
from sqlalchemy import Date, ForeignKey, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from database.models.shared.base_entity import BaseEntity


class Event(BaseEntity):
    __tablename__ = "ticketing_events"

    venue_id: Mapped[int] = mapped_column(
        ForeignKey("ticketing_venues._id"), nullable=False, index=True
    )
    venue: Mapped["Venue"] = relationship("Venue", back_populates="events")  # noqa: F821

    name: Mapped[str] = mapped_column(String, nullable=False, index=True)
    date: Mapped[datetime.date] = mapped_column(Date, nullable=False, index=True)
    event_url: Mapped[str] = mapped_column(String(1000), nullable=False, index=True)
    parking_url: Mapped[str | None] = mapped_column(String(1000), nullable=True, index=True)
    external_id: Mapped[str | None] = mapped_column(String, nullable=True, index=True)

    # Relationship
    parking_passes: Mapped[list["ParkingPass"]] = relationship(  # noqa: F821
        "ParkingPass", back_populates="event", lazy="select"
    )
