"""
ParkingPass ORM model — ticketing_parking_passes table.
"""
import datetime
from decimal import Decimal
from sqlalchemy import DateTime, ForeignKey, Numeric, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from database.models.shared.base_entity import BaseEntity


class ParkingPass(BaseEntity):
    __tablename__ = "ticketing_parking_passes"

    event_id: Mapped[int] = mapped_column(
        ForeignKey("ticketing_events._id"), nullable=False, index=True
    )
    event: Mapped["Event"] = relationship("Event", back_populates="parking_passes")  # noqa: F821

    lot_name: Mapped[str] = mapped_column(String, nullable=False, index=True)
    price: Mapped[Decimal] = mapped_column(
        Numeric(precision=10, scale=2), nullable=False, index=True
    )
    currency: Mapped[str] = mapped_column(String, nullable=False, default="USD", index=True)
    availability: Mapped[str | None] = mapped_column(String, nullable=True)
    last_scraped_at: Mapped[datetime.datetime | None] = mapped_column(
        DateTime(timezone=False), nullable=True, index=True
    )
