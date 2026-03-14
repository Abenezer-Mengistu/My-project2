"""
PriceSnapshot ORM model — normalized price monitoring rows derived from ticket_data.
"""
from decimal import Decimal

from sqlalchemy import ForeignKey, Integer, Numeric, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from database.models.shared.base_entity import BaseEntity


class PriceSnapshot(BaseEntity):
    __tablename__ = "ticket_price_snapshots"
    __table_args__ = (UniqueConstraint("ticket_data_id", name="uq_ticket_price_snapshots_ticket_data_id"),)

    ticket_data_id: Mapped[int] = mapped_column(
        ForeignKey("ticket_data._id"), nullable=False, index=True
    )
    listing_id: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    source_url: Mapped[str] = mapped_column(String(1000), nullable=False, index=True)
    quantity: Mapped[int | None] = mapped_column(Integer, nullable=True)
    ticket_price: Mapped[Decimal | None] = mapped_column(Numeric(12, 2), nullable=True)
    fees: Mapped[Decimal | None] = mapped_column(Numeric(12, 2), nullable=True)
    total: Mapped[Decimal | None] = mapped_column(Numeric(12, 2), nullable=True)
    currency_code: Mapped[str | None] = mapped_column(String(8), nullable=True, index=True)
    raw: Mapped[dict] = mapped_column(JSONB, nullable=False)
