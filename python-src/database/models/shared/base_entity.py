"""
DB models — shared base entity (SQLAlchemy 2.0).
Mirrors the MikroORM BaseEntity with _id, created_at, last_updated_at, archived_at.
"""
import datetime
from sqlalchemy import DateTime, Integer, func
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class BaseEntity(Base):
    __abstract__ = True

    _id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    created_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=False),
        server_default=func.now(),
        index=True,
    )

    last_updated_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=False),
        server_default=func.now(),
        onupdate=func.now(),
        index=True,
    )

    archived_at: Mapped[datetime.datetime | None] = mapped_column(
        DateTime(timezone=False),
        nullable=True,
        index=True,
    )
