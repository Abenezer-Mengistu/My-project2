"""
TicketData repository — stores and queries raw StubHub snapshots.
"""
from __future__ import annotations

from sqlalchemy import select

from database.connection import get_session
from database.models.ticketing.ticket_data import TicketData


class TicketDataRepository:
    async def add_snapshot(self, source_url: str, page_number: int, payload: dict) -> TicketData:
        async with get_session() as session:
            row = TicketData(
                source_url=source_url,
                page_number=page_number,
                data=payload,
            )
            session.add(row)
            await session.flush()
            await session.refresh(row)
            return row

    async def latest(self, limit: int = 20) -> list[TicketData]:
        async with get_session() as session:
            result = await session.execute(
                select(TicketData).order_by(TicketData.created_at.desc()).limit(limit)
            )
            return list(result.scalars().all())


_repo: TicketDataRepository | None = None


def get_ticket_data_repository() -> TicketDataRepository:
    global _repo
    if _repo is None:
        _repo = TicketDataRepository()
    return _repo
