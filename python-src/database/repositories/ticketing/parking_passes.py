"""
ParkingPass repository — replaces the MikroORM parking_passes repository.
"""
from __future__ import annotations

import datetime
from decimal import Decimal, InvalidOperation

from sqlalchemy import delete, select

from database.connection import get_session
from database.models.ticketing.event import Event
from database.models.ticketing.parking_pass import ParkingPass


class ParkingPassRepository:
    async def clear_for_event(self, event: Event) -> None:
        """Delete all parking passes for a given event."""
        async with get_session() as session:
            await session.execute(
                delete(ParkingPass).where(ParkingPass.event_id == event._id)
            )

    async def add_passes(self, event: Event, passes: list[dict]) -> int:
        """
        Bulk-insert parking passes for an event.
        Each pass dict should have: lot_name, price, currency (optional).
        Returns the number of passes inserted.
        """
        now = datetime.datetime.utcnow()
        async with get_session() as session:
            for p in passes:
                try:
                    price = Decimal(str(p.get("price", "0")))
                except InvalidOperation:
                    price = Decimal("0")

                pass_obj = ParkingPass(
                    event_id=event._id,
                    lot_name=p.get("lot_name", ""),
                    price=price,
                    currency=p.get("currency", "USD"),
                    availability=p.get("availability"),
                    last_scraped_at=now,
                )
                session.add(pass_obj)
        return len(passes)


_repo: ParkingPassRepository | None = None


def get_parking_pass_repository() -> ParkingPassRepository:
    global _repo
    if _repo is None:
        _repo = ParkingPassRepository()
    return _repo
