"""
Event repository — replaces the MikroORM event repository.
"""
from __future__ import annotations

import datetime

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert

from database.connection import get_session
from database.models.ticketing.event import Event
from database.models.ticketing.venue import Venue


class EventRepository:
    async def find_one(self, event_id: int) -> Event | None:
        async with get_session() as session:
            result = await session.execute(
                select(Event).where(Event._id == event_id)
            )
            return result.scalar_one_or_none()

    async def upsert_event(self, data: dict) -> Event:
        """
        Upsert an event by (venue_id, event_url).
        `data` should contain: venue (Venue), name, date, event_url, and optionally
        parking_url, external_id.
        """
        venue: Venue = data["venue"]
        async with get_session() as session:
            # Try finding existing event by event_url + venue
            result = await session.execute(
                select(Event).where(
                    Event.venue_id == venue._id,
                    Event.event_url == data["event_url"],
                )
            )
            event = result.scalar_one_or_none()

            if event is None:
                event = Event(
                    venue_id=venue._id,
                    name=data["name"],
                    date=data.get("date", datetime.date.today()),
                    event_url=data["event_url"],
                    parking_url=data.get("parking_url"),
                    external_id=data.get("external_id"),
                )
                session.add(event)
            else:
                event.name = data["name"]
                event.date = data.get("date", event.date)
                if data.get("parking_url"):
                    event.parking_url = data["parking_url"]
                if data.get("external_id"):
                    event.external_id = data["external_id"]
                event.last_updated_at = datetime.datetime.utcnow()

            await session.commit()
            await session.refresh(event)
            return event


_repo: EventRepository | None = None


def get_event_repository() -> EventRepository:
    global _repo
    if _repo is None:
        _repo = EventRepository()
    return _repo
