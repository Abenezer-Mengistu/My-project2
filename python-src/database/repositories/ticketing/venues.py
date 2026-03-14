"""
Venue repository — replaces the MikroORM venue repository.
"""
from __future__ import annotations

from sqlalchemy import select

from database.connection import get_session
from database.models.ticketing.venue import Venue


class VenueRepository:
    async def find_one(self, venue_id: int) -> Venue | None:
        async with get_session() as session:
            result = await session.execute(
                select(Venue).where(Venue._id == venue_id)
            )
            return result.scalar_one_or_none()

    async def upsert_venue(self, data: dict) -> Venue:
        async with get_session() as session:
            # Check if venue exists by name
            q = select(Venue).where(Venue.name == data["name"])
            res = await session.execute(q)
            venue = res.scalar_one_or_none()

            if venue:
                for key, value in data.items():
                    setattr(venue, key, value)
            else:
                venue = Venue(**data)
                session.add(venue)

            await session.commit()
            await session.refresh(venue)
            return venue

    async def list_all(self, limit: int = 1000) -> list[Venue]:
        async with get_session() as session:
            result = await session.execute(
                select(Venue).order_by(Venue._id.asc()).limit(limit)
            )
            return list(result.scalars().all())


_repo: VenueRepository | None = None


def get_venue_repository() -> VenueRepository:
    global _repo
    if _repo is None:
        _repo = VenueRepository()
    return _repo
