"""
PriceSnapshot repository — normalized monitoring queries.
"""
from __future__ import annotations

from decimal import Decimal

from sqlalchemy import desc, select

from database.connection import get_session
from database.models.ticketing.price_snapshot import PriceSnapshot


class PriceSnapshotRepository:
    async def upsert(self, row: dict) -> PriceSnapshot:
        async with get_session() as session:
            existing = await session.execute(
                select(PriceSnapshot).where(PriceSnapshot.ticket_data_id == row["ticket_data_id"])
            )
            entity = existing.scalar_one_or_none()
            if entity is None:
                entity = PriceSnapshot(**row)
                session.add(entity)
            else:
                for k, v in row.items():
                    setattr(entity, k, v)
            await session.flush()
            await session.refresh(entity)
            return entity

    async def latest(self, limit: int = 200) -> list[PriceSnapshot]:
        async with get_session() as session:
            result = await session.execute(
                select(PriceSnapshot).order_by(desc(PriceSnapshot.created_at)).limit(limit)
            )
            return list(result.scalars().all())

    async def latest_by_listing(self, listing_id: str, limit: int = 50) -> list[PriceSnapshot]:
        async with get_session() as session:
            result = await session.execute(
                select(PriceSnapshot)
                .where(PriceSnapshot.listing_id == listing_id)
                .order_by(desc(PriceSnapshot.created_at))
                .limit(limit)
            )
            return list(result.scalars().all())

    async def price_changes(self, limit: int = 100, listing_id: str | None = None) -> list[dict]:
        rows = await (self.latest_by_listing(listing_id, limit=200) if listing_id else self.latest(limit=1000))
        grouped: dict[str, list[PriceSnapshot]] = {}
        for r in rows:
            key = r.listing_id or f"url:{r.source_url}"
            grouped.setdefault(key, []).append(r)

        changes: list[dict] = []
        for key, snapshots in grouped.items():
            snapshots = sorted(snapshots, key=lambda x: x.created_at, reverse=True)
            if len(snapshots) < 2:
                continue
            current, previous = snapshots[0], snapshots[1]
            if current.total is None or previous.total is None:
                continue
            diff = Decimal(current.total) - Decimal(previous.total)
            changes.append(
                {
                    "listing_id": current.listing_id,
                    "source_url": current.source_url,
                    "currency_code": current.currency_code,
                    "latest_total": float(current.total),
                    "previous_total": float(previous.total),
                    "price_difference": float(diff),
                    "latest_at": current.created_at.isoformat(),
                    "previous_at": previous.created_at.isoformat(),
                }
            )
        changes.sort(key=lambda x: abs(x["price_difference"]), reverse=True)
        return changes[:limit]


_repo: PriceSnapshotRepository | None = None


def get_price_snapshot_repository() -> PriceSnapshotRepository:
    global _repo
    if _repo is None:
        _repo = PriceSnapshotRepository()
    return _repo
