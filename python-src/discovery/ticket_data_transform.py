"""
Transform raw ticket_data snapshots into normalized price snapshots.
"""
from __future__ import annotations

from decimal import Decimal
from urllib.parse import parse_qs, urlparse

from database.connection import create_tables
from database.repositories.ticketing.ticket_data import get_ticket_data_repository
from database.repositories.ticketing.price_snapshots import get_price_snapshot_repository


def _extract_listing_id(source_url: str) -> str | None:
    parsed = urlparse(source_url)
    q = parse_qs(parsed.query)
    raw_id = (q.get("ID") or [None])[0]
    if not raw_id:
        return None
    parts = raw_id.split("|")
    return parts[1] if len(parts) > 1 else raw_id


def _extract_quantity(source_url: str) -> int | None:
    parsed = urlparse(source_url)
    q = parse_qs(parsed.query)
    raw_id = (q.get("ID") or [None])[0]
    if raw_id:
        parts = raw_id.split("|")
        if len(parts) > 2 and parts[2].isdigit():
            return int(parts[2])
    return None


def _to_decimal(v) -> Decimal | None:
    if v is None:
        return None
    try:
        return Decimal(str(v))
    except Exception:
        return None


class TicketDataTransformService:
    async def normalize_recent(self, limit: int = 100) -> dict:
        # Ensure all mapped tables are registered in metadata before create_all.
        import database.models.ticketing  # noqa: F401

        await create_tables()
        ticket_repo = get_ticket_data_repository()
        price_repo = get_price_snapshot_repository()
        rows = await ticket_repo.latest(limit=limit)
        inserted = 0
        skipped = 0

        for row in rows:
            payload = row.data or {}
            model = ((payload.get("result") or {}).get("priceBreakdownModel") or {})
            if not model:
                skipped += 1
                continue

            ticket_price = (model.get("ticketPrice") or {}).get("decimalValueInDisplayCurrency")
            fees = (model.get("deliveryAndBookingFee") or {}).get("decimalValueInDisplayCurrency")
            total = (model.get("ticketPriceWithFee") or {}).get("decimalValueInDisplayCurrency")
            currency = (
                (model.get("ticketPriceWithFee") or {}).get("currencyCode")
                or (model.get("ticketPrice") or {}).get("currencyCode")
            )

            entity = await price_repo.upsert(
                {
                    "ticket_data_id": row._id,
                    "listing_id": _extract_listing_id(row.source_url),
                    "source_url": row.source_url,
                    "quantity": _extract_quantity(row.source_url),
                    "ticket_price": _to_decimal(ticket_price),
                    "fees": _to_decimal(fees),
                    "total": _to_decimal(total),
                    "currency_code": currency,
                    "raw": payload,
                }
            )
            if entity:
                inserted += 1

        return {
            "processed_rows": len(rows),
            "normalized_rows": inserted,
            "skipped_rows": skipped,
        }
