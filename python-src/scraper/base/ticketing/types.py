"""
Ticketing-specific type definitions.
Replaces scrapers/base/ticketing/types.ts.
"""
from __future__ import annotations

from typing import TypedDict, Literal


class TicketingPayload(TypedDict, total=False):
    operation: Literal["discoverEvents", "scrapeParking"]
    venue_id: int
    event_id: int
    force: bool


class DiscoveredEvent(TypedDict):
    name: str
    date: str  # ISO date
    event_url: str
    parking_url: str | None
    external_id: str | None


class ParkingPassData(TypedDict):
    lot_name: str
    price: float
    currency: str
    availability: str | None
