"""
Type definitions and protocols for scrapers.
Replaces scrapers/types.ts and scrapers/base/shared/types.ts.
"""
from __future__ import annotations

from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class IScraper(Protocol):
    type: str
    handler: str
    proxy: str | None

    async def scrape(self, *args, **kwargs) -> Any:
        ...


@runtime_checkable
class IWebsiteScraper(IScraper, Protocol):
    async def load_cookies(self) -> None:
        ...

    async def save_cookies(self) -> None:
        ...


@runtime_checkable
class ITicketingScraper(IWebsiteScraper, Protocol):
    async def discover_events(self, venue: Any) -> int:
        ...

    async def scrape_parking(self, event: Any) -> int:
        ...
