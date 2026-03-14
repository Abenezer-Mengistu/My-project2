"""
Ticketing Playwright base — replaces TicketingPlaywrightBase.ts.
Refactored to use modular 'anti_bot' component and explicit waits.
"""
from __future__ import annotations

import asyncio
from playwright.async_api import Page

from scraper.base.shared.playwright_base import PlaywrightBase
from anti_bot.stealth import StealthManager
from utils.logger import logger


class TicketingPlaywrightBase(PlaywrightBase):
    """Abstract base for all ticketing scrapers."""

    handler: str = "ticketing-playwright"

    def __init__(self):
        super().__init__()
        self.venue = None  # Set in init()

    @classmethod
    async def init(cls, venue, page: Page | None, **kwargs) -> "TicketingPlaywrightBase":  # type: ignore[override]
        instance = cls()
        instance.venue = venue
        await instance._setup(page, **kwargs)
        return instance

    async def _setup(self, page: Page | None, *args, **kwargs) -> None:
        await super()._setup(page, *args, **kwargs)
        # Apply modular stealth only if we have a page
        if self._page is not None:
            ua = getattr(self.venue, "user_agent", None) if self.venue else None
            await StealthManager.apply_stealth(self.page, user_agent=ua)
        else:
            logger.info("Skipping stealth application (no page provided).")

    async def human_delay(self) -> None:
        """Simulate human-like pause (delegate to anti_bot)."""
        await StealthManager.human_delay()

    async def wait_for_selector_safe(self, selector: str, timeout: int = 30000) -> bool:
        """Explicit wait for a selector (delegate to anti_bot)."""
        return await StealthManager.wait_for_selector_safe(self.page, selector, timeout=timeout)

    async def search(self, start_date, end_date):
        return None

    async def discover_events(self, venue) -> int:
        raise NotImplementedError("Subclasses must implement discover_events()")

    async def scrape_parking(self, event) -> int:
        raise NotImplementedError("Subclasses must implement scrape_parking()")
