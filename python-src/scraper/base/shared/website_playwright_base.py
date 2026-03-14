"""
Website Playwright base class — replaces website-playwright.base.ts.
Adds cookie persistence for Playwright-based website scrapers.
"""
from __future__ import annotations

from scraper.base.shared.playwright_base import PlaywrightBase


class WebsitePlaywrightBase(PlaywrightBase):
    """Base for Playwright scrapers that interact with a specific website entity."""

    def __init__(self):
        super().__init__()
        self.website_entity = None

    async def load_cookies(self) -> None:
        """Load cookies from the website entity into the Playwright page."""
        if self.website_entity and hasattr(self.website_entity, "cookies"):
            cookies_dict = self.website_entity.cookies or {}
            # Playwright expect list of dicts: [{name: '', value: '', url: ''}, ...]
            # For simplicity, we assume the dict is compatible or handle mapping
            playwright_cookies = []
            for name, value in cookies_dict.items():
                playwright_cookies.append({
                    "name": name,
                    "value": value,
                    "url": self.page.url if self.page.url != "about:blank" else None
                })
            
            if playwright_cookies:
                await self.page.context.add_cookies(playwright_cookies)

    async def save_cookies(self) -> None:
        """Save current Playwright page cookies back to the website entity."""
        if self._page and self.website_entity:
            cookies = await self.page.context.cookies()
            cookies_dict = {c["name"]: c["value"] for c in cookies}
            self.website_entity.cookies = cookies_dict
