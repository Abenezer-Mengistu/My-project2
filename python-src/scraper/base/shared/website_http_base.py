"""
Website HTTP base class — replaces website-got.base.ts.
Adds cookie persistence for website-specific scrapers.
"""
from __future__ import annotations

from scraper.base.shared.http_base import HttpBase


class WebsiteHttpBase(HttpBase):
    """Base for HTTP scrapers that interact with a specific website entity (cookies)."""

    def __init__(self):
        super().__init__()
        self.website_entity = None

    async def load_cookies(self) -> None:
        """Load cookies from the website entity into the HTTP client."""
        if self.website_entity and hasattr(self.website_entity, "cookies"):
            cookies = self.website_entity.cookies or {}
            self.set_cookies(cookies)

    async def save_cookies(self) -> None:
        """Save current HTTP client cookies back to the website entity."""
        if self._client and self.website_entity:
            # httpx.AsyncClient.cookies is a CookieJar-like object
            cookies_dict = dict(self._client.cookies)
            self.website_entity.cookies = cookies_dict
            # Note: Persistence to DB happens via the repository in the controller
