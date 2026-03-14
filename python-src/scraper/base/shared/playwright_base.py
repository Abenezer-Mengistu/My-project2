"""
Playwright base class — replaces playwright.base.ts.
Uses Playwright's async API.
"""
from __future__ import annotations

import random
import asyncio

from playwright.async_api import Page, Cookie

from scraper.base.shared.http_base import HttpBase, SCRAPER_TYPE_PLAYWRIGHT
from utils.functions import random_int_in_range


class PlaywrightBase(HttpBase):
    """Abstract Playwright-based scraper. Holds an async Page."""

    type: str = SCRAPER_TYPE_PLAYWRIGHT
    handler: str = "base-playwright"
    persistent: bool = False

    def __init__(self):
        super().__init__()
        self._page: Page | None = None
        self._mouse_pos: dict = {"x": 0, "y": 0}
        self.page_load_count: int = 0
        self.screenshot_count: int = 0

    @classmethod
    async def init(cls, page: Page, *args, **kwargs) -> "PlaywrightBase":  # type: ignore[override]
        instance = cls()
        await instance._setup(page, *args, **kwargs)
        return instance

    async def _setup(self, page: Page, *args, **kwargs) -> None:  # type: ignore[override]
        await super()._setup(*args, **kwargs)
        self._page = page

    @property
    def page(self) -> Page:
        if self._page is None:
            raise RuntimeError("Page not initialised — call init(page) first.")
        return self._page

    async def stop_media_load(self) -> None:
        """Block image, stylesheet, and font requests to speed up page loads."""
        async def handle_route(route):
            if route.request.resource_type in ("image", "stylesheet", "font"):
                await route.abort()
            else:
                await route.continue_()

        await self.page.route("**/*", handle_route)

    async def mouse_click(self, locator) -> None:
        bbox = await locator.bounding_box()
        if not bbox:
            raise RuntimeError("Locator has no bounding box")

        x = bbox["x"] + random_int_in_range(1, max(1, int(bbox["width"] / 2)))
        y = bbox["y"] + random_int_in_range(1, max(1, int(bbox["height"] / 2)))
        await self.mouse_move(x, y)
        await self.page.mouse.click(x, y, delay=random_int_in_range(33, 333))

    async def mouse_move(self, x: float, y: float, steps: int = 9) -> None:
        """Move the mouse in a Bezier curve from current position to (x, y)."""
        from_x, from_y = self._mouse_pos["x"], self._mouse_pos["y"]

        cp1 = {
            "x": from_x + (x - from_x) * (random.random() * 0.3 + 0.1),
            "y": from_y + (y - from_y) * (random.random() * 0.3 + 0.1),
        }
        cp2 = {
            "x": from_x + (x - from_x) * (random.random() * 0.6 + 0.3),
            "y": from_y + (y - from_y) * (random.random() * 0.6 + 0.3),
        }

        for i in range(steps + 1):
            t = i / steps
            bx = _bezier(from_x, from_y, cp1, cp2, x, y, t)
            jitter_x = bx["x"] + random.random() * 1.2 - 0.6
            jitter_y = bx["y"] + random.random() * 1.2 - 0.6
            await self.page.mouse.move(jitter_x, jitter_y)
            await asyncio.sleep(0.001 + random.random() / 1000)

        self._mouse_pos = {"x": x, "y": y}

    def convert_cookies_to_dict(self, playwright_cookies: list[dict]) -> dict:
        """Convert a Playwright cookie list to a simple {name: value} dict."""
        return {c["name"]: c["value"] for c in playwright_cookies}


def _bezier(p0x, p0y, cp1: dict, cp2: dict, p3x, p3y, t: float) -> dict:
    cx = 3 * (cp1["x"] - p0x)
    bx = 3 * (cp2["x"] - cp1["x"]) - cx
    ax = p3x - p0x - cx - bx

    cy = 3 * (cp1["y"] - p0y)
    by = 3 * (cp2["y"] - cp1["y"]) - cy
    ay = p3y - p0y - cy - by

    return {
        "x": ax * t**3 + bx * t**2 + cx * t + p0x,
        "y": ay * t**3 + by * t**2 + cy * t + p0y,
    }
