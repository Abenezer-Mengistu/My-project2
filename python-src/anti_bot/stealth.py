"""
Stealth and anti-bot utilities for Playwright.
Replaces logic previously scattered in TicketingPlaywrightBase and PlaywrightBase.
"""
from __future__ import annotations

import random
from playwright.async_api import Page, Locator
from utils.functions import random_int_in_range


class StealthManager:
    """Manages User-Agents, headers, and human-like interactions."""

    USER_AGENTS = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    ]

    @classmethod
    def get_random_user_agent(cls) -> str:
        return random.choice(cls.USER_AGENTS)

    @staticmethod
    async def apply_stealth(page: Page, user_agent: str | None = None) -> None:
        """Apply headers and UA to a Playwright page."""
        ua = user_agent or StealthManager.get_random_user_agent()
        await page.set_extra_http_headers({
            "User-Agent": ua,
            "Accept-Language": "en-US,en;q=0.9",
            "sec-ch-ua": '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
        })

    @staticmethod
    async def human_delay(min_ms: int = 2000, max_ms: int = 5000) -> None:
        """Simulate a human-like pause."""
        import asyncio
        ms = random_int_in_range(min_ms, max_ms)
        await asyncio.sleep(ms / 1000)

    @staticmethod
    async def wait_for_selector_safe(page: Page, selector: str, timeout: int = 30000) -> bool:
        """Wait for a selector, returning False instead of raising on timeout."""
        try:
            await page.wait_for_selector(selector, timeout=timeout, state="visible")
            return True
        except Exception:
            return False
