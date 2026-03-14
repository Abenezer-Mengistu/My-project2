"""
Playwright cluster manager — replaces PlaywrightClusterManager.ts.
Manages an async pool of Playwright Browser instances.
"""
from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from typing import AsyncGenerator, Callable

from playwright.async_api import async_playwright, Browser, Page
from utils.logger import logger


class PlaywrightClusterManager:
    """
    A simple browser pool that creates one browser per proxy key.
    Each call to execute() opens a new page in that browser.
    """
    _instances: dict[str | None, "PlaywrightClusterManager"] = {}

    def __init__(self, proxy_key: str | None = None):
        self._proxy_key = proxy_key
        self._browser: Browser | None = None
        self._playwright = None
        self._lock = asyncio.Lock()

    @classmethod
    async def get_or_create(cls, proxy_key: str | None = None) -> "PlaywrightClusterManager":
        if proxy_key not in cls._instances:
            manager = cls(proxy_key)
            await manager._start()
            cls._instances[proxy_key] = manager
        return cls._instances[proxy_key]

    async def _start(self) -> None:
        from config import CONFIG

        logger.info("[Playwright] Starting playwright...")
        self._playwright = await async_playwright().start()
        logger.info("[Playwright] Playwright started.")
        proxy_kwargs = {}
        if self._proxy_key:
            proxy_info = CONFIG["proxies"].get(self._proxy_key)
            if proxy_info:
                proxy_kwargs["proxy"] = {
                    "server": proxy_info["origin"],
                    "username": proxy_info.get("username"),
                    "password": proxy_info.get("password"),
                }

        logger.info(f"[Playwright] Launching chromium (headless=True)... args={['--no-sandbox', '--disable-dev-shm-usage']}")
        self._browser = await self._playwright.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage"],
            **proxy_kwargs,
        )
        logger.info("[Playwright] Chromium launched successfully.")

    async def execute(self, task: Callable) -> object:
        """Execute `task(page)` in a new browser page and return the result."""
        if self._browser is None:
            await self._start()

        logger.info("[Playwright] Creating new browser context...")
        context = await self._browser.new_context()
        logger.info("[Playwright] Creating new page...")
        page = await context.new_page()
        logger.info(f"[Playwright] Page created. Executing task {task.__name__ if hasattr(task, '__name__') else 'anonymous'}...")
        try:
            result = await task(page)
            logger.info(f"[Playwright] Task {task.__name__ if hasattr(task, '__name__') else 'anonymous'} finished successfully.")
            return result
        except Exception as exc:
            logger.error(f"[Playwright] Task {task.__name__ if hasattr(task, '__name__') else 'anonymous'} failed: {exc}")
            raise
        finally:
            await context.close()
            logger.info("[Playwright] Context closed.")

    async def close(self) -> None:
        if self._browser:
            await self._browser.close()
            self._browser = None
        if self._playwright:
            await self._playwright.stop()
            self._playwright = None
        PlaywrightClusterManager._instances.pop(self._proxy_key, None)

    @classmethod
    async def close_all(cls) -> None:
        for manager in list(cls._instances.values()):
            await manager.close()
        cls._instances.clear()


class PlaywrightPersistentClusterManager(PlaywrightClusterManager):
    """Persistent browser variant — reuses pages across requests (future use)."""
    _instances: dict[str | None, "PlaywrightPersistentClusterManager"] = {}

    @classmethod
    async def get_or_create(cls, proxy_key: str | None = None) -> "PlaywrightPersistentClusterManager":
        if proxy_key not in cls._instances:
            manager = cls(proxy_key)
            await manager._start()
            cls._instances[proxy_key] = manager
        return cls._instances[proxy_key]
