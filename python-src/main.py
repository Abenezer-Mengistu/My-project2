"""
Entry point — replaces the startServer() / shutdown() logic in app.ts.
Refactored for modular 'database', 'scraper', 'monitoring'.
"""
from __future__ import annotations

import asyncio
import signal

import uvicorn
from dotenv import load_dotenv

# Load .env before anything else
load_dotenv()

from config import CONFIG
from database.connection import initialize_orm, close_orm
from scraper.ticketing_controller import TicketingController
from scraper.playwright_cluster import PlaywrightClusterManager
from utils.logger import logger


PORT = CONFIG["app"]["port"]
HOST = "0.0.0.0"
_shutdown_lock = asyncio.Lock()
_is_shutting_down = False


async def startup() -> None:
    logger.info("Initializing modular ORM (database component)...")
    await initialize_orm()
    logger.info("ORM initialized successfully")
    logger.info(f"Event Parking Discovery API running on port {PORT}")
    logger.info(f"Loaded scrapers for {TicketingController.domain}: {TicketingController.list_scrapers()}")


async def shutdown(sig: str) -> None:
    global _is_shutting_down
    async with _shutdown_lock:
        if _is_shutting_down:
            return
        _is_shutting_down = True

        logger.info(f"{sig} received, shutting down modular components gracefully...")
        try:
            await PlaywrightClusterManager.close_all()
        except Exception as exc:
            logger.warning(f"Playwright shutdown warning: {exc}")
        try:
            await close_orm()
        except Exception as exc:
            logger.warning(f"ORM shutdown warning: {exc}")
        logger.info("Shutdown complete")


class _UvicornServer(uvicorn.Server):
    """Override to hook our startup/shutdown into Uvicorn's lifecycle."""

    async def startup(self, sockets=None):
        await startup()
        await super().startup(sockets)

    async def shutdown(self, **kwargs):
        await super().shutdown(**kwargs)
        await shutdown("shutdown")


async def main() -> None:
    config = uvicorn.Config(
        "app:app",
        host=HOST,
        port=PORT,
        log_level="warning",
        loop="asyncio",
    )
    server = _UvicornServer(config=config)

    loop = asyncio.get_running_loop()
    loop = asyncio.get_running_loop()
    try:
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, lambda s=sig.name: asyncio.create_task(shutdown(s)))
    except NotImplementedError:
        pass  # Windows does not support add_signal_handler

    await server.serve()


if __name__ == "__main__":
    asyncio.run(main())
