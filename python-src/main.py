"""
Entry point — replaces the startServer() / shutdown() logic in app.ts.

- ``python main.py`` — start the FastAPI server and run continuous StubHub auto-sync scheduler
  (phase1 + catalog refresh loops; disable with ``STUBHUB_SCHEDULER_ENABLED=0``).
- ``python main.py phase1`` — discover parking events from ``venues.xlsx`` (or DB); writes
  ``storage/exports/phase1_discovery_*.json`` (thousands of URLs, no copy-paste).
- ``python main.py run`` / ``catalog-sync`` — batch-scrape parking listings into
  ``storage/stubhub_snapshots/`` (use ``--phase1-latest`` after phase1; see ``--help``).
"""
from __future__ import annotations

import asyncio
import os
import random
import signal
import sys
import time

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
_scheduler_task: asyncio.Task | None = None
_scheduler_stop = asyncio.Event()
_phase1_job_lock = asyncio.Lock()
_catalog_job_lock = asyncio.Lock()


def _env_bool(name: str, default: bool) -> bool:
    v = (os.environ.get(name) or "").strip().lower()
    if not v:
        return default
    return v not in {"0", "false", "no", "off"}


def _env_int(name: str, default: int, minimum: int) -> int:
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        return max(minimum, int(raw))
    except ValueError:
        logger.warning(f"[scheduler] Invalid int env {name}={raw!r}, fallback={default}")
        return default


def _next_sleep(base_seconds: int, failure_count: int, jitter_cap: int = 60) -> float:
    backoff_mult = min(8, 2 ** max(0, failure_count))
    base = max(1, base_seconds * backoff_mult)
    jitter = random.uniform(0.0, min(float(jitter_cap), max(1.0, base * 0.1)))
    return float(base) + jitter


async def _run_phase1_job(reason: str) -> int:
    from cli.stubhub_phase1 import build_parser as phase1_parser_builder, run_phase1_discovery

    if _phase1_job_lock.locked():
        logger.info(f"[scheduler][phase1] skip ({reason}): already running")
        return 0
    async with _phase1_job_lock:
        started = time.perf_counter()
        logger.info(f"[scheduler][phase1] start | reason={reason}")
        args = phase1_parser_builder().parse_args([])
        code = await run_phase1_discovery(args, embedded=True)
        logger.info(
            f"[scheduler][phase1] done | reason={reason} | exit_code={code} | elapsed={time.perf_counter() - started:.1f}s"
        )
        return code


async def _run_catalog_job(reason: str) -> int:
    from cli.stubhub_catalog_sync import namespace_for_autostart, run_catalog_sync

    if _catalog_job_lock.locked():
        logger.info(f"[scheduler][catalog] skip ({reason}): already running")
        return 0
    if _phase1_job_lock.locked():
        logger.info(f"[scheduler][catalog] defer ({reason}): phase1 currently running")
        return 0
    async with _catalog_job_lock:
        started = time.perf_counter()
        args = namespace_for_autostart()
        if not args.phase1_json and not args.parking_urls:
            args.phase1_latest = True
        logger.info(
            f"[scheduler][catalog] start | reason={reason} | phase1_latest={getattr(args, 'phase1_latest', False)}"
        )
        code = await run_catalog_sync(args, embedded=True)
        logger.info(
            f"[scheduler][catalog] done | reason={reason} | exit_code={code} | elapsed={time.perf_counter() - started:.1f}s"
        )
        return code


async def _phase1_loop(interval_seconds: int) -> None:
    failures = 0
    while not _scheduler_stop.is_set():
        sleep_for = _next_sleep(interval_seconds, failures)
        logger.info(f"[scheduler][phase1] next run in {sleep_for:.1f}s")
        try:
            await asyncio.wait_for(_scheduler_stop.wait(), timeout=sleep_for)
            break
        except asyncio.TimeoutError:
            pass
        code = await _run_phase1_job("periodic")
        failures = failures + 1 if code != 0 else 0


async def _catalog_loop(interval_seconds: int) -> None:
    failures = 0
    while not _scheduler_stop.is_set():
        sleep_for = _next_sleep(interval_seconds, failures)
        logger.info(f"[scheduler][catalog] next run in {sleep_for:.1f}s")
        try:
            await asyncio.wait_for(_scheduler_stop.wait(), timeout=sleep_for)
            break
        except asyncio.TimeoutError:
            pass
        code = await _run_catalog_job("periodic")
        failures = failures + 1 if code != 0 else 0


async def _scheduler_runner() -> None:
    enabled = _env_bool("STUBHUB_SCHEDULER_ENABLED", True)
    if not enabled:
        logger.info("[scheduler] disabled via STUBHUB_SCHEDULER_ENABLED=0")
        return
    catalog_seconds = _env_int("STUBHUB_SCHEDULER_CATALOG_SECONDS", 7200, 5)
    phase1_seconds = _env_int("STUBHUB_SCHEDULER_PHASE1_SECONDS", 86400, 5)
    run_both_on_start = _env_bool("STUBHUB_SCHEDULER_RUN_BOTH_ON_START", True)
    logger.info(
        f"[scheduler] enabled | catalog_every={catalog_seconds}s | phase1_every={phase1_seconds}s | "
        f"startup_run_both={run_both_on_start}"
    )
    if run_both_on_start and not _scheduler_stop.is_set():
        await _run_phase1_job("startup")
        if not _scheduler_stop.is_set():
            await _run_catalog_job("startup")
    phase1_task = asyncio.create_task(_phase1_loop(phase1_seconds), name="scheduler-phase1-loop")
    catalog_task = asyncio.create_task(_catalog_loop(catalog_seconds), name="scheduler-catalog-loop")
    try:
        await _scheduler_stop.wait()
    finally:
        for task in (phase1_task, catalog_task):
            task.cancel()
        await asyncio.gather(phase1_task, catalog_task, return_exceptions=True)
        logger.info("[scheduler] stopped")


async def startup() -> None:
    global _scheduler_task
    logger.info("Initializing modular ORM (database component)...")
    await initialize_orm()
    logger.info("ORM initialized successfully")
    logger.info(f"Event Parking Discovery API running on port {PORT}")
    logger.info(f"Loaded scrapers for {TicketingController.domain}: {TicketingController.list_scrapers()}")
    _scheduler_stop.clear()
    _scheduler_task = asyncio.create_task(_scheduler_runner(), name="stubhub-auto-sync-scheduler")


async def shutdown(sig: str) -> None:
    global _is_shutting_down, _scheduler_task
    async with _shutdown_lock:
        if _is_shutting_down:
            return
        _is_shutting_down = True

        logger.info(f"{sig} received, shutting down modular components gracefully...")
        _scheduler_stop.set()
        if _scheduler_task and not _scheduler_task.done():
            _scheduler_task.cancel()
            await asyncio.gather(_scheduler_task, return_exceptions=True)
            logger.info("[scheduler] background task cancelled")
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
    try:
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, lambda s=sig.name: asyncio.create_task(shutdown(s)))
    except NotImplementedError:
        pass  # Windows does not support add_signal_handler

    await server.serve()


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] in ("run", "catalog-sync"):
        from cli.stubhub_catalog_sync import main as catalog_sync_main

        catalog_sync_main(sys.argv[2:])
    elif len(sys.argv) > 1 and sys.argv[1] == "phase1":
        from cli.stubhub_phase1 import main as phase1_main

        phase1_main(sys.argv[2:])
    else:
        asyncio.run(main())
