"""
BaseScraperController — replaces base.controller.ts.
Provides common execution, enrichment, and response handling.
"""
from __future__ import annotations

import asyncio
import json
from typing import Any

from fastapi import Request, Response
from fastapi.responses import JSONResponse

from config import CONFIG
from scraper.scraper_registry import MasterScraperRegistry
from scraper.base.shared.http_base import SCRAPER_TYPE_HTTP, SCRAPER_TYPE_PLAYWRIGHT
from scraper.playwright_cluster import PlaywrightClusterManager, PlaywrightPersistentClusterManager
from monitoring.task_queue_service import TaskQueueService
from utils.logger import logger


class BaseScraperController:
    domain: str = "general"
    handler: str = "general"

    _task_queue_service = TaskQueueService.__new__(TaskQueueService)

    def __init__(self):
        self.operation_payload_validators: dict = {
            "scrape": lambda payload: list(payload.values()) if isinstance(payload, dict) else [],
        }
        self.operation_result_enrichers: dict = {
            "scrape": self._default_enricher,
        }

    # ── Helpers ───────────────────────────────────────────────────────────────

    @staticmethod
    async def _default_enricher(result: Any, args: list) -> dict:
        if result is None:
            return {"processed_count": 0, "data": None}
        if isinstance(result, list):
            return {"processed_count": len(result), "data": result}
        return {"processed_count": 1, "data": result}

    @staticmethod
    def _execution_timeout(sec: int = 1800):
        async def _timeout():
            await asyncio.sleep(sec)
            raise TimeoutError(f"DEADLINE_EXCEEDED: Execution did not complete within {sec} seconds")
        return _timeout()

    @classmethod
    def register_scraper(cls, scraper_cls: type) -> None:
        MasterScraperRegistry.register(cls.domain, scraper_cls)

    @classmethod
    def get_scraper(cls, handler: str) -> type | None:
        return MasterScraperRegistry.get(cls.domain, handler)

    @classmethod
    def list_scrapers(cls) -> list[str]:
        return MasterScraperRegistry.list_for_domain(cls.domain)

    @staticmethod
    def _generate_queue_name(domain: str, handler: str, operation: str) -> str:
        return f"{domain}-{handler}-{operation}".lower().replace("_", "-")

    @staticmethod
    def _get_correct_endpoint(scraper_cls: type, operation: str) -> str:
        if operation and "document" in operation:
            return CONFIG["app"]["document_endpoint"]
        if scraper_cls.type == SCRAPER_TYPE_HTTP:
            return CONFIG["app"]["got_endpoint"]
        return CONFIG["app"]["puppeteer_endpoint"]

    # ── FastAPI response helpers ──────────────────────────────────────────────

    def _success(self, result: dict) -> JSONResponse:
        return JSONResponse({
            "success": True,
            "processed_items": result.get("processed_count", 0),
            "data": result.get("data"),
        })

    def _error(self, error: Exception, env: str = "production") -> JSONResponse:
        logger.error(f"Controller error: {error}")
        body: dict = {
            "success": False,
            "error": str(error),
        }
        if env == "development":
            import traceback
            body["stack"] = traceback.format_exc()
        return JSONResponse(body, status_code=500)

    # ── Main execute ──────────────────────────────────────────────────────────

    async def execute(self, request: Request) -> JSONResponse:
        body = await request.json()
        operation = body.get("operation", "scrape")
        handler = body.get("handler", self.__class__.handler)
        operation_payload = body.get("operationPayload", {})

        try:
            scraper_cls = self.__class__.get_scraper(handler)
            if not scraper_cls:
                raise ValueError(f"No scraper found for handler: {handler}")

            validator = self.operation_payload_validators.get(operation)
            args = await validator(operation_payload) if asyncio.iscoroutinefunction(validator) else (validator(operation_payload) if validator else list(operation_payload.values()))

            operation_results: Any = None

            if scraper_cls.type == SCRAPER_TYPE_HTTP:
                instance = await scraper_cls.init()
                if not hasattr(instance, operation):
                    raise ValueError(f"Operation not implemented: {operation}")
                operation_results = await asyncio.wait_for(
                    getattr(instance, operation)(*args), timeout=1800
                )

            elif scraper_cls.type == SCRAPER_TYPE_PLAYWRIGHT:
                cluster = (
                    await PlaywrightPersistentClusterManager.get_or_create(scraper_cls.proxy)
                    if getattr(scraper_cls, "persistent", False)
                    else await PlaywrightClusterManager.get_or_create(scraper_cls.proxy)
                )

                async def _task(page):
                    inst = await scraper_cls.init(page)
                    if not hasattr(inst, operation):
                        raise ValueError(f"Operation not implemented: {operation}")
                    return await asyncio.wait_for(
                        getattr(inst, operation)(*args), timeout=1800
                    )

                operation_results = await cluster.execute(_task)

            enricher = self.operation_result_enrichers.get(operation, self._default_enricher)
            enriched = await enricher(operation_results, args)

            return self._success(enriched)

        except Exception as exc:
            return self._error(exc, CONFIG["app"]["node_env"])

    async def healthz(self) -> JSONResponse:
        from datetime import datetime, timezone
        return JSONResponse({"status": "ok", "timestamp": datetime.now(timezone.utc).isoformat()})
