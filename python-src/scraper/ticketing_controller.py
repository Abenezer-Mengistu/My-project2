"""
TicketingController — replaces ticketing.controller.ts.
Handles the 'ticketing' domain routes.
"""
from __future__ import annotations

import asyncio
from typing import Any
from types import SimpleNamespace

from fastapi import Request
from fastapi.responses import JSONResponse

from scraper.base_controller import BaseScraperController
from scraper.scraper_registry import MasterScraperRegistry
from scraper.base.shared.http_base import SCRAPER_TYPE_PLAYWRIGHT
from scraper.playwright_cluster import PlaywrightClusterManager, PlaywrightPersistentClusterManager
from database.repositories.ticketing.venues import get_venue_repository
from database.repositories.ticketing.events import get_event_repository
from utils.logger import logger


class TicketingController(BaseScraperController):
    domain: str = "ticketing"
    _operation_method_map = {
        "discoverEvents": "discover_events",
        "discoverParkingEvents": "discover_parking_events",
        "scrapeParking": "scrape_parking",
    }

    def __init__(self):
        super().__init__()
        # Override validators and enrichers for ticketing operations
        self.operation_payload_validators = {
            "discoverEvents": self._validate_discover_events,
            "scrapeParking": self._validate_scrape_parking,
        }
        self.operation_result_enrichers = {
            "discoverEvents": lambda result, _args: {
                "processed_count": len(result) if isinstance(result, list) else result, 
                "data": result if isinstance(result, list) else {"count": result}
            },
            "scrapeParking": lambda result, _args: {"processed_count": result, "data": {"count": result}},
        }

    # ── Validators ────────────────────────────────────────────────────────────

    @staticmethod
    async def _validate_discover_events(payload: dict) -> list:
        venue_id = payload.get("venue_id")
        if not venue_id:
            raise ValueError("venue_id is required")
        venue = await get_venue_repository().find_one(int(venue_id))
        if not venue:
            raise ValueError(f"Venue {venue_id} not found")
        return [venue]

    @staticmethod
    async def _validate_scrape_parking(payload: dict) -> list:
        event_id = payload.get("event_id")
        if not event_id:
            raise ValueError("event_id is required")
        event = await get_event_repository().find_one(int(event_id))
        if not event:
            raise ValueError(f"Event {event_id} not found")
        return [event]

    # ── Execute ───────────────────────────────────────────────────────────────

    async def execute(self, request: Request) -> JSONResponse:
        body = await request.json()
        venue_id = body.get("venue_id")
        operation = body.get("operation")
        operation_payload = body.get("operationPayload", {})
        method_name = self._operation_method_map.get(operation, operation)
        dry_run = operation_payload.get("dry_run", False)
        has_ad_hoc_venue = bool(operation_payload.get("stubhub_url"))

        logger.info(f"Executing ticketing task: {operation} for venue {venue_id}")

        try:
            if dry_run:
                raise ValueError("dry_run is disabled. Use real-time execution only.")

            venue = None
            if venue_id is not None:
                venue = await get_venue_repository().find_one(int(venue_id))

            # Allow ad-hoc live execution without DB seed/config.
            if venue is None and (operation == "discoverEvents" and has_ad_hoc_venue):
                venue = SimpleNamespace(
                    _id=int(venue_id) if venue_id is not None else -1,
                    name=operation_payload.get("venue_name", "Ad Hoc Venue"),
                    stubhub_url=operation_payload.get("stubhub_url", "https://www.stubhub.com/"),
                    handler=operation_payload.get("handler", "stubhub-discovery"),
                    proxy=None,
                    user_agent=None,
                )

            if not venue:
                raise ValueError(f"Venue {venue_id} not found")

            scraper_cls = self.__class__.get_scraper(venue.handler)
            if not scraper_cls:
                raise ValueError(f"No scraper found for handler: {venue.handler}")

            validator = self.operation_payload_validators.get(operation)
            if operation == "discoverEvents" and has_ad_hoc_venue:
                args = [venue]
            else:
                args = await validator(operation_payload) if validator else []

            result: Any = None

            if scraper_cls.type == SCRAPER_TYPE_PLAYWRIGHT:
                cluster = (
                    await PlaywrightPersistentClusterManager.get_or_create(venue.proxy)
                    if getattr(scraper_cls, "persistent", False)
                    else await PlaywrightClusterManager.get_or_create(venue.proxy)
                )

                async def _task(page):
                    instance = await scraper_cls.init(venue, page)
                    if not hasattr(instance, method_name):
                        raise ValueError(f"Operation {operation} not implemented for {venue.handler}")
                    
                    return await asyncio.wait_for(
                        getattr(instance, method_name)(*args, **operation_payload), timeout=1800
                    )

                result = await cluster.execute(_task)
            else:
                raise ValueError(f"Scraper type {scraper_cls.type} not supported for ticketing yet")

            enricher = self.operation_result_enrichers.get(operation)
            enriched = enricher(result, args) if enricher else {"processed_count": 0, "data": result}
            # Enrichers here are sync lambdas
            if asyncio.iscoroutine(enriched):
                enriched = await enriched

            return self._success(enriched)

        except Exception as exc:
            logger.error(f"Ticketing task failed: {exc}")
            return self._error(exc)

    @classmethod
    def register_scraper(cls, scraper_cls: type) -> None:
        MasterScraperRegistry.register(cls.domain, scraper_cls)

    @classmethod
    def get_scraper(cls, handler: str) -> type | None:
        return MasterScraperRegistry.get(cls.domain, handler)
