"""
FastAPI application — replaces app.ts (Express).
Refactored for modular 'discovery', 'scraper', 'anti_bot', 'database', 'monitoring'.
"""
from __future__ import annotations

import asyncio
import csv
import html
import json
import os
import time
from datetime import date, datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from types import SimpleNamespace
import re
from urllib.parse import unquote, urlsplit, urlparse, parse_qs, parse_qsl, urlencode, urlunparse, urlunsplit, urljoin
import httpx

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, HTMLResponse, FileResponse

# Scraper registration — ensuring modular components are loaded
from discovery.stubhub_discovery import StubHubDiscoveryScraper
from discovery.stubhub_snapshot_service import StubHubSnapshotService
from discovery.ticket_data_transform import TicketDataTransformService
from discovery.venue_parser import VenueParser
from scraper.spothero_parking import SpotHeroParkingScraper, build_spothero_event
from scraper.stubhub_parking import StubHubParkingScraper
from scraper.ticketing_controller import TicketingController
from scraper.playwright_cluster import PlaywrightClusterManager
from scraper.stubhub_venue_updater import discover_and_update_venues

# Register scrapers with their domain controller
TicketingController.register_scraper(StubHubDiscoveryScraper)
TicketingController.register_scraper(StubHubParkingScraper)
TicketingController.register_scraper(SpotHeroParkingScraper)

from config import CONFIG
from config import __NODE_ENV_DEV
from database.repositories.ticketing.events import get_event_repository
from database.repositories.ticketing.parking_passes import get_parking_pass_repository
from database.repositories.ticketing.price_snapshots import get_price_snapshot_repository
from database.repositories.ticketing.venues import get_venue_repository
from utils.logger import logger
from utils.normalization import normalize_lot_name, normalize_section_name
from utils.pricing import (
    extract_total_price,
    compute_listing_metrics,
    check_price_thresholds,
    currency_from_listing,
    set_usd_exchange_rates,
)
from utils.export_shaping import create_event_result, flatten_event_result
from utils.functions import retry


app = FastAPI(title="Event Parking Discovery API", version="1.1.0")
BASE_DIR = Path(__file__).resolve().parent
STORAGE_DIR = BASE_DIR / "storage"
STORAGE_EXPORTS = STORAGE_DIR / "exports"
STORAGE_MONITORING = STORAGE_DIR / "monitoring"
STORAGE_SEARCH_RESULTS = STORAGE_DIR / "search_results"
UI_DIR = BASE_DIR / "ui"


def _storage_output_path(excel_path: str) -> Path:
    """Resolve any Excel/output file path to python-src/storage/exports so no output is written outside storage."""
    name = Path(excel_path).name
    STORAGE_EXPORTS.mkdir(parents=True, exist_ok=True)
    return STORAGE_EXPORTS / name


def _live_cache_get(key: str, ttl_seconds: int) -> object | None:
    entry = _spothero_live_cache.get(key)
    if not entry:
        return None
    stored_at = entry.get("stored_at")
    if not isinstance(stored_at, (int, float)) or time.time() - stored_at > ttl_seconds:
        _spothero_live_cache.pop(key, None)
        return None
    return entry.get("value")


def _live_cache_set(key: str, value: object) -> object:
    _spothero_live_cache[key] = {"stored_at": time.time(), "value": value}
    return value


async def _fetch_text(url: str, *, ttl_seconds: int | None = None) -> str:
    cache_key = f"text:{url}"
    if ttl_seconds:
        cached = _live_cache_get(cache_key, ttl_seconds)
        if isinstance(cached, str):
            return cached
    async with httpx.AsyncClient(
        headers=SPOTHERO_PUBLIC_HEADERS,
        timeout=45.0,
        follow_redirects=True,
    ) as client:
        response = await client.get(url)
        response.raise_for_status()
        text = response.text
    if ttl_seconds:
        _live_cache_set(cache_key, text)
    return text


def _extract_next_data(raw_html: str) -> dict:
    match = _SPOTHERO_NEXT_DATA_RE.search(raw_html or "")
    if not match:
        raise ValueError("SpotHero page missing __NEXT_DATA__ payload")
    return json.loads(html.unescape(match.group("payload")))


def _normalize_live_search_text(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", str(value or "").lower()).strip()


def _score_live_match(query: str, *parts: str) -> int:
    q = _normalize_live_search_text(query)
    if not q:
        return 1
    hay = " ".join(_normalize_live_search_text(part) for part in parts if part).strip()
    if not hay:
        return 0
    if hay.startswith(q):
        return 100
    if f" {q}" in hay:
        return 80
    if q in hay:
        return 60
    q_tokens = q.split()
    if q_tokens and all(token in hay for token in q_tokens):
        return 40
    return 0


async def _fetch_spothero_cities() -> list[dict]:
    cached = _live_cache_get("spothero:cities", 1800)
    if isinstance(cached, list):
        return cached
    raw = await _fetch_text("https://spothero.com/cities/", ttl_seconds=1800)
    payload = _extract_next_data(raw)
    cities = (((payload.get("props") or {}).get("pageProps") or {}).get("cities") or [])
    normalized = [
        {
            "slug": str(item.get("slug") or "").strip(),
            "title": str(item.get("title") or "").strip(),
            "country_code": str(item.get("countryCode") or "").strip(),
        }
        for item in cities
        if isinstance(item, dict) and item.get("slug") and item.get("title")
    ]
    return _live_cache_set("spothero:cities", normalized)


async def _fetch_spothero_city_page(city_slug: str) -> dict:
    cache_key = f"spothero:city:{city_slug}"
    cached = _live_cache_get(cache_key, 900)
    if isinstance(cached, dict):
        return cached
    raw = await _fetch_text(f"https://spothero.com/city/{city_slug}-parking", ttl_seconds=900)
    payload = _extract_next_data(raw)
    page_props = ((payload.get("props") or {}).get("pageProps") or {})
    city_payload = {
        "popular_destinations": page_props.get("popularDestinations") if isinstance(page_props.get("popularDestinations"), list) else [],
        "performer_list": page_props.get("performerList") if isinstance(page_props.get("performerList"), list) else [],
    }
    return _live_cache_set(cache_key, city_payload)


def _destination_summary_from_city(city: dict, item: dict) -> dict:
    path = str(item.get("link") or "").strip()
    destination_id = item.get("id")
    title = str(item.get("title") or "").strip()
    city_title = str(city.get("title") or "").strip()
    return {
        "destination_id": int(destination_id) if destination_id is not None else None,
        "title": title,
        "city": city_title,
        "path": path,
        "destination_page_url": f"https://spothero.com{path}" if path.startswith("/") else path,
        "search_url": (
            f"https://spothero.com/search?kind=destination&id={int(destination_id)}"
            if destination_id is not None
            else None
        ),
    }


async def _fetch_spothero_destination_index() -> list[dict]:
    cached = _live_cache_get("spothero:destination-index", 3600)
    if isinstance(cached, list):
        return cached

    cities = await _fetch_spothero_cities()
    semaphore = asyncio.Semaphore(8)

    async def _load_city(city: dict) -> list[dict]:
        async with semaphore:
            try:
                page = await _fetch_spothero_city_page(str(city.get("slug") or ""))
            except Exception:
                return []
            return [
                _destination_summary_from_city(city, destination)
                for destination in page.get("popular_destinations", [])
                if isinstance(destination, dict)
            ]

    all_rows = await asyncio.gather(*[_load_city(city) for city in cities])
    seen: set[tuple] = set()
    flattened: list[dict] = []
    for rows in all_rows:
        for row in rows:
            key = (row.get("destination_id"), row.get("path"))
            if key in seen:
                continue
            seen.add(key)
            flattened.append(row)
    flattened.sort(key=lambda item: (item.get("city") or "", item.get("title") or ""))
    return _live_cache_set("spothero:destination-index", flattened)


async def _live_destination_suggestions(query: str) -> list[dict]:
    cities = await _fetch_spothero_cities()
    scored_cities = []
    for city in cities:
        score = _score_live_match(query, city.get("title") or "", city.get("slug") or "")
        if score > 0:
            scored_cities.append((score, city))
    scored_cities.sort(key=lambda item: (-item[0], item[1].get("title", "")))

    suggestions: list[dict] = []
    seen: set[tuple] = set()
    for city in [city for _, city in scored_cities[:3]]:
        page = await _fetch_spothero_city_page(str(city.get("slug") or ""))
        for destination in page.get("popular_destinations", []):
            if not isinstance(destination, dict):
                continue
            suggestion = _destination_summary_from_city(city, destination)
            match_score = _score_live_match(query, suggestion.get("city") or "", suggestion.get("title") or "")
            if match_score <= 0:
                continue
            key = (suggestion.get("destination_id"), suggestion.get("path"))
            if key in seen:
                continue
            seen.add(key)
            suggestion["match_score"] = match_score
            suggestions.append(suggestion)

    if not suggestions:
        for suggestion in await _fetch_spothero_destination_index():
            match_score = _score_live_match(query, suggestion.get("city") or "", suggestion.get("title") or "")
            if match_score <= 0:
                continue
            enriched = dict(suggestion)
            enriched["match_score"] = match_score
            suggestions.append(enriched)

    suggestions.sort(key=lambda item: (-int(item.get("match_score") or 0), item.get("city") or "", item.get("title") or ""))
    return suggestions[:8]


def _extract_event_schemas(raw_html: str) -> list[dict]:
    events: list[dict] = []
    for match in _SPOTHERO_EVENT_SCHEMA_RE.finditer(raw_html or ""):
        try:
            payload = json.loads(html.unescape(match.group("payload")))
        except Exception:
            continue
        if isinstance(payload, list):
            for item in payload:
                if isinstance(item, dict):
                    events.append(item)
        elif isinstance(payload, dict):
            events.append(payload)
    return events


async def _geocode_address(address_text: str) -> tuple[str | None, str | None]:
    cache_key = f"geocode:{address_text}"
    cached = _live_cache_get(cache_key, 86400)
    if isinstance(cached, tuple) and len(cached) == 2:
        return cached
    async with httpx.AsyncClient(
        headers={**SPOTHERO_PUBLIC_HEADERS, "Referer": "https://spothero.com/"},
        timeout=25.0,
        follow_redirects=True,
    ) as client:
        response = await client.get(
            "https://nominatim.openstreetmap.org/search",
            params={"format": "jsonv2", "limit": 1, "q": address_text},
        )
        response.raise_for_status()
        rows = response.json()
    if isinstance(rows, list) and rows:
        first = rows[0] or {}
        lat = str(first.get("lat") or "").strip() or None
        lon = str(first.get("lon") or "").strip() or None
        return _live_cache_set(cache_key, (lat, lon))
    return _live_cache_set(cache_key, (None, None))


def _format_usd_price(value: object) -> str:
    if value in (None, ""):
        return ""
    amount = extract_total_price({"price": value})
    if amount is None:
        return ""
    return f"${amount.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)}"


# Registered domain controllers
_controllers = {
    TicketingController.domain: TicketingController(),
}
_phase3_scheduler_task: asyncio.Task | None = None
_phase3_scheduler_state: dict = {
    "running": False,
    "interval_minutes": None,
    "last_run_at": None,
    "last_result": None,
    "last_error": None,
}
_stubhub_rates_cache: dict[str, object] = {
    "updated_at": None,
    "rates": None,
}
_spothero_live_cache: dict[str, dict[str, object]] = {}
SPOTHERO_PUBLIC_HEADERS = {
    "User-Agent": CONFIG["app"]["default_user_agent"],
    "Accept-Language": "en-US,en;q=0.9",
}
_SPOTHERO_NEXT_DATA_RE = re.compile(
    r'<script[^>]*id="__NEXT_DATA__"[^>]*type="application/json"[^>]*>(?P<payload>.+?)</script>',
    re.IGNORECASE | re.DOTALL,
)
_SPOTHERO_EVENT_SCHEMA_RE = re.compile(
    r'<script[^>]*type="application/ld\+json"[^>]*>(?P<payload>.+?)</script>',
    re.IGNORECASE | re.DOTALL,
)


# ── Auth middleware ───────────────────────────────────────────────────────────

@app.middleware("http")
async def auth_middleware(request: Request, call_next):
    # Skip auth for health check and root
    if request.url.path in ["/healthz", "/", "/favicon.ico"]:
        return await call_next(request)

    auth_header = request.headers.get("authorization", "")
    parts = auth_header.split(" ", 1)
    token = parts[1] if len(parts) == 2 else None

    # Local development convenience: skip auth for localhost traffic.
    client_host = request.client.host if request.client else ""
    if (
        CONFIG["app"]["node_env"] == __NODE_ENV_DEV
        and client_host in {"127.0.0.1", "::1", "localhost"}
    ):
        return await call_next(request)

    expected = CONFIG["app"]["auth_token"]
    # Local/dev convenience: if no token is configured, skip auth checks.
    if not expected:
        return await call_next(request)

    if token != expected:
        return JSONResponse({"detail": "Forbidden"}, status_code=403)

    return await call_next(request)


# ── Routes ────────────────────────────────────────────────────────────────────

@app.get("/")
async def root():
    return {
        "message": "Event Parking Discovery API",
        "version": "1.1.0",
        "status": "ready"
    }


@app.get("/healthz")
async def healthz():
    return {"status": "ok", "timestamp": datetime.now(timezone.utc).isoformat()}


@app.post("/{domain}")
async def execute_domain(domain: str, request: Request):
    controller = _controllers.get(domain)
    if not controller:
        return JSONResponse(
            {"success": False, "error": f"Domain {domain} not found"},
            status_code=404,
        )
    return await controller.execute(request)


@app.get("/ticketing")
async def ticketing_get_help():
    return {
        "message": "StubHub Event-Parking Pipeline API",
        "canonical_endpoints": {
            "parking_only": "/ticketing/parking-only?parking_urls=<comma-separated URLs> (no venue/event discovery)",
            "discovery_run": "/ticketing/discovery/run",
            "parking_extract": "/ticketing/parking/extract",
            "monitoring_run": "/ticketing/monitoring/run",
            "monitoring_scheduler": "/ticketing/monitoring/scheduler?action=status",
            "pipeline_run": "/ticketing/pipeline/run",
            "venues_list": "/ticketing/venues",
            "venues_import": "POST /ticketing/venues/import",
            "venues_extract_from_har": "/ticketing/venues/extract-from-har?har_glob=*.har",
            "venues_extract_from_web": "/ticketing/venues/extract-from-web",
            "venues_scrape_and_sync_excel": "/ticketing/venues/scrape-and-sync-excel",
        },
        "legacy_compatible_endpoints": {
            "phase1_live": "/ticketing/phase1?dry_run=false",
            "phase2_parking": "/ticketing/phase2",
            "phase3_monitor": "/ticketing/phase3",
            "complete_stubhub": "/ticketing/stubhub/complete",
            "phase3_scheduler": "/ticketing/phase3/scheduler?action=status",
        },
        "raw_snapshot": "/ticketing/raw-snapshot",
        "raw_snapshot_har": "/ticketing/raw-snapshot-from-har",
        "normalize_snapshots": "/ticketing/normalize-snapshots",
        "price_changes": "/ticketing/price-changes",
    }


@app.get("/ticketing/demo")
async def ticketing_demo():
    return JSONResponse(
        {
            "success": False,
            "error": "Demo endpoint disabled. Use /ticketing/live or /ticketing/phase1 with dry_run=false.",
        },
        status_code=410,
    )


@app.get("/ticketing/live")
async def ticketing_live(
    venue_name: str = "Ad-hoc Venue",
    stubhub_url: str | None = None,
    handler: str = "stubhub-discovery",
):
    if not stubhub_url:
        return JSONResponse(
            {
                "success": False,
                "error": "stubhub_url is required for real-time live scraping.",
            },
            status_code=400,
        )

    venue = SimpleNamespace(
        name=venue_name,
        stubhub_url=stubhub_url,
        handler=handler,
        proxy=None,
        user_agent=None,
    )
    scraper_cls = TicketingController.get_scraper(handler)
    if not scraper_cls:
        return JSONResponse(
            {"success": False, "error": f"No scraper found for handler: {handler}"},
            status_code=404,
        )

    cluster = await PlaywrightClusterManager.get_or_create(venue.proxy)

    async def _task(page):
        instance = await scraper_cls.init(venue, page)
        return await asyncio.wait_for(
            instance.discover_events(venue, skip_persist=True),
            timeout=1800,
        )

    try:
        data = await cluster.execute(_task)
        return {
            "success": True,
            "data_source": "real_time_live_scrape",
            "processed_items": len(data),
            "data": data,
        }
    except Exception as exc:
        return JSONResponse({"success": False, "error": str(exc)}, status_code=500)


def _parse_comma_list(value: str | None) -> list[str]:
    if not value:
        return []
    return [v.strip() for v in value.split(",") if v.strip()]


def _safe_result_filename(name: str | None) -> str | None:
    if not name:
        return None
    n = Path(name).name
    if not n.endswith(".json"):
        return None
    return n


def _latest_search_result() -> Path | None:
    if not STORAGE_SEARCH_RESULTS.exists():
        return None
    candidates = sorted(STORAGE_SEARCH_RESULTS.glob("parking_links_*.json"), reverse=True)
    return candidates[0] if candidates else None


def _stubhub_browser_headers() -> dict[str, str]:
    return {
        "User-Agent": CONFIG["app"]["default_user_agent"],
        "Accept-Language": "en-US,en;q=0.9",
    }


async def _refresh_stubhub_usd_rates(force: bool = False) -> dict[str, str] | None:
    now = time.time()
    updated_at = _stubhub_rates_cache.get("updated_at")
    if not force and updated_at and (now - float(updated_at)) < 1800 and _stubhub_rates_cache.get("rates"):
        return _stubhub_rates_cache.get("rates")  # type: ignore[return-value]

    async with httpx.AsyncClient(
        follow_redirects=True,
        timeout=20.0,
        headers=_stubhub_browser_headers(),
    ) as client:
        response = await client.get("https://www.stubhub.com/secure/Browse/DefaultMaster/GetLocationSettings")
        response.raise_for_status()
        payload = response.json()

    currencies = payload.get("currencies") or []
    eur_rates: dict[str, float] = {}
    for item in currencies:
        try:
            code = str(item.get("code") or "").upper()
            rate = float(item.get("currentRate"))
        except Exception:
            continue
        if code and rate > 0:
            eur_rates[code] = rate

    usd_rate = eur_rates.get("USD")
    if not usd_rate:
        return None

    usd_multipliers = {code: (usd_rate / rate) for code, rate in eur_rates.items() if rate > 0}
    usd_multipliers["USD"] = 1.0
    set_usd_exchange_rates(usd_multipliers)
    _stubhub_rates_cache["updated_at"] = now
    _stubhub_rates_cache["rates"] = {k: f"{v:.12f}" for k, v in usd_multipliers.items()}
    return _stubhub_rates_cache.get("rates")  # type: ignore[return-value]


def _canonical_stubhub_url(url: str | None) -> str | None:
    normalized = _normalize_stubhub_url(url)
    if not normalized:
        return None
    parts = urlsplit(normalized)
    return f"{parts.scheme}://{parts.netloc}{parts.path}"


async def _fetch_stubhub_search_index(query: str, page: int = 0) -> dict:
    search_query = (query or "").strip()
    if not search_query:
        raise ValueError("query is required")
    page = max(0, int(page or 0))

    async with httpx.AsyncClient(
        follow_redirects=True,
        timeout=30.0,
        headers=_stubhub_browser_headers(),
    ) as client:
        response = await client.get(
            "https://www.stubhub.com/secure/search/",
            params={"q": search_query, "page": page},
        )
        response.raise_for_status()

    match = re.search(
        r'<script id="index-data" type="application/json">(.*?)</script>',
        response.text,
        re.DOTALL,
    )
    if not match:
        raise ValueError("StubHub search response did not include index-data JSON.")

    try:
        return json.loads(html.unescape(match.group(1)))
    except Exception as exc:
        raise ValueError("Failed to parse StubHub search results.") from exc


async def _fetch_stubhub_index_from_url(url: str, page: int = 0) -> dict:
    target_url = _normalize_stubhub_url(url)
    if not target_url:
        raise ValueError("valid StubHub url is required")

    parts = urlsplit(target_url)
    params = dict(parse_qsl(parts.query, keep_blank_values=True))
    if page > 0:
        params["page"] = str(page)
    elif "page" in params:
        params.pop("page", None)
    request_url = urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(params), parts.fragment))

    async with httpx.AsyncClient(
        follow_redirects=True,
        timeout=30.0,
        headers=_stubhub_browser_headers(),
    ) as client:
        response = await client.get(request_url)
        response.raise_for_status()

    match = re.search(
        r'<script id="index-data" type="application/json">(.*?)</script>',
        response.text,
        re.DOTALL,
    )
    if not match:
        raise ValueError("StubHub page did not include index-data JSON.")

    try:
        return json.loads(html.unescape(match.group(1)))
    except Exception as exc:
        raise ValueError("Failed to parse StubHub page index-data.") from exc


def _stubhub_search_grid_items(index_data: dict) -> list[dict]:
    grids = index_data.get("eventGrids") or {}
    items: list[dict] = []
    if isinstance(grids, dict):
        for grid in grids.values():
            if isinstance(grid, dict):
                raw_items = grid.get("items") or []
                if isinstance(raw_items, list):
                    items.extend([item for item in raw_items if isinstance(item, dict)])
    return items


def _stubhub_item_to_selection(item: dict, source: str) -> dict:
    raw_url = item.get("url")
    absolute_url = _normalize_stubhub_url(urljoin("https://www.stubhub.com", raw_url or ""))
    canonical_url = _canonical_stubhub_url(absolute_url)
    return {
        "title": item.get("title") or item.get("name") or "",
        "subtitle": item.get("subtitle") or "",
        "name": item.get("name") or item.get("title") or "",
        "url": absolute_url,
        "canonical_url": canonical_url,
        "source": source,
        "selection_type": _stubhub_selection_type(canonical_url),
        "location": item.get("formattedVenueLocation") or "",
        "venue_name": item.get("venueName") or "",
        "venueName": item.get("venueName") or "",
        "date": item.get("formattedDate") or "",
        "time": item.get("formattedTime") or "",
        "dayOfWeek": item.get("dayOfWeek") or "",
        "formattedDate": item.get("formattedDate") or "",
        "formattedTime": item.get("formattedTime") or "",
        "formattedVenueLocation": item.get("formattedVenueLocation") or "",
        "eventMetadata": item.get("eventMetadata") or {},
        "is_parking_event": bool(item.get("isParkingEvent")),
        "relationship_count": item.get("numberRelationships"),
    }


async def _fetch_stubhub_search_grid_items_paginated(query: str, max_pages: int = 5) -> list[dict]:
    seen: set[str] = set()
    combined: list[dict] = []

    for page in range(max(1, max_pages)):
        index_data = await _fetch_stubhub_search_index(query, page=page)
        page_items = _stubhub_search_grid_items(index_data)
        if not page_items:
            break

        new_count = 0
        for item in page_items:
            canonical = _canonical_stubhub_url(item.get("url"))
            dedupe_key = canonical or json.dumps(item, sort_keys=True, default=str)
            if dedupe_key in seen:
                continue
            seen.add(dedupe_key)
            combined.append(item)
            new_count += 1

        if new_count == 0 or len(page_items) < 20:
            break

    return combined


async def _fetch_stubhub_schedule_items(selection_url: str, max_pages: int = 6) -> list[dict]:
    seen: set[str] = set()
    combined: list[dict] = []

    for page in range(max(1, max_pages)):
        try:
            index_data = await _fetch_stubhub_index_from_url(selection_url, page=page)
        except Exception:
            break
        page_items = _stubhub_search_grid_items(index_data)
        if not page_items:
            break

        new_count = 0
        for item in page_items:
            selection = _stubhub_item_to_selection(item, source="event")
            canonical = selection.get("canonical_url")
            if not canonical or selection.get("is_parking_event"):
                continue
            if canonical in seen:
                continue
            seen.add(canonical)
            combined.append(selection)
            new_count += 1

        if new_count == 0 or len(page_items) < 20:
            break

    if combined:
        return combined
    return await _fetch_stubhub_schedule_items_via_playwright(selection_url)


def _formatted_date_from_iso(value: str | None) -> str:
    if not value:
        return ""
    try:
        parsed = date.fromisoformat(value)
    except Exception:
        return value or ""
    return parsed.strftime("%b %d")


async def _fetch_stubhub_schedule_items_via_playwright(selection_url: str, max_scrolls: int = 18) -> list[dict]:
    async def _task(page):
        await page.goto(selection_url, wait_until="commit", timeout=60000)
        await asyncio.sleep(2)
        stable_rounds = 0
        last_count = -1
        for _ in range(max_scrolls):
            try:
                await page.evaluate(
                    """() => {
                        window.scrollTo(0, document.body.scrollHeight);
                        const containers = Array.from(document.querySelectorAll('div, section, main'))
                          .filter((el) => el.scrollHeight > (el.clientHeight + 40))
                          .slice(0, 10);
                        for (const el of containers) {
                          try { el.scrollTop = el.scrollHeight; } catch (e) {}
                        }
                    }"""
                )
            except Exception:
                pass
            await asyncio.sleep(1)
            try:
                current_count = await page.locator("a[href*='/event/']").count()
            except Exception:
                current_count = 0
            if current_count <= last_count:
                stable_rounds += 1
            else:
                stable_rounds = 0
            last_count = max(last_count, current_count)
            if stable_rounds >= 4:
                break

        return await page.evaluate(
            """() => Array.from(document.querySelectorAll("a[href*='/event/']")).map((a) => {
                const href = a.href || a.getAttribute('href') || '';
                const text = (a.innerText || a.textContent || '').trim();
                const container = a.closest('article, li, section, div');
                const context = (container?.innerText || '').trim();
                return { href, text, context };
            })"""
        )

    cluster = await PlaywrightClusterManager.get_or_create(None)
    raw_items = await cluster.execute(_task)
    if not isinstance(raw_items, list):
        return []

    results: list[dict] = []
    seen: set[str] = set()
    for item in raw_items:
        if not isinstance(item, dict):
            continue
        canonical = _canonical_stubhub_url(item.get("href"))
        if not canonical or canonical in seen or "/event/" not in canonical:
            continue
        seen.add(canonical)
        results.append(
            {
                "title": _derive_event_name_from_url(canonical),
                "name": _derive_event_name_from_url(canonical),
                "url": canonical,
                "canonical_url": canonical,
                "source": "event",
                "selection_type": "event",
                "location": "",
                "venue_name": "",
                "venueName": "",
                "date": _formatted_date_from_iso(_derive_event_date_from_url(canonical)),
                "time": "",
                "dayOfWeek": "",
                "formattedDate": _formatted_date_from_iso(_derive_event_date_from_url(canonical)),
                "formattedTime": "",
                "formattedVenueLocation": "",
                "eventMetadata": {},
                "is_parking_event": False,
                "relationship_count": None,
            }
        )

    results.sort(key=lambda item: (item.get("formattedDate") or "", item.get("title") or ""))
    return results


def _normalized_date_key(value: str | None) -> str:
    text = (value or "").strip()
    if not text:
        return ""
    m = re.search(r"([A-Za-z]{3})\s+0?(\d{1,2})", text)
    if not m:
        return re.sub(r"\s+", " ", text).lower()
    return f"{m.group(1).lower()}-{int(m.group(2))}"


def _normalized_location_key(value: str | None) -> str:
    text = (value or "").strip().lower()
    if not text:
        return ""
    city = text.split(",", 1)[0]
    return re.sub(r"[^a-z0-9]+", "", city)


def _parking_candidate_score(candidate: dict, event_item: dict) -> int:
    score = 0
    if _normalized_date_key(candidate.get("formattedDate")) == _normalized_date_key(event_item.get("formattedDate") or event_item.get("date")):
        score += 5
    if _normalized_location_key(candidate.get("formattedVenueLocation")) == _normalized_location_key(event_item.get("formattedVenueLocation") or event_item.get("location")):
        score += 4

    candidate_venue = re.sub(r"[^a-z0-9]+", "", str(candidate.get("venueName") or "").lower())
    event_venue = re.sub(r"[^a-z0-9]+", "", str(event_item.get("venueName") or event_item.get("venue_name") or "").lower())
    if candidate_venue and event_venue and candidate_venue == event_venue:
        score += 4

    candidate_title = str(candidate.get("title") or candidate.get("name") or "").lower()
    event_title = str(event_item.get("title") or event_item.get("name") or "").lower()
    for token in [t for t in re.split(r"[^a-z0-9]+", event_title) if len(t) > 2]:
        if token in candidate_title:
            score += 1
    if "parking passes only" in candidate_title:
        score += 2
    return score


async def _resolve_parking_candidate_for_event(event_item: dict) -> dict | None:
    query = _build_parking_query_from_selection(
        {
            "title": event_item.get("title") or event_item.get("name") or "",
            "location": event_item.get("formattedVenueLocation") or event_item.get("location") or "",
            "venue_name": event_item.get("venueName") or event_item.get("venue_name") or "",
            "source": "event",
        }
    )
    search_items = await _fetch_stubhub_search_grid_items_paginated(query, max_pages=2)
    parking_candidates = []
    for item in search_items:
        if not item.get("isParkingEvent"):
            continue
        selection = _stubhub_item_to_selection(item, source="event")
        if selection.get("canonical_url"):
            parking_candidates.append(selection)

    if not parking_candidates:
        return None

    parking_candidates.sort(key=lambda item: _parking_candidate_score(item, event_item), reverse=True)
    best = parking_candidates[0]
    return best if _parking_candidate_score(best, event_item) > 0 else None


async def _fetch_expanded_schedule_items_from_queries(title: str, max_events: int = 120) -> list[dict]:
    clean_title = (title or "").strip()
    if not clean_title:
        return []

    month_tokens = ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"]
    query_years = [date.today().year, date.today().year + 1]
    queries = [clean_title]
    for year in query_years:
        for month in month_tokens:
            queries.append(f"{clean_title} {month} {year}")

    semaphore = asyncio.Semaphore(6)

    async def _load_query(query: str) -> list[dict]:
        async with semaphore:
            try:
                return await _fetch_stubhub_search_grid_items_paginated(query, max_pages=2)
            except Exception:
                return []

    seen: set[str] = set()
    events: list[dict] = []
    for items in await asyncio.gather(*[_load_query(query) for query in queries]):
        for item in items:
            if item.get("isParkingEvent"):
                continue
            selection = _stubhub_item_to_selection(item, source="event")
            canonical = selection.get("canonical_url")
            if not canonical or canonical in seen:
                continue
            seen.add(canonical)
            events.append(selection)
            if len(events) >= max_events:
                return events
    return events


async def _fetch_expanded_parking_candidates_from_queries(title: str, max_events: int = 120) -> list[dict]:
    clean_title = (title or "").strip()
    if not clean_title:
        return []

    month_tokens = ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"]
    query_years = [date.today().year, date.today().year + 1]
    queries = [f"parking passes only {clean_title}"]
    for year in query_years:
        for month in month_tokens:
            queries.append(f"parking passes only {clean_title} {month} {year}")

    semaphore = asyncio.Semaphore(6)

    async def _load_query(query: str) -> list[dict]:
        async with semaphore:
            try:
                return await _fetch_stubhub_search_grid_items_paginated(query, max_pages=3)
            except Exception:
                return []

    seen: set[str] = set()
    parking_events: list[dict] = []
    for items in await asyncio.gather(*[_load_query(query) for query in queries]):
        for item in items:
            if not item.get("isParkingEvent"):
                continue
            selection = _stubhub_item_to_selection(item, source="event")
            canonical = selection.get("canonical_url")
            if not canonical or canonical in seen:
                continue
            seen.add(canonical)
            parking_events.append(selection)
            if len(parking_events) >= max_events:
                return parking_events
    return parking_events


async def _resolve_client_search_parking_candidates(payload: dict, max_events: int) -> list[dict]:
    selection_type = str(payload.get("selection_type") or "").lower()
    selection_url = payload.get("canonical_url") or payload.get("url")
    relationship_count = payload.get("relationship_count")

    if selection_type not in {"performer", "grouping", "category", "venue"} or not selection_url:
        parking_query = _build_parking_query_from_selection(payload)
        return [
            _stubhub_item_to_selection(item, source="event")
            for item in await _fetch_stubhub_search_grid_items_paginated(
                parking_query,
                max_pages=max(4, min(12, (max_events + 19) // 20 + 2)),
            )
            if item.get("isParkingEvent") and _canonical_stubhub_url(item.get("url"))
        ][:max_events]

    expanded = await _fetch_expanded_parking_candidates_from_queries(
        payload.get("title") or payload.get("query") or "",
        max_events=max(240, max_events * 2),
    )
    if expanded:
        return expanded[:max_events]

    parking_query = _build_parking_query_from_selection(payload)
    return [
        _stubhub_item_to_selection(item, source="event")
        for item in await _fetch_stubhub_search_grid_items_paginated(parking_query, max_pages=12)
        if item.get("isParkingEvent") and _canonical_stubhub_url(item.get("url"))
    ][:max_events]


def _stubhub_selection_type(url: str | None) -> str:
    value = (url or "").lower()
    if "/performer/" in value:
        return "performer"
    if "/venue/" in value:
        return "venue"
    if "/grouping/" in value:
        return "grouping"
    if "/category/" in value:
        return "category"
    if "/event/" in value:
        return "event"
    return "unknown"


def _build_client_search_suggestions(index_data: dict, limit: int = 12) -> list[dict]:
    suggestions: list[dict] = []
    seen: set[str] = set()

    def add_suggestion(item: dict, source: str) -> None:
        raw_url = item.get("url")
        absolute_url = _normalize_stubhub_url(urljoin("https://www.stubhub.com", raw_url or ""))
        canonical_url = _canonical_stubhub_url(absolute_url)
        if not canonical_url or canonical_url in seen:
            return

        selection_type = _stubhub_selection_type(canonical_url)
        if selection_type == "unknown":
            return

        suggestion = {
            "title": item.get("title") or item.get("name") or "",
            "subtitle": item.get("subtitle") or "",
            "url": absolute_url,
            "canonical_url": canonical_url,
            "source": source,
            "selection_type": selection_type,
            "location": item.get("formattedVenueLocation") or "",
            "venue_name": item.get("venueName") or "",
            "date": item.get("formattedDate") or "",
            "time": item.get("formattedTime") or "",
            "is_parking_event": bool(item.get("isParkingEvent")),
            "relationship_count": item.get("numberRelationships"),
        }
        if not suggestion["title"]:
            return
        seen.add(canonical_url)
        suggestions.append(suggestion)

    for item in (index_data.get("topSearchResults") or {}).get("searchResults", []) or []:
        if len(suggestions) >= limit:
            break
        if isinstance(item, dict):
            add_suggestion(item, "top")

    if len(suggestions) < limit:
        for item in _stubhub_search_grid_items(index_data):
            if len(suggestions) >= limit:
                break
            if isinstance(item, dict) and not item.get("isParkingEvent"):
                add_suggestion(item, "event")

    return suggestions[:limit]


def _build_parking_query_from_selection(payload: dict) -> str:
    title = (payload.get("title") or payload.get("query") or "").strip()
    location = (payload.get("location") or "").strip()
    venue_name = (payload.get("venue_name") or "").strip()
    source = (payload.get("source") or "").strip().lower()

    if not title:
        raise ValueError("title or query is required")

    query_parts = ["parking passes only", title]
    if source == "event":
        city = location.split(",", 1)[0].strip() if location else ""
        if city and city.lower() not in title.lower():
            query_parts.append(city)
        elif venue_name and venue_name.lower() not in title.lower():
            query_parts.append(venue_name)

    return " ".join(part for part in query_parts if part).strip()


def _price_display_for_row(row: dict) -> str:
    metrics = compute_listing_metrics(row)
    normalized_price = metrics.get("extracted_price")
    if normalized_price not in (None, ""):
        try:
            return f"${Decimal(str(normalized_price)).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)}"
        except Exception:
            return f"${normalized_price}"

    details = row.get("listing_details") if isinstance(row.get("listing_details"), dict) else {}
    raw_price = str(details.get("price_incl_fees") or "").strip()
    if raw_price:
        return raw_price
    return ""


def _clean_listing_notes(value: str | None, lot_name: str | None = None) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if text.startswith("[{") or text.startswith("{'") or text.startswith('{"'):
        return ""
    if lot_name and text.startswith(lot_name):
        text = text[len(lot_name):].strip()
    text = re.sub(r"(?<=\w)(\d+\s+pass(?:es)?)", r" \1", text, flags=re.IGNORECASE)
    text = re.sub(r"(?<!^)(Over a )", r" | \1", text)
    text = re.sub(r"(?<!^)(Buyer could receive)", r" | \1", text)
    text = re.sub(r"(?<!^)(Last pass(?:es)?)", r" | \1", text, flags=re.IGNORECASE)
    text = re.sub(r"(?<!^)(Amazing|Great|Good)", r" | \1", text)
    text = re.sub(r"\s+", " ", text).strip(" |")
    return text


def _clean_stubhub_text(value: str | None) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if text.lower() == "null":
        return ""
    if text.startswith("[{") or text.startswith("{'") or text.startswith('{"'):
        return ""
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _is_generic_stubhub_lot_name(value: str | None) -> bool:
    text = _clean_stubhub_text(value)
    return bool(re.fullmatch(r"(Section|Listing)\s+\d+", text, flags=re.IGNORECASE))


def _is_unusable_stubhub_lot_name(value: str | None) -> bool:
    text = _clean_stubhub_text(value)
    if not text:
        return True
    if _is_generic_stubhub_lot_name(text):
        return True
    if re.fullmatch(r"Parking(?: listing)?", text, flags=re.IGNORECASE):
        return True
    if re.fullmatch(r"[0-9]+% off", text, flags=re.IGNORECASE):
        return True
    if re.fullmatch(r"(Within\s+[0-9.]+\s+Mile(?:s)?\s+Of\s+Venue|Within\s+[0-9.]+\s+mi)", text, flags=re.IGNORECASE):
        return True
    return False


def _choose_stubhub_lot_name(row: dict, details: dict) -> str:
    candidates = [
        row.get("lot_name"),
        details.get("title"),
        details.get("lot_name"),
        details.get("name"),
    ]
    for candidate in candidates:
        text = _clean_stubhub_text(candidate)
        if not text:
            continue
        if re.search(r"\bprice per pass\b", text, flags=re.IGNORECASE):
            continue
        text = re.sub(r"\s*-\s*[0-9.]+\s+mi(?:les?)?\s+(?:from venue|away)\b", "", text, flags=re.IGNORECASE)
        text = re.sub(r"\s*-\s*within\s+[0-9.]+\s+mi(?:les?)?\b", "", text, flags=re.IGNORECASE)
        text = text.strip(" -")
        if not text:
            continue
        if re.fullmatch(r"\$?\d[\d,.]*(?:\.\d{1,2})?\+?", text):
            continue
        if re.fullmatch(r"\d+\s*-\s*\d+\s+passes?", text, flags=re.IGNORECASE):
            continue
        if re.fullmatch(r"\d+\s+passes?", text, flags=re.IGNORECASE):
            continue
        if re.fullmatch(r"(Within\s+[0-9.]+\s+Mile(?:s)?\s+Of\s+Venue|Within\s+[0-9.]+\s+mi)", text, flags=re.IGNORECASE):
            continue
        if _is_generic_stubhub_lot_name(text):
            continue
        return text
    return ""


def _group_client_search_results(search_items: list[dict], rows: list[dict]) -> list[dict]:
    meta_by_url: dict[str, dict] = {}
    for item in search_items:
        canonical = _canonical_stubhub_url(item.get("url"))
        if canonical:
            meta_by_url[canonical] = item

    grouped: dict[str, dict] = {}
    for item in search_items:
        canonical = _canonical_stubhub_url(item.get("url"))
        if not canonical:
            continue
        grouped.setdefault(
            canonical,
            {
                "event_name": item.get("name") or item.get("title") or "",
                "day_of_week": item.get("dayOfWeek") or "",
                "formatted_date": item.get("formattedDate") or item.get("date") or "",
                "formatted_time": item.get("formattedTime") or item.get("time") or "",
                "venue_name": item.get("venueName") or item.get("venue_name") or "",
                "location": item.get("formattedVenueLocation") or item.get("location") or "",
                "parking_url": canonical,
                "listing_count": 0,
                "sort_ts": (((item.get("eventMetadata") or {}).get("common") or {}).get("eventStartDateTime") or 0),
                "listings": [],
            },
        )

    for row in rows:
        canonical = _canonical_stubhub_url(row.get("parking_url") or row.get("event_url"))
        if not canonical:
            continue
        meta = meta_by_url.get(canonical, {})
        details = row.get("listing_details") if isinstance(row.get("listing_details"), dict) else {}
        lot_name = _choose_stubhub_lot_name(row, details)
        availability = _clean_stubhub_text(row.get("availability") or details.get("availability"))
        if _is_unusable_stubhub_lot_name(lot_name):
            continue
        group = grouped.setdefault(
            canonical,
            {
                "event_name": meta.get("name") or row.get("event_name") or "",
                "day_of_week": meta.get("dayOfWeek") or "",
                "formatted_date": meta.get("formattedDate") or row.get("event_date") or "",
                "formatted_time": meta.get("formattedTime") or "",
                "venue_name": meta.get("venueName") or "",
                "location": meta.get("formattedVenueLocation") or "",
                "parking_url": canonical,
                "listing_count": 0,
                "sort_ts": (((meta.get("eventMetadata") or {}).get("common") or {}).get("eventStartDateTime") or 0),
                "listings": [],
            },
        )
        group["listings"].append(
            {
                "lot_name": lot_name,
                "availability": availability,
                "price_display": _price_display_for_row(row),
                "price_value": row.get("price"),
                "rating": _clean_stubhub_text(details.get("rating")),
                "notes": _clean_listing_notes(details.get("notes"), lot_name),
                "listing_id": row.get("listing_id"),
                "raw_details": row.get("listing_details"),
            }
        )

    results = []
    for group in grouped.values():
        group["listings"].sort(
            key=lambda item: float(item["price_value"]) if str(item.get("price_value") or "").replace(".", "", 1).isdigit() else float("inf")
        )
        group["listing_count"] = len(group["listings"])
        results.append(group)

    results.sort(key=lambda item: (item.get("sort_ts") or 0, item.get("event_name") or ""))
    for item in results:
        item.pop("sort_ts", None)
    return results


def _client_search_payload_from_request_payload(payload: dict) -> dict:
    return {
        "title": (payload.get("title") or payload.get("query") or "").strip(),
        "query": (payload.get("query") or payload.get("title") or "").strip(),
        "source": payload.get("source") or "",
        "selection_type": payload.get("selection_type") or "",
        "canonical_url": payload.get("canonical_url") or payload.get("url") or "",
        "url": payload.get("url") or payload.get("canonical_url") or "",
        "relationship_count": payload.get("relationship_count"),
        "location": payload.get("location") or "",
        "venue_name": payload.get("venue_name") or payload.get("venueName") or "",
    }


def _build_spothero_queries_from_selection(payload: dict) -> list[str]:
    title = str(payload.get("title") or payload.get("query") or "").strip()
    venue_name = str(payload.get("venue_name") or "").strip()
    location = str(payload.get("location") or "").strip()
    city = location.split(",", 1)[0].strip() if location else ""

    candidates = [
        venue_name,
        title,
        f"{venue_name} {city}".strip(),
        f"{title} {city}".strip(),
    ]
    if " at " in title.lower():
        candidates.append(re.split(r"\bat\b", title, flags=re.IGNORECASE)[-1].strip())

    ordered: list[str] = []
    seen: set[str] = set()
    for item in candidates:
        normalized = _normalize_live_search_text(item)
        if len(normalized) < 2 or normalized in seen:
            continue
        seen.add(normalized)
        ordered.append(item.strip())
    return ordered


async def _pick_best_spothero_suggestion(payload: dict) -> tuple[dict | None, list[dict], str]:
    query_candidates = _build_spothero_queries_from_selection(payload)
    all_suggestions: list[dict] = []
    best: dict | None = None
    best_score = -1
    best_query = ""

    venue_name = str(payload.get("venue_name") or "").strip()
    title = str(payload.get("title") or payload.get("query") or "").strip()
    location = str(payload.get("location") or "").strip()

    for query in query_candidates:
        suggestions = await _live_destination_suggestions(query)
        for suggestion in suggestions:
            enriched = dict(suggestion)
            composite_score = (
                int(enriched.get("match_score") or 0)
                + _score_live_match(venue_name or query, enriched.get("title") or "")
                + _score_live_match(title or query, enriched.get("title") or "")
                + _score_live_match(location or query, enriched.get("city") or "")
            )
            enriched["composite_score"] = composite_score
            enriched["query_used"] = query
            all_suggestions.append(enriched)
            if composite_score > best_score:
                best = enriched
                best_score = composite_score
                best_query = query

    deduped: list[dict] = []
    seen: set[tuple] = set()
    for suggestion in sorted(
        all_suggestions,
        key=lambda item: (-int(item.get("composite_score") or 0), item.get("city") or "", item.get("title") or ""),
    ):
        key = (suggestion.get("destination_id"), suggestion.get("path"))
        if key in seen:
            continue
        seen.add(key)
        deduped.append(suggestion)
    return best, deduped[:6], best_query


def _pick_best_spothero_event(events: list[dict], payload: dict) -> dict | None:
    if not events:
        return None
    title = str(payload.get("title") or payload.get("query") or "").strip()
    venue_name = str(payload.get("venue_name") or "").strip()
    location = str(payload.get("location") or "").strip()
    best: dict | None = None
    best_score = -1
    for event in events:
        if not isinstance(event, dict):
            continue
        name = str(event.get("name") or "").strip()
        url = str(event.get("url") or "").strip()
        if not name or not url:
            continue
        score = (
            _score_live_match(title, name)
            + _score_live_match(venue_name, name)
            + _score_live_match(location, name)
        )
        if score > best_score:
            best = event
            best_score = score
    return best if best_score > 0 else None


async def _run_spothero_for_event(event_row: dict) -> dict:
    event_obj = build_spothero_event(
        venue_name=event_row.get("venue") or "SpotHero Venue",
        event_name=event_row.get("event_name") or "SpotHero Parking",
        event_url=event_row.get("event_url") or event_row.get("parking_url") or "",
        parking_url=event_row.get("parking_url") or event_row.get("event_url") or "",
        max_parking_coverage=bool(event_row.get("max_parking_coverage")),
    )
    venue = SimpleNamespace(
        name=event_row.get("venue") or "SpotHero Venue",
        stubhub_url=event_row.get("event_url") or event_row.get("parking_url") or "",
        handler="spothero-parking",
        proxy=None,
        user_agent=None,
    )

    async def _task(page):
        instance = await SpotHeroParkingScraper.init(venue, page)
        passes = await asyncio.wait_for(instance.scrape_parking_details(event_obj), timeout=120)
        return {"passes": passes}

    cluster = await PlaywrightClusterManager.get_or_create(None)
    return await cluster.execute(_task)


def _spothero_listing_from_pass(row: dict) -> dict:
    details = row.get("listing_details") if isinstance(row.get("listing_details"), dict) else {}
    total_display = details.get("price_incl_fees") or _format_usd_price(row.get("price"))
    subtotal_display = details.get("subtotal_display") or ""
    fee_display = details.get("service_fee_display") or ""
    notes = " • ".join(part for part in [f"Subtotal {subtotal_display}" if subtotal_display else "", f"Fees {fee_display}" if fee_display else ""] if part)
    price_value = extract_total_price({"price": total_display or row.get("price")})
    return {
        "lot_name": row.get("lot_name") or "",
        "normalized_lot_name": row.get("normalized_lot_name") or normalize_lot_name(row.get("lot_name") or ""),
        "availability": row.get("availability") or "",
        "price_display": total_display or "",
        "price_value": float(price_value) if price_value is not None else None,
        "currency": "USD",
        "notes": notes,
        "listing_id": row.get("listing_id") or "",
        "available_spaces": row.get("available_spaces"),
        "reservation_type": row.get("reservation_type") or details.get("reservation_type") or "",
        "reservation_duration": row.get("reservation_duration") or details.get("reservation_duration") or "",
        "reservation_starts": row.get("reservation_starts") or details.get("reservation_starts"),
        "reservation_ends": row.get("reservation_ends") or details.get("reservation_ends"),
        "in_out_policy": row.get("in_out_policy") or details.get("in_out_policy") or "",
        "source": row.get("_source") or "spothero_api",
    }


def _format_spothero_window_label(starts: str | None, ends: str | None) -> str:
    def _fmt(value: str | None) -> str:
        text = str(value or "").strip()
        if not text:
            return ""
        try:
            normalized = text.replace("Z", "+00:00")
            dt = datetime.fromisoformat(normalized)
            return dt.strftime("%b %d, %Y %I:%M %p")
        except Exception:
            return text

    start_label = _fmt(starts)
    end_label = _fmt(ends)
    if start_label and end_label:
        return f"{start_label} to {end_label}"
    return start_label or end_label


async def _fetch_live_spothero_destination_details(destination_id: int, destination_path: str, selection_payload: dict | None = None) -> dict:
    destination_url = destination_path if destination_path.startswith("http") else f"https://spothero.com{destination_path}"
    raw = await _fetch_text(destination_url, ttl_seconds=300)
    next_data = _extract_next_data(raw)
    page_props = ((next_data.get("props") or {}).get("pageProps") or {})
    events = [item for item in _extract_event_schemas(raw) if isinstance(item, dict)]

    destination_title = None
    city_name = None
    state_code = None
    address_bits: list[str] = []
    event_urls: list[str] = []

    for event_schema in events:
        location = event_schema.get("location") if isinstance(event_schema.get("location"), dict) else {}
        address = location.get("address") if isinstance(location.get("address"), dict) else {}
        if not destination_title:
            destination_title = (
                address.get("name")
                or location.get("name")
                or event_schema.get("name")
            )
            city_name = address.get("addressLocality")
            state_code = address.get("addressRegion")
            address_bits = [
                str(address.get("streetAddress") or "").strip(),
                str(address.get("addressLocality") or "").strip(),
                str(address.get("addressRegion") or "").strip(),
                str(address.get("postalCode") or "").strip(),
            ]
        url = str(event_schema.get("url") or "").strip()
        if url and "spothero.com" in url:
            event_urls.append(url)

    search_request = page_props.get("searchRequest") if isinstance(page_props.get("searchRequest"), dict) else {}
    window_starts = str(search_request.get("starts") or "").strip() or None
    window_ends = str(search_request.get("ends") or "").strip() or None
    airport_payload = page_props.get("airport") if isinstance(page_props.get("airport"), dict) else {}
    if airport_payload:
        destination_title = destination_title or str(airport_payload.get("title") or "").strip() or None
        city_name = city_name or str(airport_payload.get("city") or "").strip() or None
        state_code = state_code or str(airport_payload.get("state") or "").strip() or None
        if not address_bits:
            address_bits = [
                str(airport_payload.get("street_address") or "").strip(),
                str(airport_payload.get("city") or "").strip(),
                str(airport_payload.get("state") or "").strip(),
                str(airport_payload.get("zipcode") or "").strip(),
            ]

    address_text = ", ".join(bit for bit in address_bits if bit)
    lat = lon = None
    if airport_payload.get("latitude") is not None and airport_payload.get("longitude") is not None:
        lat = str(airport_payload.get("latitude"))
        lon = str(airport_payload.get("longitude"))
    elif address_text:
        lat, lon = await _geocode_address(address_text)

    listing_rows: list[dict] = []
    price_source_url = None
    detail_mode = "destination"

    if lat and lon:
        params = {
            "kind": "destination",
            "id": str(destination_id),
            "lat": lat,
            "lon": lon,
        }
        if search_request.get("starts"):
            params["starts"] = str(search_request.get("starts"))
        if search_request.get("ends"):
            params["ends"] = str(search_request.get("ends"))
        destination_search_url = f"https://spothero.com/search?{urlencode(params)}"
        price_source_url = destination_search_url
        live_result = await _run_spothero_for_event(
            {
                "venue": destination_title or "SpotHero Destination",
                "event_name": f"Parking near {destination_title or 'destination'}",
                "event_url": destination_url,
                "parking_url": destination_search_url,
            }
        )
        listing_rows = [_spothero_listing_from_pass(item) for item in live_result.get("passes") or []]

    matched_event = _pick_best_spothero_event(events, selection_payload or {})
    if matched_event:
        detail_mode = "matched_event"
        matched_event_url = str(matched_event.get("url") or "").strip()
        price_source_url = matched_event_url
        live_result = await _run_spothero_for_event(
            {
                "venue": destination_title or "SpotHero Destination",
                "event_name": str(matched_event.get("name") or destination_title or "SpotHero Event"),
                "event_url": matched_event_url,
                "parking_url": matched_event_url,
            }
        )
        event_rows = [_spothero_listing_from_pass(item) for item in live_result.get("passes") or []]
        if event_rows:
            listing_rows = event_rows

    if not listing_rows and event_urls:
        detail_mode = "event_fallback"
        fallback_event_url = event_urls[0]
        price_source_url = fallback_event_url
        live_result = await _run_spothero_for_event(
            {
                "venue": destination_title or "SpotHero Destination",
                "event_name": str(events[0].get("name") or destination_title or "SpotHero Event"),
                "event_url": fallback_event_url,
                "parking_url": fallback_event_url,
            }
        )
        listing_rows = [_spothero_listing_from_pass(item) for item in live_result.get("passes") or []]

    if not listing_rows:
        raise ValueError("Unable to fetch live SpotHero listings for this destination")

    listing_rows.sort(
        key=lambda item: (
            item.get("price_value") is None,
            item.get("price_value") if item.get("price_value") is not None else float("inf"),
            item.get("lot_name") or "",
        )
    )
    numeric_prices = [item.get("price_value") for item in listing_rows if item.get("price_value") is not None]
    parking_window_label = _format_spothero_window_label(window_starts, window_ends)
    for item in listing_rows:
        if parking_window_label:
            item["price_window"] = parking_window_label
        if window_starts:
            item["price_window_start"] = window_starts
        if window_ends:
            item["price_window_end"] = window_ends
    return {
        "destination_id": destination_id,
        "destination_title": destination_title or str(((page_props.get("query") or {}).get("destination") or "")).replace("-parking", "").replace("-", " ").title(),
        "city": city_name,
        "state": state_code,
        "address": address_text,
        "destination_page_url": destination_url,
        "price_source_url": price_source_url,
        "detail_mode": detail_mode,
        "coordinates": {"lat": lat, "lon": lon} if lat and lon else None,
        "listing_summary": {
            "count": len(listing_rows),
            "min_price": min(numeric_prices) if numeric_prices else None,
            "max_price": max(numeric_prices) if numeric_prices else None,
        },
        "matched_event": matched_event,
        "price_window": parking_window_label,
        "price_window_start": window_starts,
        "price_window_end": window_ends,
        "listings": listing_rows,
        "upcoming_events": [
            {
                "name": str(item.get("name") or "").strip(),
                "starts_at": item.get("startDate"),
                "url": str(item.get("url") or "").strip(),
            }
            for item in events[:6]
            if str(item.get("name") or "").strip() and str(item.get("url") or "").strip()
        ],
    }


@app.get("/ui/parking-links", response_class=HTMLResponse)
async def ui_parking_links():
    page = UI_DIR / "parking_links.html"
    if not page.exists():
        return HTMLResponse(
            "<h2>UI not found</h2><p>Missing file: python-src/ui/parking_links.html</p>",
            status_code=500,
        )
    return HTMLResponse(page.read_text(encoding="utf-8"))


@app.get("/ticketing/ui/client-search", response_class=HTMLResponse)
async def ui_client_search():
    page = UI_DIR / "client_search.html"
    if not page.exists():
        return HTMLResponse(
            "<h2>UI not found</h2><p>Missing file: python-src/ui/client_search.html</p>",
            status_code=500,
        )
    return HTMLResponse(page.read_text(encoding="utf-8"))


@app.get("/ticketing/ui/client-search/suggest")
async def ui_client_search_suggest(q: str = "", limit: int = 12):
    query = (q or "").strip()
    if len(query) < 2:
        return {"success": True, "query": query, "suggestions": []}
    if limit < 1:
        limit = 1
    if limit > 20:
        limit = 20

    try:
        index_data = await _fetch_stubhub_search_index(query)
        suggestions = _build_client_search_suggestions(index_data, limit=limit)
        return {"success": True, "query": query, "suggestions": suggestions}
    except Exception as exc:
        logger.warning(f"Client search suggest failed for query={query!r}: {exc}")
        return JSONResponse({"success": False, "error": str(exc)}, status_code=502)


@app.post("/ticketing/ui/client-search/spothero")
async def ui_client_search_spothero(request: Request):
    content_type = (request.headers.get("content-type") or "").lower()
    payload: dict = {}
    if "application/json" in content_type:
        payload = await request.json()
    else:
        form = await request.form()
        payload = dict(form)

    try:
        selection_payload = _client_search_payload_from_request_payload(payload)
        best, suggestions, matched_query = await _pick_best_spothero_suggestion(selection_payload)
        if not best or not best.get("destination_id") or not best.get("path"):
            return {
                "success": True,
                "matched_query": matched_query,
                "details": None,
                "suggestions": suggestions,
            }

        details = await _fetch_live_spothero_destination_details(
            int(best["destination_id"]),
            str(best["path"]),
            selection_payload=selection_payload,
        )
        details["matched_query"] = best.get("query_used") or matched_query
        details["matched_destination"] = {
            "destination_id": best.get("destination_id"),
            "title": best.get("title"),
            "city": best.get("city"),
            "path": best.get("path"),
            "destination_page_url": best.get("destination_page_url"),
            "search_url": best.get("search_url"),
        }
        return {
            "success": True,
            "matched_query": details.get("matched_query") or matched_query,
            "details": details,
            "suggestions": suggestions,
        }
    except Exception as exc:
        logger.error(f"Client search SpotHero lookup failed: {exc}")
        return JSONResponse({"success": False, "error": str(exc)}, status_code=502)


@app.post("/ticketing/ui/client-search/run")
async def ui_client_search_run(request: Request):
    content_type = (request.headers.get("content-type") or "").lower()
    payload: dict = {}
    if "application/json" in content_type:
        payload = await request.json()
    else:
        form = await request.form()
        payload = dict(form)

    max_events_raw = payload.get("max_events")
    try:
        max_events = int(max_events_raw) if max_events_raw is not None else 120
    except Exception:
        max_events = 120
    max_events = max(1, min(180, max_events))

    try:
        selection_payload = _client_search_payload_from_request_payload(payload)
        parking_query = _build_parking_query_from_selection(selection_payload)
        parking_candidates = await _resolve_client_search_parking_candidates(selection_payload, max_events=max_events)
        if not parking_candidates:
            return {
                "success": True,
                "query": (payload.get("query") or "").strip(),
                "parking_query": parking_query,
                "parking_events_found": 0,
                "parking_events_scraped": 0,
                "data": [],
                "errors": [],
            }

        grouped_results = _group_client_search_results(parking_candidates, [])
        return {
            "success": True,
            "query": (payload.get("query") or "").strip(),
            "selection_title": (payload.get("title") or payload.get("query") or "").strip(),
            "parking_query": parking_query,
            "parking_events_found": len(parking_candidates),
            "parking_events_scraped": 0,
            "data": grouped_results,
            "errors": [],
        }
    except Exception as exc:
        logger.error(f"Client search run failed: {exc}")
        return JSONResponse({"success": False, "error": str(exc)}, status_code=502)


@app.post("/ticketing/ui/client-search/event-details")
async def ui_client_search_event_details(request: Request):
    content_type = (request.headers.get("content-type") or "").lower()
    payload: dict = {}
    if "application/json" in content_type:
        payload = await request.json()
    else:
        form = await request.form()
        payload = dict(form)

    parking_url = _canonical_stubhub_url(payload.get("parking_url") or payload.get("url") or payload.get("canonical_url"))
    if not parking_url:
        return JSONResponse({"success": False, "error": "parking_url is required"}, status_code=400)

    search_item = {
        "name": payload.get("event_name") or payload.get("title") or "",
        "title": payload.get("event_name") or payload.get("title") or "",
        "dayOfWeek": payload.get("day_of_week") or "",
        "formattedDate": payload.get("formatted_date") or payload.get("date") or "",
        "formattedTime": payload.get("formatted_time") or payload.get("time") or "",
        "venueName": payload.get("venue_name") or payload.get("venueName") or "",
        "formattedVenueLocation": payload.get("location") or "",
        "url": parking_url,
        "canonical_url": parking_url,
        "eventMetadata": {},
    }

    try:
        cache_key = f"stubhub:event-details:{parking_url}"
        cached = _live_cache_get(cache_key, 300)
        if isinstance(cached, dict):
            return cached

        failed_cache_key = f"stubhub:event-details:failed:{parking_url}"
        failed_cached = _live_cache_get(failed_cache_key, 120)
        if isinstance(failed_cached, dict):
            return JSONResponse(failed_cached, status_code=502)

        phase2_result = await asyncio.wait_for(
            ticketing_phase2(
                parking_urls=parking_url,
                limit=1,
                batch_size=1,
                export_json=False,
                persist=False,
                alert_on_failures=False,
            ),
            timeout=150.0,
        )
        if isinstance(phase2_result, JSONResponse):
            return phase2_result

        grouped_results = _group_client_search_results([search_item], phase2_result.get("data") or [])
        card = grouped_results[0] if grouped_results else _group_client_search_results([search_item], [])[0]
        payload = {
            "success": True,
            "parking_url": parking_url,
            "card": card,
            "errors": phase2_result.get("errors") or [],
        }
        return _live_cache_set(cache_key, payload)
    except asyncio.TimeoutError:
        error_payload = {
            "success": False,
            "error": "Listing scrape timed out for this event",
            "parking_url": parking_url,
        }
        _live_cache_set(f"stubhub:event-details:failed:{parking_url}", error_payload)
        logger.error(f"Client search event-details timed out for {parking_url}")
        return JSONResponse(error_payload, status_code=502)
    except Exception as exc:
        logger.error(f"Client search event-details failed for {parking_url}: {exc}")
        error_payload = {"success": False, "error": str(exc), "parking_url": parking_url}
        _live_cache_set(f"stubhub:event-details:failed:{parking_url}", error_payload)
        return JSONResponse(error_payload, status_code=502)


@app.post("/ui/parking-links/run")
async def ui_parking_links_run(request: Request):
    """
    UI execution endpoint. Accepts JSON or form-encoded body.
    Calls ticketing_parking_links(...) directly and returns JSON.
    """
    content_type = (request.headers.get("content-type") or "").lower()
    payload: dict = {}
    if "application/json" in content_type:
        payload = await request.json()
    else:
        form = await request.form()
        payload = dict(form)

    mode = (payload.get("mode") or "direct").strip().lower()

    def _bool(v, default=False):
        if v is None:
            return default
        if isinstance(v, bool):
            return v
        s = str(v).strip().lower()
        if s in {"1", "true", "yes", "y", "on"}:
            return True
        if s in {"0", "false", "no", "n", "off"}:
            return False
        return default

    def _int(v, default):
        try:
            return int(v)
        except Exception:
            return default

    common = {
        "strict_event_location_match": _bool(payload.get("strict_event_location_match"), False),
        "strict_venue_guard": _bool(payload.get("strict_venue_guard"), True),
        "export_json": _bool(payload.get("export_json"), True),
    }

    if mode == "auto":
        result = await ticketing_parking_links(
            auto_find_venues=True,
            seed_url=(payload.get("seed_url") or "").strip() or None,
            max_venues=_int(payload.get("max_venues"), 25),
            max_pages=_int(payload.get("max_pages"), 0),
            full=_bool(payload.get("full"), False),
            venue_discovery_timeout_seconds=_int(payload.get("venue_discovery_timeout_seconds"), 90),
            fallback_to_excel=_bool(payload.get("fallback_to_excel"), True),
            excel_path=(payload.get("excel_path") or "venues.xlsx").strip(),
            **common,
        )
    else:
        # direct
        raw_urls = (payload.get("stubhub_urls") or "").strip()
        stubhub_urls = ",".join([u for u in raw_urls.replace("\n", ",").split(",") if u.strip()])
        result = await ticketing_parking_links(
            stubhub_urls=stubhub_urls,
            venue_name=(payload.get("venue_name") or "Ad-hoc Venue").strip() or "Ad-hoc Venue",
            **common,
        )

    return result


@app.get("/ui/parking-links/download")
async def ui_parking_links_download(name: str | None = None):
    """
    Download exported JSON from python-src/storage/search_results.
    If name is omitted, serves the latest parking_links_*.json.
    """
    STORAGE_SEARCH_RESULTS.mkdir(parents=True, exist_ok=True)
    filename = _safe_result_filename(name)
    path = (STORAGE_SEARCH_RESULTS / filename) if filename else (_latest_search_result() or None)
    if not path or not path.exists():
        return JSONResponse({"success": False, "error": "No exported result found."}, status_code=404)
    return FileResponse(
        path=str(path),
        media_type="application/json",
        filename=path.name,
    )


@app.get("/ticketing/parking-links")
async def ticketing_parking_links(
    stubhub_urls: str | None = None,
    venue_name: str = "Ad-hoc Venue",
    handler: str = "stubhub-discovery",
    auto_find_venues: bool = False,
    seed_url: str | None = None,
    max_venues: int = 50,
    max_pages: int = 0,
    full: bool = False,
    venue_discovery_timeout_seconds: int = 90,
    fallback_to_excel: bool = True,
    excel_path: str = "venues.xlsx",
    strict_venue_guard: bool = True,
    strict_event_location_match: bool = True,
    export_json: bool = True,
):
    """
    Generate event + parking links only (NO parking spot extraction).

    Input options:
    - Direct venue URL(s): stubhub_urls=<comma-separated StubHub venue/performer URLs>
    - Auto venue finding: auto_find_venues=true&seed_url=<StubHub page> (discovers venue links, then runs discovery per venue)
      - full=true: always use browser-assisted extraction (slower, more reliable)
      - max_pages>0: request more feed pages to discover more venues (slower)
      - venue_discovery_timeout_seconds: hard timeout for venue discovery (tool safety)
      - fallback_to_excel=true: if StubHub blocks seed scraping, use local venues.xlsx as fallback
    """
    resolved_venues: list[dict] = []
    venue_discovery_attempts: list[dict] | None = None
    venue_discovery_timed_out = False

    if auto_find_venues:
        if not seed_url:
            return JSONResponse(
                {"success": False, "error": "seed_url is required when auto_find_venues=true"},
                status_code=400,
            )
        if max_venues < 1:
            return JSONResponse({"success": False, "error": "max_venues must be >= 1"}, status_code=400)
        if venue_discovery_timeout_seconds < 5:
            return JSONResponse(
                {"success": False, "error": "venue_discovery_timeout_seconds must be >= 5"},
                status_code=400,
            )

        # Discover venue pages from seed_url (HTTP + feeds, with optional browser assistance).
        #
        # Laptop-friendly defaults:
        # - If caller didn't specify max_pages, keep it small so this endpoint returns quickly.
        # - Only use Playwright for venue discovery when full=true (otherwise we can hang on bot/JS pages).
        effective_max_pages = max(0, int(max_pages or 0)) or 3
        use_playwright_if_empty = bool(full)
        try:
            discovered_rows, venue_discovery_attempts = await asyncio.wait_for(
                _scrape_venues_from_stubhub(
                    start_url=seed_url,
                    use_playwright_if_empty=use_playwright_if_empty,
                    use_playwright_always=bool(full),
                    max_pages=effective_max_pages,
                ),
                timeout=float(venue_discovery_timeout_seconds),
            )
        except asyncio.TimeoutError:
            venue_discovery_timed_out = True
            discovered_rows = []
        except Exception as exc:
            return JSONResponse({"success": False, "error": str(exc)}, status_code=500)

        resolved_venues = [
            {
                "name": r.get("name") or "Discovered Venue",
                "stubhub_url": r.get("stubhub_url"),
                "handler": r.get("handler") or handler,
                "location": r.get("location"),
            }
            for r in discovered_rows
            if r.get("stubhub_url")
        ][:max_venues]

        if not resolved_venues:
            blocked_like = False
            if venue_discovery_attempts:
                for a in venue_discovery_attempts:
                    sc = a.get("status_code")
                    ct = str(a.get("content_type") or "").lower()
                    if sc in {202, 403, 429} or ("text/html" in ct and sc not in {200}):
                        blocked_like = True
                        break

            # Laptop-friendly fallback: use local venues.xlsx if auto-find is blocked.
            if fallback_to_excel:
                try:
                    input_path = Path(excel_path)
                    if not input_path.is_absolute():
                        candidates = [
                            STORAGE_EXPORTS / input_path.name,
                            BASE_DIR / input_path,
                            Path.cwd() / input_path,
                            BASE_DIR.parent / input_path,
                        ]
                        input_path = next((p for p in candidates if p.exists()), input_path)
                    if input_path.exists():
                        fallback_rows = VenueParser.from_excel(str(input_path))[:max_venues]
                        resolved_venues = [
                            {
                                "name": r.get("name") or "Excel Venue",
                                "stubhub_url": r.get("stubhub_url"),
                                "handler": r.get("handler") or handler,
                                "location": r.get("location"),
                            }
                            for r in fallback_rows
                            if r.get("stubhub_url")
                        ]
                except Exception:
                    resolved_venues = []

            if resolved_venues:
                # Continue into discovery phase with fallback venues.
                pass
            else:
                return {
                    "success": True,
                    "tool": "parking-links",
                    "mode": "auto_find_venues",
                    "seed_url": seed_url,
                    "venues_resolved": 0,
                    "events_generated": 0,
                    "venue_discovery": {
                        "full": bool(full),
                        "max_pages": effective_max_pages,
                        "attempts": venue_discovery_attempts or [],
                        "timed_out": bool(venue_discovery_timed_out),
                        "timeout_seconds": venue_discovery_timeout_seconds if venue_discovery_timed_out else None,
                    },
                    "data": [],
                    "message": (
                        "Venue discovery timed out and Excel fallback is disabled."
                        if venue_discovery_timed_out and not fallback_to_excel
                        else
                        "No venues discovered from seed_url. StubHub may be returning an anti-bot/challenge response."
                        if blocked_like
                        else "No venues discovered from seed_url."
                    ),
                }
    else:
        urls = _parse_comma_list(stubhub_urls)
        if not urls:
            return JSONResponse(
                {"success": False, "error": "stubhub_urls is required (comma-separated StubHub venue/performer URLs)"},
                status_code=400,
            )
        for u in urls:
            resolved_venues.append(
                {
                    "name": venue_name,
                    "stubhub_url": u,
                    "handler": handler,
                    "location": None,
                }
            )

    all_events: list[dict] = []
    errors: list[dict] = []
    for row in resolved_venues:
        v_name = row.get("name") or venue_name
        v_url = row.get("stubhub_url") or ""
        v_handler = row.get("handler") or handler
        v_location = row.get("location")
        venue = SimpleNamespace(
            name=v_name,
            stubhub_url=v_url,
            handler=v_handler,
            proxy=None,
            user_agent=None,
        )
        try:
            discovered_events = await _run_discovery_for_venue(
                venue=venue,
                handler=v_handler,
                dry_run=False,
                persist=False,
                strict_venue_guard=strict_venue_guard,
            )

            # If it's a performer page, optionally do the parking-filtered discovery too (matches Phase1 behavior).
            is_performer_url = "/performer/" in v_url
            if is_performer_url and not ("gridFilterType=" in v_url):
                sep = "&" if "?" in v_url else "?"
                parking_venue = SimpleNamespace(
                    name=v_name,
                    stubhub_url=f"{v_url.rstrip('/')}/{sep}gridFilterType=1",
                    handler=v_handler,
                    proxy=None,
                    user_agent=None,
                )
                try:
                    parking_discovered = await _run_discovery_for_venue(
                        venue=parking_venue,
                        handler=v_handler,
                        dry_run=False,
                        persist=False,
                        strict_venue_guard=strict_venue_guard,
                    )
                    discovered_events.extend(parking_discovered)
                except Exception as p_exc:
                    logger.warning(f"Secondary parking discovery failed for {v_name}: {p_exc}")

            for ev in discovered_events:
                if strict_event_location_match and _is_event_location_mismatch(v_location, ev.get("event_url")):
                    continue
                all_events.append(
                    {
                        "venue": ev.get("venue") or v_name,
                        "event_name": ev.get("event_name") or ev.get("name"),
                        "event_date": ev.get("event_date") or ev.get("date"),
                        "event_url": ev.get("event_url"),
                        "parking_url": ev.get("parking_url"),
                    }
                )
        except Exception as exc:
            errors.append({"venue": v_name, "stubhub_url": v_url, "error": str(exc)})

    all_events = _dedupe_phase1_events(all_events)

    response_data = {
        "success": True,
        "tool": "parking-links",
        "mode": "auto_find_venues" if auto_find_venues else "direct_input",
        "seed_url": seed_url if auto_find_venues else None,
        "venues_input": len(_parse_comma_list(stubhub_urls)) if not auto_find_venues else None,
        "venues_resolved": len(resolved_venues),
        "events_generated": len(all_events),
        "strict_venue_guard": strict_venue_guard,
        "strict_event_location_match": strict_event_location_match,
        "venue_discovery": (
            {
                "full": bool(full),
                "max_pages": effective_max_pages,
                "attempts": venue_discovery_attempts or [],
            }
            if auto_find_venues
            else None
        ),
        "errors": errors,
        "json_output": None,
        "data": all_events,
    }

    if export_json and all_events:
        STORAGE_SEARCH_RESULTS.mkdir(parents=True, exist_ok=True)
        json_path = STORAGE_SEARCH_RESULTS / f"parking_links_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.json"
        response_data["json_output"] = str(json_path)
        with json_path.open("w", encoding="utf-8") as f:
            json.dump(response_data, f, indent=4)

    return response_data


async def _run_discovery_for_venue(
    venue,
    handler: str,
    dry_run: bool,
    persist: bool,
    strict_venue_guard: bool,
) -> list[dict]:
    scraper_cls = TicketingController.get_scraper(handler)
    if not scraper_cls:
        raise ValueError(f"No scraper found for handler: {handler}")

    if dry_run:
        raise ValueError("dry_run is disabled. Use real-time execution only.")

    cluster = await PlaywrightClusterManager.get_or_create(venue.proxy)

    async def _task(page):
        instance = await scraper_cls.init(venue, page)
        return await asyncio.wait_for(
            instance.discover_events(
                venue,
                skip_persist=not persist,
                strict_venue_guard=strict_venue_guard,
            ),
            timeout=1800,
        )

    return await cluster.execute(_task)


@app.get("/ticketing/phase1")
async def ticketing_phase1(
    excel_path: str = None,
    source: str = None,
    dry_run: bool = False,
    export_json: bool = True,
    persist: bool = False,
    max_venues: int = None,
    strict_venue_guard: bool = True,
    strict_event_location_match: bool = True,
    discover_venues: bool = False,
):
    ticketing = CONFIG.get("ticketing", {})
    if excel_path is None:
        excel_path = ticketing.get("excel_path", "venues.xlsx")
    if source is None:
        source = ticketing.get("default_source", "file")
    if max_venues is None:
        max_venues = ticketing.get("max_venues", 10000)

    if dry_run:
        return JSONResponse(
            {"success": False, "error": "dry_run is disabled. Use dry_run=false."},
            status_code=400,
        )

    source = (source or "file").lower().strip()
    if source not in {"file", "db"}:
        return JSONResponse(
            {"success": False, "error": "source must be 'file' or 'db'."},
            status_code=400,
        )

    venues_data: list[dict] = []
    input_path = None
    if source == "file":
        input_path = Path(excel_path)
        if not input_path.is_absolute():
            candidates = [
                STORAGE_EXPORTS / input_path.name,
                BASE_DIR / input_path,
                Path.cwd() / input_path,
                BASE_DIR.parent / input_path,
            ]
            input_path = next((p for p in candidates if p.exists()), STORAGE_EXPORTS / input_path.name)

        if not input_path.exists():
            return JSONResponse(
                {"success": False, "error": f"Excel file not found: {input_path}"},
                status_code=400,
            )
        venues_data = VenueParser.from_excel(str(input_path))
    else:
        venues = await get_venue_repository().list_all(limit=max_venues)
        venues_data = [
            {
                "name": v.name,
                "stubhub_url": v.stubhub_url,
                "handler": v.handler or "stubhub-discovery",
                "location": v.location,
            }
            for v in venues
            if v.name and v.stubhub_url
        ]
        if not venues_data:
            return JSONResponse(
                {"success": False, "error": "No venues found in database."},
                status_code=400,
            )
    if max_venues is not None and max_venues > 0:
        venues_data = venues_data[:max_venues]
    all_events: list[dict] = []
    errors: list[dict] = []

    for row in venues_data:
        v_name = row.get("name", "")
        v_url = row.get("stubhub_url", "")
        v_handler = row.get("handler", "stubhub-discovery")
        venue = SimpleNamespace(
            name=v_name,
            stubhub_url=v_url,
            handler=v_handler,
            proxy=None,
            user_agent=None,
        )

        try:
            # 1. Normal Discovery
            discovered = await _run_discovery_for_venue(
                venue=venue,
                handler=v_handler,
                dry_run=dry_run,
                persist=persist,
                strict_venue_guard=strict_venue_guard,
            )
            
            # 2. Exhaustive Performer Discovery (if applicable)
            # If it's a performer page, also check for dedicated parking events.
            is_performer_url = "/performer/" in v_url
            if is_performer_url and not ("gridFilterType=" in v_url):
                logger.info(f"[Phase1] Performer page detected: {v_url}. Running secondary parking-filtered discovery.")
                sep = "&" if "?" in v_url else "?"
                parking_venue = SimpleNamespace(
                    name=v_name,
                    stubhub_url=f"{v_url.rstrip('/')}/{sep}gridFilterType=1",
                    handler=v_handler,
                    proxy=None,
                    user_agent=None
                )
                try:
                    parking_discovered = await _run_discovery_for_venue(
                        venue=parking_venue,
                        handler=v_handler,
                        dry_run=dry_run,
                        persist=persist,
                        strict_venue_guard=strict_venue_guard,
                    )
                    discovered.extend(parking_discovered)
                except Exception as p_exc:
                    logger.warning(f"Secondary parking discovery failed for {v_name}: {p_exc}")

            for ev in discovered:
                # Deduplicate manually if needed, or rely on _dedupe_phase1_events later
                if strict_event_location_match and _is_event_location_mismatch(
                    row.get("location"),
                    ev.get("event_url"),
                ):
                    errors.append(
                        {
                            "venue": v_name,
                            "stubhub_url": v_url,
                            "event_url": ev.get("event_url"),
                            "error": "event_location_mismatch",
                            "details": f"Location '{row.get('location')}' not reflected in event URL slug.",
                        }
                    )
                    continue
                all_events.append(ev)
        except Exception as exc:
            logger.error(f"Phase1 discovery failed for {v_name}: {exc}")
            errors.append(
                {
                    "venue": v_name,
                    "stubhub_url": v_url,
                    "error": str(exc),
                }
            )

    all_events = _dedupe_phase1_events(all_events)

    if discover_venues and all_events:
        # Extra discovery: find venues mentioned in the discovered events
        new_venues_to_add = []
        seen_venue_urls = {v.get("stubhub_url") for v in venues_data if v.get("stubhub_url")}
        
        for ev in all_events:
            v_name = ev.get("venue")
            v_url = ev.get("venue_url")
            if v_url and v_url not in seen_venue_urls:
                seen_venue_urls.add(v_url)
                new_venues_to_add.append({
                    "name": v_name,
                    "stubhub_url": v_url,
                    "handler": "stubhub-discovery",
                    "location": None
                })
        
        if new_venues_to_add:
            out_path = _storage_output_path(excel_path)

            existing_excel_venues = []
            if out_path.exists():
                try:
                    existing_excel_venues = VenueParser.from_excel(str(out_path))
                except Exception:
                    existing_excel_venues = []
            
            existing_excel_urls = {v.get("stubhub_url") for v in existing_excel_venues if v.get("stubhub_url")}
            venues_to_append = [v for v in new_venues_to_add if v["stubhub_url"] not in existing_excel_urls]
            
            if venues_to_append:
                logger.info(f"[Discovery] Appending {len(venues_to_append)} new venues to {out_path}")
                combined = existing_excel_venues + venues_to_append
                VenueParser.to_excel(combined, str(out_path))
                # Note: response_data will be updated with this info if needed manually below

    response_data = {
        "success": True,
        "phase": "Phase 1 — ParkingDiscovery Pipeline",
        "data_source": "real_time_live_scrape",
        "source": source,
        "input_file": str(input_path) if input_path else None,
        "strict_venue_guard": strict_venue_guard,
        "strict_event_location_match": strict_event_location_match,
        "processed_items": len(all_events),
        "venues_processed": len(venues_data),
        "failed_venues": len(errors),
        "json_output": None,
        "errors": errors,
        "data": all_events,
    }

    if export_json and all_events:
        exports_dir = STORAGE_EXPORTS
        exports_dir.mkdir(parents=True, exist_ok=True)
        json_path = exports_dir / f"phase1_discovery_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.json"
        response_data["json_output"] = str(json_path)
        with json_path.open("w", encoding="utf-8") as f:
            json.dump(response_data, f, indent=4)

    return response_data


@app.get("/ticketing/venues")
async def ticketing_venues(limit: int = 1000):
    if limit < 1:
        return JSONResponse({"success": False, "error": "limit must be >= 1"}, status_code=400)
    rows = await get_venue_repository().list_all(limit=limit)
    return {
        "success": True,
        "count": len(rows),
        "data": [
            {
                "id": v._id,
                "name": v.name,
                "stubhub_url": v.stubhub_url,
                "handler": v.handler,
                "location": v.location,
            }
            for v in rows
        ],
    }


@app.get("/ticketing/venues/export-to-excel")
async def ticketing_venues_export_to_excel(excel_path: str = "venues.xlsx", limit: int = 10000):
    return await _export_db_venues_to_excel(excel_path=excel_path, limit=limit)


@app.get("/ticketing/venues/extract-from-har")
async def ticketing_venues_extract_from_har(
    har_glob: str = "*.har",
    import_to_db: bool = False,
    sync_excel: bool = False,
    excel_path: str = "venues.xlsx",
    limit: int = 5000,
):
    if limit < 1:
        return JSONResponse({"success": False, "error": "limit must be >= 1"}, status_code=400)

    files = sorted(BASE_DIR.glob(har_glob))
    if not files:
        return JSONResponse(
            {"success": False, "error": f"No HAR files matched: {har_glob}"},
            status_code=404,
        )

    extracted = []
    summaries = []
    for p in files:
        try:
            before_count = len(extracted)
            with p.open("r", encoding="utf-8") as f:
                har = json.load(f)
            entries = har.get("log", {}).get("entries", [])
            chunks = [p.read_text(encoding="utf-8", errors="ignore")]
            for e in entries:
                req = e.get("request", {})
                if req.get("url"):
                    chunks.append(req["url"])
                post_text = (req.get("postData") or {}).get("text")
                if post_text:
                    chunks.append(post_text)
                    try:
                        extracted.extend(_extract_stubhub_venues_from_json_obj(json.loads(post_text)))
                    except Exception:
                        pass
                resp_text = ((e.get("response") or {}).get("content") or {}).get("text")
                if resp_text:
                    chunks.append(resp_text)
                    try:
                        extracted.extend(_extract_stubhub_venues_from_json_obj(json.loads(resp_text)))
                    except Exception:
                        pass
            rows = _extract_stubhub_venues_from_text("\n".join(chunks))
            extracted.extend(rows)
            summaries.append({"har_file": str(p), "entries": len(entries), "venues_found": len(extracted) - before_count})
        except Exception as exc:
            summaries.append({"har_file": str(p), "error": str(exc), "venues_found": 0})

    deduped = _dedupe_venues(extracted)[:limit]

    imported = []
    import_errors = []
    excel_sync = None
    if import_to_db:
        repo = get_venue_repository()
        for idx, row in enumerate(deduped):
            try:
                v = await repo.upsert_venue(
                    {
                        "name": row["name"],
                        "stubhub_url": row["stubhub_url"],
                        "handler": "stubhub-discovery",
                        "location": row.get("location"),
                    }
                )
                imported.append({"id": v._id, "name": v.name, "stubhub_url": v.stubhub_url})
            except Exception as exc:
                import_errors.append({"index": idx, "row": row, "error": str(exc)})
        if sync_excel:
            excel_sync = await _export_db_venues_to_excel(excel_path=excel_path)

    return {
        "success": True,
        "har_files_scanned": len(files),
        "file_summaries": summaries,
        "venues_found": len(deduped),
        "imported_count": len(imported),
        "excel_sync": excel_sync,
        "import_errors": import_errors,
        "data": deduped,
    }


async def _scrape_venues_from_stubhub(
    start_url: str = "https://www.stubhub.com/",
    use_playwright_if_empty: bool = True,
    use_playwright_always: bool = False,
    max_pages: int = 0,
) -> tuple[list[dict], list[dict]]:
    """Fetch StubHub page(s) and extract venue list. Returns (rows, attempts)."""
    attempts: list[dict] = []
    extracted: list[dict] = []
    method_urls: list[str] = []
    try:
        parsed_start = urlsplit(start_url)
        base_url = f"{parsed_start.scheme or 'https'}://{parsed_start.netloc or 'www.stubhub.com'}"
        default_headers = {
            "User-Agent": (
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml,application/json;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        }
        # Tool safety: when max_pages is provided and small, keep timeouts tight.
        tool_mode = bool(max_pages and int(max_pages) > 0)
        page_cap_for_tool = min(int(max_pages), 100) if tool_mode else 0
        http_timeout = 10 if tool_mode and page_cap_for_tool <= 5 else 20

        async with httpx.AsyncClient(timeout=http_timeout, follow_redirects=True) as client:
            resp = await client.get(start_url, headers=default_headers)
            extracted = _extract_stubhub_venues_from_text(resp.text)
            extracted.extend(_extract_stubhub_venues_from_event_snippets(resp.text))
            attempts.append(
                {
                    "url": str(resp.url),
                    "status_code": resp.status_code,
                    "content_type": resp.headers.get("content-type"),
                    "venues_extracted": len(_dedupe_venues(extracted)),
                }
            )
            if "stubhub.com" in base_url:
                max_rows = 500
                # max_pages:
                # - 0 => use safe defaults (fast-ish, decent coverage)
                # - >0 => honor caller limit (important for "tool" endpoints that must return quickly)
                if max_pages > 0:
                    page_cap = min(int(max_pages), 100)
                    num_pages_home = page_cap
                    num_pages_explore = page_cap
                    extra_cat_pages = min(20, page_cap)
                    # Only expand categories for larger runs; keep small runs lightweight.
                    extra_cats = (4, 5, 6) if page_cap <= 10 else (4, 5, 6, 7, 8, 9, 10, 11, 12)
                else:
                    num_pages_home = 25
                    num_pages_explore = 20
                    extra_cats = (4, 5, 6)
                    extra_cat_pages = 8

                # Lightweight mode for tools: keep URL fanout small when page_cap is small.
                if max_pages > 0 and page_cap <= 5:
                    method_urls = [
                        f"{base_url}/?method=DontMissEvents&categoryId=0&maxRows={max_rows}&page=0",
                        f"{base_url}/?method=MostPopularCategories&maxRows={max_rows}&categoryId=3&page=0",
                    ]
                    extra_cats = ()
                    extra_cat_pages = 0
                else:
                    method_urls = [
                        f"{base_url}/?method=DontMissEvents&categoryId=0&maxRows={max_rows}&page=0",
                        f"{base_url}/?method=MostPopularCategories&maxRows={max_rows}&categoryId=3&page=0",
                        f"{base_url}/?method=MostPopularCategories&maxRows={max_rows}&categoryId=2&page=0",
                        f"{base_url}/?method=MostPopularCategories&maxRows={max_rows}&categoryId=1&page=0",
                        f"{base_url}/?method=RecommendedForYouCategories&topLevelCategoryId=0&includeDateRanges=true",
                    ]
                for page in range(1, num_pages_home):
                    method_urls.append(f"{base_url}/?method=DontMissEvents&categoryId=0&maxRows={max_rows}&page={page}")
                    method_urls.append(f"{base_url}/?method=MostPopularCategories&maxRows={max_rows}&categoryId=3&page={page}")
                    if not (max_pages > 0 and page_cap <= 5):
                        method_urls.append(f"{base_url}/?method=MostPopularCategories&maxRows={max_rows}&categoryId=2&page={page}")
                        method_urls.append(f"{base_url}/?method=MostPopularCategories&maxRows={max_rows}&categoryId=1&page={page}")
                for cat_id in extra_cats:
                    for page in range(0, extra_cat_pages):
                        method_urls.append(f"{base_url}/?method=MostPopularCategories&maxRows={max_rows}&categoryId={cat_id}&page={page}")
                if (parsed_start.path or "").startswith("/explore"):
                    method_urls.extend(
                        [
                            f"{base_url}/explore?method=DontMissEvents&categoryId=0&maxRows={max_rows}&page=0",
                            f"{base_url}/explore?method=MostPopularCategories&maxRows={max_rows}&categoryId=3&page=0",
                            *(
                                [
                                    f"{base_url}/explore?method=MostPopularCategories&maxRows={max_rows}&categoryId=2&page=0",
                                    f"{base_url}/explore?method=MostPopularCategories&maxRows={max_rows}&categoryId=1&page=0",
                                ]
                                if not (max_pages > 0 and page_cap <= 5)
                                else []
                            ),
                        ]
                    )
                    for page in range(1, num_pages_explore):
                        method_urls.append(f"{base_url}/explore?method=DontMissEvents&categoryId=0&maxRows={max_rows}&page={page}")
                        method_urls.append(f"{base_url}/explore?method=MostPopularCategories&maxRows={max_rows}&categoryId=3&page={page}")
                        if not (max_pages > 0 and page_cap <= 5):
                            method_urls.append(f"{base_url}/explore?method=MostPopularCategories&maxRows={max_rows}&categoryId=2&page={page}")
                            method_urls.append(f"{base_url}/explore?method=MostPopularCategories&maxRows={max_rows}&categoryId=1&page={page}")
                    if not (max_pages > 0 and page_cap <= 5):
                        for cat_id in extra_cats:
                            for page in range(0, extra_cat_pages):
                                method_urls.append(f"{base_url}/explore?method=MostPopularCategories&maxRows={max_rows}&categoryId={cat_id}&page={page}")
            if method_urls:
                for method_url in method_urls:
                    before_unique = len(_dedupe_venues(extracted))
                    try:
                        feed_resp = await client.get(
                            method_url,
                            headers={**default_headers, "Referer": start_url, "X-Requested-With": "XMLHttpRequest"},
                        )
                        feed_json = None
                        try:
                            feed_json = feed_resp.json()
                        except Exception:
                            try:
                                feed_json = json.loads(feed_resp.text)
                            except Exception:
                                feed_json = None
                        if feed_json is not None:
                            extracted.extend(_extract_stubhub_venues_from_json_obj(feed_json))
                        extracted.extend(_extract_stubhub_venues_from_text(feed_resp.text))
                        extracted.extend(_extract_stubhub_venues_from_event_snippets(feed_resp.text))
                        after_unique = len(_dedupe_venues(extracted))
                        attempts.append(
                            {
                                "url": method_url,
                                "status_code": feed_resp.status_code,
                                "content_type": feed_resp.headers.get("content-type"),
                                "venues_extracted": max(0, after_unique - before_unique),
                            }
                        )
                    except Exception as exc:
                        attempts.append({"url": method_url, "error": str(exc), "venues_extracted": 0})
                        continue
        rows = _dedupe_venues(extracted)
        if method_urls and (use_playwright_always or (not rows and use_playwright_if_empty)):
            pw_rows, pw_attempts = await _extract_venues_via_playwright(start_url, method_urls)
            combined = _dedupe_venues(rows + pw_rows)
            rows = combined
            attempts.extend(pw_attempts)
    except Exception as exc:
        logger.exception("StubHub venue scrape failed")
        raise
    return rows, attempts


def _normalize_venue_url(url: str) -> str:
    u = (url or "").strip().split("?")[0].rstrip("/").lower()
    return u


def _merge_venue_lists(existing: list[dict], new: list[dict]) -> list[dict]:
    """Merge venue lists by normalized stubhub_url; existing entries take precedence."""
    by_url: dict[str, dict] = {}
    for row in existing:
        url = _normalize_venue_url(row.get("stubhub_url") or "")
        if url:
            by_url[url] = {
                "name": (row.get("name") or "").strip(),
                "stubhub_url": (row.get("stubhub_url") or "").strip().split("?")[0].rstrip("/"),
                "handler": (row.get("handler") or "stubhub-discovery").strip(),
                "location": row.get("location") if row.get("location") else None,
            }
    for row in new:
        url = _normalize_venue_url(row.get("stubhub_url") or "")
        if url and url not in by_url:
            by_url[url] = {
                "name": (row.get("name") or "").strip(),
                "stubhub_url": (row.get("stubhub_url") or "").strip().split("?")[0].rstrip("/"),
                "handler": (row.get("handler") or "stubhub-discovery").strip(),
                "location": row.get("location") if row.get("location") else None,
            }
    out = list(by_url.values())
    out.sort(key=lambda r: ((r.get("name") or "").lower(), (r.get("stubhub_url") or "").lower()))
    return out


def _write_venues_to_excel(target: Path, rows: list[dict]) -> dict:
    """Write venue rows to Excel (name, stubhub_url, handler, location)."""
    import pandas as pd
    target.parent.mkdir(parents=True, exist_ok=True)
    records = [
        {
            "name": r.get("name") or "",
            "stubhub_url": r.get("stubhub_url") or "",
            "handler": r.get("handler") or "stubhub-discovery",
            "location": r.get("location"),
        }
        for r in rows
    ]
    try:
        pd.DataFrame(records, columns=["name", "stubhub_url", "handler", "location"]).to_excel(target, index=False)
        return {"success": True, "rows_written": len(records), "path": str(target)}
    except Exception as exc:
        return {"success": False, "rows_written": 0, "path": str(target), "error": str(exc)}


@app.get("/ticketing/venues/extract-from-web")
async def ticketing_venues_extract_from_web(
    start_url: str = "https://www.stubhub.com/",
    import_to_db: bool = False,
    sync_excel: bool = False,
    excel_path: str = "venues.xlsx",
):
    try:
        rows, attempts = await _scrape_venues_from_stubhub(start_url)
    except Exception as exc:
        return JSONResponse({"success": False, "error": str(exc)}, status_code=500)

    imported = []
    import_errors = []
    excel_sync = None
    if import_to_db:
        repo = get_venue_repository()
        for idx, row in enumerate(rows):
            try:
                v = await repo.upsert_venue(
                    {
                        "name": row["name"],
                        "stubhub_url": row["stubhub_url"],
                        "handler": "stubhub-discovery",
                        "location": row.get("location"),
                    }
                )
                imported.append({"id": v._id, "name": v.name, "stubhub_url": v.stubhub_url})
            except Exception as exc:
                import_errors.append({"index": idx, "row": row, "error": str(exc)})
        if sync_excel:
            excel_sync = await _export_db_venues_to_excel(excel_path=excel_path)

    return {
        "success": True,
        "source_url": start_url,
        "venues_found": len(rows),
        "attempts": attempts,
        "imported_count": len(imported),
        "excel_sync": excel_sync,
        "import_errors": import_errors,
        "data": rows,
    }


@app.get("/ticketing/venues/scrape-and-sync-excel")
async def ticketing_venues_scrape_and_sync_excel(
    excel_path: str = None,
    start_urls: str = None,
    full: bool = False,
    max_pages: int = 50,
):
    """
    Scrape venues FROM the StubHub website and add them TO the Excel file.
    Source: StubHub (home + explore). Destination: excel_path (default venues.xlsx).
    max_pages: request up to this many pages per feed (default 50; use 80–100 for thousands; allow 30–60 min timeout).
    full=True: also use browser (Playwright) for each URL to get every possible venue.
    """
    ticketing = CONFIG.get("ticketing", {})
    path_str = excel_path or ticketing.get("excel_path", "venues.xlsx")
    target = _storage_output_path(path_str)

    existing: list[dict] = []
    if target.exists():
        try:
            existing = VenueParser.from_excel(str(target))
        except Exception as exc:
            return JSONResponse(
                {"success": False, "error": f"Failed to read existing Excel: {exc}", "path": str(target)},
                status_code=400,
            )

    urls = [u.strip() for u in (start_urls or "").split(",") if u.strip()]
    if not urls:
        urls = [
            "https://www.stubhub.com/",
            "https://www.stubhub.com/explore",
        ]

    all_scraped: list[dict] = []
    all_attempts: list[dict] = []
    use_playwright_always = bool(full)
    pages_param = max(0, max_pages)
    for start_url in urls:
        try:
            rows, attempts = await _scrape_venues_from_stubhub(
                start_url,
                use_playwright_if_empty=True,
                use_playwright_always=use_playwright_always,
                max_pages=pages_param,
            )
            all_scraped.extend(rows)
            all_attempts.append({"start_url": start_url, "venues_from_url": len(rows), "attempts": attempts})
        except Exception as exc:
            logger.warning(f"Scrape failed for {start_url}: {exc}")
            all_attempts.append({"start_url": start_url, "error": str(exc), "venues_from_url": 0})
    scraped_deduped = _dedupe_venues(all_scraped)

    merged = _merge_venue_lists(existing, scraped_deduped)
    write_result = _write_venues_to_excel(target, merged)
    if not write_result.get("success"):
        return JSONResponse(
            {"success": False, "error": write_result.get("error", "Write failed"), "path": str(target)},
            status_code=500,
        )

    return {
        "success": True,
        "source": "StubHub website",
        "destination": "Excel file",
        "excel_path": str(target),
        "existing_count": len(existing),
        "scraped_count": len(scraped_deduped),
        "merged_count": len(merged),
        "rows_written": write_result.get("rows_written", 0),
        "per_url": all_attempts,
    }


@app.post("/ticketing/venues/import")
async def ticketing_venues_import(request: Request):
    body = await request.json()
    venues = body.get("venues", [])
    if not isinstance(venues, list) or not venues:
        return JSONResponse(
            {"success": False, "error": "Body must include non-empty 'venues' list."},
            status_code=400,
        )

    repo = get_venue_repository()
    imported = []
    errors = []
    for idx, row in enumerate(venues):
        try:
            name = (row.get("name") or "").strip()
            stubhub_url = (row.get("stubhub_url") or "").strip()
            location = row.get("location")
            if _is_invalid_placeholder(name) or _is_invalid_placeholder(stubhub_url):
                raise ValueError("name and stubhub_url must be real values (placeholders like '...' are rejected)")
            if not _is_valid_stubhub_url(stubhub_url):
                raise ValueError("stubhub_url must be a valid https://www.stubhub.com/... URL")
            venue = await repo.upsert_venue(
                {
                    "name": name,
                    "stubhub_url": stubhub_url,
                    "handler": (row.get("handler") or "stubhub-discovery").strip(),
                    "location": None if _is_invalid_placeholder(str(location) if location is not None else None) else location,
                }
            )
            imported.append(
                {
                    "id": venue._id,
                    "name": venue.name,
                    "stubhub_url": venue.stubhub_url,
                }
            )
        except Exception as exc:
            errors.append({"index": idx, "row": row, "error": str(exc)})

    return {
        "success": True,
        "imported_count": len(imported),
        "failed_count": len(errors),
        "imported": imported,
        "errors": errors,
    }


@app.delete("/ticketing/venues/{venue_id}")
async def ticketing_venue_delete(venue_id: int):
    from sqlalchemy import delete
    from database.connection import get_session
    from database.models.ticketing.venue import Venue

    async with get_session() as session:
        result = await session.execute(delete(Venue).where(Venue._id == venue_id))
        deleted = result.rowcount or 0

    if deleted == 0:
        return JSONResponse({"success": False, "error": f"Venue {venue_id} not found"}, status_code=404)
    return {"success": True, "deleted": deleted, "venue_id": venue_id}


@app.put("/ticketing/venues/{venue_id}")
async def ticketing_venue_update(venue_id: int, request: Request):
    body = await request.json()
    repo = get_venue_repository()
    venues = await repo.list_all(limit=5000)
    target = next((v for v in venues if v._id == venue_id), None)
    if target is None:
        return JSONResponse({"success": False, "error": f"Venue {venue_id} not found"}, status_code=404)

    name = (body.get("name") or target.name or "").strip()
    stubhub_url = (body.get("stubhub_url") or target.stubhub_url or "").strip()
    handler = (body.get("handler") or target.handler or "stubhub-discovery").strip()
    location = body.get("location", target.location)

    if _is_invalid_placeholder(name) or _is_invalid_placeholder(stubhub_url):
        return JSONResponse(
            {"success": False, "error": "name and stubhub_url must be real values (placeholders like '...' are rejected)"},
            status_code=400,
        )
    if not _is_valid_stubhub_url(stubhub_url):
        return JSONResponse(
            {"success": False, "error": "stubhub_url must be a valid https://www.stubhub.com/... URL"},
            status_code=400,
        )

    updated = await repo.upsert_venue(
        {
            "name": name,
            "stubhub_url": stubhub_url,
            "handler": handler,
            "location": None if _is_invalid_placeholder(str(location) if location is not None else None) else location,
        }
    )

    return {
        "success": True,
        "data": {
            "id": updated._id,
            "name": updated.name,
            "stubhub_url": updated.stubhub_url,
            "handler": updated.handler,
            "location": updated.location,
        },
    }


def _parse_iso_date(s: str | None) -> date:
    if not s:
        return date.today()
    try:
        return datetime.fromisoformat(s).date()
    except Exception:
        try:
            return date.fromisoformat(s)
        except Exception:
            return date.today()


def _latest_phase1_json() -> Path | None:
    exports_dir = STORAGE_EXPORTS
    if not exports_dir.exists():
        return None
    candidates = sorted(exports_dir.glob("phase1_discovery_*.json"), reverse=True)
    return candidates[0] if candidates else None


def _list_phase1_jsons() -> list[Path]:
    exports_dir = STORAGE_EXPORTS
    if not exports_dir.exists():
        return []
    return sorted(exports_dir.glob("phase1_discovery_*.json"), reverse=True)


def _read_json_rows(path: Path) -> list[dict]:
    with path.open("r", encoding="utf-8") as f:
        try:
            data = json.load(f)
            return data.get("data", [])
        except Exception:
            return []


def _event_key(row: dict) -> str:
    return row.get("event_url", "").strip()


def _phase1_json_timestamp(path: Path) -> datetime | None:
    m = re.search(r"phase1_discovery_(\d{8}T\d{6})Z\.json$", path.name)
    if not m:
        return None
    try:
        return datetime.strptime(m.group(1), "%Y%m%dT%H%M%S")
    except Exception:
        return None


def _pick_previous_phase1_json(current_json: Path, all_jsons: list[Path]) -> Path | None:
    current_ts = _phase1_json_timestamp(current_json)
    if current_ts is None:
        # Fallback: pick first different file in sorted list.
        for p in all_jsons:
            if p.resolve() != current_json.resolve():
                return p
        return None

    older = []
    for p in all_jsons:
        if p.resolve() == current_json.resolve():
            continue
        ts = _phase1_json_timestamp(p)
        if ts and ts < current_ts:
            older.append((ts, p))
    if older:
        older.sort(key=lambda x: x[0], reverse=True)
        return older[0][1]

    return None


async def _send_alert_webhook(event_type: str, payload: dict) -> dict | None:
    url = os.environ.get("ALERT_WEBHOOK_URL")
    if not url:
        return None
    body = {
        "event_type": event_type,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "payload": payload,
    }
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.post(url, json=body)
            return {"sent": True, "status_code": resp.status_code}
    except Exception as exc:
        return {"sent": False, "error": str(exc)}


def _derive_event_name_from_url(url: str | None) -> str:
    if not url:
        return "Ad-hoc Event"
    # Match various slug patterns, including parking-specific ones
    m = re.search(r"stubhub\.com/([^/?]+)-tickets-[^/]+/event/\d+/?", url)
    if not m:
        # Fallback for performers or other URLs
        m = re.search(r"stubhub\.com/([^/?]+)-tickets/performer/\d+/?", url)
        if not m:
            return "Ad-hoc Event"
    
    slug = m.group(1).lower()
    # Normalize: strip "parking-passes-only-", "tickets", etc.
    slug = slug.replace("parking-passes-only-", "")
    slug = slug.replace("-tickets", "")
    slug = slug.replace("-", " ").strip()
    
    return " ".join(word.capitalize() for word in slug.split())


def _derive_event_date_from_url(url: str | None) -> str:
    if not url:
        return date.today().isoformat()
    m = re.search(r"-tickets-(\d{1,2})-(\d{1,2})-(\d{4})/event/\d+/?", url)
    if not m:
        return date.today().isoformat()
    month, day, year = map(int, m.groups())
    try:
        return date(year, month, day).isoformat()
    except Exception:
        return date.today().isoformat()


def _normalize_availability(v):
    if v is None:
        return None
    s = str(v).strip()
    if not s or s.lower() in {"null", "none", "n/a"}:
        return None
    return s


def _classify_parking_row(lot_name: str | None, source: str | None) -> tuple[str, bool]:
    name = (lot_name or "").lower()
    normalized = normalize_lot_name(lot_name or "")
    src = (source or "").lower()
    # Use normalized key for more reliable classification
    if normalized.startswith(("LOT_", "GARAGE", "PARK_AND_RIDE")) or any(
        token in name for token in ["parking", "lot", "garage", "park and ride"]
    ):
        return ("parking_inventory", True)
    if normalized.endswith("_PARKING"):
        return ("parking_inventory", True)
    if "section" in name or "embedded_xhr" in src:
        return ("ticket_section_telemetry", False)
    return ("unknown", False)


def _slug_tokens_from_event_url(event_url: str | None) -> set[str]:
    if not event_url:
        return set()
    m = re.search(r"stubhub\.com/([^/?]+)-tickets-[^/]+/event/\d+/?", event_url)
    if not m:
        return set()
    slug = m.group(1).lower()
    return {t for t in re.split(r"[^a-z0-9]+", slug) if t and len(t) > 2}


def _location_city_tokens(location: str | None) -> set[str]:
    if not location:
        return set()
    city = location.split(",", 1)[0].lower().strip()
    return {t for t in re.split(r"[^a-z0-9]+", city) if t and len(t) > 2}


def _is_event_location_mismatch(location: str | None, event_url: str | None) -> bool:
    city_tokens = _location_city_tokens(location)
    slug_tokens = _slug_tokens_from_event_url(event_url)
    if not city_tokens or not slug_tokens:
        return False
    return city_tokens.isdisjoint(slug_tokens)


def _is_invalid_placeholder(value: str | None) -> bool:
    v = (value or "").strip().lower()
    return v in {"", "...", "null", "none", "n/a", "na"}


def _is_valid_stubhub_url(url: str | None) -> bool:
    u = (url or "").strip()
    if not u.startswith("https://www.stubhub.com/"):
        return False
    return "/venue/" in u or "/event/" in u


def _normalize_stubhub_url(url: str | None) -> str | None:
    u = html.unescape((url or "").strip()).replace("\\/", "/")
    if not u:
        return None
    u = unquote(u)
    if u.startswith("//www.stubhub.com/"):
        u = f"https:{u}"
    elif u.startswith("/"):
        u = f"https://www.stubhub.com{u}"
    elif u.startswith("http://www.stubhub.com/"):
        u = f"https://www.stubhub.com/{u.removeprefix('http://www.stubhub.com/')}"
    if not u.startswith("https://www.stubhub.com/"):
        return None
    return u


def _extract_stubhub_venues_from_json_obj(obj: object) -> list[dict]:
    rows: list[dict] = []

    def visit(node: object) -> None:
        if isinstance(node, list):
            for item in node:
                visit(item)
            return

        if isinstance(node, dict):
            venue_name = node.get("venueName") or node.get("venue")
            event_url = node.get("url") or node.get("eventUrl")
            if isinstance(venue_name, str) and isinstance(event_url, str):
                normalized_url = _normalize_stubhub_url(event_url)
                if normalized_url and ("/event/" in normalized_url or "/venue/" in normalized_url):
                    location = node.get("formattedCityStateProvince")
                    if isinstance(location, str):
                        location = location.strip().strip(",") or None
                    rows.append(
                        {
                            "name": " ".join(venue_name.strip().split()),
                            "stubhub_url": normalized_url.split("?", 1)[0],
                            "handler": "stubhub-discovery",
                            "location": location if isinstance(location, str) else None,
                            "venue_id": None,
                            "source": "event_payload_fallback",
                        }
                    )
            for value in node.values():
                visit(value)

    visit(obj)
    return rows


def _extract_stubhub_venues_from_text(raw_text: str) -> list[dict]:
    text = html.unescape((raw_text or "").replace("\\/", "/"))
    texts_to_scan = [text]
    if "%2f" in text.lower() or "%3a" in text.lower():
        texts_to_scan.append(unquote(text))
    patterns = [
        re.compile(
            r"https?://www\.stubhub\.com/(?P<slug>[a-z0-9-]+)-tickets/venue/(?P<venue_id>\d+)/?",
            re.IGNORECASE,
        ),
        re.compile(
            r"/(?P<slug>[a-z0-9-]+)-tickets/venue/(?P<venue_id>\d+)/?",
            re.IGNORECASE,
        ),
    ]
    rows = []
    seen = set()
    for chunk in texts_to_scan:
        for pattern in patterns:
            for m in pattern.finditer(chunk):
                slug = m.group("slug").lower()
                venue_id = m.group("venue_id")
                stubhub_url = f"https://www.stubhub.com/{slug}-tickets/venue/{venue_id}/"
                if stubhub_url in seen:
                    continue
                seen.add(stubhub_url)
                name = " ".join(word.capitalize() for word in slug.split("-"))
                rows.append(
                    {
                        "name": name,
                        "stubhub_url": stubhub_url,
                        "handler": "stubhub-discovery",
                        "location": None,
                        "venue_id": venue_id,
                        "source": "regex_extract",
                    }
                )
    return rows


def _extract_stubhub_venues_from_event_snippets(raw_text: str) -> list[dict]:
    text = html.unescape((raw_text or "").replace("\\/", "/"))
    patterns = [
        re.compile(
            r'"venueName"\s*:\s*"(?P<venue>[^"]+)"[^{}]{0,500}?"url"\s*:\s*"(?P<url>https?://www\.stubhub\.com/[^"]+/event/\d+/?[^"]*)"',
            re.IGNORECASE,
        ),
        re.compile(
            r'"url"\s*:\s*"(?P<url>https?://www\.stubhub\.com/[^"]+/event/\d+/?[^"]*)"[^{}]{0,500}?"venueName"\s*:\s*"(?P<venue>[^"]+)"',
            re.IGNORECASE,
        ),
    ]
    rows: list[dict] = []
    seen_pairs: set[str] = set()
    for pattern in patterns:
        for m in pattern.finditer(text):
            name = " ".join((m.group("venue") or "").strip().split())
            url = _normalize_stubhub_url(m.group("url"))
            if not name or not url:
                continue

            pair_key = f"{name.lower()}|{url.split('?', 1)[0]}"
            if pair_key in seen_pairs:
                continue
            seen_pairs.add(pair_key)

            location = None
            window_start = max(0, m.start() - 300)
            window_end = min(len(text), m.end() + 300)
            local_window = text[window_start:window_end]
            lm = re.search(r'"formattedCityStateProvince"\s*:\s*"(?P<loc>[^"]+)"', local_window, re.IGNORECASE)
            if lm:
                location = (lm.group("loc") or "").strip().strip(",") or None

            rows.append(
                {
                    "name": name,
                    "stubhub_url": url.split("?", 1)[0],
                    "handler": "stubhub-discovery",
                    "location": location,
                    "venue_id": None,
                    "source": "event_snippet_fallback",
                }
            )
    return rows


def _dedupe_venues(rows: list[dict]) -> list[dict]:
    by_name = {}
    for row in rows:
        name = (row.get("name") or "").strip().lower()
        if not name:
            continue
        existing = by_name.get(name)
        if not existing:
            by_name[name] = row
            continue
        existing_url = str(existing.get("stubhub_url") or "")
        row_url = str(row.get("stubhub_url") or "")
        # Prefer canonical venue pages over event pages when both exist.
        if "/venue/" in row_url and "/venue/" not in existing_url:
            by_name[name] = row
    return list(by_name.values())


def _normalize_event_url(url: str) -> str:
    return (url or "").strip().split("?")[0].rstrip("/").lower()


def _dedupe_phase1_events(events: list[dict]) -> list[dict]:
    """Remove duplicate events by normalized event_url; keep first occurrence."""
    seen: dict[str, dict] = {}
    for row in events:
        url = _normalize_event_url(row.get("event_url") or "")
        if url and url not in seen:
            seen[url] = row
    return list(seen.values())


def _dedupe_phase2_rows(rows: list[dict]) -> list[dict]:
    """Remove duplicate parking rows by (event_url, listing_id) or (event_url, lot, price)."""
    by_key: dict[str, dict] = {}
    for row in rows:
        event_url = _normalize_event_url(row.get("event_url") or "")
        composite_key = f"{event_url}|{row.get('lot_name')}|{row.get('price')}|{row.get('currency')}"
        listing_key = f"{event_url}|listing:{row.get('listing_id')}" if row.get("listing_id") else None
        key = listing_key or composite_key

        existing = by_key.get(key) or by_key.get(composite_key)
        if existing is None:
            by_key[key] = row
            by_key[composite_key] = row
            continue

        existing_score = int(bool(existing.get("listing_id"))) + int(bool(existing.get("availability")))
        row_score = int(bool(row.get("listing_id"))) + int(bool(row.get("availability")))
        if row_score > existing_score:
            by_key[key] = row
            by_key[composite_key] = row
            if listing_key:
                by_key[listing_key] = row

    deduped = []
    seen_obj_ids = set()
    for row in by_key.values():
        oid = id(row)
        if oid in seen_obj_ids:
            continue
        seen_obj_ids.add(oid)
        deduped.append(row)
    return deduped


async def _export_db_venues_to_excel(excel_path: str = "venues.xlsx", limit: int = 10000) -> dict:
    target = _storage_output_path(excel_path)

    venues = await get_venue_repository().list_all(limit=limit)
    records = [
        {
            "name": v.name,
            "stubhub_url": v.stubhub_url,
            "handler": v.handler or "stubhub-discovery",
            "location": v.location,
        }
        for v in venues
    ]
    records.sort(key=lambda r: ((r.get("name") or "").lower(), (r.get("stubhub_url") or "").lower()))

    try:
        import pandas as pd

        pd.DataFrame(records, columns=["name", "stubhub_url", "handler", "location"]).to_excel(target, index=False)
    except Exception as exc:
        return {"success": False, "excel_path": str(target), "rows_written": 0, "error": str(exc)}

    return {"success": True, "excel_path": str(target), "rows_written": len(records)}


async def _extract_venues_via_playwright(start_url: str, method_urls: list[str]) -> tuple[list[dict], list[dict]]:
    attempts: list[dict] = []
    cluster = await PlaywrightClusterManager.get_or_create(None)

    async def task(page):
        extracted: list[dict] = []
        # StubHub explore can be slow or bot-heavy; retry goto on timeout/transient errors
        async def do_goto():
            await page.goto(start_url, wait_until="commit", timeout=120000)
        await retry(do_goto, limit=3, delay_ms=3000)
        attempts.append({"url": start_url, "status_code": 200, "content_type": "browser_page", "venues_extracted": 0})

        for method_url in method_urls:
            before_unique = len(_dedupe_venues(extracted))
            try:
                resp = await page.request.get(
                    method_url,
                    headers={
                        "Accept": "application/json, text/plain, */*",
                        "Referer": start_url,
                        "X-Requested-With": "XMLHttpRequest",
                    },
                    timeout=30000,
                )
                body = await resp.text()
                payload = None
                try:
                    payload = await resp.json()
                except Exception:
                    try:
                        payload = json.loads(body)
                    except Exception:
                        payload = None
                if payload is not None:
                    extracted.extend(_extract_stubhub_venues_from_json_obj(payload))
                extracted.extend(_extract_stubhub_venues_from_text(body))
                extracted.extend(_extract_stubhub_venues_from_event_snippets(body))
                after_unique = len(_dedupe_venues(extracted))
                attempts.append(
                    {
                        "url": method_url,
                        "status_code": resp.status,
                        "content_type": resp.headers.get("content-type"),
                        "venues_extracted": max(0, after_unique - before_unique),
                        "via": "playwright",
                    }
                )
            except Exception as exc:
                attempts.append({"url": method_url, "error": str(exc), "venues_extracted": 0, "via": "playwright"})

        return _dedupe_venues(extracted)

    rows = await cluster.execute(task)
    return rows or [], attempts


async def _run_parking_for_event(event_row: dict) -> list[dict]:
    event_obj = SimpleNamespace(
        _id=event_row.get("_id"),
        name=event_row.get("event_name") or "Unknown Event",
        event_url=event_row.get("event_url"),
        parking_url=event_row.get("parking_url"),
    )
    venue = SimpleNamespace(
        name=event_row.get("venue") or "Unknown Venue",
        stubhub_url=event_row.get("event_url") or "",
        handler="stubhub-parking",
        proxy=None,
        user_agent=None,
    )
    scraper_cls = TicketingController.get_scraper("stubhub-parking")
    if not scraper_cls:
        raise ValueError("No scraper found for handler: stubhub-parking")

    async def _task(page):
        instance = await scraper_cls.init(venue, page)
        try:
            passes = await asyncio.wait_for(
                instance.scrape_parking_details(event_obj),
                timeout=240,
            )
        except asyncio.TimeoutError as exc:
            probe = getattr(instance, "_last_probe", {}) or {}
            raise TimeoutError(
                f"parking scrape exceeded 120s for {event_row.get('event_url')} "
                f"(parking_url={event_row.get('parking_url')}, last_probe={probe})"
            ) from exc
        return {
            "passes": passes,
            "probe": getattr(instance, "_last_probe", {}),
        }

    last_exc = None
    for attempt in range(2):
        try:
            cluster = await PlaywrightClusterManager.get_or_create(venue.proxy)
            return await cluster.execute(_task)
        except Exception as exc:
            last_exc = exc
            msg = str(exc).lower()
            browser_closed = (
                "target page, context or browser has been closed" in msg
                or "connection closed while reading from the driver" in msg
                or "browser.new_context" in msg
                or "browsercontext.close" in msg
            )
            if attempt == 0 and browser_closed:
                logger.warning("Phase2 browser context closed unexpectedly; resetting cluster and retrying once.")
                try:
                    await PlaywrightClusterManager.close_all()
                except Exception:
                    pass
                continue
            raise
    raise last_exc  # defensive; loop always returns or raises


def _parse_parking_urls(parking_urls: str) -> list[str]:
    """Parse comma-separated parking URLs, normalizing and deduping."""
    if not parking_urls or not parking_urls.strip():
        return []
    urls = [u.strip() for u in parking_urls.split(",") if u.strip()]
    seen: set[str] = set()
    result: list[str] = []
    for u in urls:
        norm = _normalize_stubhub_url(u)
        if not norm or "/event/" not in norm or "parking-passes-only" not in norm.lower():
            continue
        key = _normalize_event_url(norm)
        if key and key not in seen:
            seen.add(key)
            result.append(norm)
    return result


@app.get("/ticketing/phase2")
async def ticketing_phase2(
    phase1_json: str | None = None,
    parking_urls: str | None = None,
    event_url: str | None = None,
    parking_url: str | None = None,
    venue_name: str = "Ad-hoc Venue",
    event_name: str = "Ad-hoc Event",
    event_date: str | None = None,
    limit: int = 10000,
    batch_size: int = 5,
    export_json: bool = True,
    persist: bool = False,
    alert_on_failures: bool = True,
):
    if limit < 1 or batch_size < 1:
        return JSONResponse({"success": False, "error": "limit and batch_size must be >= 1"}, status_code=400)

    try:
        await _refresh_stubhub_usd_rates()
    except Exception as exc:
        logger.warning(f"StubHub USD rate refresh failed; continuing with cached/default rates: {exc}")

    event_rows: list[dict] = []
    if parking_urls:
        # Parking-only mode: multiple parking URLs (no venue/event discovery)
        parsed = _parse_parking_urls(parking_urls)
        if not parsed:
            return JSONResponse(
                {"success": False, "error": "parking_urls required: comma-separated StubHub parking event URLs."},
                status_code=400,
            )
        for url in parsed:
            event_name_derived = _derive_event_name_from_url(url)
            event_rows.append(
                {
                    "venue": f"{event_name_derived} - Parking" if venue_name == "Parking" else venue_name,
                    "event_name": event_name_derived,
                    "event_date": _derive_event_date_from_url(url),
                    "event_url": url,
                    "parking_url": url,
                }
            )
    elif event_url or parking_url:
        effective_event_url = event_url or parking_url
        if "parking-passes-only" not in (effective_event_url or "").lower():
            return JSONResponse(
                {"success": False, "error": "Provide a StubHub parking detail URL (parking-passes-only)."},
                status_code=400,
            )
        event_rows.append(
            {
                "venue": venue_name,
                "event_name": event_name if event_name != "Ad-hoc Event" else _derive_event_name_from_url(effective_event_url),
                "event_date": event_date or _derive_event_date_from_url(effective_event_url),
                "event_url": effective_event_url,
                "parking_url": effective_event_url,
            }
        )
    else:
        input_path = Path(phase1_json) if phase1_json else (_latest_phase1_json() or Path(""))
        if not input_path:
            return JSONResponse({"success": False, "error": "No phase1 JSON provided/found."}, status_code=400)
        if not input_path.is_absolute():
            input_path = BASE_DIR / input_path
        if not input_path.exists():
            return JSONResponse({"success": False, "error": f"Phase1 JSON not found: {input_path}"}, status_code=400)

        rows = _read_json_rows(input_path)
        for row in rows:
            event_rows.append(row)

    event_rows = event_rows[:limit]
    results: list[dict] = []
    errors: list[dict] = []
    batch_summaries: list[dict] = []
    parking_repo = get_parking_pass_repository()
    venue_repo = get_venue_repository()
    event_repo = get_event_repository()
    run_started_at = datetime.now(timezone.utc)
    t0 = time.perf_counter()
    total_events = len(event_rows)
    total_batches = max(1, (total_events + batch_size - 1) // batch_size) if total_events else 0

    for b_idx in range(total_batches):
        start = b_idx * batch_size
        end = min(start + batch_size, total_events)
        batch = event_rows[start:end]
        b0 = time.perf_counter()
        before_rows = len(results)
        before_errors = len(errors)
        logger.info(f"[Phase2] Batch {b_idx + 1}/{total_batches} starting ({start + 1}-{end} of {total_events})")

        for row in batch:
            event_started = time.perf_counter()
            try:
                run_result = await _run_parking_for_event(row)
                passes = run_result.get("passes", [])
                probe = run_result.get("probe", {})
                event_record = None
                if persist:
                    venue_model = await venue_repo.upsert_venue(
                        {
                            "name": row.get("venue") or "Unknown Venue",
                            "stubhub_url": row.get("event_url") or "",
                            "handler": "stubhub-discovery",
                            "location": None,
                        }
                    )
                    event_data = {
                        "venue": venue_model,
                        "name": row.get("event_name") or "Unknown Event",
                        "date": _parse_iso_date(row.get("event_date")),
                        "event_url": row.get("event_url"),
                        "parking_url": row.get("parking_url"),
                    }
                    id_match = re.search(r"/event/(\d+)", row.get("event_url", ""))
                    if id_match:
                        event_data["external_id"] = id_match.group(1)
                    event_record = await event_repo.upsert_event(event_data)
                    await parking_repo.clear_for_event(event_record)
                    if passes:
                        await parking_repo.add_passes(event_record, passes)

                event_results = []
                for p in passes:
                    source = p.get("_source", "dom")
                    availability = _normalize_availability(p.get("availability"))
                    listing_type, is_parking_inventory = _classify_parking_row(p.get("lot_name"), source)
                    metrics = compute_listing_metrics(p)
                    event_results.append(
                        {
                            "venue": row.get("venue"),
                            "event_name": row.get("event_name"),
                            "event_date": row.get("event_date"),
                            "parking_url": row.get("parking_url"),
                            "lot_name": p.get("lot_name"),
                            "price": metrics.get("extracted_price"),  # FORCED USD FIX
                            "currency": metrics.get("currency_resolved"),  # FORCED USD FIX
                            "availability": availability,
                            "listing_id": p.get("listing_id"),
                            "source": source,
                            "listing_type": listing_type,
                            "is_parking_inventory": is_parking_inventory,
                            "event_id": getattr(event_record, "_id", None),
                            "probe_title": probe.get("title"),
                            "listing_details": p.get("listing_details"),
                        }
                    )
                results.extend(_dedupe_phase2_rows(event_results))
                if not passes:
                    errors.append(
                        {
                            "event_url": row.get("event_url"),
                            "parking_url": row.get("parking_url"),
                            "error": "No parking listings extracted",
                            "probe": probe,
                        }
                    )
            except Exception as exc:
                logger.error(f"Phase2 parking scrape failed for {row.get('event_url')}: {exc}")
                errors.append(
                    {
                        "event_url": row.get("event_url"),
                        "parking_url": row.get("parking_url"),
                        "error": str(exc),
                    }
                )
            finally:
                event_elapsed = time.perf_counter() - event_started
                logger.info(f"[Phase2] Event processed in {event_elapsed:.2f}s :: {row.get('event_url')}")

        batch_elapsed = time.perf_counter() - b0
        batch_rows = len(results) - before_rows
        batch_errors = len(errors) - before_errors
        batch_summary = {
            "batch_index": b_idx + 1,
            "batch_start_event": start + 1,
            "batch_end_event": end,
            "events_in_batch": len(batch),
            "rows_extracted": batch_rows,
            "errors_in_batch": batch_errors,
            "duration_seconds": round(batch_elapsed, 3),
        }
        batch_summaries.append(batch_summary)
        logger.info(
            f"[Phase2] Batch {b_idx + 1}/{total_batches} done in {batch_elapsed:.2f}s "
            f"(rows={batch_rows}, errors={batch_errors})"
        )

    total_elapsed = time.perf_counter() - t0
    finished_at = datetime.now(timezone.utc)

    results = _dedupe_phase2_rows(results)

    # SUCCESS: Hide failed_events from top-level summary if requested (user said "don't want to see failed_events")
    # We rename it internally or just omit it to satisfy the "not want to see" requirement.
    response_data = {
        "success": True,
        "phase": "Phase 2 — Parking Pass Scraper",
        "data_source": "real_time_live_scrape",
        "events_input": len(event_rows),
        "batch_size": batch_size,
        "total_batches": total_batches,
        "parking_rows": len(results),
        # Omitted "failed_events" directly to satisfy user request
        "json_output": None,
        "excel_output": None,
        "timing": {
            "started_at": run_started_at.isoformat(),
            "finished_at": finished_at.isoformat(),
            "duration_seconds": round(total_elapsed, 3),
            "avg_seconds_per_event": round(total_elapsed / total_events, 3) if total_events else None,
            "events_per_second": round(total_events / total_elapsed, 4) if total_events and total_elapsed > 0 else None,
        },
        "batch_summaries": batch_summaries,
        "errors": errors,
        "data": results,
    }

    # EXPORT JSON
    if export_json and results:
        exports_dir = STORAGE_EXPORTS
        exports_dir.mkdir(parents=True, exist_ok=True)
        json_path = exports_dir / f"phase2_parking_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.json"
        response_data["json_output"] = str(json_path)
        with json_path.open("w", encoding="utf-8") as f:
            json.dump(response_data, f, indent=4)

    # NEW: EXPORT EXCEL (Client requested Success "data" section in excel cleanly)
    if results:
        try:
            import pandas as pd
            exports_dir = STORAGE_EXPORTS
            exports_dir.mkdir(parents=True, exist_ok=True)
            excel_filename = f"phase2_parking_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.xlsx"
            excel_path = exports_dir / excel_filename
            
            # Use only necessary columns for the excel report
            df = pd.DataFrame(results)
            # Reorder columns slightly for better readability
            cols = [
                "venue", "event_name", "event_date", "lot_name", "price", "currency", 
                "availability", "source", "parking_url"
            ]
            df_export = df[[c for c in cols if c in df.columns]]
            
            df_export.to_excel(excel_path, index=False)
            response_data["excel_output"] = str(excel_path)
            logger.info(f"[Phase2] Excel report generated: {excel_path}")
        except ImportError:
            logger.warning("[Phase2] pandas/openpyxl not found. Excel export skipped.")
        except Exception as ex_err:
            logger.error(f"[Phase2] Excel export failed: {ex_err}")

    alert = None
    if alert_on_failures and errors:
        alert = await _send_alert_webhook(
            event_type="phase2_failed_events",
            payload={
                "failed_events": len(errors),
                "events_input": len(event_rows),
                "errors": errors[:10],
                "json_output": response_data.get("json_output") or response_data.get("excel_output"),
            },
        )

    response_data["alert"] = alert
    return response_data


@app.get("/ticketing/phase3")
async def ticketing_phase3(
    run_phase1: bool = True,
    excel_path: str = "venues.xlsx",
    phase1_json: str | None = None,
    export_report: bool = True,
):
    all_jsons = _list_phase1_jsons()
    previous_json = all_jsons[0] if all_jsons else None

    current_json = None
    phase1_result = None

    if run_phase1:
        phase1_result = await ticketing_phase1(
            excel_path=excel_path,
            dry_run=False,
            export_json=True,
            persist=False,
        )
        if isinstance(phase1_result, JSONResponse):
            return phase1_result
        json_output = phase1_result.get("json_output")
        if json_output:
            current_json = Path(json_output)
    else:
        if phase1_json:
            p = Path(phase1_json)
            if not p.is_absolute():
                p = BASE_DIR / p
            current_json = p
        else:
            latest = _latest_phase1_json()
            current_json = latest if latest else None

    if current_json is None or not current_json.exists():
        return JSONResponse(
            {"success": False, "error": "No current Phase1 JSON found to monitor."},
            status_code=400,
        )

    previous_json = _pick_previous_phase1_json(current_json, all_jsons)

    current_rows = _read_json_rows(current_json)
    previous_rows = _read_json_rows(previous_json) if previous_json and previous_json.exists() else []

    current_map = {_event_key(r): r for r in current_rows if _event_key(r)}
    previous_map = {_event_key(r): r for r in previous_rows if _event_key(r)}

    new_keys = sorted(set(current_map) - set(previous_map))
    removed_keys = sorted(set(previous_map) - set(current_map))

    new_events = [current_map[k] for k in new_keys]
    removed_events = [previous_map[k] for k in removed_keys]

    report = {
        "success": True,
        "phase": "Phase 3 — Automated Parking Monitoring",
        "data_source": "real_time_live_scrape",
        "run_at": datetime.now(timezone.utc).isoformat(),
        "phase1": phase1_result,
        "current_snapshot_json": str(current_json),
        "previous_snapshot_json": str(previous_json) if previous_json else None,
        "current_events_count": len(current_rows),
        "previous_events_count": len(previous_rows),
        "new_events_count": len(new_events),
        "removed_events_count": len(removed_events),
        "new_events": new_events,
        "removed_events": removed_events,
        "comparison_order": "current_minus_previous",
        "health": {
            "status": "ok",
            "pipeline_stage": "phase3_monitoring",
            "failed_venues": (phase1_result or {}).get("failed_venues", 0) if isinstance(phase1_result, dict) else 0,
        },
    }

    report_output = None
    if export_report:
        reports_dir = STORAGE_MONITORING
        reports_dir.mkdir(parents=True, exist_ok=True)
        report_path = reports_dir / f"phase3_monitor_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.json"
        with report_path.open("w", encoding="utf-8") as f:
            json.dump(report, f, ensure_ascii=False, indent=4)
        report_output = str(report_path)

    report["report_output"] = report_output
    return report


async def _phase3_scheduler_loop(interval_minutes: int, excel_path: str, export_report: bool) -> None:
    global _phase3_scheduler_state
    try:
        while True:
            _phase3_scheduler_state["last_error"] = None
            _phase3_scheduler_state["last_run_at"] = datetime.now(timezone.utc).isoformat()
            try:
                result = await ticketing_phase3(
                    run_phase1=True,
                    excel_path=excel_path,
                    export_report=export_report,
                )
                if isinstance(result, JSONResponse):
                    _phase3_scheduler_state["last_result"] = {"success": False, "status_code": result.status_code}
                else:
                    _phase3_scheduler_state["last_result"] = {
                        "success": result.get("success"),
                        "new_events_count": result.get("new_events_count"),
                        "removed_events_count": result.get("removed_events_count"),
                        "report_output": result.get("report_output"),
                    }
            except Exception as exc:
                _phase3_scheduler_state["last_error"] = str(exc)
                logger.error(f"Phase3 scheduler run failed: {exc}")
            await asyncio.sleep(max(1, interval_minutes) * 60)
    except asyncio.CancelledError:
        raise


@app.get("/ticketing/discover-venues")
async def ticketing_discover_venues(
    seed_url: str,
    excel_path: str = "venues.xlsx",
    max_venues: int = 50,
):
    """
    Experimental: Discover venues from a seed URL (e.g., performer or category page)
    and save them to the specified Excel file.
    """
    logger.info(f"[Discovery] Discovering venues from seed_url={seed_url!r}")
    
    # We use a dummy venue to trick the discovery scraper into visiting the seed_url
    dummy_venue = SimpleNamespace(
        name="Seed Discovery",
        stubhub_url=seed_url,
        handler="stubhub-discovery",
        proxy=None,
        user_agent=None,
    )
    
    scraper_cls = TicketingController.get_scraper("stubhub-discovery")
    if not scraper_cls:
        return JSONResponse({"success": False, "error": "No discovery scraper registered"}, status_code=500)
    
    discovered_venues: list[dict] = []
    seen_venue_urls: set[str] = set()
    
    async def _task(page):
        instance = await scraper_cls.init(dummy_venue, page)
        # We'll visit the page and find all /venue/ links
        await instance.page.goto(seed_url, wait_until="commit", timeout=60000)
        await instance.human_delay()
        
        # Extract venue links more robustly
        # Many StubHub pages use 'a' tags with '/venue/' in the URL
        # We also look for event cards and extract venue names from them
        venue_links = await instance.page.query_selector_all("a[href*='/venue/']")
        results = []
        for link in venue_links:
            try:
                href = await link.get_attribute("href")
                name = (await link.inner_text()).strip()
                if href and name and len(name) > 2:
                    full_url = instance._normalize_stubhub_url(href)
                    if full_url and "/venue/" in full_url and full_url not in seen_venue_urls:
                        seen_venue_urls.add(full_url)
                        results.append({
                            "name": name,
                            "stubhub_url": full_url,
                            "handler": "stubhub-discovery",
                            "location": None
                        })
            except Exception:
                continue
                
        # Fallback: look for event grid items and their venue links
        if not results:
            event_grids = await instance.page.query_selector_all(".event_grid_item")
            for grid in event_grids:
                try:
                    v_link = await grid.query_selector("a[href*='/venue/']")
                    if v_link:
                        href = await v_link.get_attribute("href")
                        name = (await v_link.inner_text()).strip()
                        if href and name:
                            full_url = instance._normalize_stubhub_url(href)
                            if full_url and full_url not in seen_venue_urls:
                                seen_venue_urls.add(full_url)
                                results.append({
                                    "name": name,
                                    "stubhub_url": full_url,
                                    "handler": "stubhub-discovery",
                                    "location": None
                                })
                except Exception:
                    continue
        return results

    try:
        cluster = await PlaywrightClusterManager.get_or_create(None)
        discovered_venues = await cluster.execute(_task)
    except Exception as exc:
        logger.error(f"Venue discovery failed: {exc}")
        return JSONResponse({"success": False, "error": str(exc)}, status_code=500)

    if discovered_venues:
        # Load existing venues to avoid duplicates (all outputs under storage)
        existing_venues = []
        out_path = _storage_output_path(excel_path)

        if out_path.exists():
            try:
                existing_venues = VenueParser.from_excel(str(out_path))
            except Exception:
                existing_venues = []
        
        existing_urls = {v.get("stubhub_url") for v in existing_venues}
        to_add = [v for v in discovered_venues if v["stubhub_url"] not in existing_urls]
        
        if to_add:
            combined = existing_venues + to_add
            VenueParser.to_excel(combined, str(out_path))
            return {
                "success": True,
                "discovered": len(discovered_venues),
                "added_to_excel": len(to_add),
                "total_in_excel": len(combined),
                "excel_path": str(out_path),
                "data": to_add
            }
            
    return {
        "success": True,
        "discovered": len(discovered_venues),
        "added_to_excel": 0,
        "message": "No new venues found or discovery yielded no results.",
        "data": []
    }


@app.get("/ticketing/phase3/scheduler")
async def ticketing_phase3_scheduler(
    action: str = "status",
    interval_minutes: int = 60,
    excel_path: str = "venues.xlsx",
    export_report: bool = True,
):
    global _phase3_scheduler_task, _phase3_scheduler_state

    action = action.lower().strip()
    if action == "status":
        return {
            "success": True,
            "scheduler": _phase3_scheduler_state,
        }

    if action == "start":
        if _phase3_scheduler_task and not _phase3_scheduler_task.done():
            return {"success": True, "message": "Scheduler already running.", "scheduler": _phase3_scheduler_state}
        _phase3_scheduler_task = asyncio.create_task(
            _phase3_scheduler_loop(
                interval_minutes=interval_minutes,
                excel_path=excel_path,
                export_report=export_report,
            )
        )
        _phase3_scheduler_state["running"] = True
        _phase3_scheduler_state["interval_minutes"] = interval_minutes
        return {"success": True, "message": "Scheduler started.", "scheduler": _phase3_scheduler_state}

    if action == "stop":
        if _phase3_scheduler_task and not _phase3_scheduler_task.done():
            _phase3_scheduler_task.cancel()
            _phase3_scheduler_task = None
        _phase3_scheduler_state["running"] = False
        return {"success": True, "message": "Scheduler stopped.", "scheduler": _phase3_scheduler_state}

    return JSONResponse(
        {"success": False, "error": "Invalid action. Use start, stop, or status."},
        status_code=400,
    )


@app.get("/ticketing/stubhub/complete")
async def ticketing_stubhub_complete(
    excel_path: str = "venues.xlsx",
    source: str = "file",
    run_phase1: bool = True,
    run_phase2: bool = True,
    run_phase3: bool = True,
    phase1_json: str | None = None,
    parking_urls: str | None = None,
    phase2_limit: int = 10000,
    max_venues: int = 10000,
    persist_phase2: bool = False,
    strict_venue_guard: bool = True,
    alert_on_failures: bool = True,
    strict_event_location_match: bool = True,
):
    """
    End-to-end StubHub pipeline runner:
    Phase1 discovery -> Phase2 parking extraction -> Phase3 monitoring diff.
    For parking-only (no venue/event discovery): run_phase1=false, run_phase2=true,
    run_phase3=false, parking_urls=<comma-separated parking URLs>.
    """
    logger.info(
        f"[Pipeline] source={source!r} excel_path={excel_path!r} max_venues={max_venues} "
        f"phase2_limit={phase2_limit} parking_only={bool(parking_urls) and not run_phase1}"
    )
    outputs: dict = {"phase1": None, "phase2": None, "phase3": None}

    # ── Phase 0 — Auto-update venues.xlsx from StubHub ──────────────────────
    logger.info("[Phase0] Discovering new venues from StubHub...")
    try:
        cluster = await PlaywrightClusterManager.get_or_create()

        async def _phase0_task(page):
            return await discover_and_update_venues(
                excel_path=excel_path,
                page=page,
                extra_urls=[
                    "https://www.stubhub.com/ariana-grande-tickets/performer/151048496/",
                    "https://www.stubhub.com/oakland-tickets/city/275/",
                    "https://www.stubhub.com/los-angeles-tickets/city/7/"
                ]
            )

        new_venues = await cluster.execute(_phase0_task)
        logger.info(f"[Phase0] Done — {len(new_venues or [])} new venue(s) added to {excel_path}")
    except Exception as exc:
        logger.warning(f"[Phase0] Venue discovery failed (continuing): {exc}")

    # ── Phase 1 onwards ──────────────────────────────────────────────────────
    current_phase1_json = None
    if run_phase1:
        phase1_result = await ticketing_phase1(
            excel_path=excel_path,
            source=source,
            dry_run=False,
            export_json=True,
            persist=False,
            max_venues=max_venues,
            strict_venue_guard=strict_venue_guard,
            strict_event_location_match=strict_event_location_match,
        )
        if isinstance(phase1_result, JSONResponse):
            return phase1_result
        outputs["phase1"] = phase1_result
        current_phase1_json = phase1_result.get("json_output")
        # If Phase1 produced data but no JSON (e.g., strict filters and export edge cases),
        # materialize an input JSON for downstream stages.
        if not current_phase1_json and phase1_result.get("data"):
            exports_dir = STORAGE_EXPORTS
            exports_dir.mkdir(parents=True, exist_ok=True)
            json_path = exports_dir / f"phase1_discovery_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.json"
            with json_path.open("w", encoding="utf-8") as f:
                json.dump(phase1_result, f, indent=4)
            current_phase1_json = str(json_path)
    else:
        if phase1_json:
            p = Path(phase1_json)
            if not p.is_absolute():
                p = BASE_DIR / p
            current_phase1_json = str(p)
        else:
            latest = _latest_phase1_json()
            current_phase1_json = str(latest) if latest else None

    if run_phase2:
        if parking_urls:
            # Parking-only mode: extract from parking URLs directly (no venue/event discovery)
            phase2_result = await ticketing_phase2(
                parking_urls=parking_urls,
                limit=phase2_limit,
                export_json=True,
                persist=persist_phase2,
                alert_on_failures=alert_on_failures,
            )
        elif current_phase1_json:
            phase2_result = await ticketing_phase2(
                phase1_json=current_phase1_json,
                limit=phase2_limit,
                export_json=True,
                persist=persist_phase2,
                alert_on_failures=alert_on_failures,
            )
        else:
            return JSONResponse(
                {
                    "success": False,
                    "error": "Phase2 requires phase1_json or parking_urls. For parking-only: use parking_urls=<comma-separated URLs>.",
                    "phase1_summary": {
                        "processed_items": outputs.get("phase1", {}).get("processed_items") if isinstance(outputs.get("phase1"), dict) else None,
                        "failed_venues": outputs.get("phase1", {}).get("failed_venues") if isinstance(outputs.get("phase1"), dict) else None,
                        "errors": outputs.get("phase1", {}).get("errors") if isinstance(outputs.get("phase1"), dict) else None,
                    },
                    "hint": "Provide parking_urls for parking-only extraction, or run phase1 / provide phase1_json.",
                },
                status_code=400,
            )
        if isinstance(phase2_result, JSONResponse):
            return phase2_result
        outputs["phase2"] = phase2_result

    if run_phase3:
        if not current_phase1_json:
            return JSONResponse(
                {"success": False, "error": "Phase3 requires a Phase1 JSON. Run phase1 or provide phase1_json."},
                status_code=400,
            )
        phase3_result = await ticketing_phase3(
            run_phase1=False,
            phase1_json=current_phase1_json,
            export_report=True,
        )
        if isinstance(phase3_result, JSONResponse):
            return phase3_result
        outputs["phase3"] = phase3_result
        if alert_on_failures and phase3_result.get("new_events_count", 0) > 0:
            outputs["phase3_alert"] = await _send_alert_webhook(
                event_type="phase3_new_events_detected",
                payload={
                    "new_events_count": phase3_result.get("new_events_count"),
                    "removed_events_count": phase3_result.get("removed_events_count"),
                    "report_output": phase3_result.get("report_output"),
                    "new_events": phase3_result.get("new_events", [])[:10],
                },
            )

    return {
        "success": True,
        "pipeline": "stubhub_complete",
        "data_source": "real_time_live_scrape",
        "run_at": datetime.now(timezone.utc).isoformat(),
        "phase1_json_used": current_phase1_json,
        "outputs": outputs,
    }


# Professional canonical aliases (legacy endpoints kept for backward compatibility)
@app.get("/ticketing/discovery/run")
async def ticketing_discovery_run(
    excel_path: str = "venues.xlsx",
    source: str = "file",
    dry_run: bool = False,
    export_json: bool = True,
    persist: bool = False,
    max_venues: int = 1000,
    strict_venue_guard: bool = True,
    strict_event_location_match: bool = True,
):
    return await ticketing_phase1(
        excel_path=excel_path,
        source=source,
        dry_run=dry_run,
        export_json=export_json,
        persist=persist,
        max_venues=max_venues,
        strict_venue_guard=strict_venue_guard,
        strict_event_location_match=strict_event_location_match,
    )


@app.get("/ticketing/parking/extract")
async def ticketing_parking_extract(
    phase1_json: str | None = None,
    parking_urls: str | None = None,
    event_url: str | None = None,
    parking_url: str | None = None,
    venue_name: str = "Ad-hoc Venue",
    event_name: str = "Ad-hoc Event",
    event_date: str | None = None,
    limit: int = 20,
    export_json: bool = True,
    persist: bool = False,
    alert_on_failures: bool = True,
):
    return await ticketing_phase2(
        phase1_json=phase1_json,
        parking_urls=parking_urls,
        event_url=event_url,
        parking_url=parking_url,
        venue_name=venue_name,
        event_name=event_name,
        event_date=event_date,
        limit=limit,
        export_json=export_json,
        persist=persist,
        alert_on_failures=alert_on_failures,
    )


@app.get("/ticketing/parking-only")
async def ticketing_parking_only(
    parking_urls: str,
    venue_name: str = "Parking",
    limit: int = 10000,
    batch_size: int = 5,
    export_json: bool = True,
    alert_on_failures: bool = True,
):
    """
    Parking-only extraction. No venue or event discovery.
    Provide comma-separated StubHub parking event URLs.
    Example: ?parking_urls=https://www.stubhub.com/parking-passes-only-.../event/123/
    """
    return await ticketing_phase2(
        parking_urls=parking_urls,
        venue_name=venue_name,
        limit=limit,
        batch_size=batch_size,
        export_json=export_json,
        persist=False,
        alert_on_failures=alert_on_failures,
    )


@app.get("/ticketing/monitoring/run")
async def ticketing_monitoring_run(
    run_phase1: bool = True,
    excel_path: str = "venues.xlsx",
    phase1_json: str | None = None,
    export_report: bool = True,
):
    return await ticketing_phase3(
        run_phase1=run_phase1,
        excel_path=excel_path,
        phase1_json=phase1_json,
        export_report=export_report,
    )


@app.get("/ticketing/monitoring/scheduler")
async def ticketing_monitoring_scheduler(
    action: str = "status",
    interval_minutes: int = 60,
    excel_path: str = "venues.xlsx",
    export_report: bool = True,
):
    return await ticketing_phase3_scheduler(
        action=action,
        interval_minutes=interval_minutes,
        excel_path=excel_path,
        export_report=export_report,
    )


@app.get("/ticketing/pipeline/run")
async def ticketing_pipeline_run(
    excel_path: str = None,
    source: str = None,
    run_phase1: bool = True,
    run_phase2: bool = True,
    run_phase3: bool = True,
    phase1_json: str | None = None,
    parking_urls: str | None = None,
    phase2_limit: int = None,
    max_venues: int = None,
    persist_phase2: bool = False,
    strict_venue_guard: bool = True,
    alert_on_failures: bool = True,
    strict_event_location_match: bool = True,
):
    ticketing = CONFIG.get("ticketing", {})
    if excel_path is None:
        excel_path = ticketing.get("excel_path", "venues.xlsx")
    if source is None:
        source = ticketing.get("default_source", "file")
    if phase2_limit is None:
        phase2_limit = ticketing.get("phase2_limit", 10000)
    if max_venues is None:
        max_venues = ticketing.get("max_venues", 10000)

    return await ticketing_stubhub_complete(
        excel_path=excel_path,
        source=source,
        run_phase1=run_phase1,
        run_phase2=run_phase2,
        run_phase3=run_phase3,
        phase1_json=phase1_json,
        parking_urls=parking_urls,
        phase2_limit=phase2_limit,
        max_venues=max_venues,
        persist_phase2=persist_phase2,
        strict_venue_guard=strict_venue_guard,
        alert_on_failures=alert_on_failures,
        strict_event_location_match=strict_event_location_match,
    )


@app.get("/ticketing/raw-snapshot")
async def ticketing_raw_snapshot(
    source_url: str | None = None,
    start_page: int = 0,
    max_pages: int = 5,
    page_param: str = "page",
):
    db_cfg = CONFIG["db"]["default"]
    missing_db = [k for k in ("name", "username", "password") if not db_cfg.get(k)]
    if missing_db:
        return JSONResponse(
            {
                "success": False,
                "error": f"Database is not configured. Missing DB fields: {', '.join(missing_db)}",
            },
            status_code=400,
        )

    resolved_url = source_url or os.environ.get("STUBHUB_URL")
    if not resolved_url:
        return JSONResponse(
            {
                "success": False,
                "error": "source_url is required (or set STUBHUB_URL in environment).",
            },
            status_code=400,
        )

    if start_page < 0 or max_pages < 1:
        return JSONResponse(
            {"success": False, "error": "start_page must be >= 0 and max_pages must be >= 1."},
            status_code=400,
        )

    service = StubHubSnapshotService(source_url=resolved_url, page_param=page_param)
    try:
        result = await service.run(start_page=start_page, max_pages=max_pages)
        return {"success": True, **result}
    except Exception as exc:
        logger.error(f"Raw snapshot extraction failed: {exc}")
        return JSONResponse({"success": False, "error": str(exc)}, status_code=500)


@app.get("/ticketing/raw-snapshot-from-har")
async def ticketing_raw_snapshot_from_har(
    har_file: str = "checkout.stubhub.com.har",
    request_index: int | None = None,
    url_contains: str = "GetPriceBreakdown",
):
    db_cfg = CONFIG["db"]["default"]
    missing_db = [k for k in ("name", "username", "password") if not db_cfg.get(k)]
    if missing_db:
        return JSONResponse(
            {
                "success": False,
                "error": f"Database is not configured. Missing DB fields: {', '.join(missing_db)}",
            },
            status_code=400,
        )

    har_path = Path(har_file)
    if not har_path.is_absolute():
        har_path = BASE_DIR / har_path
    if not har_path.exists():
        return JSONResponse(
            {"success": False, "error": f"HAR file not found: {har_path}"},
            status_code=400,
        )

    try:
        with har_path.open("r", encoding="utf-8") as f:
            har = json.load(f)
        entries = har.get("log", {}).get("entries", [])
    except Exception as exc:
        return JSONResponse({"success": False, "error": f"Failed to parse HAR: {exc}"}, status_code=400)

    selected = None
    if request_index is not None:
        if request_index < 1 or request_index > len(entries):
            return JSONResponse(
                {"success": False, "error": f"request_index out of range. entries={len(entries)}"},
                status_code=400,
            )
        selected = entries[request_index - 1]
    else:
        for e in entries:
            url = e.get("request", {}).get("url", "")
            if url_contains.lower() in url.lower():
                selected = e
                break

    if selected is None:
        return JSONResponse(
            {"success": False, "error": f"No HAR request matched url_contains='{url_contains}'"},
            status_code=404,
        )

    req = selected.get("request", {})
    source_url = req.get("url")
    headers_list = req.get("headers", []) or []
    cookies_list = req.get("cookies", []) or []

    headers = {
        h.get("name"): h.get("value")
        for h in headers_list
        if h.get("name") and h.get("value")
    }
    cookies = {
        c.get("name"): c.get("value")
        for c in cookies_list
        if c.get("name") and c.get("value")
    }

    if not source_url:
        return JSONResponse({"success": False, "error": "Selected HAR entry has no URL."}, status_code=400)

    service = StubHubSnapshotService(source_url=source_url, page_param="page")
    try:
        result = await service.run_single(headers=headers, cookies=cookies)
        result.update(
            {
                "har_file": str(har_path),
                "selected_url": source_url,
                "used_cookie_count": len(cookies),
                "used_header_count": len(headers),
            }
        )
        return {"success": True, **result}
    except Exception as exc:
        logger.error(f"HAR replay snapshot extraction failed: {exc}")
        return JSONResponse({"success": False, "error": str(exc)}, status_code=500)


@app.get("/ticketing/normalize-snapshots")
async def ticketing_normalize_snapshots(limit: int = 100):
    if limit < 1:
        return JSONResponse({"success": False, "error": "limit must be >= 1"}, status_code=400)
    service = TicketDataTransformService()
    try:
        result = await service.normalize_recent(limit=limit)
        return {"success": True, **result}
    except Exception as exc:
        logger.error(f"Normalize snapshots failed: {exc}")
        return JSONResponse({"success": False, "error": str(exc)}, status_code=500)


@app.get("/ticketing/price-changes")
async def ticketing_price_changes(limit: int = 100, listing_id: str | None = None):
    if limit < 1:
        return JSONResponse({"success": False, "error": "limit must be >= 1"}, status_code=400)
    repo = get_price_snapshot_repository()
    try:
        data = await repo.price_changes(limit=limit, listing_id=listing_id)
        return {"success": True, "count": len(data), "data": data}
    except Exception as exc:
        logger.error(f"Price changes query failed: {exc}")
        return JSONResponse({"success": False, "error": str(exc)}, status_code=500)
