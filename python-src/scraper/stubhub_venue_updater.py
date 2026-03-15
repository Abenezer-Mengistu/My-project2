"""
Phase 0 — StubHub Venue Updater

Scrapes StubHub's popular events and category pages to discover new
performer / event pages that have parking passes available, then
appends any NEW entries to venues.xlsx before the main pipeline runs.

URL pattern used for venues:
  https://www.stubhub.com/<slug>-tickets/performer/<ID>?gridFilterType=1

gridFilterType=1  →  Show only parking-pass events for that performer.

Called automatically at the start of ticketing_stubhub_complete().
"""
from __future__ import annotations

import re
import asyncio
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlparse, parse_qs, urlencode, urlunparse

import pandas as pd

from utils.logger import logger


# ── StubHub pages to crawl for popular performers ───────────────────────────

STUBHUB_DISCOVERY_URLS = [
    "https://www.stubhub.com/concert-tickets/",
    "https://www.stubhub.com/sports-tickets/",
    "https://www.stubhub.com/theater-tickets/",
    "https://www.stubhub.com/trending-tickets/",
    "https://www.stubhub.com/parking-passes-only-tickets/",
    "https://www.stubhub.com/explore?sections=top_events",
    "https://www.stubhub.com/",
]

# Regex matching a StubHub performer page
_PERFORMER_RE = re.compile(
    r"https?://(?:www\.)?stubhub\.com/([a-z0-9\-]+)-tickets/performer/(\d+)",
    re.IGNORECASE,
)

# ── Helpers ──────────────────────────────────────────────────────────────────


def _parking_url_for_performer(raw_url: str) -> str:
    """Append gridFilterType=1 (parking-only filter) to a performer URL."""
    parsed = urlparse(raw_url)
    qs = parse_qs(parsed.query, keep_blank_values=True)
    qs["gridFilterType"] = ["1"]
    new_query = urlencode({k: v[0] for k, v in qs.items()})
    return urlunparse(parsed._replace(query=new_query))


def _performer_name_from_url(url: str) -> str:
    """Convert 'taylor-swift-tickets/performer/...' → 'Taylor Swift'."""
    m = _PERFORMER_RE.search(url)
    if not m:
        return url
    slug = m.group(1)
    return slug.replace("-", " ").title()


def _load_existing_urls(excel_path: str | Path) -> set[str]:
    """Return the set of stubhub_urls already in venues.xlsx."""
    p = Path(excel_path)
    if not p.exists():
        return set()
    try:
        df = pd.read_excel(p)
        col = next(
            (c for c in df.columns if c.lower() in ("stubhub_url", "stubhub url", "url")),
            None,
        )
        if col:
            return set(str(v).strip() for v in df[col].dropna())
    except Exception as exc:
        logger.warning(f"[VenueUpdater] Could not read existing venues: {exc}")
    return set()


def _append_to_excel(new_rows: list[dict], excel_path: str | Path) -> None:
    """Append new venue rows to venues.xlsx, creating it if needed."""
    p = Path(excel_path)
    if p.exists():
        existing = pd.read_excel(p)
    else:
        existing = pd.DataFrame(columns=["name", "stubhub_url", "handler"])

    additions = pd.DataFrame(new_rows)
    combined = pd.concat([existing, additions], ignore_index=True)
    combined.to_excel(p, index=False)
    logger.info(f"[VenueUpdater] Saved {len(combined)} venues to {p}")


# ── Browser-based discovery ──────────────────────────────────────────────────


async def _extract_performer_links_from_page(page) -> list[str]:
    """
    Pull all href values from the current page that match a StubHub performer URL.
    Returns deduplicated list of bare performer URLs (no params yet).
    """
    try:
        hrefs: list[str] = await page.evaluate(
            """() => {
                const links = Array.from(document.querySelectorAll('a[href]'));
                return links.map(a => a.href).filter(h => h.includes('/performer/'));
            }"""
        )
    except Exception as exc:
        logger.warning(f"[VenueUpdater] JS href extract failed: {exc}")
        return []

    found: set[str] = set()
    for href in hrefs:
        m = _PERFORMER_RE.search(href)
        if m:
            # Normalise: keep only up to /performer/<ID>
            base = f"https://www.stubhub.com/{m.group(1)}-tickets/performer/{m.group(2)}"
            found.add(base)
    return list(found)


async def discover_and_update_venues(
    excel_path: str | Path,
    page,
    extra_urls: list[str] | None = None,
    min_new_to_log: int = 1,
) -> list[dict[str, Any]]:
    """
    Main Phase-0 entry point.

    Visits STUBHUB_DISCOVERY_URLS (and any extra_urls) with the already-open
    Playwright `page`, extracts performer links, filters out existing ones,
    and appends new entries to venues.xlsx.

    Returns list of new venue dicts that were added.
    """
    excel_path = Path(excel_path)
    existing_urls = _load_existing_urls(excel_path)
    urls_to_crawl = list(STUBHUB_DISCOVERY_URLS) + (extra_urls or [])

    discovered_performers: dict[str, str] = {}  # base_url → name

    for crawl_url in urls_to_crawl:
        try:
            logger.info(f"[VenueUpdater] Crawling: {crawl_url}")
            await page.goto(crawl_url, timeout=30_000, wait_until="domcontentloaded")
            await asyncio.sleep(2)  # Let dynamic content settle

            links = await _extract_performer_links_from_page(page)
            for base_url in links:
                if base_url not in discovered_performers:
                    discovered_performers[base_url] = _performer_name_from_url(base_url)
        except Exception as exc:
            logger.warning(f"[VenueUpdater] Failed to crawl {crawl_url}: {exc}")
            continue

    # Convert to parking-filtered URLs
    new_rows: list[dict] = []
    for base_url, name in sorted(discovered_performers.items(), key=lambda x: x[1]):
        parking_url = _parking_url_for_performer(base_url)

        # Skip if already in venues.xlsx (check both base and parking URL)
        if base_url in existing_urls or parking_url in existing_urls:
            continue

        new_rows.append(
            {
                "name": name,
                "stubhub_url": parking_url,
                "handler": "stubhub-discovery",
            }
        )
        logger.info(f"[VenueUpdater] New venue: {name} → {parking_url}")

    if new_rows:
        _append_to_excel(new_rows, excel_path)
        logger.info(
            f"[VenueUpdater] Added {len(new_rows)} new venues to {excel_path}"
        )
    else:
        logger.info("[VenueUpdater] No new venues found — venues.xlsx is up to date.")

    return new_rows
