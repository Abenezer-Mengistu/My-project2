"""
StubHub Discovery Scraper — replaces StubHubDiscoveryScraper.ts.
Discovers events from a StubHub venue page.
Refactored for modular 'discovery/' component and explicit waits.
"""
from __future__ import annotations

import asyncio
import datetime
import html
import re
from urllib.parse import parse_qs, urlparse
import pendulum

from scraper.base.ticketing.ticketing_playwright_base import TicketingPlaywrightBase
from database.repositories.ticketing.events import get_event_repository
from utils.logger import logger


class StubHubDiscoveryScraper(TicketingPlaywrightBase):
    handler: str = "stubhub-discovery"

    @staticmethod
    def _is_event_url(url: str) -> bool:
        return bool(re.search(r"/event/\d+", url))


    @staticmethod
    def _normalize_event_url(href: str | None) -> str | None:
        if not href:
            return None
        href = html.unescape(href.strip())
        if "/event/" not in href:
            return None
        # Handle malformed relative forms like "/www.stubhub.com/..." or "//www.stubhub.com/...".
        if href.startswith("//www.stubhub.com/"):
            full = f"https:{href}"
        elif href.startswith("/www.stubhub.com/"):
            full = f"https:/{href}"
        else:
            full = href if href.startswith("http") else f"https://www.stubhub.com{href}"
        # Canonicalize to stable event page URL; drop noisy query/fragment tracking params.
        m = re.search(r"(https?://[^/]+/.*/event/\d+/?)", full)
        if m:
            parsed = urlparse(m.group(1))
            path = parsed.path if parsed.path.endswith("/") else f"{parsed.path}/"
            return f"{parsed.scheme}://{parsed.netloc}{path}"
        return full

    @staticmethod
    def _normalize_venue_url(href: str | None) -> str | None:
        if not href:
            return None
        href = html.unescape(href.strip())
        if "/venue/" not in href and "/performer/" not in href:
            return None
        if href.startswith("//"):
            full = f"https:{href}"
        elif href.startswith("/"):
            full = f"https://www.stubhub.com{href}"
        else:
            full = href if href.startswith("http") else f"https://www.stubhub.com{href}"
        
        # Canonicalize to stable venue/performer URL
        m = re.search(r"(https?://[^/]+/.*/(venue|performer)/\d+/?)", full)
        if m:
            parsed = urlparse(m.group(1))
            path = parsed.path if parsed.path.endswith("/") else f"{parsed.path}/"
            return f"{parsed.scheme}://{parsed.netloc}{path}"
        return full

    @staticmethod
    def _extract_date_from_event_url(event_url: str) -> datetime.date | None:
        # Typical StubHub slug pattern: ...-tickets-3-8-2026/event/<id>/
        m = re.search(r"-tickets-(\d{1,2})-(\d{1,2})-(\d{4})/event/\d+/?$", event_url)
        if not m:
            return None
        month, day, year = map(int, m.groups())
        try:
            return datetime.date(year, month, day)
        except Exception:
            return None

    @staticmethod
    def _name_from_event_url(event_url: str) -> str | None:
        m = re.search(r"stubhub\.com/([^/?]+)-tickets-[^/]+/event/\d+/?", event_url or "")
        if not m:
            return None
        slug = m.group(1).replace("-", " ").strip()
        if not slug:
            return None
        return " ".join(word.capitalize() for word in slug.split())

    @staticmethod
    def _clean_event_name(name: str | None) -> str | None:
        if not name:
            return None
        cleaned = re.sub(r"\s*\|\s*StubHub\s*$", "", name.strip(), flags=re.IGNORECASE)
        cleaned = re.sub(r"\s*Tickets\s*-\s*StubHub\s*$", "", cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r"\s*Tickets\s*$", "", cleaned, flags=re.IGNORECASE)
        return cleaned.strip() or None

    @staticmethod
    def _extract_venue_id(venue_url: str) -> str | None:
        m = re.search(r"/(venue|performer)/(\d+)", venue_url or "")
        return m.group(2) if m else None

    @staticmethod
    def _extract_venue_id_from_backurl(href: str) -> str | None:
        parsed = urlparse(href)
        q = parse_qs(parsed.query)
        back_url = (q.get("backUrl") or [None])[0]
        if not back_url:
            return None
        decoded = html.unescape(back_url)
        m = re.search(r"/(venue|performer)/(\d+)", decoded)
        return m.group(2) if m else None

    @staticmethod
    def _extract_event_urls_from_html(page_html: str) -> list[str]:
        text = html.unescape(page_html or "").replace("\\/", "/")
        patterns = [
            r"https?://www\.stubhub\.com/[^\"'\s<>]*/event/\d+/?",
            r"/(?!/)(?!www\.stubhub\.com/)[^\"'\s<>]*/event/\d+/?",
        ]
        found: set[str] = set()
        for pat in patterns:
            for match in re.findall(pat, text):
                found.add(match)
        return sorted(found)

    async def _safe_page_title(self) -> str:
        try:
            return (await self.page.title()).strip()
        except Exception:
            return ""

    async def _discover_single_event_from_event_page(self, venue, skip_persist: bool = False) -> dict:
        event_url = self._normalize_event_url(self.page.url or venue.stubhub_url) or (self.page.url or venue.stubhub_url)
        if "parking-passes-only" not in (event_url or "").lower():
            return {}
        external_id_match = re.search(r"/event/(\d+)", event_url)
        external_id = external_id_match.group(1) if external_id_match else None

        name = None
        for sel, expr in [
            ("h1", "el => el.textContent?.trim()"),
            ("meta[property='og:title']", "el => el.getAttribute('content')?.trim()"),
        ]:
            try:
                name = await self.page.eval_on_selector(sel, expr) or None
                if name:
                    break
            except Exception:
                pass

        if not name:
            try:
                ld_json_name = await self.page.eval_on_selector(
                    "script[type='application/ld+json']",
                    "el => { try { const x = JSON.parse(el.textContent || '{}'); return x.name || null; } catch { return null; } }",
                )
                name = ld_json_name or None
            except Exception:
                pass

        if not name:
            try:
                tw_name = await self.page.eval_on_selector(
                    "meta[name='twitter:title']",
                    "el => el.getAttribute('content')?.trim()",
                )
                name = tw_name or None
            except Exception:
                pass

        if not name:
            name = await self._safe_page_title()

        if not name:
            name = self._name_from_event_url(event_url)

        name = self._clean_event_name(name)
        if not name:
            name = self._name_from_event_url(event_url)
        if not name:
            name = f"Event {external_id}" if external_id else "Event"

        event_date = self._extract_date_from_event_url(event_url) or datetime.date.today()
        try:
            dt_raw = await self.page.eval_on_selector(
                "time[datetime]",
                "el => el.getAttribute('datetime')",
            )
            if dt_raw:
                event_date = pendulum.parse(dt_raw).date()
            else:
                dt_text = await self.page.eval_on_selector("time", "el => el.textContent?.trim()")
                if dt_text:
                    event_date = pendulum.parse(dt_text, strict=False).date()
        except Exception:
            pass

        if not skip_persist:
            event_repo = get_event_repository()
            event_data = {
                "venue": venue,
                "name": name,
                "date": event_date,
                "event_url": event_url,
            }
            if external_id:
                event_data["external_id"] = external_id
            await event_repo.upsert_event(event_data)

        return {
            "venue": venue.name,
            "event_name": name,
            "event_date": event_date.isoformat(),
            "event_url": event_url,
            "parking_url": event_url,
        }

    async def discover_events(self, venue, dry_run: bool = False, **kwargs) -> list[dict]:
        if dry_run:
            raise ValueError("dry_run is disabled. Real-time execution only.")

        url = venue.stubhub_url
        is_event_url = self._is_event_url(url)
        skip_persist = kwargs.get("skip_persist", False)
        venue_id = self._extract_venue_id(url)
        strict_venue_guard = bool(kwargs.get("strict_venue_guard", False))
        # StubHub event pages can be slow; use longer timeout and "commit" to avoid domcontentloaded stall
        for attempt in range(3):
            try:
                await self.page.goto(url, wait_until="commit", timeout=120000)
                break
            except Exception as exc:
                if attempt == 2:
                    if is_event_url:
                        logger.warning(f"Event page load failed ({exc}).")
                    raise
                await asyncio.sleep(3.0)

        # Strict guard: reject clear venue URL redirects/mismatches.
        if strict_venue_guard and venue_id and not is_event_url:
            current_url = self.page.url or ""
            redirected_venue_id = self._extract_venue_id(current_url)
            if redirected_venue_id and redirected_venue_id != venue_id:
                raise ValueError(
                    f"Venue context mismatch: expected venue/{venue_id}, got venue/{redirected_venue_id} ({current_url})"
                )

        await self.human_delay()
        # Retry once if StubHub served a challenge/placeholder page.
        title = (await self._safe_page_title()).lower()
        if any(token in title for token in ["just a moment", "access denied", "attention required"]):
            await asyncio.sleep(2)
            await self.page.reload(wait_until="commit", timeout=120000)
            await self.human_delay()

        if is_event_url:
            single = await self._discover_single_event_from_event_page(venue, skip_persist=skip_persist)
            return [single] if single else []

        event_repo = None if skip_persist else get_event_repository()
        container_selector = ".eventGridListItem__container"
        await self.wait_for_selector_safe(container_selector, timeout=10000)
        await self.human_delay()
        
        event_elements = await self.page.query_selector_all(container_selector)
        if not event_elements:
            # Fallback for StubHub class-name changes: discover by event URL pattern.
            event_elements = await self.page.query_selector_all("a[href*='/event/']")
        if not event_elements:
            # Last fallback: parse URLs from raw HTML (works when DOM hydration is partial).
            page_html = await self.page.content()
            raw_urls = self._extract_event_urls_from_html(page_html)
            if not raw_urls:
                logger.info(f"No events found for venue: {venue.name}")
                return []
            discovered_from_html = []
            seen: set[str] = set()
            for raw in raw_urls:
                full_url = self._normalize_event_url(raw)
                if not full_url or full_url in seen:
                    continue
                if "parking-passes-only" not in full_url.lower():
                    continue
                id_match = re.search(r"/event/(\d+)", full_url)
                dedup_key = id_match.group(1) if id_match else full_url
                if dedup_key in seen:
                    continue
                seen.add(dedup_key)
                slug_match = re.search(r"stubhub\.com/([^/?]+)-tickets-[^/]+/event/\d+", full_url)
                if not slug_match:
                    continue
                slug = slug_match.group(1).replace("-", " ").strip()
                name = " ".join(word.capitalize() for word in slug.split())
                event_date = self._extract_date_from_event_url(full_url) or datetime.date.today()
                event_data = {
                    "venue": venue,
                    "name": name,
                    "date": event_date,
                    "event_url": full_url,
                }
                if id_match:
                    event_data["external_id"] = id_match.group(1)
                if event_repo is not None:
                    await event_repo.upsert_event(event_data)
                discovered_from_html.append(
                    {
                        "venue": venue.name,
                        "event_name": name,
                        "event_date": event_date.isoformat(),
                        "event_url": full_url,
                        "parking_url": full_url,
                    }
                )
            if discovered_from_html:
                return discovered_from_html
            logger.info(f"No events found for venue: {venue.name}")
            return []

        discovered_events = []
        seen_urls: set[str] = set()

        for el in event_elements:
            # Explicitly wait for nested elements if needed, or query them directly
            name = None
            for sel in [".eventGridListItemContent__title", "h3"]:
                try:
                    name = await el.eval_on_selector(sel, "s => s.innerText?.trim()") or None
                    if name:
                        break
                except Exception:
                    pass
            if name is None:
                name = await el.get_attribute("title")

            raw_href = await el.get_attribute("href")
            event_url = self._normalize_event_url(raw_href)
            if not event_url:
                continue
            # Keep most links, but reject clearly mismatched venue backlinks.
            if venue_id and raw_href:
                back_venue_id = self._extract_venue_id_from_backurl(html.unescape(raw_href))
                if back_venue_id and back_venue_id != venue_id:
                    continue

            # Extract date text
            date_text = None
            for sel in [".eventGridListItemContent__date", "time"]:
                try:
                    date_text = await el.eval_on_selector(sel, "t => t.innerText?.trim()") or None
                    if date_text:
                        break
                except Exception:
                    pass

            # Extract external_id
            full_url = event_url
            if not full_url:
                continue
                
            id_match = re.search(r"/event/(\d+)", full_url)
            external_id = id_match.group(1) if id_match else None
            if full_url in seen_urls:
                continue
            seen_urls.add(full_url)

            # Fallback name from slug when visible card title selector is missing.
            card_text = (await el.inner_text()).lower()
            is_parking_event = "parking-passes-only" in full_url.lower() or "parking passes only" in card_text
            if not is_parking_event:
                continue
            
            if not name:
                slug_match = re.search(r"stubhub\.com/([^/?]+)-tickets-[^/]+/event/\d+", full_url)
                if slug_match:
                    slug = slug_match.group(1).replace("-", " ").strip()
                    name = " ".join(word.capitalize() for word in slug.split())
            
            if is_parking_event and name and "parking" not in name.lower():
                name = f"Parking: {name}"

            if not name:
                continue

            # Parse date: prefer canonical date from URL when available.
            url_date = self._extract_date_from_event_url(full_url)
            event_date = url_date or datetime.date.today()
            if date_text and url_date is None:
                try:
                    # Try common StubHub date formats from event cards.
                    for fmt in ["%b %d %a", "%b %d", "%a, %b %d", "%m/%d/%Y"]:
                        try:
                            parsed = pendulum.from_format(date_text, fmt)
                            now = pendulum.now()
                            # If input omits year, map to this/next year.
                            if fmt in {"%b %d %a", "%b %d", "%a, %b %d"}:
                                parsed = parsed.set(year=now.year)
                                if parsed < now.subtract(days=30):
                                    parsed = parsed.add(years=1)
                            event_date = parsed.date()
                            break
                        except Exception:
                            pass
                except Exception:
                    pass

            event_data = {
                "venue": venue,
                "name": name,
                "date": event_date,
                "event_url": full_url,
            }
            if external_id:
                event_data["external_id"] = external_id

            if event_repo is not None:
                await event_repo.upsert_event(event_data)
            
            # Extract venue information from the card (especially important for performer pages)
            inner_venue_name = None
            inner_venue_url = None
            try:
                # StubHub often has a link to the venue inside the event information
                v_link = await el.query_selector("a[href*='/venue/']")
                if v_link:
                    raw_v_href = await v_link.get_attribute("href")
                    inner_venue_url = self._normalize_venue_url(raw_v_href)
                    inner_venue_name = (await v_link.inner_text()).strip()
            except Exception:
                pass

            # Formatted output for API response
            discovered_events.append({
                "venue": inner_venue_name or venue.name,
                "venue_url": inner_venue_url,
                "event_name": name,
                "event_date": event_date.isoformat(),
                "event_url": full_url,
                "parking_url": full_url,
            })

        return discovered_events

    async def discover_parking_events(self, venue, dry_run: bool = False, **kwargs) -> list[dict]:
        """Look for parking events using the venue-page Parking filter checkbox."""
        if dry_run:
            raise ValueError("dry_run is disabled. Real-time execution only.")

        url = venue.stubhub_url
        await self.page.goto(url, wait_until="commit", timeout=120000)
        await self.human_delay()

        parking_filter_selector = 'div[role="checkbox"]:has-text("Parking")'
        filter_exists = await self.wait_for_selector_safe(parking_filter_selector)
        if filter_exists:
            await self.page.click(parking_filter_selector)
            await self.human_delay()
            return await self.discover_events(venue)
        return []
