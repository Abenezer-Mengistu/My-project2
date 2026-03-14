"""
StubHub Parking Scraper — replaces StubHubParkingScraper.ts.
Scrapes parking pass listings for a specific event.
Refactored for modular 'scraper/' component and explicit waits.
"""
from __future__ import annotations

import asyncio
import html
import re
import json

from scraper.base.ticketing.ticketing_playwright_base import TicketingPlaywrightBase
from database.repositories.ticketing.parking_passes import get_parking_pass_repository
from utils.normalization import normalize_lot_name


class StubHubParkingScraper(TicketingPlaywrightBase):
    handler: str = "stubhub-parking"

    @staticmethod
    def _currency_from_text(price_text: str) -> str:
        return "USD"

    @staticmethod
    def _numeric_price(price_text: str) -> str | None:
        m = re.search(r"([0-9][0-9,]*(?:\.[0-9]{2})?)", price_text or "")
        return m.group(1).replace(",", "") if m else None

    @staticmethod
    def _availability_from_text(text: str) -> str | None:
        if not text:
            return None
        t = text.lower()
        m = re.search(r"(\d+)\s*-\s*(\d+)\s*pass", t)
        if m:
            return f"{m.group(1)} - {m.group(2)} passes"
        m = re.search(r"(\d+)\s*pass", t)
        if m:
            n = m.group(1)
            return f"{n} pass" if n == "1" else f"{n} passes"
        return None

    @staticmethod
    def _availability_from_quantities(min_q: str | int | None, max_q: str | int | None) -> str | None:
        try:
            min_v = int(min_q) if min_q is not None else None
        except Exception:
            min_v = None
        try:
            max_v = int(max_q) if max_q is not None else None
        except Exception:
            max_v = None
        if min_v is None and max_v is None:
            return None
        if min_v is None:
            return f"1 - {max_v} passes" if max_v and max_v > 1 else "1 pass"
        if max_v is None or min_v == max_v:
            return f"{min_v} pass" if min_v == 1 else f"{min_v} passes"
        return f"{min_v} - {max_v} passes"

    @staticmethod
    def _availability_from_quantity_list(q_list) -> str | None:
        if not isinstance(q_list, list) or not q_list:
            return None
        try:
            vals = sorted({int(v) for v in q_list if v is not None})
        except Exception:
            return None
        if not vals:
            return None
        if len(vals) == 1:
            n = vals[0]
            return f"{n} pass" if n == 1 else f"{n} passes"
        return f"{vals[0]} - {vals[-1]} passes"

    @staticmethod
    def _price_from_object(obj: dict) -> str | None:
        if not isinstance(obj, dict):
            return None
        for key in ["formattedPrice", "price", "rawPrice", "listingPrice", "unitPrice"]:
            if key in obj and obj[key] is not None:
                return str(obj[key])
        for key in ["displayPrice", "priceInfo"]:
            if isinstance(obj.get(key), dict):
                nested = obj[key]
                for nk in ["formatted", "display", "amount", "value"]:
                    if nk in nested and nested[nk] is not None:
                        return str(nested[nk])
        return None

    @staticmethod
    def _collect_listing_objects(root) -> list[dict]:
        results: list[dict] = []
        seen_obj: set[int] = set()
        stack = [root]
        while stack:
            cur = stack.pop()
            if not isinstance(cur, (dict, list)):
                continue
            if isinstance(cur, list):
                for item in cur:
                    stack.append(item)
                continue
            obj_id = id(cur)
            if obj_id in seen_obj:
                continue
            seen_obj.add(obj_id)
            listing_id = cur.get("listingId") or cur.get("listing_id") or cur.get("id")
            price = StubHubParkingScraper._price_from_object(cur)
            if listing_id and price:
                results.append(cur)
            for v in cur.values():
                if isinstance(v, (dict, list)):
                    stack.append(v)
        return results

    async def _extract_passes_from_dom(self) -> list[dict]:
        async def _extract_from_context(ctx) -> list[dict]:
            selectors = [
                'div[data-listing-id]',
                '[data-testid*="listing"]',
                '[data-testid*="listing-card"]',
                '[data-testid*="ticket"]',
            ]
            listings = []
            for selector in selectors:
                listings = await ctx.query_selector_all(selector)
                if listings:
                    break

            passes: list[dict] = []
            seen: set[str] = set()
            for listing in listings:
                try:
                    data = await listing.evaluate(
                        """node => {
                            // Primary price: data-price attribute (e.g. "$166", "$41.50")
                            const attrPrice = node.getAttribute('data-price') || null;

                            const listingId =
                                node.getAttribute('data-listing-id')
                                || node.getAttribute('data-listingid')
                                || node.dataset?.listingId
                                || null;

                            // Title: h3 inside the listing CTA div
                            const title = node.querySelector('h3')?.textContent?.trim() || null;

                            // Availability: sc-1t1b4cp-12 class (contains "1 pass", "1 - 10 passes")
                            const availEl = node.querySelector('[class*="sc-1t1b4cp-12"]');
                            const rawAvail = availEl ? availEl.textContent.trim() : null;

                            // Price text: sc-1t1b4cp-1 class (the "$166" text node)
                            const priceEl = node.querySelector('[class*="sc-1t1b4cp-1"]');
                            const priceText = priceEl ? priceEl.textContent.trim() : null;

                            // Rating: sc-j0edf5-4 (e.g. "Amazing", "Great")
                            const ratingEl = node.querySelector('[class*="sc-j0edf5-4"]');
                            const rating = ratingEl ? ratingEl.textContent.trim() : null;

                            // Notes: secondary text like "Last pass", "Buyer could receive different lot"
                            const noteEl = node.querySelector('[class*="sc-fxFQKK"]');
                            const notes = noteEl ? noteEl.textContent.trim() : null;

                            // Fallback full text
                            const text = (node.innerText || node.textContent || '').trim();

                            return {
                                listingId,
                                title,
                                attrPrice,
                                priceText,
                                rawAvail,
                                rating,
                                notes,
                                text
                            };
                        }"""
                    )
                except Exception:
                    continue

                if not data:
                    continue

                listing_id = data.get("listingId")
                title = data.get("title")
                text = data.get("text") or ""

                # Price: prefer data-price attr, then sc-1t1b4cp-1 text, then regex on full text
                price_raw = data.get("attrPrice") or data.get("priceText") or text
                price_match = re.search(r"[$€£]\s?([0-9][0-9,]*(?:\.[0-9]{1,2})?)", price_raw)
                if not price_match:
                    continue
                price = price_match.group(1).replace(",", "")

                availability = self._availability_from_text(data.get("rawAvail") or text)

                lot_name = title
                if not lot_name:
                    first_line = text.splitlines()[0].strip()
                    lot_name = first_line[:120] if first_line else None
                if not lot_name:
                    continue

                dedup_key = f"{listing_id}" if listing_id else f"{lot_name}|{price}|{availability}"
                if dedup_key in seen:
                    continue
                seen.add(dedup_key)

                passes.append(
                    {
                        "lot_name": lot_name,
                        "normalized_lot_name": normalize_lot_name(lot_name),
                        "price": price,
                        "currency": "USD",
                        "availability": availability,
                        "listing_id": listing_id,
                        "listing_details": {
                            "title": lot_name,
                            "price_incl_fees": data.get("attrPrice") or data.get("priceText"),
                            "availability": data.get("rawAvail"),
                            "rating": data.get("rating"),
                            "notes": data.get("notes"),
                        },
                    }
                )
            return passes

        passes = await _extract_from_context(self.page)
        if passes:
            return passes

        for frame in self.page.frames:
            if frame == self.page.main_frame:
                continue
            try:
                passes = await _extract_from_context(frame)
            except Exception:
                passes = []
            if passes:
                return passes

        # Fallback: fuzzy DOM scan for listing-like cards.
        try:
            raw_cards = await self.page.evaluate(
                """() => {
                    const nodes = Array.from(document.querySelectorAll('article, li, div'));
                    const out = [];
                    for (const node of nodes) {
                        const text = (node.innerText || '').trim();
                        if (!text) continue;
                        if (!text.includes('$') || !text.toLowerCase().includes('pass')) continue;
                        if (!text.toLowerCase().includes('incl. fees') && !text.toLowerCase().includes('incl fees')) continue;
                        const lines = text.split(/\\n+/).filter(Boolean);
                        if (lines.length > 10) continue;
                        const heading = (node.querySelector('h1,h2,h3,h4,strong')?.textContent || '').trim() || null;
                        const listingId =
                            node.getAttribute('data-listing-id')
                            || node.getAttribute('data-listingid')
                            || node.dataset?.listingId
                            || null;
                        out.push({text, heading, listingId});
                    }
                    return out;
                }"""
            )
        except Exception:
            return []

        passes = []
        seen = set()
        for data in raw_cards or []:
            text = data.get("text") or ""
            heading = data.get("heading")
            price_match = re.search(r"([$€£])\s?([0-9][0-9,]*(?:\.[0-9]{2})?)", text)
            if not price_match:
                continue
            price = price_match.group(2).replace(",", "")
            availability = self._availability_from_text(text)
            lot_name = heading or text.splitlines()[0].strip()[:120]
            listing_id = data.get("listingId")
            dedup_key = f"{listing_id}" if listing_id else f"{lot_name}|{price}|USD|{availability}"
            if dedup_key in seen:
                continue
            seen.add(dedup_key)
            passes.append(
                {
                    "lot_name": lot_name,
                    "normalized_lot_name": normalize_lot_name(lot_name),
                    "price": price,
                    "currency": "USD",
                    "availability": availability,
                    "listing_id": listing_id,
                    "details": text if len(text) < 500 else text[:500] + "...",
                }
            )
        return passes

    async def _extract_passes_from_state(self) -> list[dict]:
        payload = await self.page.evaluate(
            """() => {
                const roots = [];
                for (const key of ["__PRELOADED_STATE__", "__INITIAL_STATE__", "__NEXT_DATA__", "SHAppApi"]) {
                    const v = window[key];
                    if (v) roots.push(v);
                }
                return roots;
            }"""
        )
        if not payload:
            return []

        found: list[dict] = []
        for root in payload:
            found.extend(self._collect_listing_objects(root))

        passes: list[dict] = []
        seen: set[str] = set()
        for row in found:
            listing_id = row.get("listingId") or row.get("listing_id") or row.get("id")
            price_val = self._price_from_object(row)
            price = self._numeric_price(str(price_val)) if price_val is not None else None
            if not listing_id or not price:
                continue
            lot_name = (
                row.get("listingTitle")
                or row.get("sectionName")
                or row.get("ticketClassName")
                or row.get("zoneName")
                or row.get("name")
            )
            if not lot_name:
                continue
            availability = self._availability_from_quantity_list(row.get("availableQuantities"))
            if not availability:
                availability = self._availability_from_quantities(
                    row.get("minQuantity") or row.get("minQty") or row.get("quantityMin"),
                    row.get("maxQuantity") or row.get("maxQty") or row.get("quantityMax") or row.get("availableQuantity"),
                )
            dedup_key = f"{listing_id}"
            if dedup_key in seen:
                continue
            seen.add(dedup_key)
            passes.append(
                {
                    "lot_name": lot_name,
                    "normalized_lot_name": normalize_lot_name(lot_name),
                    "price": price,
                    "currency": "USD",
                    "availability": availability,
                    "listing_id": listing_id,
                    "_source": "state",
                    "details": row.get("listingNotes") or row.get("description"),
                }
            )
        return passes

    def _extract_passes_from_json_payload(self, raw_text: str) -> list[dict]:
        try:
            data = json.loads(raw_text)
        except Exception:
            return []
        found = self._collect_listing_objects(data)
        if not found:
            return []
        passes: list[dict] = []
        seen: set[str] = set()
        for row in found:
            listing_id = row.get("listingId") or row.get("listing_id") or row.get("id")
            price_val = self._price_from_object(row)
            price = self._numeric_price(str(price_val)) if price_val is not None else None
            if not listing_id or not price:
                continue
            lot_name = (
                row.get("listingTitle")
                or row.get("sectionName")
                or row.get("ticketClassName")
                or row.get("zoneName")
                or row.get("name")
            )
            if not lot_name:
                continue
            availability = self._availability_from_quantity_list(row.get("availableQuantities"))
            if not availability:
                availability = self._availability_from_quantities(
                    row.get("minQuantity") or row.get("minQty") or row.get("quantityMin"),
                    row.get("maxQuantity") or row.get("maxQty") or row.get("quantityMax") or row.get("availableQuantity"),
                )
            dedup_key = f"{listing_id}"
            if dedup_key in seen:
                continue
            seen.add(dedup_key)
            passes.append(
                {
                    "lot_name": lot_name,
                    "normalized_lot_name": normalize_lot_name(lot_name),
                    "price": price,
                    "currency": "USD",
                    "availability": availability,
                    "listing_id": listing_id,
                    "_source": "payload_json",
                    "details": row.get("listingNotes") or row.get("description"),
                }
            )
        return passes

    @staticmethod
    def _extract_passes_from_text(raw_text: str, source: str) -> list[dict]:
        text = html.unescape(raw_text or "").replace("\\/", "/")
        text = text.replace('\\"', '"')

        passes: list[dict] = []
        seen: set[str] = set()

        # Pattern from embedded "visiblePopups" objects.
        popup_pattern = re.compile(
            r'"(?P<key>\d+_(?P<section>\d+))"\s*:\s*\{[^{}]*?"rawPrice"\s*:\s*(?P<raw>\d+)'
            r'[^{}]*?"formattedPrice"\s*:\s*"(?P<formatted>[^"]+)"'
            r'[^{}]*?"listingId"\s*:\s*(?P<listing>\d+)(?:[^{}]*?"popupValue"\s*:\s*"?(?P<avail>[^",}]*)"?){0,1}',
            flags=re.DOTALL,
        )
        for m in popup_pattern.finditer(text):
            listing_id = m.group("listing")
            formatted = m.group("formatted")
            section = m.group("section")
            price = StubHubParkingScraper._numeric_price(formatted) or m.group("raw")
            availability = m.group("avail") or None
            dedup_key = f"listing:{listing_id}"
            if dedup_key in seen:
                continue
            seen.add(dedup_key)
            lot = f"Section {section}"
            passes.append(
                {
                    "lot_name": lot,
                    "normalized_lot_name": normalize_lot_name(lot),
                    "price": price,
                    "currency": "USD",
                    "availability": availability,
                    "listing_id": listing_id,
                    "_source": source,
                }
            )

        # Pattern from embedded "VisibleSectionPopupsOnLoad" records.
        section_pattern = re.compile(
            r'"sectionId"\s*:\s*"(?P<section>\d+)"\s*,\s*"price"\s*:\s*"(?P<price>[^"]+)"',
            flags=re.DOTALL,
        )
        for m in section_pattern.finditer(text):
            section = m.group("section")
            price_text = m.group("price")
            price = StubHubParkingScraper._numeric_price(price_text)
            if not price:
                continue
            dedup_key = f"section:{section}|price:{price}"
            if dedup_key in seen:
                continue
            seen.add(dedup_key)
            lot = f"Section {section}"
            passes.append(
                {
                    "lot_name": lot,
                    "normalized_lot_name": normalize_lot_name(lot),
                    "price": price,
                    "currency": "USD",
                    "availability": None,
                    "_source": source,
                }
            )

        # Loose fallback: token-based extraction around listingId for escaped/partial payloads.
        listing_pat = re.compile(r'"listingId"\s*:\s*(?P<listing>\d+)')
        for lm in listing_pat.finditer(text):
            listing_id = lm.group("listing")
            start = max(0, lm.start() - 280)
            end = min(len(text), lm.end() + 320)
            chunk = text[start:end]

            section_m = re.search(r'"sectionId"\s*:\s*"(?P<section>\d+)"', chunk)
            formatted_m = re.search(r'"formattedPrice"\s*:\s*"(?P<formatted>[^"]+)"', chunk)
            raw_m = re.search(r'"rawPrice"\s*:\s*(?P<raw>\d+)', chunk)
            avail_m = re.search(r'"popupValue"\s*:\s*"?(?P<avail>[^",}]*)"?', chunk)

            price_text = formatted_m.group("formatted") if formatted_m else ""
            price = StubHubParkingScraper._numeric_price(price_text) if price_text else None
            if not price and raw_m:
                price = raw_m.group("raw")
            if not price:
                continue

            lot_name = f"Section {section_m.group('section')}" if section_m else f"Listing {listing_id}"
            dedup_key = f"listing:{listing_id}"
            if dedup_key in seen:
                continue
            seen.add(dedup_key)

            passes.append(
                {
                    "lot_name": lot_name,
                    "normalized_lot_name": normalize_lot_name(lot_name),
                    "price": price,
                    "currency": "USD",
                    "availability": (avail_m.group("avail") if avail_m else None),
                    "listing_id": listing_id,
                    "_source": f"{source}_loose",
                }
            )

        return passes

    async def _extract_passes_from_embedded_json(self) -> list[dict]:
        page_html = await self.page.content()
        return self._extract_passes_from_text(page_html, source="embedded_html")

    async def scrape_parking_details(self, event) -> list[dict]:
        async def _attempt(url: str, label: str) -> tuple[list[dict], dict]:
            captured_payloads: list[str] = []
            captured_urls: list[str] = []
            captured_request_payloads: list[str] = []
            captured_request_urls: list[str] = []

            async def _capture_response(resp):
                try:
                    ct = (await resp.all_headers()).get("content-type", "").lower()
                    url_l = resp.url.lower()
                    interesting = any(
                        token in url_l
                        for token in ["event", "listing", "inventory", "map", "log", "telemetry"]
                    )
                    if not interesting and "json" not in ct:
                        return
                    txt = await resp.text()
                    if any(k in txt for k in ["visiblePopups", "listingId", "formattedPrice", "rawPrice", "listings", "listing", "availableQuantity"]):
                        captured_payloads.append(txt)
                        captured_urls.append(resp.url)
                except Exception:
                    return

            def _capture_request(req):
                try:
                    pd = req.post_data or ""
                    if not pd:
                        return
                    if any(k in pd for k in ["visiblePopups", "VisibleSectionPopupsOnLoad", "listingId", "rawPrice", "formattedPrice"]):
                        captured_request_payloads.append(pd)
                        captured_request_urls.append(req.url)
                except Exception:
                    return

            self.page.on("response", lambda resp: asyncio.create_task(_capture_response(resp)))
            self.page.on("request", _capture_request)
            try:
                await self.page.goto(url, wait_until="networkidle", timeout=60000)
            except Exception:
                await self.page.goto(url, wait_until="domcontentloaded", timeout=60000)
            await self.human_delay()
            await self.human_delay()
            await asyncio.sleep(2)
            
            # Handle "Show more" button and lazy loading
            try:
                for _ in range(15):  # Safety limit for clicking Show More
                    show_more = self.page.get_by_role("button", name="Show more")
                    if await show_more.is_visible():
                        await show_more.click()
                        await asyncio.sleep(1.5)
                    else:
                        break
                
                # Final scroll to trigger hidden elements
                for _ in range(5):
                    await self.page.evaluate("() => window.scrollTo(0, document.body.scrollHeight)")
                    await asyncio.sleep(1.0)
            except Exception:
                pass

            passes = await self._extract_passes_from_dom()
            if not passes:
                passes = await self._extract_passes_from_state()
            if not passes:
                passes = await self._extract_passes_from_embedded_json()
            if not passes:
                merged: list[dict] = []
                seen: set[str] = set()
                for payload in captured_payloads + captured_request_payloads:
                    extracted = self._extract_passes_from_json_payload(payload)
                    if not extracted:
                        extracted = self._extract_passes_from_text(payload, source=f"embedded_xhr_{label}")
                    for p in extracted:
                        key = p.get("listing_id") or f"{p.get('lot_name')}|{p.get('price')}|{p.get('currency')}|{p.get('availability')}"
                        if key in seen:
                            continue
                        seen.add(key)
                        merged.append(p)
                passes = merged

            probe = {
                "attempt": label,
                "url": self.page.url,
                "title": (await self.page.title()),
                "captured_payloads": len(captured_payloads),
                "captured_urls": captured_urls[:5],
                "captured_request_payloads": len(captured_request_payloads),
                "captured_request_urls": captured_request_urls[:5],
            }
            return passes, probe

        primary_url = event.parking_url
        if not primary_url or "parking-passes-only" not in primary_url.lower():
            self._last_probe = {
                "attempt": "parking_url",
                "url": primary_url,
                "title": "",
                "captured_payloads": 0,
                "captured_urls": [],
                "captured_request_payloads": 0,
                "captured_request_urls": [],
                "error": "parking_url_missing_or_not_parking_detail",
            }
            return []

        # Append quantity=0 to see all listings as requested by user
        if "?" in primary_url:
            if "quantity=" not in primary_url:
                primary_url += "&quantity=0"
            else:
                primary_url = re.sub(r"quantity=\d+", "quantity=0", primary_url)
        else:
            primary_url += "?quantity=0"

        passes, probe = await _attempt(primary_url, "parking_url")
        if passes:
            self._last_probe = probe
            return passes

        self._last_probe = probe
        return []

    async def scrape_parking(self, event) -> int:
        passes = await self.scrape_parking_details(event)
        if not passes or not getattr(event, "_id", None):
            return len(passes)
        parking_repo = get_parking_pass_repository()
        await parking_repo.clear_for_event(event)
        await parking_repo.add_passes(event, passes)
        return len(passes)
    @staticmethod
    def _collect_listing_objects(root) -> list[dict]:
        results: list[dict] = []
        seen_obj: set[int] = set()
        stack = [root]
        while stack:
            cur = stack.pop()
            if not isinstance(cur, (dict, list)):
                continue
            if isinstance(cur, list):
                for item in cur:
                    stack.append(item)
                continue
            obj_id = id(cur)
            if obj_id in seen_obj:
                continue
            seen_obj.add(obj_id)
            listing_id = cur.get("listingId") or cur.get("listing_id") or cur.get("id")
            price = cur.get("price") or cur.get("rawPrice") or cur.get("formattedPrice")
            if listing_id and price:
                results.append(cur)
            for v in cur.values():
                if isinstance(v, (dict, list)):
                    stack.append(v)
        return results
