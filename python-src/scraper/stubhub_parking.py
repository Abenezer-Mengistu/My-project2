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
import shutil
import time

from scraper.base.ticketing.ticketing_playwright_base import TicketingPlaywrightBase
from database.repositories.ticketing.parking_passes import get_parking_pass_repository
from utils.normalization import normalize_lot_name
from pathlib import Path
BASE_DIR = Path(__file__).resolve().parent.parent
STORAGE_DIR = BASE_DIR / 'storage'
from loguru import logger


class StubHubParkingScraper(TicketingPlaywrightBase):
    handler: str = "stubhub-parking"
    _debug_dir: Path | None = None

    @staticmethod
    def _listing_identifier(row: dict) -> str | None:
        for key in ["listing_id", "inventory_id", "listing_key", "id"]:
            value = row.get(key)
            if value not in (None, ""):
                return str(value)
        details = row.get("listing_details") if isinstance(row.get("listing_details"), dict) else {}
        for key in ["listingId", "inventoryId", "listingKey", "id"]:
            value = details.get(key)
            if value not in (None, ""):
                return str(value)
        return None

    @staticmethod
    def _listing_fallback_identity(row: dict) -> str:
        details = row.get("listing_details") if isinstance(row.get("listing_details"), dict) else {}
        parts = [
            row.get("normalized_lot_name") or row.get("lot_name") or "",
            row.get("price") or "",
            row.get("availability") or "",
            details.get("price_incl_fees") or "",
            details.get("notes") or "",
            details.get("title") or "",
            row.get("_source") or "",
        ]
        return "|".join(str(part).strip() for part in parts)

    @staticmethod
    def _is_generic_lot_name(value: str | None) -> bool:
        text = (value or "").strip()
        if not text:
            return True
        return bool(re.fullmatch(r"(Section\s+\d+|Listing\s+\d+)", text, flags=re.IGNORECASE))

    @staticmethod
    def _details_score(listing: dict) -> int:
        score = 0
        if listing.get("lot_name") and not StubHubParkingScraper._is_generic_lot_name(listing.get("lot_name")):
            score += 4
        if listing.get("availability"):
            score += 2
        details = listing.get("listing_details") if isinstance(listing.get("listing_details"), dict) else {}
        if details.get("notes"):
            score += 2
        if details.get("rating"):
            score += 1
        if listing.get("price"):
            score += 2
        return score

    @staticmethod
    def _merge_pass_collections(*collections: list[dict]) -> list[dict]:
        merged: dict[str, dict] = {}
        for collection in collections:
            for row in collection or []:
                listing_id = StubHubParkingScraper._listing_identifier(row)
                key = f"id:{listing_id}" if listing_id else f"fallback:{StubHubParkingScraper._listing_fallback_identity(row)}"
                existing = merged.get(key)
                if not existing:
                    merged[key] = dict(row)
                    continue

                current = dict(existing)
                if StubHubParkingScraper._details_score(row) > StubHubParkingScraper._details_score(current):
                    preferred, fallback = dict(row), current
                else:
                    preferred, fallback = current, dict(row)

                if StubHubParkingScraper._is_generic_lot_name(preferred.get("lot_name")) and not StubHubParkingScraper._is_generic_lot_name(fallback.get("lot_name")):
                    preferred["lot_name"] = fallback.get("lot_name")
                    preferred["normalized_lot_name"] = fallback.get("normalized_lot_name")
                if not preferred.get("availability") and fallback.get("availability"):
                    preferred["availability"] = fallback.get("availability")
                if not preferred.get("price") and fallback.get("price"):
                    preferred["price"] = fallback.get("price")
                if not preferred.get("currency") and fallback.get("currency"):
                    preferred["currency"] = fallback.get("currency")

                preferred_details = preferred.get("listing_details") if isinstance(preferred.get("listing_details"), dict) else {}
                fallback_details = fallback.get("listing_details") if isinstance(fallback.get("listing_details"), dict) else {}
                if fallback_details:
                    preferred["listing_details"] = {
                        **fallback_details,
                        **preferred_details,
                    }

                source_parts = []
                for source in [current.get("_source"), row.get("_source")]:
                    if source and source not in source_parts:
                        source_parts.append(source)
                if source_parts:
                    preferred["_source"] = "+".join(source_parts)

                merged[key] = preferred
        return list(merged.values())

    @staticmethod
    def _filter_telemetry_rows(rows: list[dict]) -> list[dict]:
        if not rows:
            return rows

        real_rows = [
            row for row in rows
            if not StubHubParkingScraper._is_generic_lot_name(row.get("lot_name"))
            and (row.get("availability") or (row.get("listing_details") or {}).get("availability"))
        ]
        if not real_rows:
            return rows

        filtered = []
        for row in rows:
            details = row.get("listing_details") if isinstance(row.get("listing_details"), dict) else {}
            if StubHubParkingScraper._is_generic_lot_name(row.get("lot_name")):
                continue
            if not row.get("availability") and not details.get("availability") and "embedded_xhr" in str(row.get("_source") or "").lower():
                continue
            filtered.append(row)
        return filtered or real_rows

    @staticmethod
    def _currency_from_text(price_text: str) -> str:
        text = (price_text or "").strip()
        if not text:
            return ""
        upper = text.upper()
        if "USD" in upper or "$" in text:
            return "USD"
        if "EUR" in upper or "€" in text:
            return "EUR"
        if "GBP" in upper or "£" in text:
            return "GBP"
        if "ZAR" in upper:
            return "ZAR"
        return ""

    @staticmethod
    def _normalize_price_text_display(price_text: str | None) -> str | None:
        text = (price_text or "").strip()
        if not text:
            return None
        # StubHub's DOM sometimes emits a bare "R" glyph for USD on US pages.
        if re.fullmatch(r"R\s?[0-9][0-9,]*(?:\.[0-9]{1,2})?", text):
            return re.sub(r"^R", "$", text, count=1)
        return text

    @staticmethod
    def _numeric_price(price_text: str) -> str | None:
        m = re.search(r"([0-9][0-9,]*(?:\.[0-9]{2})?)", price_text or "")
        return m.group(1).replace(",", "") if m else None

    @staticmethod
    def _normalize_raw_price(raw_value) -> str | None:
        """
        Normalize raw numeric prices that are often returned as integer cents.
        If the value is an integer-like string with no decimal, treat it as cents
        when it is >= 100 (e.g. "1222" -> "12.22").
        """
        try:
            s = str(raw_value).strip()
        except Exception:
            return None
        if not s:
            return None
        if re.search(r"[.,]", s):
            return s
        if not re.fullmatch(r"\d+", s):
            return s
        val = int(s)
        if val >= 100:
            return f"{val / 100:.2f}"
        return s

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

        def pick(value) -> str | None:
            if value is None:
                return None
            if isinstance(value, dict):
                for nested_key in [
                    "formatted",
                    "display",
                    "amount",
                    "value",
                    "formattedPrice",
                    "currentPrice",
                    "discountedPrice",
                    "rawPrice",
                ]:
                    if value.get(nested_key) is not None:
                        nested_value = value.get(nested_key)
                        if nested_key == "rawPrice":
                            normalized = StubHubParkingScraper._normalize_raw_price(nested_value)
                            if normalized is not None:
                                return str(normalized)
                        return str(nested_value)
                return None
            return str(value)

        # Prefer the live sale/current price that the site surfaces to users.
        for key in [
            "currentPrice",
            "currentPriceWithFees",
            "priceWithFees",
            "discountedPrice",
            "salePrice",
            "displayPrice",
            "priceInfo",
            "priceDisplay",
            "formattedPrice",
            "price",
            "listingPrice",
            "unitPrice",
        ]:
            picked = pick(obj.get(key))
            if picked is not None:
                return picked

        if obj.get("rawPrice") is not None:
            normalized = StubHubParkingScraper._normalize_raw_price(obj.get("rawPrice"))
            if normalized is not None:
                return str(normalized)
        return None

    async def _count_visible_listing_nodes(self) -> int:
        selectors = [
            'div[role="button"].sc-194s59m-4',
            '[data-testid*="listing"]',
            'div[data-listing-id]',
            '[role="listitem"]',
        ]
        max_count = 0
        for selector in selectors:
            try:
                max_count = max(max_count, await self.page.locator(selector).count())
            except Exception:
                continue
        return max_count

    @staticmethod
    def _extract_total_listing_count(body_text: str) -> int | None:
        match = re.search(r"\b([0-9][0-9,]*)\s+listings\b", body_text or "", flags=re.IGNORECASE)
        if not match:
            return None
        try:
            return int(match.group(1).replace(",", ""))
        except Exception:
            return None

    async def _load_all_listing_inventory(self) -> None:
        max_duration_seconds = 45.0
        started_at = time.perf_counter()
        stable_rounds = 0
        last_visible_count = -1
        target_listing_count: int | None = None

        for _ in range(24):
            if time.perf_counter() - started_at >= max_duration_seconds:
                logger.info(
                    f"[Scraper] Inventory expansion budget exhausted after {max_duration_seconds:.1f}s; proceeding with extraction."
                )
                break
            try:
                show_more = self.page.get_by_role("button", name=re.compile("Show more|See more|Load more", re.IGNORECASE))
                if await show_more.count() > 0 and await show_more.first.is_visible():
                    await show_more.first.click()
                    await asyncio.sleep(1.5)
            except Exception:
                pass

            try:
                await self.page.evaluate(
                    """() => {
                        window.scrollTo(0, document.body.scrollHeight);
                        const candidates = Array.from(document.querySelectorAll('div, section, ul, main'))
                            .filter((el) => {
                                const style = window.getComputedStyle(el);
                                const scrollable = el.scrollHeight > (el.clientHeight + 40);
                                const overflowY = style.overflowY || '';
                                return scrollable && ['auto', 'scroll'].includes(overflowY);
                            })
                            .sort((a, b) => b.scrollHeight - a.scrollHeight)
                            .slice(0, 8);
                        for (const el of candidates) {
                            try { el.scrollTop = el.scrollHeight; } catch (e) {}
                        }
                    }"""
                )
            except Exception:
                pass

            try:
                await self.page.wait_for_load_state("networkidle", timeout=3000)
            except Exception:
                await asyncio.sleep(1.2)

            try:
                body_text = await self.page.evaluate("""() => document.body.innerText || ''""")
                target_listing_count = max(target_listing_count or 0, self._extract_total_listing_count(body_text) or 0) or target_listing_count
            except Exception:
                pass

            visible_count = await self._count_visible_listing_nodes()
            if visible_count <= last_visible_count:
                stable_rounds += 1
            else:
                stable_rounds = 0
            last_visible_count = max(last_visible_count, visible_count)

            if target_listing_count and last_visible_count < target_listing_count:
                continue

            if stable_rounds >= 4:
                break

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
            listing_id = (
                cur.get("listingId")
                or cur.get("listing_id")
                or cur.get("inventoryId")
                or cur.get("listingKey")
                or cur.get("id")
            )
            price = StubHubParkingScraper._price_from_object(cur)
            if listing_id and price:
                results.append(cur)
            for v in cur.values():
                if isinstance(v, (dict, list)):
                    stack.append(v)
        return results

    async def _wait_for_listings(self, timeout_ms: int = 20000) -> bool:
        """Wait for listing elements or listing count to appear."""
        selectors = [
            'div[role="button"].sc-194s59m-4',
            '[data-testid*="listing"]',
            'h3',
            'div:contains("listings")',
            'div:contains("passes")'
        ]
        start = asyncio.get_event_loop().time()
        while (asyncio.get_event_loop().time() - start) * 1000 < timeout_ms:
            for s in selectors:
                try:
                    if await self.page.locator(s).count() > 0:
                        return True
                except Exception:
                    continue
            await asyncio.sleep(1)
        return False

    async def _extract_passes_from_dom(self) -> list[dict]:
        async def _extract_from_context(ctx) -> list[dict]:
            selectors = [
                'div[role="button"].sc-194s59m-4',
                '[data-testid*="listing"]',
                'div[data-listing-id]',
                'div.sc-194s59m-4',
                '[role="listitem"]',
            ]
            listings = []
            for selector in selectors:
                try:
                    listings = await ctx.query_selector_all(selector)
                    if len(listings) > 2:
                        break
                except Exception:
                    continue

            passes: list[dict] = []
            seen: set[str] = set()
            for listing in listings:
                try:
                    data = await listing.evaluate(
                        """node => {
                            const listingId =
                                node.getAttribute('data-listing-id')
                                || node.getAttribute('data-listingid')
                                || node.dataset?.listingId
                                || null;
                            const title = node.querySelector('h3')?.textContent?.trim() || null;
                            let priceText = null;
                            const priceNodes = Array.from(node.querySelectorAll('div, span, b, p'));
                            for (const p of priceNodes) {
                                const t = p.textContent.trim();
                                if (/^[$€£R]\s?[0-9,.]+$/.test(t) || /^[A-Z]{1,3}\s?[0-9,.]+$/.test(t) || /^[0-9,.]+\s?[$€£R]/.test(t)) {
                                    if (!t.toLowerCase().includes('pass')) {
                                        priceText = t;
                                        break;
                                    }
                                }
                            }
                            if (!priceText) {
                                const pEl = node.querySelector('[class*="sc-1t1b4cp-1"]');
                                if (pEl) {
                                    const t = pEl.textContent.trim();
                                    if (!t.toLowerCase().includes('pass')) {
                                        priceText = t;
                                    }
                                }
                            }
                            if (!priceText) {
                                for (const p of priceNodes) {
                                    const t = p.textContent.trim();
                                    const match = t.match(/([$€£R]\s?[0-9,.]+|[0-9,.]+\s?[$€£R]|[A-Z]{2,3}\s?[0-9,.]+)/);
                                    if (match && !t.toLowerCase().includes('pass')) {
                                        priceText = match[0];
                                        break;
                                    }
                                }
                            }
                            let rawAvail = null;
                            const availNodes = Array.from(node.querySelectorAll('div, span'));
                            for (const a of availNodes) {
                                const t = a.textContent.toLowerCase();
                                if (t.includes('pass') || t.includes('passes')) {
                                    rawAvail = a.textContent.trim();
                                    break;
                                }
                            }
                            const rating = node.querySelector('[class*="Amazing"], [class*="Great"], [class*="Good"]')?.textContent?.trim() || null;
                            const notes = Array.from(node.querySelectorAll('div')).map(d => d.textContent.trim()).find(t => t.includes('walk') || t.includes('mi from venue')) || null;
                            const text = (node.innerText || node.textContent || '').trim();
                            return { listingId, title, priceText, rawAvail, rating, notes, text };
                        }"""
                    )
                except Exception:
                    continue

                if not data:
                    continue

                listing_id = data.get("listingId")
                title = data.get("title")
                text = data.get("text") or ""
                price_text = self._normalize_price_text_display(data.get("priceText") or "") or ""

                price_match = re.search(r"([0-9][0-9,]*(?:\.[0-9]{1,2})?)", price_text)
                if not price_match and text:
                    price_match = re.search(r"[$€£R]\s?([0-9][0-9,]*(?:\.[0-9]{1,2})?)", text)
                if not price_match:
                    continue

                price = price_match.group(1).replace(",", "")
                currency = self._currency_from_text(price_text or text)
                if "." not in price:
                    price = f"{price}.00"

                availability = self._availability_from_text(data.get("rawAvail") or text)
                lot_name = title
                if not lot_name:
                    first_lines = [l.strip() for l in text.splitlines() if l.strip()]
                    lot_name = first_lines[0][:120] if first_lines else None
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
                        "currency": currency or None,
                        "availability": availability,
                        "listing_id": listing_id,
                        "listing_details": {
                            "title": lot_name,
                            "price_incl_fees": price_text or f"${price}",
                            "availability": availability,
                            "rating": data.get("rating"),
                            "notes": data.get("notes"),
                        },
                    }
                )
            return passes

        # First, wait for dynamic content
        logger.info(f"[Scraper] Waiting for listings on {self.page.url}")
        is_ready = await self._wait_for_listings()
        logger.info(f"[Scraper] Listings ready status: {is_ready}")
        
        # Capture screenshot for debugging
        debug_base = self._debug_dir or STORAGE_DIR
        debug_ss_path = debug_base / f"debug_phase2_{int(asyncio.get_event_loop().time())}.png"
        try:
            await self.page.screenshot(path=str(debug_ss_path))
            logger.info(f"[Scraper] Debug screenshot saved to {debug_ss_path}")
        except Exception as e:
            logger.warning(f"[Scraper] Failed to save debug screenshot: {e}")

        await self.human_delay()
        dom_passes = await _extract_from_context(self.page)
        logger.info(f"[Scraper] DOM extraction found {len(dom_passes or [])} passes")

        frame_passes: list[dict] = []
        for frame in self.page.frames:
            if frame == self.page.main_frame:
                continue
            try:
                frame_passes.extend(await _extract_from_context(frame))
            except Exception:
                continue

        # Fallback: scan for listing panels that include a price + "incl. fees".
        try:
            panel_cards = await self.page.evaluate(
                """() => {
                    const priceRe = /[$€£R]\\s?\\d/;
                    const feeRe = /incl\\.?\\s*fees/i;
                    const nodes = Array.from(document.querySelectorAll('div,li,article'));
                    const out = [];
                    const seen = new Set();
                    for (const node of nodes) {
                        const text = (node.innerText || '').trim();
                        if (!text) continue;
                        if (!priceRe.test(text) || !feeRe.test(text)) continue;
                        // Find a reasonable container for the listing card.
                        let cur = node;
                        for (let i = 0; i < 4 && cur && cur.parentElement; i++) {
                            const t = (cur.innerText || '').trim();
                            const lines = t.split(/\\n+/).filter(Boolean);
                            if (lines.length >= 2 && lines.length <= 12) break;
                            cur = cur.parentElement;
                        }
                        if (!cur) continue;
                        const cText = (cur.innerText || '').trim();
                        if (!cText) continue;
                        const cLines = cText.split(/\\n+/).filter(Boolean);
                        if (cLines.length < 2 || cLines.length > 12) continue;
                        const heading =
                            (cur.querySelector('h1,h2,h3,h4,strong')?.textContent || '').trim() || null;
                        const listingId =
                            cur.getAttribute('data-listing-id')
                            || cur.getAttribute('data-listingid')
                            || cur.dataset?.listingId
                            || null;
                        const key = (listingId || '') + '|' + cLines[0] + '|' + cLines[1];
                        if (seen.has(key)) continue;
                        seen.add(key);
                        out.push({text: cText, heading, listingId});
                    }
                    return out;
                }"""
            )
        except Exception:
            panel_cards = []

        if panel_cards:
            panel_passes = []
            seen = set()
            for data in panel_cards:
                text = data.get("text") or ""
                heading = data.get("heading")
                # Detect symbol like '$', '€', '£', or 'R'
                price_match = re.search(r"([$€£R])\\s?([0-9][0-9,]*(?:\\.[0-9]{2})?)", text)
                if not price_match:
                    continue
                
                symbol = price_match.group(1)
                price = price_match.group(2).replace(",", "")
                availability = self._availability_from_text(text)
                lot_name = heading or text.splitlines()[0].strip()[:120]
                listing_id = data.get("listingId")
                currency_code = self._currency_from_text(symbol) or self._currency_from_text(text)
                
                dedup_key = f"{listing_id}" if listing_id else f"{lot_name}|{price}|{currency_code}|{availability}"
                if dedup_key in seen:
                    continue
                seen.add(dedup_key)
                panel_passes.append(
                    {
                        "lot_name": lot_name,
                        "normalized_lot_name": normalize_lot_name(lot_name),
                        "price": price,
                        "currency": currency_code or None,
                        "availability": availability,
                        "listing_id": listing_id,
                        "details": text if len(text) < 500 else text[:500] + "...",
                    }
                )
        else:
            panel_passes = []

        # Fallback: fuzzy DOM scan for listing-like cards.
        try:
            raw_cards = await self.page.evaluate(
                """() => {
                    const nodes = Array.from(document.querySelectorAll('article, li, div'));
                    const out = [];
                    for (const node of nodes) {
                        const text = (node.innerText || '').trim();
                        if (!text) continue;
                        if (!/[$€£R]/.test(text)) continue;
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

        fuzzy_passes = []
        seen = set()
        for data in raw_cards or []:
            text = data.get("text") or ""
            heading = data.get("heading")
            price_match = re.search(r"([$€£])\s?([0-9][0-9,]*(?:\.[0-9]{2})?)", text)
            if not price_match:
                continue
            symbol = price_match.group(1)
            price = price_match.group(2).replace(",", "")
            availability = self._availability_from_text(text)
            lot_name = heading or text.splitlines()[0].strip()[:120]
            listing_id = data.get("listingId")
            currency_code = self._currency_from_text(symbol) or self._currency_from_text(text)
            dedup_key = f"{listing_id}" if listing_id else f"{lot_name}|{price}|{currency_code}|{availability}"
            if dedup_key in seen:
                continue
            seen.add(dedup_key)
            fuzzy_passes.append(
                {
                    "lot_name": lot_name,
                    "normalized_lot_name": normalize_lot_name(lot_name),
                    "price": price,
                    "currency": currency_code or None,
                    "availability": availability,
                    "listing_id": listing_id,
                    "details": text if len(text) < 500 else text[:500] + "...",
                }
            )
        return self._merge_pass_collections(dom_passes, frame_passes, panel_passes, fuzzy_passes)

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
            listing_id = (
                row.get("listingId")
                or row.get("listing_id")
                or row.get("inventoryId")
                or row.get("listingKey")
                or row.get("id")
            )
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
                    "currency": None,
                    "availability": availability,
                    "listing_id": listing_id,
                    "inventory_id": row.get("inventoryId"),
                    "listing_key": row.get("listingKey"),
                    "_source": "state",
                    "listing_details": {
                        "listingId": listing_id,
                        "inventoryId": row.get("inventoryId"),
                        "listingKey": row.get("listingKey"),
                        "title": lot_name,
                        "price_incl_fees": price_val,
                        "availability": availability,
                        "notes": row.get("listingNotes") or row.get("description"),
                    },
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
            listing_id = (
                row.get("listingId")
                or row.get("listing_id")
                or row.get("inventoryId")
                or row.get("listingKey")
                or row.get("id")
            )
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
                    "currency": None,
                    "availability": availability,
                    "listing_id": listing_id,
                    "inventory_id": row.get("inventoryId"),
                    "listing_key": row.get("listingKey"),
                    "_source": "payload_json",
                    "listing_details": {
                        "listingId": listing_id,
                        "inventoryId": row.get("inventoryId"),
                        "listingKey": row.get("listingKey"),
                        "title": lot_name,
                        "price_incl_fees": price_val,
                        "availability": availability,
                        "notes": row.get("listingNotes") or row.get("description"),
                    },
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
            price = (
                StubHubParkingScraper._numeric_price(formatted)
                or StubHubParkingScraper._normalize_raw_price(m.group("raw"))
            )
            currency = StubHubParkingScraper._currency_from_text(formatted)
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
                    "currency": currency or None,
                    "availability": availability,
                    "listing_id": listing_id,
                    "_source": source,
                    "listing_details": {
                        "title": lot,
                        "price_incl_fees": formatted,
                        "availability": availability,
                    },
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
            currency = StubHubParkingScraper._currency_from_text(price_text)
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
                    "currency": currency or None,
                    "availability": None,
                    "_source": source,
                    "listing_details": {
                        "title": lot,
                        "price_incl_fees": price_text,
                    },
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
                price = StubHubParkingScraper._normalize_raw_price(raw_m.group("raw"))
            if not price:
                continue
            currency = StubHubParkingScraper._currency_from_text(price_text)

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
                    "currency": currency or None,
                    "availability": (avail_m.group("avail") if avail_m else None),
                    "listing_id": listing_id,
                    "_source": f"{source}_loose",
                    "listing_details": {
                        "title": lot_name,
                        "price_incl_fees": price_text or (raw_m.group("raw") if raw_m else None),
                        "availability": (avail_m.group("avail") if avail_m else None),
                    },
                }
            )

        return passes

    async def _extract_passes_from_embedded_json(self) -> list[dict]:
        page_html = await self.page.content()
        return self._extract_passes_from_text(page_html, source="embedded_html")

    async def scrape_parking_details(self, event) -> list[dict]:
        # Create a fresh debug folder per run; delete after scraping is done.
        self._debug_dir = STORAGE_DIR / f"debug_phase2_{int(time.time())}"
        try:
            self._debug_dir.mkdir(parents=True, exist_ok=True)
        except Exception:
            self._debug_dir = STORAGE_DIR
        async def _attempt(url: str, label: str) -> tuple[list[dict], dict]:
            captured_payloads: list[str] = []
            captured_urls: list[str] = []
            captured_request_payloads: list[str] = []
            captured_request_urls: list[str] = []
            response_tasks: set[asyncio.Task] = set()

            async def _capture_response(resp):
                try:
                    ct = (await resp.all_headers()).get("content-type", "").lower()
                    url_l = resp.url.lower()
                    interesting = any(
                        token in url_l
                        for token in ["event", "listing", "inventory", "map", "log", "telemetry", "api", "search", "tickets"]
                    )
                    if not interesting and "json" not in ct:
                        return
                    txt = await resp.text()
                    if any(k in txt for k in ["visiblePopups", "listingId", "formattedPrice", "rawPrice", "listings", "listing", "availableQuantity", "ticketClassName", "sectionName", "zoneName", "currentPrice", "priceWithFees"]):
                        captured_payloads.append(txt)
                        captured_urls.append(resp.url)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    return

            def _capture_request(req):
                try:
                    pd = req.post_data or ""
                    if not pd:
                        return
                    if any(k in pd for k in ["visiblePopups", "VisibleSectionPopupsOnLoad", "listingId", "rawPrice", "formattedPrice", "currentPrice", "priceWithFees"]):
                        captured_request_payloads.append(pd)
                        captured_request_urls.append(req.url)
                except Exception:
                    return

            try:
                advertised_total = None

                def _handle_response(resp):
                    task = asyncio.create_task(_capture_response(resp))
                    response_tasks.add(task)
                    task.add_done_callback(response_tasks.discard)

                self.page.on("response", _handle_response)
                self.page.on("request", _capture_request)
                logger.info(f"[Scraper] Navigating to {url}")
                try:
                    await self.page.goto(url, wait_until="domcontentloaded", timeout=45000)
                except Exception as e:
                    logger.warning(f"[Scraper] Initial goto failed/timed out: {e}")
                    await self.page.goto(url, wait_until="commit", timeout=45000)

                logger.info(f"[Scraper] Navigation complete. Waiting for stabilization...")
                await self.human_delay()
                await asyncio.sleep(2)

                logger.info("[Scraper] Expanding and scrolling listing inventory...")
                try:
                    body_text = await self.page.evaluate("""() => document.body.innerText || ''""")
                    visible_before_expand = await self._count_visible_listing_nodes()
                    total_listing_count = self._extract_total_listing_count(body_text)
                    advertised_total = total_listing_count or advertised_total
                    should_expand = (
                        total_listing_count is None
                        or visible_before_expand < total_listing_count
                        or visible_before_expand < 40
                    )
                    if should_expand:
                        await self._load_all_listing_inventory()
                    else:
                        logger.info(
                            f"[Scraper] Listing pane already appears expanded ({visible_before_expand}/{total_listing_count or visible_before_expand} visible)."
                        )
                except Exception as exc:
                    logger.warning(f"[Scraper] Inventory expansion failed; continuing with partial page state: {exc}")

                dom_passes = await self._extract_passes_from_dom()
                state_passes = await self._extract_passes_from_state()
                embedded_passes = await self._extract_passes_from_embedded_json()

                if response_tasks:
                    await asyncio.gather(*list(response_tasks), return_exceptions=True)

                xhr_passes: list[dict] = []
                for payload in captured_payloads + captured_request_payloads:
                    extracted = self._extract_passes_from_json_payload(payload)
                    if not extracted:
                        extracted = self._extract_passes_from_text(payload, source=f"embedded_xhr_{label}")
                    xhr_passes.extend(extracted)

                passes = self._merge_pass_collections(dom_passes, state_passes, embedded_passes, xhr_passes)
                passes = self._filter_telemetry_rows(passes)

                if advertised_total and len(passes) + 5 < advertised_total:
                    logger.info(
                        f"[Scraper] Extracted {len(passes)} listings but page advertises {advertised_total}; retrying expansion."
                    )
                    try:
                        await asyncio.sleep(1.5)
                        await self._load_all_listing_inventory()
                        dom_retry = await self._extract_passes_from_dom()
                        state_retry = await self._extract_passes_from_state()
                        embedded_retry = await self._extract_passes_from_embedded_json()
                        xhr_retry: list[dict] = []
                        for payload in captured_payloads + captured_request_payloads:
                            extracted = self._extract_passes_from_json_payload(payload)
                            if not extracted:
                                extracted = self._extract_passes_from_text(payload, source=f"embedded_xhr_retry_{label}")
                            xhr_retry.extend(extracted)
                        passes = self._merge_pass_collections(
                            passes,
                            dom_retry,
                            state_retry,
                            embedded_retry,
                            xhr_retry,
                        )
                        passes = self._filter_telemetry_rows(passes)
                    except Exception as retry_exc:
                        logger.warning(f"[Scraper] Retry expansion/extraction failed: {retry_exc}")

                probe = {
                    "attempt": label,
                    "url": self.page.url,
                    "title": (await self.page.title()),
                    "captured_payloads": len(captured_payloads),
                    "captured_urls": captured_urls[:5],
                    "captured_request_payloads": len(captured_request_payloads),
                    "captured_request_urls": captured_request_urls[:5],
                    "dom_passes": len(dom_passes),
                    "state_passes": len(state_passes),
                    "embedded_passes": len(embedded_passes),
                    "xhr_passes": len(xhr_passes),
                    "merged_passes": len(passes),
                    "advertised_total": advertised_total,
                }
                return passes, probe
            finally:
                try:
                    self.page.remove_listener("response", _handle_response)
                except Exception:
                    pass
                try:
                    self.page.remove_listener("request", _capture_request)
                except Exception:
                    pass
                for task in list(response_tasks):
                    if not task.done():
                        task.cancel()
                if response_tasks:
                    await asyncio.gather(*list(response_tasks), return_exceptions=True)

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

        try:
            passes, probe = await _attempt(primary_url, "parking_url")
            if passes:
                self._last_probe = probe
                return passes

            self._last_probe = probe
            return []
        finally:
            if self._debug_dir and self._debug_dir != STORAGE_DIR:
                try:
                    shutil.rmtree(self._debug_dir, ignore_errors=True)
                except Exception:
                    pass
            self._debug_dir = None

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
