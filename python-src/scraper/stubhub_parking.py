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
            StubHubParkingScraper._price_identity_token(str(row.get("price") or "")),
            row.get("availability") or "",
            details.get("price_incl_fees") or "",
            details.get("notes") or "",
            details.get("title") or "",
            details.get("listingId") or "",
            details.get("inventoryId") or "",
            details.get("listingKey") or "",
            row.get("_source") or "",
        ]
        return "|".join(str(part).strip() for part in parts)

    @staticmethod
    def _is_stubhub_ui_chaff_lot_name(value: str | None) -> bool:
        """Labels from column headers / chips / ratings — not a parking lot title."""
        text = (value or "").strip()
        if not text:
            return True
        lower = text.lower()
        if re.fullmatch(r"price\s+per\s+pass", lower):
            return True
        if re.fullmatch(r"parking\s+pass(?:es)?", lower):
            return True
        if re.fullmatch(r"parking(?:\s+listing)?", lower):
            return True
        if re.fullmatch(r"incl\.?\s*fees", lower):
            return True
        if re.fullmatch(r"(amazing|great|good|poor)", lower):
            return True
        if re.fullmatch(r"\d{1,2}\.\d", lower):
            return True
        if re.fullmatch(r"\d+%\s*off", lower):
            return True
        if re.fullmatch(r"only\s+\d+\s+left", lower):
            return True
        if re.fullmatch(r"last\s+pass(?:es)?", lower):
            return True
        if re.fullmatch(r"\d+\s+listings?", lower):
            return True
        if re.fullmatch(r"each", lower):
            return True
        return False

    @staticmethod
    def _is_stubhub_ui_chaff_line(line: str) -> bool:
        s = (line or "").strip()
        if not s:
            return True
        if StubHubParkingScraper._is_stubhub_ui_chaff_lot_name(s):
            return True
        if re.fullmatch(r"\d+\s*(?:-|to)\s*\d+\s*passes?", s, flags=re.IGNORECASE):
            return True
        if re.fullmatch(r"\d+\s*pass(?:es)?", s, flags=re.IGNORECASE):
            return True
        if re.fullmatch(r"[$€£R]\s*[\d,]+(?:\.\d{2})?\+?", s):
            return True
        if re.search(r"buyer could receive", s, flags=re.IGNORECASE):
            return True
        return False

    @staticmethod
    def _dom_pick_lot_name(title: str | None, lines: list[str]) -> str | None:
        for raw in lines:
            s = raw.strip()
            if re.match(r"^Lot\s+\d+\b", s, flags=re.IGNORECASE):
                return s[:120]
        for raw in lines:
            s = raw.strip()
            if re.match(r"^Private\s+Lots?\b", s, flags=re.IGNORECASE):
                return s[:120]
        t = (title or "").strip()
        if t and not StubHubParkingScraper._is_stubhub_ui_chaff_lot_name(t):
            return t[:120]
        for raw in lines:
            s = raw.strip()
            if not s or len(s) > 200:
                continue
            if StubHubParkingScraper._is_stubhub_ui_chaff_line(s):
                continue
            if StubHubParkingScraper._is_stubhub_ui_chaff_lot_name(s):
                continue
            return s[:120]
        return None

    @staticmethod
    def _currency_amounts_in_order(text: str) -> list[tuple[str, float]]:
        out: list[tuple[str, float]] = []
        for sym, code in (("$", "USD"), ("€", "EUR"), ("£", "GBP")):
            for m in re.finditer(re.escape(sym) + r"\s*([\d,]+(?:\.\d{1,2})?)\+?", text):
                raw = m.group(1).replace(",", "")
                try:
                    out.append((code, float(raw)))
                except ValueError:
                    continue
        return out

    @staticmethod
    def _primary_price_value_from_card_text(text: str) -> tuple[str | None, str | None]:
        """
        Pick the price the buyer pays when the card shows crossed + sale, or a single price.
        Returns (normalized_price_string, currency_code_or_empty).
        """
        amounts = StubHubParkingScraper._currency_amounts_in_order(text or "")
        if not amounts:
            return None, None
        codes = {c for c, _ in amounts}
        primary_code = amounts[0][0] if len(codes) == 1 else "USD"
        same = [v for c, v in amounts if c == primary_code]
        if not same:
            same = [v for _, v in amounts]
        lower = (text or "").lower()
        discounted = bool(re.search(r"\d+\s*%\s*off", lower)) or ("% off" in lower)
        if len(same) >= 2 and discounted:
            pick = min(same)
        elif len(same) >= 2:
            pick = same[-1]
        else:
            pick = same[0]
        return f"{pick:.2f}", primary_code

    @staticmethod
    def _price_identity_token(value: str | None) -> str:
        """Stable token for deduping fallback merge keys (125 vs 125.00)."""
        p = StubHubParkingScraper._numeric_price(str(value or ""))
        if not p:
            return ""
        if "." not in p:
            return f"{p}.00"
        a, _, b = p.partition(".")
        b = (b + "00")[:2]
        return f"{a}.{b}"

    @staticmethod
    def _is_bundle_extracted_source(src: str | None) -> bool:
        s = str(src or "")
        return bool(
            s == "payload_json"
            or "payload_json" in s
            or "embedded_xhr" in s
            or "embedded_html" in s
        )

    @staticmethod
    def _is_generic_lot_name(value: str | None) -> bool:
        text = (value or "").strip()
        if not text:
            return True
        if StubHubParkingScraper._is_stubhub_ui_chaff_lot_name(text):
            return True
        if re.fullmatch(r"(Section\s+\d+|Listing\s+\d+)", text, flags=re.IGNORECASE):
            return True
        # Distance-only/location-only placeholders are not useful lot names.
        if re.fullmatch(r"Within\s+\d+(?:\.\d+)?\s*(?:mi|km)", text, flags=re.IGNORECASE):
            return True
        if re.fullmatch(r"\d+\s*(?:-|to)\s*\d+\s*passes?", text, flags=re.IGNORECASE):
            return True
        if re.fullmatch(r"\d+\s*min(?:ute)?s?\s+walk", text, flags=re.IGNORECASE):
            return True
        return False

    @staticmethod
    def _is_raw_section_idish(value: str | None) -> bool:
        """Bare numeric StubHub section id (not a human title like 'Lot 6')."""
        text = (value or "").strip()
        if not text:
            return False
        return bool(re.fullmatch(r"\d{4,12}", text))

    @staticmethod
    def _inventory_display_name_candidates(obj: dict) -> list[str]:
        keys = [
            "listingTitle",
            "parkingTitle",
            "inventoryTitle",
            "ticketClassName",
            "zoneName",
            "name",
            "sectionName",
        ]
        out: list[str] = []

        def _push(v) -> None:
            if v is None or isinstance(v, (dict, list)):
                return
            s = str(v).strip()
            if s:
                out.append(s)

        for k in keys:
            _push(obj.get(k))
        for nest_key in ("parking", "location", "inventory"):
            sub = obj.get(nest_key)
            if isinstance(sub, dict):
                for k in keys:
                    _push(sub.get(k))
        return out

    @staticmethod
    def _pick_display_lot_name_from_inventory(obj: dict) -> str | None:
        """
        Prefer human-facing titles (e.g. Lot 6) over internal Section / numeric ids.
        """
        cands = StubHubParkingScraper._inventory_display_name_candidates(obj)
        if not cands:
            return None
        return sorted(cands, key=StubHubParkingScraper._lot_name_quality_score, reverse=True)[0]

    @staticmethod
    def _lot_name_quality_score(value: str | None) -> int:
        text = (value or "").strip()
        if not text:
            return -100
        lower = text.lower()
        score = 0
        if StubHubParkingScraper._is_raw_section_idish(text):
            score -= 30
        if StubHubParkingScraper._is_generic_lot_name(text):
            score -= 20
        # Prefer labels that look like real lot names / addresses.
        if re.search(r"\b(garage|lot|center|center garage|parking)\b", lower):
            score += 8
        if re.search(r"\b(st|street|ave|avenue|blvd|boulevard|dr|drive|rd|road|pl|place)\b", lower):
            score += 10
        if re.search(r"\b\d{1,5}\s+[a-z]", lower):
            score += 8
        # Distance-only phrasing is weaker than named lots.
        if re.search(r"\b(within\s+[0-9.]+\s*(mi|km)|[0-9.]+\s*(mi|km)\s+from venue)\b", lower):
            score -= 8
        if re.search(r"\b\d+\s*(-|to)\s*\d+\s*passes?\b", lower):
            score -= 6
        return score

    @staticmethod
    def _merge_pick_better_lot_name(name_a: str | None, name_b: str | None) -> str | None:
        """Prefer higher-quality human-facing lot names."""
        a, b = (name_a or "").strip(), (name_b or "").strip()
        if not a:
            return b or None
        if not b:
            return a
        sa = StubHubParkingScraper._lot_name_quality_score(a)
        sb = StubHubParkingScraper._lot_name_quality_score(b)
        if sb > sa:
            return b
        return a

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

                better = StubHubParkingScraper._merge_pick_better_lot_name(
                    preferred.get("lot_name"), fallback.get("lot_name")
                )
                if better:
                    preferred["lot_name"] = better
                    preferred["normalized_lot_name"] = normalize_lot_name(better)
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

                if better:
                    merged_d = preferred.get("listing_details") if isinstance(preferred.get("listing_details"), dict) else {}
                    if StubHubParkingScraper._is_generic_lot_name(str(merged_d.get("title") or "")):
                        preferred["listing_details"] = {**merged_d, "title": better}

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
        return [row for row in rows if StubHubParkingScraper._is_real_listing_row(row)]

    @staticmethod
    def _has_parseable_price(row: dict) -> bool:
        direct = StubHubParkingScraper._numeric_price(str(row.get("price") or ""))
        if direct:
            return True
        details = row.get("listing_details") if isinstance(row.get("listing_details"), dict) else {}
        detail_price = details.get("price_incl_fees") or details.get("price") or details.get("formattedPrice")
        return bool(StubHubParkingScraper._numeric_price(str(detail_price or "")))

    @staticmethod
    def _is_real_listing_row(row: dict) -> bool:
        if not isinstance(row, dict):
            return False
        lot_name = str(row.get("lot_name") or "").strip()
        listing_id = StubHubParkingScraper._listing_identifier(row)
        has_price = StubHubParkingScraper._has_parseable_price(row)
        if not has_price:
            return False
        if listing_id:
            return True
        if StubHubParkingScraper._is_stubhub_ui_chaff_lot_name(lot_name):
            return False
        details = row.get("listing_details") if isinstance(row.get("listing_details"), dict) else {}
        src = str(row.get("_source") or "")
        avail = row.get("availability") or details.get("availability")
        # HTML/JSON bundle extracts often carry Section ids without listing ids — drop as telemetry.
        if StubHubParkingScraper._is_bundle_extracted_source(src):
            if StubHubParkingScraper._is_generic_lot_name(lot_name):
                return False
            return bool(lot_name or avail or details.get("notes") or details.get("rating"))

        if StubHubParkingScraper._is_generic_lot_name(lot_name):
            return bool(avail)

        return bool(
            lot_name
            or avail
            or (details.get("title") or details.get("name"))
        )

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
        """Largest 'N listings' in body text (first match alone often hits a smaller UI fragment)."""
        pat = re.compile(r"\b([0-9][0-9,]*)\s+listings\b", flags=re.IGNORECASE)
        best: int | None = None
        for m in pat.finditer(body_text or ""):
            try:
                val = int(m.group(1).replace(",", ""))
            except Exception:
                continue
            if val > 1_000_000:
                continue
            if best is None or val > best:
                best = val
        return best

    @staticmethod
    def _expansion_budget_for_advertised(advertised: int | None) -> tuple[float, int]:
        """
        Longer scroll/load-more budget when StubHub reports many listings (virtualized lists).
        Returns (max_duration_seconds, max_inner_rounds).
        """
        if advertised is None or advertised < 1:
            return (45.0, 36)
        adv = min(int(advertised), 2000)
        duration = min(180.0, max(45.0, 45.0 + adv * 0.35))
        rounds = min(90, max(36, 24 + adv // 6))
        return (duration, rounds)

    async def _load_all_listing_inventory(
        self,
        *,
        max_duration_seconds: float | None = None,
        max_rounds: int | None = None,
    ) -> dict:
        max_duration_seconds = 45.0 if max_duration_seconds is None else float(max_duration_seconds)
        max_rounds = 36 if max_rounds is None else int(max_rounds)
        max_rounds = max(12, min(max_rounds, 120))

        started_at = time.perf_counter()
        no_growth_rounds = 0
        last_visible_count = -1
        last_id_count = -1
        target_listing_count: int | None = None
        load_more_clicks = 0
        stop_reason = "max_iterations"

        stall_retry_budget = 2
        for _ in range(max_rounds):
            if time.perf_counter() - started_at >= max_duration_seconds:
                logger.info(
                    f"[Scraper] Inventory expansion budget exhausted after {max_duration_seconds:.1f}s; proceeding with extraction."
                )
                stop_reason = "timeout"
                break
            load_more_available = False
            load_more_clicked = False
            try:
                show_more = self.page.get_by_role("button", name=re.compile("Show more|See more|Load more", re.IGNORECASE))
                if await show_more.count() > 0 and await show_more.first.is_visible():
                    load_more_available = True
                    disabled = await show_more.first.get_attribute("disabled")
                    aria_disabled = await show_more.first.get_attribute("aria-disabled")
                    if disabled is None and str(aria_disabled or "").lower() != "true":
                        await show_more.first.click()
                        await asyncio.sleep(1.5)
                        load_more_clicks += 1
                        load_more_clicked = True
            except Exception:
                pass

            try:
                await self.page.evaluate(
                    """() => {
                        window.scrollTo(0, document.body.scrollHeight);
                        const containers = Array.from(document.querySelectorAll('div, section, ul, main'))
                            .filter((el) => {
                                const style = window.getComputedStyle(el);
                                const overflowY = style.overflowY || '';
                                return el.scrollHeight > (el.clientHeight + 40) && ['auto', 'scroll'].includes(overflowY);
                            })
                            .sort((a, b) => {
                                const aCards = a.querySelectorAll('[data-listing-id], [data-listingid], [role="listitem"], [data-testid*="listing"]').length;
                                const bCards = b.querySelectorAll('[data-listing-id], [data-listingid], [role="listitem"], [data-testid*="listing"]').length;
                                if (bCards !== aCards) return bCards - aCards;
                                return b.scrollHeight - a.scrollHeight;
                            });
                        const dominant = containers[0];
                        if (dominant) {
                            const step = Math.max(80, Math.floor(dominant.clientHeight * 0.85));
                            dominant.scrollTop = Math.min(dominant.scrollHeight, dominant.scrollTop + step);
                            dominant.scrollTop = Math.min(dominant.scrollHeight, dominant.scrollTop + step);
                        }
                        for (const el of containers.slice(1, 5)) {
                            try { el.scrollTop = el.scrollHeight; } catch (e) {}
                        }
                    }"""
                )
            except Exception:
                pass

            at_bottom = False
            try:
                at_bottom = bool(
                    await self.page.evaluate(
                        """() => {
                            const containers = Array.from(document.querySelectorAll('div, section, ul, main'))
                                .filter((el) => {
                                    const style = window.getComputedStyle(el);
                                    const overflowY = style.overflowY || '';
                                    return el.scrollHeight > (el.clientHeight + 40) && ['auto', 'scroll'].includes(overflowY);
                                })
                                .sort((a, b) => b.scrollHeight - a.scrollHeight)
                                .slice(0, 1);
                            const el = containers[0];
                            if (!el) {
                                const top = window.scrollY || window.pageYOffset || 0;
                                return (window.innerHeight + top) >= ((document.body && document.body.scrollHeight) || 0) - 40;
                            }
                            return (el.scrollTop + el.clientHeight) >= (el.scrollHeight - 24);
                        }"""
                    )
                )
            except Exception:
                at_bottom = False

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
            listing_ids_seen = 0
            try:
                listing_ids_seen = int(
                    await self.page.evaluate(
                        """() => {
                            const ids = new Set();
                            for (const el of document.querySelectorAll('[data-listing-id], [data-listingid]')) {
                                const id = el.getAttribute('data-listing-id') || el.getAttribute('data-listingid');
                                if (id) ids.add(String(id));
                            }
                            return ids.size;
                        }"""
                    )
                )
            except Exception:
                listing_ids_seen = 0

            grew_visible = visible_count > last_visible_count
            grew_ids = listing_ids_seen > last_id_count
            if not grew_visible and not grew_ids:
                no_growth_rounds += 1
            else:
                no_growth_rounds = 0
            last_visible_count = max(last_visible_count, visible_count)
            last_id_count = max(last_id_count, listing_ids_seen)

            if target_listing_count and last_visible_count < target_listing_count and no_growth_rounds < 8:
                continue

            if load_more_available and not load_more_clicked and no_growth_rounds >= 3:
                stop_reason = "load_more_disabled"
                break
            if no_growth_rounds >= 6 and at_bottom and not load_more_available:
                stop_reason = "bottom_no_growth"
                break
            if no_growth_rounds >= 8:
                if stall_retry_budget > 0:
                    stall_retry_budget -= 1
                    no_growth_rounds = 0
                    stop_reason = "stall_retry"
                    await asyncio.sleep(1.0)
                    continue
                stop_reason = "no_growth"
                break

        return {
            "load_more_clicks": load_more_clicks,
            "no_growth_rounds": no_growth_rounds,
            "unique_ids_seen": max(last_id_count, 0),
            "stop_reason": stop_reason,
            "stall_retries_used": 2 - stall_retry_budget,
            "expansion_max_duration_seconds": max_duration_seconds,
            "expansion_max_rounds": max_rounds,
        }

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
        async def _extract_from_context(ctx, source_tag: str) -> list[dict]:
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
                lines = [l.strip() for l in text.splitlines() if l.strip()]

                price, currency = self._primary_price_value_from_card_text(text)
                price_text = self._normalize_price_text_display(data.get("priceText") or "") or ""
                if not price:
                    price_match = re.search(r"([0-9][0-9,]*(?:\.[0-9]{1,2})?)", price_text)
                    if not price_match and text:
                        price_match = re.search(r"[$€£R]\s?([0-9][0-9,]*(?:\.[0-9]{1,2})?)", text)
                    if not price_match:
                        continue
                    price = price_match.group(1).replace(",", "")
                    if "." not in price:
                        price = f"{price}.00"
                    currency = self._currency_from_text(price_text or text)
                else:
                    currency = currency or self._currency_from_text(text)

                availability = self._availability_from_text(data.get("rawAvail") or text)
                lot_name = self._dom_pick_lot_name(title, lines)
                if not lot_name:
                    continue

                display_price = f"${price}" if price else (price_text or "")
                dedup_key = (
                    f"{listing_id}"
                    if listing_id
                    else f"{lot_name}|{self._price_identity_token(price)}|{availability}"
                )
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
                        "_source": source_tag,
                        "listing_details": {
                            "title": lot_name,
                            "price_incl_fees": display_price or price_text,
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
        dom_passes = await _extract_from_context(self.page, "dom")
        logger.info(f"[Scraper] DOM extraction found {len(dom_passes or [])} passes")

        frame_passes: list[dict] = []
        for frame in self.page.frames:
            if frame == self.page.main_frame:
                continue
            try:
                frame_passes.extend(await _extract_from_context(frame, "frame"))
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
                lines = [l.strip() for l in text.splitlines() if l.strip()]
                price, currency_code = self._primary_price_value_from_card_text(text)
                if not price:
                    price_match = re.search(
                        r"([$€£R])\s?([0-9][0-9,]*(?:\.[0-9]{2})?)", text
                    )
                    if not price_match:
                        continue
                    price = price_match.group(2).replace(",", "")
                    if "." not in price:
                        price = f"{price}.00"
                    currency_code = self._currency_from_text(price_match.group(1)) or self._currency_from_text(
                        text
                    )
                availability = self._availability_from_text(text)
                lot_name = self._dom_pick_lot_name(heading, lines)
                if not lot_name:
                    continue
                listing_id = data.get("listingId")
                dedup_key = (
                    f"{listing_id}"
                    if listing_id
                    else f"{lot_name}|{self._price_identity_token(price)}|{currency_code}|{availability}"
                )
                if dedup_key in seen:
                    continue
                seen.add(dedup_key)
                panel_passes.append(
                    {
                        "lot_name": lot_name,
                        "normalized_lot_name": normalize_lot_name(lot_name),
                        "price": price,
                        "currency": (currency_code or None) or self._currency_from_text(text) or None,
                        "availability": availability,
                        "listing_id": listing_id,
                        "_source": "dom_panel",
                        "listing_details": {
                            "title": lot_name,
                            "price_incl_fees": f"${price}",
                            "availability": availability,
                        },
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
            raw_cards = []

        fuzzy_passes = []
        seen = set()
        for data in raw_cards or []:
            text = data.get("text") or ""
            heading = data.get("heading")
            lines = [l.strip() for l in text.splitlines() if l.strip()]
            if len(lines) < 2:
                continue
            price, currency_code = self._primary_price_value_from_card_text(text)
            if not price:
                price_match = re.search(r"([$€£])\s?([0-9][0-9,]*(?:\.[0-9]{2})?)", text)
                if not price_match:
                    continue
                price = price_match.group(2).replace(",", "")
                if "." not in price:
                    price = f"{price}.00"
                currency_code = self._currency_from_text(price_match.group(1)) or self._currency_from_text(text)
            availability = self._availability_from_text(text)
            lot_name = self._dom_pick_lot_name(heading, lines)
            if not lot_name:
                continue
            listing_id = data.get("listingId")
            dedup_key = (
                f"{listing_id}"
                if listing_id
                else f"{lot_name}|{self._price_identity_token(price)}|{currency_code}|{availability}"
            )
            if dedup_key in seen:
                continue
            seen.add(dedup_key)
            fuzzy_passes.append(
                {
                    "lot_name": lot_name,
                    "normalized_lot_name": normalize_lot_name(lot_name),
                    "price": price,
                    "currency": (currency_code or None) or self._currency_from_text(text) or None,
                    "availability": availability,
                    "listing_id": listing_id,
                    "_source": "dom_fuzzy",
                    "listing_details": {
                        "title": lot_name,
                        "price_incl_fees": f"${price}",
                        "availability": availability,
                    },
                    "details": text if len(text) < 500 else text[:500] + "...",
                }
            )
        return self._merge_pass_collections(dom_passes, frame_passes, panel_passes, fuzzy_passes)

    async def _extract_passes_from_state(self) -> list[dict]:
        payload = await self.page.evaluate(
            """() => {
                const roots = [];
                for (const key of ["__PRELOADED_STATE__", "__INITIAL_STATE__", "__NEXT_DATA__", "SHAppApi", "__APOLLO_STATE__", "__NUXT__"]) {
                    const v = window[key];
                    if (v) roots.push(v);
                }
                // Also inspect JSON script tags that often contain listing state.
                for (const script of Array.from(document.querySelectorAll('script[type="application/json"], script#__NEXT_DATA__'))) {
                    try {
                        const txt = (script.textContent || '').trim();
                        if (!txt) continue;
                        roots.push(JSON.parse(txt));
                    } catch (e) {}
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
            lot_name = StubHubParkingScraper._pick_display_lot_name_from_inventory(row)
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
        data = None
        try:
            data = json.loads(raw_text)
        except Exception:
            text = (raw_text or "").strip()
            idx_obj = text.find("{")
            idx_arr = text.find("[")
            starts = [i for i in [idx_obj, idx_arr] if i >= 0]
            if not starts:
                return []
            start = min(starts)
            trimmed = text[start:]
            # Drop common anti-CSRF prefix like )]}'
            trimmed = re.sub(r"^\)\]\}',?\s*", "", trimmed)
            try:
                data = json.loads(trimmed)
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
            lot_name = StubHubParkingScraper._pick_display_lot_name_from_inventory(row)
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
            # Section-only telemetry rows (without listing identity) inflate counts.
            # Do not emit them as listing rows.
            continue

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
                expansion_meta = {
                    "load_more_clicks": 0,
                    "no_growth_rounds": 0,
                    "unique_ids_seen": 0,
                    "stop_reason": "not_started",
                    "stall_retries_used": 0,
                }
                budget_dur, budget_rounds = StubHubParkingScraper._expansion_budget_for_advertised(None)

                def _merge_expansion_meta(acc: dict, nxt: dict) -> None:
                    acc["load_more_clicks"] = int(acc.get("load_more_clicks", 0)) + int(
                        nxt.get("load_more_clicks", 0)
                    )
                    acc["no_growth_rounds"] = max(
                        int(acc.get("no_growth_rounds", 0)),
                        int(nxt.get("no_growth_rounds", 0)),
                    )
                    acc["unique_ids_seen"] = max(
                        int(acc.get("unique_ids_seen", 0)),
                        int(nxt.get("unique_ids_seen", 0)),
                    )
                    acc["stop_reason"] = str(
                        nxt.get("stop_reason") or acc.get("stop_reason") or ""
                    )
                    acc["stall_retries_used"] = max(
                        int(acc.get("stall_retries_used", 0)),
                        int(nxt.get("stall_retries_used", 0)),
                    )

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
                    budget_dur, budget_rounds = StubHubParkingScraper._expansion_budget_for_advertised(
                        advertised_total
                    )
                    should_expand = (
                        total_listing_count is None
                        or visible_before_expand < total_listing_count
                        or visible_before_expand < 40
                    )
                    if should_expand:
                        expansion_meta = await self._load_all_listing_inventory(
                            max_duration_seconds=budget_dur,
                            max_rounds=budget_rounds,
                        )
                    else:
                        logger.info(
                            f"[Scraper] Listing pane already appears expanded ({visible_before_expand}/{total_listing_count or visible_before_expand} visible)."
                        )
                        expansion_meta["stop_reason"] = "already_expanded"
                    expansion_meta["expansion_primary_duration_seconds"] = budget_dur
                    expansion_meta["expansion_primary_rounds"] = budget_rounds
                except Exception as exc:
                    logger.warning(f"[Scraper] Inventory expansion failed; continuing with partial page state: {exc}")
                    expansion_meta["stop_reason"] = "expansion_exception"
                    expansion_meta["expansion_primary_duration_seconds"] = budget_dur
                    expansion_meta["expansion_primary_rounds"] = budget_rounds

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

                shortfall_passes = 0
                max_shortfall_rounds = 3
                while (
                    advertised_total
                    and len(passes) + 5 < advertised_total
                    and shortfall_passes < max_shortfall_rounds
                ):
                    prev_len = len(passes)
                    shortfall_passes += 1
                    logger.info(
                        f"[Scraper] Extracted {len(passes)} listings but page advertises {advertised_total}; "
                        f"shortfall expansion pass {shortfall_passes}/{max_shortfall_rounds}."
                    )
                    try:
                        await asyncio.sleep(1.5)
                        follow_dur = min(120.0, budget_dur * 0.55 + 20.0)
                        follow_rounds = min(budget_rounds, max(40, budget_rounds // 2 + 14))
                        retry_meta = await self._load_all_listing_inventory(
                            max_duration_seconds=follow_dur,
                            max_rounds=follow_rounds,
                        )
                        _merge_expansion_meta(expansion_meta, retry_meta)
                        dom_retry = await self._extract_passes_from_dom()
                        state_retry = await self._extract_passes_from_state()
                        embedded_retry = await self._extract_passes_from_embedded_json()
                        xhr_retry: list[dict] = []
                        for payload in captured_payloads + captured_request_payloads:
                            extracted = self._extract_passes_from_json_payload(payload)
                            if not extracted:
                                extracted = self._extract_passes_from_text(
                                    payload, source=f"embedded_xhr_retry_{label}_{shortfall_passes}"
                                )
                            xhr_retry.extend(extracted)
                        passes = self._merge_pass_collections(
                            passes,
                            dom_retry,
                            state_retry,
                            embedded_retry,
                            xhr_retry,
                        )
                        passes = self._filter_telemetry_rows(passes)
                        if len(passes) < prev_len + 3:
                            logger.info(
                                "[Scraper] Shortfall pass produced minimal growth; stopping shortfall retries."
                            )
                            break
                    except Exception as retry_exc:
                        logger.warning(f"[Scraper] Shortfall expansion/extraction failed: {retry_exc}")
                        break
                expansion_meta["shortfall_extra_passes_used"] = shortfall_passes

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
                    "load_more_clicks": expansion_meta.get("load_more_clicks", 0),
                    "no_growth_rounds": expansion_meta.get("no_growth_rounds", 0),
                    "unique_ids_seen": expansion_meta.get("unique_ids_seen", 0),
                    "stop_reason": expansion_meta.get("stop_reason", ""),
                    "stall_retries_used": expansion_meta.get("stall_retries_used", 0),
                    "expansion_primary_duration_seconds": expansion_meta.get(
                        "expansion_primary_duration_seconds", budget_dur
                    ),
                    "expansion_primary_rounds": expansion_meta.get(
                        "expansion_primary_rounds", budget_rounds
                    ),
                    "shortfall_extra_passes_used": expansion_meta.get(
                        "shortfall_extra_passes_used", 0
                    ),
                    "unique_ids_vs_advertised_gap": (
                        int(advertised_total) - int(expansion_meta.get("unique_ids_seen", 0))
                        if advertised_total is not None
                        else None
                    ),
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
