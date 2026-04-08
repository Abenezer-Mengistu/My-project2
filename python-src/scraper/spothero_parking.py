"""
SpotHero parking scraper for event and destination search URLs.
"""
from __future__ import annotations

import re
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
from types import SimpleNamespace
from urllib.parse import parse_qs, urljoin, urlparse

from database.repositories.ticketing.parking_passes import get_parking_pass_repository
from scraper.base.ticketing.ticketing_playwright_base import TicketingPlaywrightBase
from utils.logger import logger
from utils.normalization import normalize_lot_name


def _quote_price_breakdown_cents(
    quote: dict,
    total_cents: int | None,
    advertised_cents: int | None,
) -> tuple[int | None, int | None]:
    items = quote.get("items") or []
    if not items and isinstance(quote.get("order"), list) and quote["order"]:
        items = (quote["order"][0] or {}).get("items") or []

    subtotal = 0
    subtotal_found = False
    fee_sum = 0
    for item in items if isinstance(items, list) else []:
        if not isinstance(item, dict):
            continue
        price_value = (item.get("price") or {}).get("value")
        if price_value is None:
            continue
        try:
            cents = int(price_value)
        except (TypeError, ValueError):
            continue
        if cents <= 0:
            continue
        item_type = str(item.get("type") or "").lower()
        if item_type == "rental":
            subtotal += cents
            subtotal_found = True
        else:
            fee_sum += cents

    subtotal_out = subtotal if subtotal_found else advertised_cents
    fee_out = None
    if total_cents is not None and subtotal_out is not None:
        fee_out = max(0, int(total_cents) - int(subtotal_out))
    elif fee_sum > 0:
        fee_out = fee_sum
    return subtotal_out, fee_out


def _quote_price_breakdown_lines(
    quote: dict,
    total_cents: int | None,
    advertised_cents: int | None,
) -> list[tuple[str, int]]:
    items = quote.get("items") or []
    if not items and isinstance(quote.get("order"), list) and quote["order"]:
        items = (quote["order"][0] or {}).get("items") or []

    lines: list[tuple[str, int]] = []
    for item in items if isinstance(items, list) else []:
        if not isinstance(item, dict):
            continue
        price_value = (item.get("price") or {}).get("value")
        if price_value is None:
            continue
        try:
            cents = int(price_value)
        except (TypeError, ValueError):
            continue
        if cents <= 0:
            continue
        item_type = str(item.get("type") or "").lower()
        short_description = str(item.get("short_description") or "").strip()
        if item_type == "rental":
            label = "Subtotal"
        elif short_description:
            label = short_description
        else:
            label = item_type.replace("_", " ").title() or "Fee"
        lines.append((label, cents))

    if lines:
        if total_cents is not None and lines[-1][0] != "Total":
            lines.append(("Total", int(total_cents)))
        return lines

    subtotal_cents, fee_cents = _quote_price_breakdown_cents(quote, total_cents, advertised_cents)
    fallback: list[tuple[str, int]] = []
    if subtotal_cents is not None:
        fallback.append(("Subtotal", int(subtotal_cents)))
    if fee_cents is not None:
        fallback.append(("Fees", int(fee_cents)))
    if total_cents is not None:
        fallback.append(("Total", int(total_cents)))
    return fallback


def _extract_quote_total_and_advertised(quote: dict) -> tuple[int | None, int | None]:
    total_payload = quote.get("total_price")
    advertised_payload = quote.get("advertised_price")
    total_value = total_payload.get("value") if isinstance(total_payload, dict) else None
    advertised_value = advertised_payload.get("value") if isinstance(advertised_payload, dict) else None

    if total_value is None and isinstance(quote.get("order"), list) and quote["order"]:
        order0 = quote["order"][0] or {}
        total_payload = order0.get("total_price")
        if isinstance(total_payload, dict):
            total_value = total_payload.get("value")
        if advertised_value is None:
            advertised_payload = order0.get("advertised_price")
            if isinstance(advertised_payload, dict):
                advertised_value = advertised_payload.get("value")

    try:
        total_cents = int(total_value) if total_value is not None else None
    except (TypeError, ValueError):
        total_cents = None
    try:
        advertised_cents = int(advertised_value) if advertised_value is not None else None
    except (TypeError, ValueError):
        advertised_cents = None
    return total_cents, advertised_cents


def _last_facility_id_from_transient_results(results: list) -> str | None:
    for result in reversed(results or []):
        if not isinstance(result, dict):
            continue
        facility = result.get("facility") or {}
        common = facility.get("common") if isinstance(facility, dict) else {}
        if not isinstance(common, dict):
            continue
        facility_id = common.get("id")
        if facility_id is not None:
            return str(facility_id)
    return None


def _format_cents(cents: int | None, currency: str = "USD") -> str:
    if cents is None:
        return ""
    dollars = (Decimal(cents) / Decimal(100)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    if (currency or "").upper() == "USD":
        return f"${format(dollars, 'f')}"
    return f"{currency.upper()} {format(dollars, 'f')}"


def _hours_between(starts: str | None, ends: str | None) -> str:
    start_text = str(starts or "").strip()
    end_text = str(ends or "").strip()
    if not start_text or not end_text:
        return ""
    try:
        start_dt = datetime.fromisoformat(start_text.replace("Z", "+00:00"))
        end_dt = datetime.fromisoformat(end_text.replace("Z", "+00:00"))
        total_hours = max(0, (end_dt - start_dt).total_seconds() / 3600)
        if total_hours.is_integer():
            return f"{int(total_hours)} hours"
        return f"{round(total_hours, 1)} hours"
    except Exception:
        return ""


class SpotHeroParkingScraper(TicketingPlaywrightBase):
    handler: str = "spothero-parking"

    @staticmethod
    def _first_lat_lon_pair(obj: object) -> tuple[str | None, str | None]:
        if isinstance(obj, dict):
            lat = obj.get("latitude")
            lon = obj.get("longitude")
            if lat is not None and lon is not None:
                try:
                    return str(float(lat)), str(float(lon))
                except (TypeError, ValueError):
                    pass
            for value in obj.values():
                found_lat, found_lon = SpotHeroParkingScraper._first_lat_lon_pair(value)
                if found_lat and found_lon:
                    return found_lat, found_lon
        elif isinstance(obj, list):
            for value in obj:
                found_lat, found_lon = SpotHeroParkingScraper._first_lat_lon_pair(value)
                if found_lat and found_lon:
                    return found_lat, found_lon
        return None, None

    async def _fetch_lat_lon_from_facility(self, facility_id: str) -> tuple[str | None, str | None]:
        try:
            response = await self.page.request.get(f"https://api.spothero.com/v2/facilities/{facility_id}")
            if response.status != 200:
                return None, None
            data = await response.json()
            return self._first_lat_lon_pair(data)
        except Exception as exc:
            logger.warning(f"SpotHero facility geo lookup failed id={facility_id}: {exc}")
            return None, None

    async def scrape_parking_details(self, event) -> list[dict]:
        url = event.parking_url or event.event_url
        match = re.search(r"[?&]id=(\d+)", str(url or ""))
        if not match:
            logger.error(f"Could not extract id= from SpotHero URL: {url}")
            return []

        entity_id = match.group(1)
        parsed_url = urlparse(str(url))
        source_query = parse_qs(parsed_url.query)
        kind = ((source_query.get("kind") or ["event"])[0] or "event").lower()
        is_destination = kind == "destination"
        max_coverage = bool(getattr(event, "max_parking_coverage", False))

        def _with_tracking(params: dict) -> dict:
            copy = dict(params)
            copy.setdefault("session_id", str(uuid.uuid4()))
            copy.setdefault("search_id", str(uuid.uuid4()))
            copy.setdefault("action_id", str(uuid.uuid4()))
            copy.setdefault("fingerprint", str(uuid.uuid4()))
            return copy

        def _iso(dt: datetime) -> str:
            return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")

        if is_destination:
            base_params = {
                "destination_id": entity_id,
                "action": "LIST_DESTINATION",
                "include_walking_distance": "true",
                "sort_by": "relevance",
                "show_unavailable": "false",
                "initial_search": "true",
                "oversize": "false",
            }
        else:
            base_params = {
                "event_id": entity_id,
                "action": "LIST_EVENT",
                "include_walking_distance": "true",
                "sort_by": "relevance",
                "show_unavailable": "false",
                "initial_search": "true",
                "oversize": "false",
            }
        reservation_starts = str(base_params.get("starts") or "").strip() or None
        reservation_ends = str(base_params.get("ends") or "").strip() or None

        for key in ("lat", "lon", "starts", "ends", "fingerprint", "session_id", "search_id", "action_id", "spot_id", "spot-id"):
            value = (source_query.get(key) or [None])[0]
            if value:
                normalized_key = "spot_id" if key == "spot-id" else key
                base_params[normalized_key] = value

        event_root_cache: dict[str, dict] = {}

        async def _load_event_root() -> dict | None:
            if is_destination:
                return None
            if entity_id in event_root_cache:
                return event_root_cache[entity_id]
            try:
                response = await self.page.request.get(f"https://api.spothero.com/v2/events/{entity_id}")
                if response.status != 200:
                    return None
                payload = await response.json()
                root = payload.get("data") if isinstance(payload, dict) and isinstance(payload.get("data"), dict) else payload
                if isinstance(root, dict):
                    event_root_cache[entity_id] = root
                    return root
            except Exception as exc:
                logger.warning(f"SpotHero event payload load failed id={entity_id}: {exc}")
            return None

        if not base_params.get("lat") or not base_params.get("lon"):
            lat = lon = None
            spot_anchor = base_params.get("spot_id")
            if spot_anchor:
                lat, lon = await self._fetch_lat_lon_from_facility(str(spot_anchor))
            if (not lat or not lon) and not is_destination:
                event_root = await _load_event_root()
                if event_root:
                    lat, lon = self._first_lat_lon_pair(event_root)
            if lat and lon:
                base_params["lat"] = lat
                base_params["lon"] = lon

        if not base_params.get("starts") or not base_params.get("ends"):
            event_root = await _load_event_root()
            parking_window = event_root.get("parking_window") if isinstance(event_root, dict) else {}
            starts = None
            ends = None
            if isinstance(parking_window, dict):
                starts = parking_window.get("starts")
                ends = parking_window.get("ends")
            if not starts and isinstance(event_root, dict):
                starts = event_root.get("starts")
                ends = event_root.get("ends")
            if starts and ends:
                base_params["starts"] = str(starts)
                base_params["ends"] = str(ends)
                reservation_starts = str(starts)
                reservation_ends = str(ends)

        fallback_now = datetime.now(timezone.utc)
        fallback_time_window = {
            "starts": _iso(fallback_now - timedelta(hours=2)),
            "ends": _iso(fallback_now + timedelta(hours=6)),
        }
        default_geo = {"lat": "40.750504", "lon": "-73.993439"}

        rich_params = dict(base_params)
        if "starts" not in rich_params or "ends" not in rich_params:
            rich_params.update(fallback_time_window)
        if "lat" not in rich_params or "lon" not in rich_params:
            rich_params.update(default_geo)

        primary_params = [_with_tracking(rich_params)]
        if max_coverage:
            for overlay in (
                {"show_unavailable": "true"},
                {"oversize": "true"},
                {"sort_by": "price"},
            ):
                primary_params.append(_with_tracking({**rich_params, **overlay}))

        def _pass_key(row: dict) -> tuple:
            listing_id = str(row.get("listing_id") or "").strip() or f"noid:{normalize_lot_name(row.get('lot_name') or '')}"
            rate_external_id = str(row.get("rate_external_id") or "")
            rate_index = int(row.get("rate_index") or 0)
            return (listing_id, rate_external_id or f"idx:{rate_index}")

        def _raw_result_to_passes(result: dict) -> list[dict]:
            facility = result.get("facility") or {}
            common = facility.get("common") if isinstance(facility, dict) else {}
            if not isinstance(common, dict):
                common = {}
            lot_name = common.get("title") or ((common.get("address") or {}).get("street_address"))
            if not lot_name:
                return []

            listing_id = str(common.get("id") or facility.get("id") or "")
            distance = result.get("distance") or {}
            duration_seconds = distance.get("duration_seconds") if isinstance(distance, dict) else None
            walking_minutes = round((duration_seconds or 0) / 60.0, 1) if duration_seconds is not None else None
            availability = result.get("availability") or {}
            is_available = availability.get("available") if isinstance(availability, dict) else None
            available_spaces = availability.get("available_spaces") if isinstance(availability, dict) else None
            base_availability = f"{walking_minutes} min walk" if walking_minutes is not None else ("Available" if is_available else "Unavailable")
            if is_available is False and walking_minutes is not None:
                base_availability = f"{walking_minutes} min walk (unavailable)"

            rates = result.get("rates") or []
            if not isinstance(rates, list) or not rates:
                return [
                    {
                        "lot_name": lot_name,
                        "normalized_lot_name": normalize_lot_name(lot_name),
                        "price": None,
                        "currency": "USD",
                        "availability": base_availability,
                        "listing_id": listing_id,
                        "available_spaces": available_spaces,
                        "reservation_type": "Parking Reservation",
                        "reservation_duration": _hours_between(reservation_starts, reservation_ends),
                        "reservation_starts": reservation_starts,
                        "reservation_ends": reservation_ends,
                        "in_out_policy": "",
                        "rate_index": 0,
                        "rate_external_id": "",
                        "listing_details": {},
                        "_source": "spothero_api",
                    }
                ]

            rows: list[dict] = []
            for rate_index, rate in enumerate(rates):
                if not isinstance(rate, dict):
                    continue
                quote = rate.get("quote") or {}
                total_cents, advertised_cents = _extract_quote_total_and_advertised(quote)
                subtotal_cents, fee_cents = _quote_price_breakdown_cents(quote, total_cents, advertised_cents)
                breakdown_lines = _quote_price_breakdown_lines(quote, total_cents, advertised_cents)
                quote_total = quote.get("total_price") or {}
                currency = str(quote_total.get("currency_code") or "USD").upper()
                total_display = _format_cents(total_cents, currency)
                subtotal_display = _format_cents(subtotal_cents, currency)
                fee_display = _format_cents(fee_cents, currency)
                rate_external_id = rate.get("id")
                if rate_external_id is None:
                    rate_external_id = (quote.get("meta") or {}).get("quote_mac")
                if rate_external_id is None and isinstance(quote.get("order"), list) and quote["order"]:
                    rate_external_id = (quote["order"][0] or {}).get("rate_id")
                quote_meta = quote.get("meta") if isinstance(quote.get("meta"), dict) else {}
                policy_parts: list[str] = []
                rate_name = str(rate.get("name") or rate.get("title") or "").strip()
                if rate_name:
                    policy_parts.append(rate_name)
                restrictions = rate.get("restrictions")
                if isinstance(restrictions, list):
                    for item in restrictions:
                        if isinstance(item, str) and item.strip():
                            policy_parts.append(item.strip())
                        elif isinstance(item, dict):
                            label = str(item.get("label") or item.get("title") or item.get("name") or "").strip()
                            if label:
                                policy_parts.append(label)
                for key in ("reentry_policy", "in_out_policy", "access_policy"):
                    value = str(quote_meta.get(key) or "").strip()
                    if value:
                        policy_parts.append(value)
                deduped_policy: list[str] = []
                seen_policy: set[str] = set()
                for value in policy_parts:
                    lowered = value.lower()
                    if lowered in seen_policy:
                        continue
                    seen_policy.add(lowered)
                    deduped_policy.append(value)
                in_out_policy = " | ".join(deduped_policy)
                rows.append(
                    {
                        "lot_name": lot_name,
                        "normalized_lot_name": normalize_lot_name(lot_name),
                        "price": format((Decimal(total_cents) / Decimal(100)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP), "f")
                        if total_cents is not None
                        else None,
                        "currency": currency,
                        "availability": base_availability,
                        "listing_id": listing_id,
                        "available_spaces": available_spaces,
                        "reservation_type": "Parking Reservation",
                        "reservation_duration": _hours_between(reservation_starts, reservation_ends),
                        "reservation_starts": reservation_starts,
                        "reservation_ends": reservation_ends,
                        "in_out_policy": in_out_policy,
                        "rate_index": rate_index,
                        "rate_external_id": str(rate_external_id or ""),
                        "listing_details": {
                            "price_incl_fees": total_display,
                            "subtotal_display": subtotal_display,
                            "service_fee_display": fee_display,
                            "reservation_type": "Parking Reservation",
                            "reservation_duration": _hours_between(reservation_starts, reservation_ends),
                            "reservation_starts": reservation_starts,
                            "reservation_ends": reservation_ends,
                            "in_out_policy": in_out_policy,
                            "price_breakdown": [
                                {"label": label, "display": _format_cents(cents, currency)}
                                for label, cents in breakdown_lines
                            ],
                        },
                        "_source": "spothero_api",
                    }
                )
            return rows

        async def _fetch(params: dict) -> list[dict]:
            api_base = "https://api.spothero.com/v2/search/transient"
            visited_next: set[str] = set()
            seen_after: set[str] = set()
            page_num = 0
            after_followups = 0
            max_after_followups = 200 if max_coverage else 0
            max_page_iters = 2000 if max_coverage else 100
            all_raw: list[dict] = []
            next_url: str | None = None
            pending_after: str | None = None

            def _normalize_next(url_value: str) -> str:
                value = str(url_value or "").strip()
                if value.startswith("/"):
                    return urljoin("https://api.spothero.com", value)
                if not value.startswith("http"):
                    return urljoin("https://api.spothero.com/", value)
                return value

            while page_num < max_page_iters:
                if next_url:
                    normalized_next = _normalize_next(next_url)
                    next_url = None
                    if normalized_next in visited_next:
                        break
                    visited_next.add(normalized_next)
                    response = await self.page.request.get(normalized_next)
                elif pending_after is not None:
                    if pending_after in seen_after:
                        break
                    seen_after.add(pending_after)
                    with_after = dict(params)
                    with_after["after"] = pending_after
                    pending_after = None
                    response = await self.page.request.get(api_base, params=_with_tracking(with_after))
                    after_followups += 1
                else:
                    response = await self.page.request.get(api_base, params=params)

                if response.status != 200:
                    logger.warning(f"SpotHero API returned status {response.status} for {url}")
                    return []

                data = await response.json()
                results = data.get("results") or []
                if not isinstance(results, list):
                    return []
                all_raw.extend(result for result in results if isinstance(result, dict))
                next_value = data.get("@next")
                if isinstance(next_value, str) and next_value.strip():
                    next_url = _normalize_next(next_value)
                    page_num += 1
                    continue

                display = data.get("display") or {}
                total_hint = display.get("total_results") or display.get("results_count")
                try:
                    total_hint_int = int(float(total_hint)) if total_hint is not None else None
                except (TypeError, ValueError):
                    total_hint_int = None
                if (
                    max_coverage
                    and results
                    and after_followups < max_after_followups
                    and ((len(results) >= 200) or (total_hint_int is not None and len(all_raw) < total_hint_int))
                ):
                    last_id = _last_facility_id_from_transient_results(results)
                    if last_id and last_id not in seen_after:
                        pending_after = last_id
                        page_num += 1
                        continue
                break

            deduped: dict[tuple, dict] = {}
            for result in all_raw:
                for row in _raw_result_to_passes(result):
                    key = _pass_key(row)
                    previous = deduped.get(key)
                    if not previous or (not previous.get("price") and row.get("price")):
                        deduped[key] = row
            return list(deduped.values())

        passes_by_key: dict[tuple, dict] = {}
        candidate_params = primary_params if max_coverage else primary_params[:1]
        for params in candidate_params:
            for row in await _fetch(params):
                key = _pass_key(row)
                previous = passes_by_key.get(key)
                if not previous or (not previous.get("price") and row.get("price")):
                    passes_by_key[key] = row

        return list(passes_by_key.values())

    async def scrape_parking(self, event) -> int:
        passes = await self.scrape_parking_details(event)
        if not passes or not getattr(event, "_id", None):
            return len(passes)
        repo = get_parking_pass_repository()
        await repo.clear_for_event(event)
        await repo.add_passes(event, passes)
        return len(passes)


def build_spothero_event(
    *,
    venue_name: str,
    event_name: str,
    event_url: str,
    parking_url: str,
    max_parking_coverage: bool = False,
) -> SimpleNamespace:
    return SimpleNamespace(
        _id=None,
        name=event_name,
        event_url=event_url,
        parking_url=parking_url,
        venue_name=venue_name,
        event_title=event_name,
        max_parking_coverage=max_parking_coverage,
    )
