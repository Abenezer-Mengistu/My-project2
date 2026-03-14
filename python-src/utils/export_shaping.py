"""
Result shaping and export flattening utilities.

Adapted from TMScraper's create_event_result + AutomatiqCSVExporter patterns.
Provides:
  - Nested event→listings result building
  - Flat row export for CSV/BigQuery
  - Consistent field naming across export targets
"""
from __future__ import annotations

import csv
import json
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

from utils.normalization import normalize_lot_name
from utils.pricing import extract_total_price, currency_from_listing, compute_listing_metrics


# ── Nested result building ───────────────────────────────────────────────────

def create_event_result(
    venue_name: str,
    event_name: str,
    event_date: str,
    event_url: str,
    parking_url: str | None,
    listings: list[dict],
    event_id: int | None = None,
    external_id: str | None = None,
    metadata: dict | None = None,
) -> dict[str, Any]:
    """
    Build a rich, nested event result object with attached listings.

    This keeps the full structure during in-app processing. Use
    flatten_event_result() to convert to row-based format for export.

    Args:
        venue_name: Name of the venue.
        event_name: Name of the event.
        event_date: ISO date string.
        event_url: StubHub event URL.
        parking_url: StubHub parking URL (if any).
        listings: List of parking pass dicts from scraping.
        event_id: Internal DB event ID (if persisted).
        external_id: StubHub event ID.
        metadata: Additional metadata to attach.
    """
    enriched_listings = []
    for listing in listings:
        enriched = {
            **listing,
            "normalized_lot_name": normalize_lot_name(listing.get("lot_name", "")),
            "extracted_price": str(extract_total_price(listing)) if extract_total_price(listing) is not None else "",
            "currency_resolved": currency_from_listing(listing),
        }
        enriched_listings.append(enriched)

    total_listings = len(enriched_listings)
    prices = [extract_total_price(l) for l in listings]
    valid_prices = [p for p in prices if p is not None]

    return {
        "venue": venue_name,
        "event_name": event_name,
        "event_date": event_date,
        "event_url": event_url,
        "parking_url": parking_url,
        "event_id": event_id,
        "external_id": external_id,
        "summary": {
            "total_listings": total_listings,
            "min_price": str(min(valid_prices)) if valid_prices else None,
            "max_price": str(max(valid_prices)) if valid_prices else None,
            "avg_price": str(round(sum(valid_prices) / len(valid_prices), 2)) if valid_prices else None,
        },
        "listings": enriched_listings,
        "metadata": metadata or {},
    }


# ── Flat row export ──────────────────────────────────────────────────────────

_FLAT_FIELDNAMES = [
    "venue",
    "event_name",
    "event_date",
    "parking_url",
    "event_id",
    "external_id",
    "lot_name",
    "normalized_lot_name",
    "price",
    "extracted_price",
    "currency",
    "availability",
    "listing_id",
    "source",
    "listing_details",
]


def flatten_event_result(event_result: dict) -> list[dict]:
    """
    Flatten a nested event result into one row per listing.

    Each row contains the event-level metadata merged with the
    listing-level fields. This is the shape needed for CSV and
    BigQuery exports.
    """
    event_fields = {
        "venue": event_result.get("venue"),
        "event_name": event_result.get("event_name"),
        "event_date": event_result.get("event_date"),
        "parking_url": event_result.get("parking_url"),
        "event_id": event_result.get("event_id"),
        "external_id": event_result.get("external_id"),
    }

    rows = []
    for listing in event_result.get("listings", []):
        row = {**event_fields}
        row["lot_name"] = listing.get("lot_name")
        row["normalized_lot_name"] = listing.get("normalized_lot_name", "")
        row["price"] = listing.get("price")
        row["extracted_price"] = listing.get("extracted_price")
        row["currency"] = listing.get("currency")
        row["availability"] = listing.get("availability")
        row["listing_id"] = listing.get("listing_id")
        row["source"] = listing.get("_source", listing.get("source"))
        row["listing_details"] = listing.get("listing_details")
        rows.append(row)

    return rows


def flatten_multiple_events(event_results: list[dict]) -> list[dict]:
    """Flatten a list of nested event results into a single flat row list."""
    rows = []
    for event_result in event_results:
        rows.extend(flatten_event_result(event_result))
    return rows


# ── CSV export ───────────────────────────────────────────────────────────────

def export_flat_rows_to_csv(
    rows: list[dict],
    output_dir: str | Path,
    filename_prefix: str = "export",
    fieldnames: list[str] | None = None,
) -> str:
    """
    Write flat rows to a timestamped CSV file.

    Returns the full path to the written CSV.
    """
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    csv_path = out_dir / f"{filename_prefix}_{timestamp}.csv"

    fields = fieldnames or _FLAT_FIELDNAMES
    with csv_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)

    return str(csv_path)


# ── JSON export ──────────────────────────────────────────────────────────────

class _DecimalEncoder(json.JSONEncoder):
    """JSON encoder that handles Decimal values."""

    def default(self, o: object) -> object:
        if isinstance(o, Decimal):
            return float(o)
        return super().default(o)


def export_event_results_to_json(
    event_results: list[dict],
    output_dir: str | Path,
    filename_prefix: str = "export",
) -> str:
    """
    Write nested event results to a timestamped JSON file.

    Returns the full path to the written JSON.
    """
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    json_path = out_dir / f"{filename_prefix}_{timestamp}.json"

    with json_path.open("w", encoding="utf-8") as f:
        json.dump(event_results, f, ensure_ascii=False, indent=2, cls=_DecimalEncoder)

    return str(json_path)
