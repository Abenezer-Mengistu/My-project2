"""
Price handling utilities for StubHub parking passes.

Adapted from TMScraper's extract_stubhub_total_price / calculate_price_ratio
patterns. Provides:
  - Total price extraction from scraped listing dicts
  - Price delta computation between snapshots
  - Threshold-based alert flagging
  - Per-listing derived metrics
"""
from __future__ import annotations

import re
from decimal import Decimal, InvalidOperation
from typing import Any


# ── Price extraction ─────────────────────────────────────────────────────────

def extract_numeric_price(value: Any) -> Decimal | None:
    """
    Extract a numeric Decimal price from various input formats.

    Handles:
      - Raw numbers (int, float)
      - String prices with currency symbols ("$45.00", "€120", "£30.50")
      - Comma-separated thousands ("1,200.00")
      - None / empty / malformed → None
    """
    if value is None:
        return None

    if isinstance(value, Decimal):
        return value

    if isinstance(value, (int, float)):
        return Decimal(str(value))

    s = str(value).strip()
    if not s:
        return None

    # Strip currency symbols and whitespace
    m = re.search(r"([0-9][0-9,]*(?:\.[0-9]{1,2})?)", s)
    if not m:
        return None

    try:
        return Decimal(m.group(1).replace(",", ""))
    except InvalidOperation:
        return None


def extract_total_price(listing: dict) -> Decimal | None:
    """
    Extract the total price from a parking pass listing dict.

    Checks fields in priority order:
      1. "total" (pre-computed total if available)
      2. "price" (the primary price field)
      3. "formattedPrice" / "rawPrice" (StubHub embedded JSON variants)

    Returns None if no valid price is found.
    """
    for key in ("total", "price", "formattedPrice", "rawPrice"):
        val = listing.get(key)
        price = extract_numeric_price(val)
        if price is not None:
            return price
    return None


def currency_from_listing(listing: dict) -> str:
    """
    Determine the currency code from a listing dict.

    Falls back to USD if not specified.
    """
    return "USD"


# ── Price delta / comparison ─────────────────────────────────────────────────

def calculate_price_delta(
    current_price: Decimal | None,
    previous_price: Decimal | None,
) -> dict[str, Any]:
    """
    Compute the delta between two prices.

    Returns a dict with:
      - absolute_delta: current - previous (Decimal or None)
      - percentage_change: percentage change (float or None)
      - direction: "up", "down", "unchanged", or None (if either price missing)
    """
    if current_price is None or previous_price is None:
        return {
            "absolute_delta": None,
            "percentage_change": None,
            "direction": None,
        }

    delta = current_price - previous_price

    if previous_price == Decimal("0"):
        pct = None
    else:
        pct = round(float(delta / previous_price) * 100, 2)

    if delta > 0:
        direction = "up"
    elif delta < 0:
        direction = "down"
    else:
        direction = "unchanged"

    return {
        "absolute_delta": delta,
        "percentage_change": pct,
        "direction": direction,
    }


def calculate_price_ratio(
    price_a: Decimal | None,
    price_b: Decimal | None,
) -> float | None:
    """
    Calculate the ratio between two prices (price_a / price_b).

    Useful for comparing StubHub prices against a baseline (face value,
    competitor, or previous snapshot).

    Returns None if either price is missing or price_b is zero.
    """
    if price_a is None or price_b is None:
        return None
    if price_b == Decimal("0"):
        return None
    return round(float(price_a / price_b), 4)


# ── Threshold alerts ─────────────────────────────────────────────────────────

class PriceAlert:
    """Represents a price alert triggered by threshold logic."""

    def __init__(
        self,
        alert_type: str,
        lot_name: str,
        current_price: Decimal | None,
        threshold: Decimal | None = None,
        previous_price: Decimal | None = None,
        message: str = "",
    ):
        self.alert_type = alert_type
        self.lot_name = lot_name
        self.current_price = current_price
        self.threshold = threshold
        self.previous_price = previous_price
        self.message = message

    def to_dict(self) -> dict:
        return {
            "alert_type": self.alert_type,
            "lot_name": self.lot_name,
            "current_price": str(self.current_price) if self.current_price is not None else None,
            "threshold": str(self.threshold) if self.threshold is not None else None,
            "previous_price": str(self.previous_price) if self.previous_price is not None else None,
            "message": self.message,
        }


def check_price_thresholds(
    listing: dict,
    floor_price: Decimal | None = None,
    ceiling_price: Decimal | None = None,
    previous_price: Decimal | None = None,
    max_drop_pct: float | None = None,
    max_spike_pct: float | None = None,
) -> list[PriceAlert]:
    """
    Check a parking pass listing against configurable price thresholds.

    Returns a list of PriceAlert objects for any triggered conditions:
      - "below_floor": price dropped below floor_price
      - "above_ceiling": price exceeded ceiling_price
      - "price_drop": price dropped more than max_drop_pct from previous
      - "price_spike": price rose more than max_spike_pct from previous

    Args:
        listing: Pass dict with at least "lot_name" and "price" keys.
        floor_price: Minimum acceptable price threshold.
        ceiling_price: Maximum acceptable price threshold.
        previous_price: Price from a previous snapshot for delta comparison.
        max_drop_pct: Maximum allowed percentage drop (e.g. 20.0 for 20%).
        max_spike_pct: Maximum allowed percentage spike (e.g. 50.0 for 50%).
    """
    alerts: list[PriceAlert] = []
    lot_name = listing.get("lot_name", "Unknown")
    current = extract_total_price(listing)

    if current is None:
        return alerts

    if floor_price is not None and current < floor_price:
        alerts.append(PriceAlert(
            alert_type="below_floor",
            lot_name=lot_name,
            current_price=current,
            threshold=floor_price,
            message=f"{lot_name}: ${current} is below floor ${floor_price}",
        ))

    if ceiling_price is not None and current > ceiling_price:
        alerts.append(PriceAlert(
            alert_type="above_ceiling",
            lot_name=lot_name,
            current_price=current,
            threshold=ceiling_price,
            message=f"{lot_name}: ${current} exceeds ceiling ${ceiling_price}",
        ))

    if previous_price is not None and previous_price > Decimal("0"):
        delta = calculate_price_delta(current, previous_price)
        pct = delta.get("percentage_change")

        if pct is not None and max_drop_pct is not None and pct < -abs(max_drop_pct):
            alerts.append(PriceAlert(
                alert_type="price_drop",
                lot_name=lot_name,
                current_price=current,
                previous_price=previous_price,
                message=f"{lot_name}: price dropped {abs(pct):.1f}% (${previous_price} -> ${current})",
            ))

        if pct is not None and max_spike_pct is not None and pct > abs(max_spike_pct):
            alerts.append(PriceAlert(
                alert_type="price_spike",
                lot_name=lot_name,
                current_price=current,
                previous_price=previous_price,
                message=f"{lot_name}: price spiked {pct:.1f}% (${previous_price} -> ${current})",
            ))

    return alerts


# ── Per-listing metrics ──────────────────────────────────────────────────────

def compute_listing_metrics(
    listing: dict,
    previous_price: Decimal | None = None,
    baseline_price: Decimal | None = None,
) -> dict[str, Any]:
    """
    Compute derived metrics for a single parking pass listing.

    This is the per-listing enrichment step adapted from TMScraper's
    process_event_listings pattern. Returns a dict of metrics that can
    be merged into the listing for export.

    Args:
        listing: Pass dict with at least "price" key.
        previous_price: Price from a previous snapshot (for delta).
        baseline_price: Reference price for ratio computation (e.g. face value).
    """
    current = extract_total_price(listing)
    currency = currency_from_listing(listing)

    metrics: dict[str, Any] = {
        "extracted_price": str(current) if current is not None else None,
        "currency_resolved": currency,
    }

    if previous_price is not None and current is not None:
        delta = calculate_price_delta(current, previous_price)
        metrics["price_delta"] = str(delta["absolute_delta"]) if delta["absolute_delta"] is not None else None
        metrics["price_change_pct"] = delta["percentage_change"]
        metrics["price_direction"] = delta["direction"]
    else:
        metrics["price_delta"] = None
        metrics["price_change_pct"] = None
        metrics["price_direction"] = None

    if baseline_price is not None and current is not None:
        metrics["price_ratio_vs_baseline"] = calculate_price_ratio(current, baseline_price)
    else:
        metrics["price_ratio_vs_baseline"] = None

    return metrics
