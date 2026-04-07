"""
Price handling utilities for StubHub parking passes.

Provides:
  - Total price extraction from scraped listing dicts
  - Forced USD conversion with fixed exchange rates
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
    """
    details = listing.get("listing_details") if isinstance(listing.get("listing_details"), dict) else {}

    # Prefer the live user-facing/sale price before generic list/base price fields.
    candidate_values = [
        listing.get("current_price"),
        details.get("current_price"),
        details.get("price_incl_fees"),
        details.get("discounted_price"),
        listing.get("total"),
        listing.get("priceWithFees"),
        listing.get("currentPrice"),
        listing.get("formattedPrice"),
        listing.get("price"),
        listing.get("rawPrice"),
    ]

    for val in candidate_values:
        price = extract_numeric_price(val)
        if price is not None:
            return price
    return None


CURRENCY_MAP = {
    "$": "USD",
    "€": "EUR",
    "£": "GBP",
    "R": "ZAR",
    "C$": "CAD",
    "A$": "AUD",
}


def currency_from_listing(listing: dict) -> str:
    """
    Determine the currency code from a listing dict.
    """
    raw_currency = listing.get("currency")
    if raw_currency and len(raw_currency) == 3:
        return raw_currency.upper()
    details = listing.get("listing_details") if isinstance(listing.get("listing_details"), dict) else {}
    price_str = details.get("price_incl_fees") or listing.get("price_incl_fees") or listing.get("price") or ""
    if isinstance(price_str, str):
        for symbol, code in CURRENCY_MAP.items():
            if symbol in price_str:
                return code
    return "USD"


# ── Currency Conversion (Forced USD) ─────────────────────────────────────────

USD_EXCHANGE_RATES = {
    "ZAR": Decimal("0.054"),
    "EUR": Decimal("1.09"),
    "GBP": Decimal("1.27"),
    "CAD": Decimal("0.74"),
    "AUD": Decimal("0.66"),
    "USD": Decimal("1.00"),
}


def set_usd_exchange_rates(rates: dict[str, Decimal | str | float]) -> None:
    """
    Replace the in-memory USD conversion table with fresher values.
    Values are direct multipliers to USD, e.g. 1 ZAR * rate => USD amount.
    """
    normalized: dict[str, Decimal] = {}
    for code, value in (rates or {}).items():
        try:
            normalized[str(code).upper()] = Decimal(str(value))
        except Exception:
            continue
    if "USD" not in normalized:
        normalized["USD"] = Decimal("1.00")
    USD_EXCHANGE_RATES.clear()
    USD_EXCHANGE_RATES.update(normalized)


def convert_to_usd(amount: Decimal | None, from_currency: str) -> Decimal | None:
    """
    Convert a Decimal amount to USD based on the source currency.
    """
    if amount is None:
        return None
    rate = USD_EXCHANGE_RATES.get(from_currency.upper(), Decimal("1.00"))
    return (amount * rate).quantize(Decimal("0.01"))


# ── Price delta / comparison ─────────────────────────────────────────────────

def calculate_price_delta(
    current_price: Decimal | None,
    previous_price: Decimal | None,
) -> dict[str, Any]:
    """
    Compute the delta between two prices.
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
    """
    if price_a is None or price_b is None:
        return None
    if price_b == Decimal("0"):
        return None
    return round(float(price_a / price_b), 4)


# ── Threshold alerts ─────────────────────────────────────────────────────────

class PriceAlert:
    """Represents a price alert triggered by threshold logic."""
    def __init__(self, alert_type: str, lot_name: str, current_price: Decimal | None, threshold: Decimal | None = None, previous_price: Decimal | None = None, message: str = ""):
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
    """
    alerts: list[PriceAlert] = []
    lot_name = listing.get("lot_name", "Unknown")
    current = extract_total_price(listing)
    if current is None:
        return alerts
    if floor_price is not None and current < floor_price:
        alerts.append(PriceAlert("below_floor", lot_name, current, threshold=floor_price, message=f"{lot_name}: ${current} is below floor ${floor_price}"))
    if ceiling_price is not None and current > ceiling_price:
        alerts.append(PriceAlert("above_ceiling", lot_name, current, threshold=ceiling_price, message=f"{lot_name}: ${current} exceeds ceiling ${ceiling_price}"))
    if previous_price is not None and previous_price > Decimal("0"):
        delta = calculate_price_delta(current, previous_price)
        pct = delta.get("percentage_change")
        if pct is not None and max_drop_pct is not None and pct < -abs(max_drop_pct):
            alerts.append(PriceAlert("price_drop", lot_name, current, previous_price=previous_price, message=f"{lot_name}: price dropped {abs(pct):.1f}% (${previous_price} -> ${current})"))
        if pct is not None and max_spike_pct is not None and pct > abs(max_spike_pct):
            alerts.append(PriceAlert("price_spike", lot_name, current, previous_price=previous_price, message=f"{lot_name}: price spiked {pct:.1f}% (${previous_price} -> ${current})"))
    return alerts


# ── Per-listing metrics ──────────────────────────────────────────────────────

def compute_listing_metrics(
    listing: dict,
    previous_price: Decimal | None = None,
    baseline_price: Decimal | None = None,
) -> dict[str, Any]:
    """
    Compute derived metrics for a single parking pass listing.
    FORCED USD: Converts extracted price to USD before returning.
    """
    current_raw = extract_total_price(listing)
    raw_currency = currency_from_listing(listing)
    current_usd = convert_to_usd(current_raw, raw_currency)

    metrics: dict[str, Any] = {
        "extracted_price": str(current_usd) if current_usd is not None else None,
        "currency_resolved": "USD",
        "original_price": str(current_raw) if current_raw is not None else None,
        "original_currency": raw_currency,
    }
    if previous_price is not None and current_usd is not None:
        delta = calculate_price_delta(current_usd, previous_price)
        metrics["price_delta"] = str(delta["absolute_delta"]) if delta["absolute_delta"] is not None else None
        metrics["price_change_pct"] = delta["percentage_change"]
        metrics["price_direction"] = delta["direction"]
    else:
        metrics["price_delta"] = None
        metrics["price_change_pct"] = None
        metrics["price_direction"] = None
    if baseline_price is not None and current_usd is not None:
        metrics["price_ratio_vs_baseline"] = calculate_price_ratio(current_usd, baseline_price)
    else:
        metrics["price_ratio_vs_baseline"] = None
    return metrics
