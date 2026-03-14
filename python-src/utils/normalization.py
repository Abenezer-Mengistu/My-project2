"""
Section and lot name normalization utilities.

Adapted from TMScraper's normalize_section_name pattern.
Turns messy external labels (from StubHub DOM, embedded JSON, XHR) into
stable internal keys for:
  - Comparing snapshots over time
  - Grouping parking passes by lot/zone
  - De-duplicating records from different scraping strategies
"""
from __future__ import annotations

import re


# ── Noise prefixes to strip ──────────────────────────────────────────────────
_STRIP_PREFIXES = re.compile(
    r"^(?:section|sec|lot|parking\s*lot|garage|zone|area)\s+",
    re.IGNORECASE,
)

# ── Directional / positional prefixes and suffixes to collapse ────────────────
_DIRECTIONAL_PREFIXES = re.compile(
    r"^(?:left|right|east|west|north|south|upper|lower)\s+",
    re.IGNORECASE,
)
_DIRECTIONAL_SUFFIXES = re.compile(
    r"\s*\b(?:left|right|east|west|north|south|upper|lower)\s*$",
    re.IGNORECASE,
)

# ── Parking-specific canonical patterns ──────────────────────────────────────
_PARKING_LOT_PATTERN = re.compile(
    r"(?:parking\s*)?\blot\s*([A-Z0-9]+)",
    re.IGNORECASE,
)
_GARAGE_PATTERN = re.compile(
    r"(?:parking\s*)?garage\s*([A-Z0-9]*)",
    re.IGNORECASE,
)
_PARK_AND_RIDE_PATTERN = re.compile(
    r"park\s*(?:and|&|n)\s*ride",
    re.IGNORECASE,
)

# ── Hyphenated section collapse (e.g. "101-A" → "101A") ─────────────────────
_HYPHENATED = re.compile(r"(\d+)\s*[-–]\s*([A-Za-z])\b")


def normalize_section_name(section: str) -> str:
    """
    Normalize a StubHub section/lot name into a canonical internal key.

    Examples:
        "Section 101"       → "101"
        "Lot A"             → "LOT_A"
        "Parking Lot B"     → "LOT_B"
        "Garage 3"          → "GARAGE_3"
        "Upper 216"         → "216"
        "101-A"             → "101A"
        "Park and Ride"     → "PARK_AND_RIDE"
        "  Section 42 Left" → "42"
    """
    s = (section or "").strip()
    if not s:
        return ""

    # Collapse whitespace
    s = " ".join(s.split())

    # Check parking-specific patterns first (before stripping prefixes)
    park_ride = _PARK_AND_RIDE_PATTERN.search(s)
    if park_ride:
        return "PARK_AND_RIDE"

    lot_match = _PARKING_LOT_PATTERN.search(s)
    if lot_match:
        return f"LOT_{lot_match.group(1).upper()}"

    garage_match = _GARAGE_PATTERN.search(s)
    if garage_match:
        suffix = garage_match.group(1).strip().upper()
        return f"GARAGE_{suffix}" if suffix else "GARAGE"

    # Strip noise prefixes
    s = _STRIP_PREFIXES.sub("", s).strip()

    # Strip directional prefixes and suffixes
    s = _DIRECTIONAL_PREFIXES.sub("", s).strip()
    s = _DIRECTIONAL_SUFFIXES.sub("", s).strip()

    # Collapse hyphenated patterns ("101-A" → "101A")
    s = _HYPHENATED.sub(r"\1\2", s)

    # Final cleanup
    s = s.strip()
    return s if s else (section or "").strip()


def normalize_lot_name(lot_name: str) -> str:
    """
    Normalize a parking lot name specifically.

    This is a convenience wrapper around normalize_section_name that also
    handles common parking-specific variations:
        "Parking Lot A"       → "LOT_A"
        "Lot A"               → "LOT_A"
        "General Parking"     → "GENERAL_PARKING"
        "VIP Parking"         → "VIP_PARKING"
        "Covered Parking"     → "COVERED_PARKING"
        "Section 5"           → "5"
    """
    s = (lot_name or "").strip()
    if not s:
        return ""

    # Attempt structured normalization first
    normalized = normalize_section_name(s)

    # If normalize_section_name didn't produce a canonical parking key,
    # check for common parking qualifiers and produce a stable key.
    s_lower = s.lower().strip()
    for qualifier in ("general", "vip", "premier", "preferred", "covered", "uncovered", "surface", "reserved"):
        if qualifier in s_lower and "parking" in s_lower:
            return f"{qualifier.upper()}_PARKING"

    return normalized


def canonical_lot_key(lot_name: str, price: str | None, currency: str | None) -> str:
    """
    Build a canonical deduplication key for a parking pass record.

    Combines the normalized lot name with price and currency so that records
    from different scraping strategies (DOM, embedded JSON, XHR) can be
    de-duplicated reliably.
    """
    norm = normalize_lot_name(lot_name)
    p = (price or "0").strip()
    c = (currency or "USD").strip().upper()
    return f"{norm}|{p}|{c}"
