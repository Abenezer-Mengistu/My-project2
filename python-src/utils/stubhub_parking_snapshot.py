"""
Durable JSON snapshots per StubHub parking event URL for fast client-search event-details
and incremental listing diffs (add/remove/price change).

Files live under ``storage/stubhub_snapshots/`` (one SHA-256-named JSON per canonical URL).

**Batch refresh (“download all” in your universe):** the API triggers a background Playwright
run when a snapshot is stale (see ``app._stubhub_snapshot_background_refresh``). For a cron job,
iterate your known ``parking_url`` values and either POST ``/ticketing/ui/client-search/event-details``
for each or call ``ticketing_phase2`` with rate limits appropriate to StubHub.
"""
from __future__ import annotations

import hashlib
import json
import re
import tempfile
import time
from pathlib import Path
from typing import Any

from loguru import logger

# Default: 30 minutes "fresh"; beyond that serve stale-while-revalidate if snapshot exists.
DEFAULT_SNAPSHOT_MAX_AGE_S = 1800


def snapshot_dir(base_storage: Path) -> Path:
    d = base_storage / "stubhub_snapshots"
    d.mkdir(parents=True, exist_ok=True)
    return d


def snapshot_file_path(base_storage: Path, parking_url: str) -> Path:
    key = hashlib.sha256(parking_url.encode("utf-8")).hexdigest()
    return snapshot_dir(base_storage) / f"{key}.json"


def load_snapshot(base_storage: Path, parking_url: str) -> dict[str, Any] | None:
    path = snapshot_file_path(base_storage, parking_url)
    if not path.is_file():
        return None
    try:
        raw = path.read_text(encoding="utf-8")
        data = json.loads(raw)
    except Exception as exc:
        logger.warning(f"StubHub snapshot corrupt or unreadable {path}: {exc}")
        return None
    if not isinstance(data, dict):
        return None
    return data


def save_snapshot_atomic(base_storage: Path, parking_url: str, envelope: dict[str, Any]) -> None:
    """Write snapshot envelope: scraped_at (unix float), response (API payload dict)."""
    path = snapshot_file_path(base_storage, parking_url)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = None
    try:
        fd, tmp = tempfile.mkstemp(suffix=".json", dir=str(path.parent))
        with open(fd, "w", encoding="utf-8") as f:
            json.dump(envelope, f, ensure_ascii=False, indent=2)
        Path(tmp).replace(path)
    except Exception as exc:
        logger.error(f"Failed to write StubHub snapshot {path}: {exc}")
        if tmp:
            try:
                Path(tmp).unlink(missing_ok=True)
            except OSError:
                pass


def _listing_key(row: dict[str, Any]) -> str | None:
    lid = row.get("listing_id")
    if lid is not None and str(lid).strip():
        return f"id:{str(lid).strip()}"
    lot = str(row.get("lot_name") or "").strip().lower()
    price = str(row.get("price_display") or row.get("price_value") or "").strip()
    avail = str(row.get("availability") or "").strip().lower()
    if lot or price:
        return f"fb:{lot}|{price}|{avail}"
    return None


def _norm_price_for_diff(row: dict[str, Any]) -> str:
    pv = row.get("price_value")
    if pv is not None and str(pv).strip():
        return str(pv).strip()
    pd = str(row.get("price_display") or "").strip()
    m = re.search(r"([0-9][0-9,]*(?:\.[0-9]{1,2})?)", pd)
    return m.group(1).replace(",", "") if m else pd


def diff_card_listings(old_listings: list[Any], new_listings: list[Any]) -> dict[str, Any]:
    """Compare grouped client-search listing dicts by listing_id with fallback key."""
    old_rows = [x for x in (old_listings or []) if isinstance(x, dict)]
    new_rows = [x for x in (new_listings or []) if isinstance(x, dict)]

    def by_key(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
        out: dict[str, dict[str, Any]] = {}
        for r in rows:
            k = _listing_key(r)
            if k:
                out[k] = r
        return out

    o, n = by_key(old_rows), by_key(new_rows)
    old_keys, new_keys = set(o), set(n)

    added = [n[k] for k in sorted(new_keys - old_keys)]
    removed = [o[k] for k in sorted(old_keys - new_keys)]
    price_changed: list[dict[str, Any]] = []
    for k in sorted(old_keys & new_keys):
        po, pn = _norm_price_for_diff(o[k]), _norm_price_for_diff(n[k])
        ao = str(o[k].get("availability") or "").strip()
        an = str(n[k].get("availability") or "").strip()
        if po != pn or ao != an:
            price_changed.append(
                {
                    "key": k,
                    "lot_name": n[k].get("lot_name"),
                    "listing_id": n[k].get("listing_id"),
                    "old_price": po,
                    "new_price": pn,
                    "old_availability": ao,
                    "new_availability": an,
                }
            )

    return {
        "added_count": len(added),
        "removed_count": len(removed),
        "price_or_avail_changed_count": len(price_changed),
        "added_listing_ids": [x.get("listing_id") for x in added if x.get("listing_id")],
        "removed_listing_ids": [x.get("listing_id") for x in removed if x.get("listing_id")],
        "price_changed": price_changed[:50],
    }
