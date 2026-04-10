"""
Phase 1 CLI — discover parking-passes-only event URLs from venues (Excel or DB).

Writes ``storage/exports/phase1_discovery_<timestamp>.json``. Feed that file (or
``python main.py catalog-sync --phase1-latest``) to catalog sync — no manual URL list.

Run from the ``python-src`` directory (same folder as ``main.py``).
"""
from __future__ import annotations

import argparse
import asyncio
import json
import sys

from database.connection import close_orm, initialize_orm
from scraper.playwright_cluster import PlaywrightClusterManager
from utils.logger import logger


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="StubHub Phase 1 — venue list → parking event URLs (exports JSON under storage/exports/).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument(
        "--excel-path",
        default=None,
        help="venues.xlsx path (default: ticketing config / venues.xlsx)",
    )
    p.add_argument(
        "--source",
        choices=["file", "db"],
        default=None,
        help="Where to load venues (default: TICKETING_SOURCE / config)",
    )
    p.add_argument("--max-venues", type=int, default=None, help="Cap venues processed")
    p.add_argument("--persist", action="store_true", help="Persist discovered events to the database")
    p.add_argument(
        "--no-export-json",
        action="store_true",
        help="Do not write phase1_discovery_*.json (normally you want export on)",
    )
    p.add_argument(
        "--discover-venues",
        action="store_true",
        help="Append newly seen venue URLs from events into the venues Excel output",
    )
    p.add_argument("--no-strict-venue-guard", action="store_true")
    p.add_argument("--no-strict-event-location-match", action="store_true")
    return p


async def run_phase1_discovery(args: argparse.Namespace, *, embedded: bool = False) -> int:
    from app import ticketing_phase1
    from starlette.responses import JSONResponse

    if not embedded:
        await initialize_orm()
    try:
        result = await ticketing_phase1(
            excel_path=args.excel_path,
            source=args.source,
            dry_run=False,
            export_json=not args.no_export_json,
            persist=args.persist,
            max_venues=args.max_venues,
            strict_venue_guard=not args.no_strict_venue_guard,
            strict_event_location_match=not args.no_strict_event_location_match,
            discover_venues=args.discover_venues,
        )
    finally:
        if not embedded:
            try:
                await PlaywrightClusterManager.close_all()
            except Exception as exc:
                logger.warning(f"PlaywrightClusterManager.close_all: {exc}")
            try:
                await close_orm()
            except Exception as exc:
                logger.warning(f"close_orm: {exc}")

    if isinstance(result, JSONResponse):
        raw = result.body
        if isinstance(raw, memoryview):
            raw = raw.tobytes()
        try:
            err = json.loads(raw.decode())
        except Exception:
            err = {"detail": (raw.decode(errors="replace") if isinstance(raw, bytes) else str(raw))[:500]}
        logger.error(f"Phase1 failed: {err}")
        return 1

    logger.info(
        f"Phase1 complete: events={result.get('processed_items')} "
        f"venues_processed={result.get('venues_processed')} failed_venues={result.get('failed_venues')} "
        f"json_output={result.get('json_output')}"
    )
    return 0


def main(argv: list[str] | None = None) -> None:
    args = build_parser().parse_args(argv)
    code = asyncio.run(run_phase1_discovery(args))
    sys.exit(code)


if __name__ == "__main__":
    main()
