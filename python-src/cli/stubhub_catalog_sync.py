"""
Batch-scrape StubHub parking events into storage/stubhub_snapshots/ + stubhub_catalog_index.json.

Run from the ``python-src`` directory (same folder as ``main.py``). File paths are looked up
relative to the current working directory first, then relative to the project root (``BASE_DIR``).

Usage (bulk — no URL paste):
  python main.py phase1
  python main.py catalog-sync --phase1-latest --limit 5000

Usage (small URL list):
  python main.py catalog-sync --urls-file storage/parking_urls.txt --limit 50
  python main.py run --parking-urls "https://www.stubhub.com/parking-passes-only-.../event/123/"
  python main.py catalog-sync --phase1-json storage/exports/phase1_discovery_....json

When you start ``python main.py`` (API server), scheduler jobs call this in embedded mode.
If no URLs are configured yet, the embedded run skips quietly; use ``storage/parking_urls.txt``
or env vars listed below.
Override sources with ``STUBHUB_CATALOG_SYNC_URLS_FILE``, ``STUBHUB_CATALOG_SYNC_PARKING_URLS``,
or ``STUBHUB_CATALOG_SYNC_PHASE1_JSON``.
"""
from __future__ import annotations

import argparse
import os
import asyncio
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from database.connection import close_orm, initialize_orm
from scraper.playwright_cluster import PlaywrightClusterManager
from utils.logger import logger


def resolve_cli_input_path(user_path: str, base_dir: Path) -> Path:
    """
    Resolve a user-supplied file path: try cwd first, then project root (python-src).
    """
    p = Path(user_path).expanduser()
    candidates: list[Path] = []
    if p.is_absolute():
        candidates.append(p)
    else:
        candidates.append(Path.cwd() / p)
        candidates.append((base_dir / p).resolve())
    tried = []
    for c in candidates:
        r = c.resolve()
        tried.append(str(r))
        if r.is_file():
            return r
    raise FileNotFoundError(
        f"File not found: {user_path!r}\n"
        f"  Looked in (in order):\n    " + "\n    ".join(tried)
    )


def _urls_file_line_hint(urls_file_arg: str | None, base_dir: Path) -> str:
    """Short hint for logs: how many non-``#`` lines exist in the configured urls file."""
    if not urls_file_arg:
        return ""
    try:
        uf = resolve_cli_input_path(urls_file_arg, base_dir)
    except FileNotFoundError:
        return f" | urls_file lookup: not found ({urls_file_arg!r})"
    try:
        text = uf.read_text(encoding="utf-8")
    except OSError as exc:
        return f" | urls_file={uf} (read failed: {exc})"
    non_comment = sum(
        1 for line in text.splitlines() if (s := line.strip()) and not s.startswith("#")
    )
    return (
        f" | urls_file={uf} non_comment_lines={non_comment} "
        "(only lines without leading # count; each must be a stubhub.com parking-passes-only /event/ URL)"
    )


def parse_stubhub_urls_file(path: Path) -> list[str]:
    """One URL per line; strip; skip empty and # comments; dedupe; canonicalize via app."""
    from app import _canonical_stubhub_url

    if not path.is_file():
        raise FileNotFoundError(f"URLs file not found: {path}")
    out: list[str] = []
    seen: set[str] = set()
    for line in path.read_text(encoding="utf-8").splitlines():
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        c = _canonical_stubhub_url(s)
        if c and "parking-passes-only" in c.lower() and c not in seen:
            seen.add(c)
            out.append(c)
    return out


def urls_from_phase1_json(resolved_path: Path) -> list[str]:
    from app import _canonical_stubhub_url, _read_json_rows

    if not resolved_path.is_file():
        raise FileNotFoundError(f"Phase1 JSON not found: {resolved_path}")
    rows = _read_json_rows(resolved_path)
    out: list[str] = []
    seen: set[str] = set()
    for row in rows:
        u = row.get("parking_url") or row.get("event_url")
        c = _canonical_stubhub_url(u)
        if c and "parking-passes-only" in c.lower() and c not in seen:
            seen.add(c)
            out.append(c)
    return out


def dedupe_preserve_order(urls: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for u in urls:
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out


def catalog_sync_on_start_enabled() -> bool:
    """
    Run catalog sync after ``python main.py`` starts (default: yes).

    Set ``STUBHUB_CATALOG_SYNC_ON_START`` to ``0`` / ``false`` / ``no`` / ``off`` to skip.
    """
    env = os.environ.get("STUBHUB_CATALOG_SYNC_ON_START")
    if env is None:
        return True
    v = env.strip().lower()
    if v in ("0", "false", "no", "off", ""):
        return False
    return True


def namespace_for_autostart() -> argparse.Namespace:
    """CLI-equivalent args for the server-started background sync (env overrides)."""
    args = build_arg_parser().parse_args([])
    pu = os.environ.get("STUBHUB_CATALOG_SYNC_PARKING_URLS")
    args.parking_urls = pu.strip() if pu and pu.strip() else None
    pj = os.environ.get("STUBHUB_CATALOG_SYNC_PHASE1_JSON")
    args.phase1_json = pj.strip() if pj and pj.strip() else None
    if "STUBHUB_CATALOG_SYNC_URLS_FILE" in os.environ:
        uf = os.environ["STUBHUB_CATALOG_SYNC_URLS_FILE"].strip()
        args.urls_file = uf or None
    else:
        args.urls_file = "storage/parking_urls.txt"
    if lim := os.environ.get("STUBHUB_CATALOG_SYNC_LIMIT"):
        args.limit = max(1, int(lim))
    if d := os.environ.get("STUBHUB_CATALOG_SYNC_DELAY_SECONDS"):
        args.delay_seconds = float(d)
    args.phase1_latest = os.environ.get("STUBHUB_CATALOG_SYNC_PHASE1_LATEST", "").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )
    return args


def _elapsed_s(start: float) -> str:
    return f"{time.perf_counter() - start:.1f}s"


def _wall_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def _diff_summary(catalog: dict) -> dict | None:
    cd = catalog.get("catalog_diff") if isinstance(catalog, dict) else None
    if not isinstance(cd, dict):
        return None
    return {
        "added_count": cd.get("added_count"),
        "removed_count": cd.get("removed_count"),
        "price_or_avail_changed_count": cd.get("price_or_avail_changed_count"),
    }


async def run_catalog_sync(args: argparse.Namespace, *, embedded: bool = False) -> int:
    from app import (
        BASE_DIR,
        STORAGE_DIR,
        _parse_parking_urls,
        scrape_stubhub_catalog_snapshot_for_url,
    )

    urls: list[str] = []
    try:
        if args.parking_urls:
            urls.extend(_parse_parking_urls(args.parking_urls))
        if args.urls_file:
            uf = resolve_cli_input_path(args.urls_file, BASE_DIR)
            urls.extend(parse_stubhub_urls_file(uf))
        if args.phase1_json:
            pj = resolve_cli_input_path(args.phase1_json, BASE_DIR)
            urls.extend(urls_from_phase1_json(pj))
        if getattr(args, "phase1_latest", False):
            from app import _latest_phase1_json

            latest = _latest_phase1_json()
            if latest is not None and latest.is_file():
                urls.extend(urls_from_phase1_json(latest.resolve()))
            else:
                logger.warning(
                    "[catalog-sync] --phase1-latest: no storage/exports/phase1_discovery_*.json "
                    "(run: python main.py phase1)"
                )
    except FileNotFoundError as exc:
        if embedded:
            logger.info(
                f"[catalog-sync on start] {_wall_utc()} | skipped (missing file): {exc}\n"
                "  Add storage/parking_urls.txt or set STUBHUB_CATALOG_SYNC_PARKING_URLS / "
                "STUBHUB_CATALOG_SYNC_PHASE1_JSON."
            )
            return 0
        logger.error(str(exc))
        return 1

    urls = dedupe_preserve_order(urls)[: max(1, args.limit)]

    if not urls:
        if embedded:
            hint = _urls_file_line_hint(args.urls_file, BASE_DIR)
            logger.info(
                f"[catalog-sync on start] {_wall_utc()} | skipped: no parking URLs loaded from file/env.{hint} "
                "Set STUBHUB_CATALOG_SYNC_PARKING_URLS or STUBHUB_CATALOG_SYNC_PHASE1_JSON, or add a bare URL line "
                "(no #) to storage/parking_urls.txt."
            )
            return 0
        logger.error(
            "No parking URLs to scrape. Use --parking-urls, --urls-file (see storage/parking_urls.example.txt), "
            "or --phase1-json with a real exported JSON path (not a placeholder name)."
        )
        return 1

    try:
        from app import _refresh_stubhub_usd_rates

        await _refresh_stubhub_usd_rates()
    except Exception as exc:
        logger.warning(f"StubHub USD rate refresh skipped: {exc}")

    if not embedded:
        await initialize_orm()
    run_label = "[catalog-sync on start]" if embedded else "[catalog-sync]"
    t_run = time.perf_counter()
    logger.info(
        f"{run_label} START {_wall_utc()} | urls={len(urls)} | limit={args.limit} | "
        f"delay={args.delay_seconds}s between events"
    )
    entries: list[dict] = []
    ok = 0
    fail = 0
    try:
        for i, parking_url in enumerate(urls):
            logger.info(
                f"{run_label} progress {i + 1}/{len(urls)} | status=scraping | ok={ok} fail={fail} | "
                f"elapsed {_elapsed_s(t_run)} | {parking_url}"
            )
            try:
                result = await scrape_stubhub_catalog_snapshot_for_url(parking_url)
            except Exception as exc:
                logger.error(f"{run_label} progress {i + 1}/{len(urls)} | exception: {exc}")
                fail += 1
                entries.append(
                    {
                        "parking_url": parking_url,
                        "ok": False,
                        "error": str(exc),
                        "scraped_at": time.time(),
                    }
                )
                logger.info(
                    f"{run_label} progress {i + 1}/{len(urls)} | status=failed | ok={ok} fail={fail} | "
                    f"elapsed {_elapsed_s(t_run)}"
                )
            else:
                if result.get("ok"):
                    ok += 1
                    body = result.get("body") or {}
                    card = body.get("card") or {}
                    catalog = result.get("catalog") or {}
                    n_list = len(card.get("listings") or [])
                    adv = card.get("advertised_total")
                    entries.append(
                        {
                            "parking_url": result.get("parking_url"),
                            "ok": True,
                            "scraped_at": time.time(),
                            "listing_count": n_list,
                            "advertised_total": adv,
                            "scrape_incomplete": card.get("scrape_incomplete"),
                            "last_catalog_diff_summary": _diff_summary(catalog),
                        }
                    )
                    logger.info(
                        f"{run_label} progress {i + 1}/{len(urls)} | status=ok | listings={n_list} "
                        f"advertised_total={adv!r} | ok={ok} fail={fail} | elapsed {_elapsed_s(t_run)}"
                    )
                else:
                    fail += 1
                    err = str(result.get("error", "unknown"))[:200]
                    entries.append(
                        {
                            "parking_url": result.get("parking_url", parking_url),
                            "ok": False,
                            "error": result.get("error", "unknown"),
                            "scraped_at": time.time(),
                        }
                    )
                    logger.info(
                        f"{run_label} progress {i + 1}/{len(urls)} | status=failed | error={err!r} | "
                        f"ok={ok} fail={fail} | elapsed {_elapsed_s(t_run)}"
                    )

            if args.delay_seconds > 0 and i < len(urls) - 1:
                logger.info(
                    f"{run_label} progress {i + 1}/{len(urls)} | status=waiting {args.delay_seconds}s before next…"
                )
                await asyncio.sleep(args.delay_seconds)

        index_path = STORAGE_DIR / "stubhub_catalog_index.json"
        STORAGE_DIR.mkdir(parents=True, exist_ok=True)
        index_payload = {
            "run_finished_at": datetime.now(timezone.utc).isoformat(),
            "urls_attempted": len(urls),
            "urls_ok": ok,
            "urls_failed": fail,
            "entries": entries,
        }
        index_path.write_text(json.dumps(index_payload, indent=2, ensure_ascii=False), encoding="utf-8")
        logger.info(
            f"{run_label} COMPLETE {_wall_utc()} | total_elapsed {_elapsed_s(t_run)} | "
            f"attempted={len(urls)} ok={ok} failed={fail} | wrote {index_path}"
        )
        return 0 if fail == 0 else 2
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


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Batch StubHub parking catalog → storage/stubhub_snapshots/",
        epilog="Bulk workflow (no manual URL paste): "
        "(1) python main.py phase1  →  storage/exports/phase1_discovery_*.json  "
        "(2) python main.py catalog-sync --phase1-latest --limit 2000  "
        "Paths resolve from cwd then project root.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument(
        "--urls-file",
        help="Text file: one parking-passes-only URL per line (# comments ok). Tried in cwd, then project root.",
    )
    p.add_argument("--parking-urls", help="Comma-separated parking event URLs")
    p.add_argument("--phase1-json", help="Phase1 JSON path (rows with parking_url / event_url)")
    p.add_argument(
        "--phase1-latest",
        action="store_true",
        help="Also load URLs from newest storage/exports/phase1_discovery_*.json (from: python main.py phase1)",
    )
    p.add_argument("--limit", type=int, default=500, help="Max events to scrape (default 500)")
    p.add_argument(
        "--delay-seconds",
        type=float,
        default=3.0,
        help="Pause between events to reduce rate limits (default 3)",
    )
    return p


def main(argv: list[str] | None = None) -> None:
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    code = asyncio.run(run_catalog_sync(args))
    sys.exit(code)


if __name__ == "__main__":
    main()
