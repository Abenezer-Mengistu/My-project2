# Event Parking Discovery Pipeline (StubHub)

This project runs a 3-phase live scraping pipeline:

1. **Phase 1** – Event discovery (venues → events + parking URLs)
2. **Phase 2** – Parking extraction (lot names, prices, availability per event)
3. **Phase 3** – Snapshot monitoring (new/removed events diff)

---

## How to run it

### 1. Setup (once)

```bash
cd python-src
python3 -m venv venv
./venv/bin/pip install -r requirements.txt
```

### 2. Start the API

```bash
cd python-src
./venv/bin/python main.py
```

Server runs at **http://127.0.0.1:5556** (default port 5556).

### 3. Run the full pipeline (all 3 phases)

Use **file** mode (reads `python-src/venues.xlsx`) — no database required:

```bash
curl -s 'http://127.0.0.1:5556/ticketing/pipeline/run?source=file&export_csv=true' --max-time 1800 -o pipeline_result.json
```

- **Timeout:** Use `--max-time 1800` (30 minutes); the pipeline can take 15–30+ minutes depending on venue count.
- **Result:** `pipeline_result.json` contains Phase 1/2/3 summaries. Exports are written to disk (see below).

### 4. Where outputs are written

| Output | Location |
|--------|----------|
| Phase 1 CSV (events) | `python-src/storage/exports/phase1_discovery_<timestamp>.csv` |
| Phase 2 CSV (parking rows) | `python-src/storage/exports/phase2_parking_<timestamp>.csv` |
| Phase 3 report (new/removed events) | `python-src/storage/monitoring/phase3_monitor_<timestamp>.json` |

```bash
ls -lt python-src/storage/exports/
ls -lt python-src/storage/monitoring/
```

### 5. Parking-only (no venue/event discovery)

Extract parking details from StubHub parking event URLs directly — no venue or event listing:

```bash
curl -s 'http://127.0.0.1:5556/ticketing/parking-only?parking_urls=https://www.stubhub.com/parking-passes-only-harry-styles-new-york-tickets-8-26-2026/event/160334493/' --max-time 600 -o parking_result.json
```

- **parking_urls:** Comma-separated StubHub parking event URLs (e.g. `parking-passes-only-.../event/123/`)
- **Output:** Same Phase 2 CSV (`phase2_parking_<timestamp>.csv`)

### 6. Optional: run phases separately

- **Phase 1 only:** `GET /ticketing/phase1?source=file&export_csv=true`
- **Phase 2 only:** `GET /ticketing/phase2` (uses latest Phase 1 CSV) or `GET /ticketing/phase2?parking_urls=<urls>` for parking-only
- **Phase 3 only:** `GET /ticketing/phase3` (compares latest Phase 1 CSV to previous)

### 7. Scrape thousands of venues into venues.xlsx

To pull as many venues/events as possible from StubHub into `python-src/venues.xlsx` (no duplicates, merged with existing):

```bash
curl -s 'http://127.0.0.1:5556/ticketing/venues/scrape-and-sync-excel?max_pages=80&full=0' --max-time 3600 -o venue_scrape.json
```

- **max_pages=80** (or 100): request up to that many pages per feed for maximum coverage (thousands of venues).
- **full=0**: HTTP only (faster). Use **full=1** to also run the browser for each URL (more venues, much longer).
- **Timeout:** Use `--max-time 3600` (60 minutes) for exhaustive runs; the request can take 30–60 minutes.
- **Result:** All scraped venues are merged into `python-src/venues.xlsx` (by URL; existing rows kept). Check `venue_scrape.json` for `merged_count` and `rows_written`.

---

## Prerequisites

- Linux/macOS shell
- Python 3.10+ (recommended 3.11+)
- Network access for live scraping
- For **file** mode: place `venues.xlsx` in `python-src/` (columns: name, stubhub_url, handler, location)

## Quick health check

```bash
curl -s http://127.0.0.1:5556/healthz
```

## Recommended client demo flow

### 1) Discover/import live venues from StubHub explore

```bash
curl -s 'http://127.0.0.1:5556/ticketing/venues/extract-from-web?start_url=https://www.stubhub.com/explore?lat=LTI2LjE4Mw%3D%3D&lon=MjguMzE3&import_to_db=true&sync_excel=true&excel_path=venues.xlsx'
```

### 2) Optional: export DB venues to Excel

```bash
curl -s 'http://127.0.0.1:5556/ticketing/venues/export-to-excel?excel_path=venues.xlsx'
```

### 3) Run full pipeline using DB venues (live, non-dry-run)

```bash
curl -s 'http://127.0.0.1:5556/ticketing/pipeline/run?source=db&max_venues=1000&strict_venue_guard=true&strict_event_location_match=false&alert_on_failures=true'
```

### 4) Show generated outputs

```bash
ls -lt python-src/storage/exports | head
ls -lt python-src/storage/monitoring | head
```

## Main endpoints

- `GET /ticketing/parking-only` – parking extraction only (no venue/event discovery)
- `GET /ticketing/pipeline/run` – full pipeline (Phase 1 → 2 → 3) or parking-only with `parking_urls`
- `GET /ticketing/phase1` – discovery only
- `GET /ticketing/phase2` – parking extraction (phase1_csv, parking_urls, or single event_url)
- `GET /ticketing/phase3` – monitoring diff only
- `GET /ticketing/discovery/run`
- `GET /ticketing/parking/extract`
- `GET /ticketing/monitoring/run`
- `GET /ticketing/venues`
- `GET /ticketing/venues/extract-from-har`
- `GET /ticketing/venues/extract-from-web`
- `GET /ticketing/venues/scrape-and-sync-excel` – scrape venues from StubHub into `venues.xlsx`
- `GET /ticketing/venues/export-to-excel`
- `POST /ticketing/venues/import`
- `PUT /ticketing/venues/{venue_id}`
- `DELETE /ticketing/venues/{venue_id}`

## Notes

- This project is configured for real-time live scraping (no dry-run in production workflow).
- StubHub can return anti-bot/challenge pages; reruns may produce different counts.
- If a new endpoint returns `Method Not Allowed`, restart `main.py` to load latest code.
