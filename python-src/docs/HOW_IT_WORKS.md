# Event Parking Discovery Pipeline — How It Works

This document describes how the StubHub Event Parking Discovery system operates end-to-end.

---

## Overview

The system is a **3-phase live scraping pipeline** that:

1. Discovers events from StubHub venue pages
2. Extracts parking lot data (names, prices, availability) for each event
3. Monitors changes by comparing snapshots (new/removed events)

It runs as a **FastAPI** service (default port 5556) and uses **Playwright** (browser automation) plus **HTTP** for scraping StubHub.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        FastAPI Application (app.py)                  │
├─────────────────────────────────────────────────────────────────────┤
│  Auth Middleware  │  /ticketing/* routes  │  /healthz, /             │
└─────────────────────────────────────────────────────────────────────┘
                                    │
        ┌───────────────────────────┼───────────────────────────┐
        ▼                           ▼                           ▼
┌───────────────┐         ┌─────────────────┐         ┌─────────────────┐
│  Discovery    │         │  Scraper Layer  │         │  Database       │
│  stubhub_     │         │  Ticketing      │         │  Venues         │
│  discovery    │         │  Controller     │         │  Events         │
│  venue_parser │         │  Playwright     │         │  Parking Passes │
└───────────────┘         │  Cluster        │         │  Price Snapshots│
                          └─────────────────┘         └─────────────────┘
```

---

## The 3 Phases

### Phase 1 — Event Discovery

**Purpose:** Turn venues into a list of events and parking URLs.

| Aspect      | Details                                                                 |
|-------------|-------------------------------------------------------------------------|
| **Input**   | `venues.xlsx` (file mode) or DB venues (db mode)                        |
| **Columns** | `name`, `stubhub_url`, `handler`, `location`                            |
| **Process** | For each venue, Playwright loads the StubHub venue page; scraper extracts events and their parking links |
| **Output**  | CSV: `venue`, `event_name`, `event_date`, `event_url`, `parking_url`    |
| **Location**| `storage/exports/phase1_discovery_<timestamp>.csv`                      |

### Phase 2 — Parking Extraction

**Purpose:** Extract lot names, prices, and availability for each event’s parking page.

| Aspect      | Details                                                                 |
|-------------|-------------------------------------------------------------------------|
| **Input**   | Phase 1 CSV, comma-separated `parking_urls`, or single `event_url` / `parking_url` |
| **Process** | Visit each parking page; scrape listing rows; classify (parking vs ticket sections) |
| **Output**  | CSV with lot name, price, currency, availability, listing_id, etc.      |
| **Location**| `storage/exports/phase2_parking_<timestamp>.csv`                        |
| **Optional**| Persist to DB; webhook on failures (`ALERT_WEBHOOK_URL`)                |

### Phase 3 — Snapshot Monitoring

**Purpose:** Detect new and removed events between two discovery runs.

| Aspect      | Details                                                                 |
|-------------|-------------------------------------------------------------------------|
| **Input**   | Current Phase 1 CSV vs previous Phase 1 CSV                             |
| **Process** | Compare by `event_url`; compute new_events, removed_events              |
| **Output**  | JSON report with counts and event lists                                 |
| **Location**| `storage/monitoring/phase3_monitor_<timestamp>.json`                    |
| **Optional**| Scheduler for periodic runs; webhook when new events are detected       |

---

## Data Flow

```
venues.xlsx or DB
        │
        ▼
   ┌─────────┐     phase1_discovery_*.csv
   │ Phase 1 │ ──────────────────────────►
   └─────────┘              │
        │                   │
        │                   ▼
        │            ┌─────────┐     phase2_parking_*.csv
        │            │ Phase 2 │ ──────────────────────────►
        │            └─────────┘
        │                   │
        │                   │ (same phase1 CSV)
        │                   ▼
        │            ┌─────────┐     phase3_monitor_*.json
        └───────────►│ Phase 3 │ ──────────────────────────►
                     └─────────┘
                     (current vs previous phase1 CSVs)
```

---

## Main Components

### TicketingController
- Registers scrapers: `stubhub-discovery`, `stubhub-parking`
- Routes requests to the correct scraper by handler
- Lives in `scraper/ticketing_controller.py`

### PlaywrightClusterManager
- Manages shared browser instances
- Executes scraping tasks in a headless Playwright environment
- Handles shutdown and cleanup
- Lives in `scraper/playwright_cluster.py`

### VenueParser
- Reads `venues.xlsx` into structured venue rows
- Columns: `name`, `stubhub_url`, `handler`, `location`
- Lives in `discovery/venue_parser.py`

### StubHubDiscoveryScraper
- Loads venue pages and extracts events + parking URLs
- Uses Playwright for JavaScript-rendered content
- Lives in `discovery/stubhub_discovery.py`

### StubHubParkingScraper
- Loads parking pages and extracts lot names, prices, availability
- Classifies rows (parking inventory vs ticket sections)
- Lives in `scraper/stubhub_parking.py`

---

## Parking-Only Mode

For clients who only need parking data (no venue or event discovery):

- **Input:** Comma-separated StubHub parking event URLs (e.g. `parking-passes-only-.../event/123/`)
- **Endpoint:** `GET /ticketing/parking-only?parking_urls=<url1>,<url2>,...`
- **Pipeline:** `GET /ticketing/pipeline/run?run_phase1=false&run_phase2=true&run_phase3=false&parking_urls=<urls>`
- **Output:** Phase 2 CSV with lot names, prices, availability

## Key Endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /ticketing/parking-only` | Parking extraction only (no venue/event discovery) |
| `GET /ticketing/pipeline/run` | Full pipeline (Phase 1 → 2 → 3) or parking-only with `parking_urls` |
| `GET /ticketing/phase1` | Discovery only |
| `GET /ticketing/phase2` | Parking extraction only |
| `GET /ticketing/phase3` | Monitoring diff only |
| `GET /ticketing/venues` | List venues from DB |
| `GET /ticketing/venues/scrape-and-sync-excel` | Scrape StubHub into `venues.xlsx` |
| `GET /ticketing/phase3/scheduler` | Start/stop/status for Phase 3 scheduler |
| `GET /healthz` | Health check |

---

## Configuration

- **Config:** `config/` (loaded from env and config modules)
- **Auth:** Optional `auth_token` in config; localhost often skips auth in dev
- **Webhooks:** `ALERT_WEBHOOK_URL` for Phase 2 failures and Phase 3 new-event alerts

---

## Output Locations

| Output | Path |
|--------|------|
| Phase 1 CSV | `python-src/storage/exports/phase1_discovery_<timestamp>.csv` |
| Phase 2 CSV | `python-src/storage/exports/phase2_parking_<timestamp>.csv` |
| Phase 3 JSON | `python-src/storage/monitoring/phase3_monitor_<timestamp>.json` |

---

## Notes

- **Live scraping only** — no dry-run mode in production
- **Anti-bot:** StubHub may return challenge pages; reruns can differ
- **Timeouts:** Pipeline can take 15–30+ minutes for many venues; use `--max-time 1800` with `curl`
- **Venue sources:** `source=file` (Excel) or `source=db` (database)
