# Cross-Check: Technical Plan & STUBHUB_REUSABLE_COMPONENTS

This document verifies that the StubHub Event Parking Discovery Pipeline satisfies the **Event Parking Discovery Pipeline Technical Plan** and aligns with **STUBHUB_REUSABLE_COMPONENTS.md** (including normalization and export patterns).

---

## 1. Technical Plan – Requirements vs Implementation

### 1.1 Project Objective

| Requirement | Status | Implementation |
|-------------|--------|----------------|
| Discovering events per venue | ✅ | `discovery/stubhub_discovery.py` – `StubHubDiscoveryScraper.discover_events()`; Phase 1 reads venues from Excel/DB and discovers events per venue |
| Generating parking pass page links per event | ✅ | Phase 1 output includes `parking_url` per event; `stubhub_discovery` builds `?quantity=0&parking=true` URLs |
| Scraping parking pass data (names + prices) | ✅ | `scraper/stubhub_parking.py` – `StubHubParkingScraper.scrape_parking_details()`; Phase 2 extracts lot names, prices, availability |
| Automated discovery of new events over time | ✅ | Phase 3 monitoring compares snapshots, detects new/removed events; scheduler `GET /ticketing/phase3/scheduler?action=start` |
| Production pipeline (scalability, stability, long-term automation) | ✅ | 3-phase orchestration, config-driven, CSV/JSON exports, health checks, logging |

### 1.2 Phase 1 – Event Discovery

| Requirement | Status | Implementation |
|-------------|--------|----------------|
| Input: Excel (or structured input) | ✅ | `VenueParser.from_excel()`; also DB via `source=db` and `get_venue_repository().list_all()` |
| Parse venue list | ✅ | `discovery/venue_parser.py` |
| Discover events per venue | ✅ | `_run_discovery_for_venue()` → `StubHubDiscoveryScraper.discover_events()` |
| Navigate to event-specific parking pages / generate parking URLs | ✅ | Event URLs + `?quantity=0&parking=true`; discovery visits venue/event pages |
| Output: Venue, Event Name, Event Date, Event URL, Parking Pass URL | ✅ | Phase 1 CSV and API response: `venue`, `event_name`, `event_date`, `event_url`, `parking_url` |
| Store: Database + optional CSV export | ✅ | `export_csv=True` → `phase1_discovery_<timestamp>.csv`; optional persist to DB via event repo |

### 1.3 Phase 2 – Parking Pass Scraper

| Requirement | Status | Implementation |
|-------------|--------|----------------|
| Visit each parking pass page | ✅ | `stubhub_parking.py` – navigates to `parking_url`, handles DOM + XHR |
| Extract: parking lot names, competitor pricing, availability | ✅ | Lot/section, price, currency, availability; `normalized_lot_name`, `extracted_price`, `currency_resolved` |
| Handle: pagination, dynamic content (JS), rate limits, anti-bot | ✅ | Playwright for JS; `anti_bot/stealth.py` (delays, challenge detection); proxy support in config; retries in Phase 2 |
| Store structured output in database | ✅ | Optional `persist_phase2`; `ParkingPass`, `PriceSnapshot` models and repos; CSV export always available |

### 1.4 Phase 3 – Automated Event Monitoring

| Requirement | Status | Implementation |
|-------------|--------|----------------|
| Schedule periodic checks per venue | ✅ | `GET /ticketing/phase3/scheduler?action=start&interval_minutes=30` |
| Detect newly added events | ✅ | Phase 3 compares current vs previous Phase 1 CSV; `new_events` / `removed_events` |
| Avoid duplicates | ✅ | `_dedupe_phase1_events()` and `_dedupe_phase2_rows()`; snapshot comparison by event identity |
| Log changes and failures | ✅ | Logger throughout; Phase 3 JSON report; optional alert webhook |
| Health monitoring | ✅ | `GET /healthz`; Phase 3 report includes `health.status` |

### 1.5 Tech Stack (Plan vs Actual)

| Plan | Actual |
|------|--------|
| Python 3.11+ | ✅ Python 3.11+ |
| Playwright (recommended) or Selenium | ✅ Playwright (headless Chromium) – `scraper/playwright_cluster.py`, `playwright_base.py` |
| Requests + BeautifulSoup for non-JS | ✅ httpx + BeautifulSoup where used (e.g. venue extract-from-web) |
| Rotating proxies, delays, backoff, User-Agent | ✅ Config: proxy rotation (PacketStream, SocialProxy, Webshare, IPRoyal); `anti_bot` delays; retries |
| Pandas / OpenPyXL | ✅ VenueParser (pandas), Excel export (pandas/openpyxl) |
| PostgreSQL + SQLAlchemy ORM | ✅ async SQLAlchemy 2.0 + asyncpg; models: Venue, Event, ParkingPass, PriceSnapshot, TicketData |
| Task scheduling (APScheduler / Celery) | ✅ Phase 3 scheduler (APScheduler-style interval) |
| Logging, .env config | ✅ Loguru; CONFIG from env (`.env`) |
| Docker / CI/CD / GitHub Actions | ⏳ Pending (checklist 6.4 / 6.5) |

### 1.6 Architecture (Plan vs Actual)

| Plan modules | Actual |
|--------------|--------|
| discovery/ | ✅ `discovery/` – stubhub_discovery, venue_parser, ticket_data_transform, stubhub_snapshot_service |
| scraper/ | ✅ `scraper/` – stubhub_parking, playwright_cluster, ticketing_controller, base/ |
| anti_bot/ | ✅ `anti_bot/` – stealth (delays, challenge detection, wait_for_selector_safe) |
| database/ | ✅ `database/` – connection, models (ticketing), repositories |
| scheduler/ | ✅ Integrated in app (phase3 scheduler); monitoring/task_queue_service |
| monitoring/ | ✅ `monitoring/` – task_queue, bigquery; Phase 3 JSON reports |

### 1.7 Quality Requirements

| Requirement | Status |
|-------------|--------|
| Stable long-running execution | ✅ Async pipeline, 30-min timeouts, retries |
| Conservative anti-bot behavior | ✅ Delays, stealth, proxy rotation, challenge detection |
| Clean modular architecture | ✅ discovery / scraper / anti_bot / database / monitoring / utils |
| Clear logging & observability | ✅ Logger in app and modules; Phase 3 reports |
| Scalability for future expansion | ✅ Config-driven, pipeline orchestration, export formats |

---

## 2. STUBHUB_REUSABLE_COMPONENTS – Alignment

### 2.1 Price Handling (Section 2 of STUBHUB_REUSABLE)

| Reusable idea | Our implementation |
|---------------|--------------------|
| `extract_stubhub_total_price(listing)` | ✅ `utils/pricing.py`: `extract_total_price(listing)`, `extract_numeric_price(value)` – same idea, adapted to scraped dicts |
| `calculate_price_ratio(stubhub, other)` | ✅ `calculate_price_ratio(current, baseline_price)`; also `calculate_price_delta()` for absolute and % change |
| Per-listing derived metrics (ratios, deltas, flags) | ✅ `compute_listing_metrics(listing)` – fills `price_delta`, `price_change_pct`, `price_direction`, `price_ratio_vs_baseline` |
| Replace “other platform” with own baseline | ✅ Baseline is configurable (previous snapshot / internal benchmark); Phase 2 CSV includes price_delta, price_direction |

### 2.2 Section / Lot Normalization (Section 3 of STUBHUB_REUSABLE)

| Reusable idea | Our implementation |
|---------------|--------------------|
| `normalize_section_name(section)` – strip prefixes, hyphenated, directionals | ✅ `utils/normalization.py`: `normalize_section_name(section)` – strips "section", "lot", "garage", etc.; collapses "101-A" → "101A"; directionals stripped |
| Parking-specific patterns: Lot [A-Z0-9], Garage, Parking | ✅ Same module: `_PARKING_LOT_PATTERN` → `LOT_A`; `_GARAGE_PATTERN` → `GARAGE_3`; `_PARK_AND_RIDE_PATTERN` → `PARK_AND_RIDE`; plus `normalize_lot_name()` for VIP_PARKING, GENERAL_PARKING, etc. |
| Canonical key for comparing snapshots, grouping, de-duplicating | ✅ `canonical_lot_key(lot_name, price, currency)` for dedupe; `normalized_lot_name` and `normalized_section_key` in Phase 2 output |
| Use for DB schema / canonical section | ✅ StubHubParkingScraper and Phase 2 use `normalize_lot_name()` on all extraction paths; `_classify_parking_row()` uses it for classification |

### 2.3 Event/Listing Result Shaping (Section 4 of STUBHUB_REUSABLE)

| Reusable idea | Our implementation |
|---------------|--------------------|
| `create_event_result(...)` – nested event + listings | ✅ `utils/export_shaping.py`: `create_event_result(venue_name, event_name, event_date, event_url, parking_url, listings, ...)` – builds nested event with enriched listings |
| Flatten event+listings into one row per listing (CSV/BigQuery) | ✅ `flatten_event_result(event_result)`, `flatten_multiple_events()`, `export_flat_rows_to_csv()` – same pattern as Automatiq exporter |
| Event metadata + listing metadata + derived metrics in export | ✅ Phase 2 CSV: venue, event_name, event_date, event_url, parking_url, lot_name, normalized_lot_name, price, currency, price_delta, price_direction, listing_id, etc. |
| DB models (ParkingPass, PriceSnapshot) | ✅ `database/models/ticketing/parking_pass.py`, `price_snapshot.py` – field naming aligned with listing shape |

### 2.4 Suggested Reuse Checklist (Section 7 of STUBHUB_REUSABLE)

| Item | Status |
|------|--------|
| Copy/reimplement `extract_stubhub_total_price` logic (adapted to scraped structure) | ✅ `extract_total_price` + `extract_numeric_price` in `utils/pricing.py` |
| Section/lot normalization from `normalize_section_name` | ✅ `normalize_section_name` + `normalize_lot_name` + parking patterns in `utils/normalization.py` |
| Event→listings→row shaping from `create_event_result` + AutomatiqCSVExporter | ✅ `create_event_result`, `flatten_event_result`, `export_flat_rows_to_csv` in `utils/export_shaping.py`; Phase 2 uses flattened rows for CSV |
| Align monitoring/export schema: event metadata, listing metadata, derived metrics | ✅ Phase 2 CSV and Phase 3 JSON include event + listing + metrics (price_delta, price_direction, normalized_lot_name) |

---

## 3. Normalization – Direct Comparison with STUBHUB_REUSABLE

STUBHUB_REUSABLE (Section 3) says:

- **Strip noise prefixes** (e.g. "Section") → We do: `_STRIP_PREFIXES` for "section", "sec", "lot", "parking lot", "garage", "zone", "area".
- **Regularize hyphenated patterns** (e.g. "101-A" → "101A") → We do: `_HYPHENATED` replaces with `\1\2`.
- **Collapse directional suffixes** (Left/Right, etc.) → We do: `_DIRECTIONAL_PREFIXES` and `_DIRECTIONAL_SUFFIXES` strip "left", "right", "east", "west", "north", "south", "upper", "lower".
- **Parking-specific: Lot [A-Z0-9], Garage, Parking** → We do: `LOT_*`, `GARAGE_*`, `PARK_AND_RIDE`, plus qualifiers (VIP_PARKING, GENERAL_PARKING, etc.) in `normalize_lot_name`.
- **Use for canonical section key, grouping, de-duplicating** → We do: `normalized_lot_name` and `normalized_section_key` in Phase 2; `canonical_lot_key()` for dedupe; `_dedupe_phase2_rows()` uses event_url + lot/listing/price.

So the **normalization** approach in this project matches and extends the STUBHUB_REUSABLE section normalization and parking patterns.

---

## 4. Summary

| Document | Overall |
|----------|---------|
| **Event Parking Discovery Pipeline Technical Plan** | ✅ Satisfied: all three phases (discovery, parking scrape, monitoring), tech stack (Python, Playwright, PostgreSQL, Pandas, scheduling, logging), modular layout, and quality goals are met. Optional items (Docker, CI/CD) remain as future work. |
| **STUBHUB_REUSABLE_COMPONENTS.md** | ✅ Aligned: price handling (extract total, ratio, delta, per-listing metrics), section/lot normalization (including parking patterns), and event/listing result shaping (create_event_result, flatten, export) are implemented and used in the pipeline. Normalization is explicitly aligned with the “strip prefixes, hyphenated, directionals, parking patterns” guidance. |

No mandatory gaps remain for the Technical Plan or the STUBHUB_REUSABLE checklist; the pipeline satisfies both the requirement page and the reusable components (including normalization) as specified.
