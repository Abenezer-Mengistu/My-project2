# StubHub Event-Parking Pipeline Flow

Technical plan and scope: [docs/TECHNICAL_OVERVIEW_AND_IMPLEMENTATION_PLAN.md](docs/TECHNICAL_OVERVIEW_AND_IMPLEMENTATION_PLAN.md).

## Overview
This service implements a 3-stage scraping pipeline for StubHub:

1. Discovery (`Phase 1`): discover events from venue inputs.
2. Parking Extraction (`Phase 2`): extract listing/price/availability details for event parking pages.
3. Monitoring (`Phase 3`): compare snapshots and detect incremental changes.

## Canonical Endpoints

- `GET /ticketing/discovery/run`
  - Runs Phase 1.
  - Input: `excel_path`, `source=file|db`, `dry_run=false`, `export_csv`, `persist`, `strict_venue_guard=true`, `strict_event_location_match=true`.
  - Output: discovered events + `phase1_discovery_*.csv`.

- `GET /ticketing/parking/extract`
  - Runs Phase 2.
  - Input:
    - either `phase1_csv=...`
    - or `event_url` / `parking_url` for ad-hoc extraction.
  - Output: extracted rows + `phase2_parking_*.csv`.
  - Optional alerting: `alert_on_failures=true` (posts to webhook if failures occur).

- `GET /ticketing/monitoring/run`
  - Runs Phase 3.
  - Input: `run_phase1`, `excel_path`, `phase1_csv`, `export_report`.
  - Output: `new_events`, `removed_events`, monitoring health + `phase3_monitor_*.json`.

- `GET /ticketing/monitoring/scheduler`
  - Controls recurring monitoring runs.
  - Actions:
    - `action=start&interval_minutes=30`
    - `action=status`
    - `action=stop`

- `GET /ticketing/pipeline/run`
  - End-to-end orchestration in one call.
  - Executes Phase 1 -> Phase 2 -> Phase 3 based on flags.
  - Supports: `strict_venue_guard`, `strict_event_location_match`, `alert_on_failures`.

## Legacy Endpoint Compatibility

Legacy routes are still supported:

- `/ticketing/phase1`
- `/ticketing/phase2`
- `/ticketing/phase3`
- `/ticketing/phase3/scheduler`
- `/ticketing/stubhub/complete`

## Response Contract (Common)

Most pipeline endpoints return:

- `success`: boolean
- `data_source`: `real_time_live_scrape`
- stage-specific counts and data arrays
- output artifact paths (`csv_output`, `report_output`) when generated

## Alerting

Set environment variable:

- `ALERT_WEBHOOK_URL`

When configured:

- Phase 2 sends an alert event on extraction failures.
- Pipeline run sends an alert event when Phase 3 detects new events.

## Output Artifacts

- `storage/exports/phase1_discovery_<timestamp>.csv`
- `storage/exports/phase2_parking_<timestamp>.csv`
- `storage/monitoring/phase3_monitor_<timestamp>.json`

## Recommended Execution Order

1. Run discovery:
   - `GET /ticketing/discovery/run?excel_path=venues.xlsx`
2. Run parking extraction from latest discovery:
   - `GET /ticketing/parking/extract`
3. Run monitoring diff:
   - `GET /ticketing/monitoring/run?run_phase1=false`
4. Enable scheduler for automation:
   - `GET /ticketing/monitoring/scheduler?action=start&interval_minutes=30`
