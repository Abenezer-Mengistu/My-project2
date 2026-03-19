# Parking Links Tool (`/ticketing/parking-links`)

This endpoint is a **links-only** tool for StubHub:

- It **generates event URLs + parking event URLs** (the `parking-passes-only-.../event/...` links).
- It **does not** scrape parking inventory/spot listings (no Phase 2 extraction).

Server (default): `http://127.0.0.1:5556`

---

## Direct mode (recommended)

Use this when you already have one or more StubHub venue/performer URLs.

### Input

- `stubhub_urls` (required): **comma-separated** StubHub venue or performer URLs
- `venue_name` (optional): label used in output rows (useful when passing a single URL)
- `handler` (optional): default is `stubhub-discovery`
- `strict_event_location_match` (optional): `true|false` (often `false` is more permissive)
- `export_json` (optional): `true|false`

### Example (single URL)

```bash
curl -sS 'http://127.0.0.1:5556/ticketing/parking-links?stubhub_urls=https://www.stubhub.com/ariana-grande-tickets/performer/511927?gridFilterType=1&venue_name=Ariana%20Grande&strict_event_location_match=false&export_json=true' \
  --max-time 1800 \
  -o parking_links.json
```

### Example (multiple URLs)

```bash
curl -sS 'http://127.0.0.1:5556/ticketing/parking-links?stubhub_urls=https://www.stubhub.com/ariana-grande-tickets/performer/511927?gridFilterType=1,https://www.stubhub.com/bruno-mars-tickets/performer/492103?gridFilterType=1&strict_event_location_match=false&export_json=true' \
  --max-time 1800 \
  -o parking_links.json
```

---

## Auto mode (seed → venues → parking links)

Use this when you want the tool to **discover venues automatically** from a `seed_url`, then generate parking links for the discovered venues.

### Input

- `auto_find_venues=true` (required)
- `seed_url` (required): a StubHub page to use as a starting point (e.g. `https://www.stubhub.com/explore`)
- `max_venues` (optional): limit venues to process (default 50)
- `max_pages` (optional): limits feed pagination for venue discovery
  - If omitted/0, the tool uses a **small laptop-friendly default** internally.
- `full` (optional): `true|false`
  - `full=true` enables browser-assisted venue discovery (slower; sometimes more reliable).
- `venue_discovery_timeout_seconds` (optional): hard server-side cutoff for venue discovery (default 90)
- `fallback_to_excel` (optional): `true|false` (default **true**)
- `excel_path` (optional): Excel file to use for fallback (default `venues.xlsx`)
- `export_json` (optional): `true|false`

### Example (auto mode with Excel fallback)

This works well on local laptops when StubHub seed pages are bot/JS-heavy:

```bash
curl -sS 'http://127.0.0.1:5556/ticketing/parking-links?auto_find_venues=true&seed_url=https://www.stubhub.com/explore&max_venues=10&fallback_to_excel=true&excel_path=venues.xlsx&strict_event_location_match=false&export_json=true' \
  --max-time 1800 \
  -o parking_links.json
```

### Example (try harder before fallback)

```bash
curl -sS 'http://127.0.0.1:5556/ticketing/parking-links?auto_find_venues=true&seed_url=https://www.stubhub.com/explore&max_venues=10&max_pages=10&full=true&venue_discovery_timeout_seconds=90&fallback_to_excel=true&excel_path=venues.xlsx&strict_event_location_match=false&export_json=true' \
  --max-time 1800 \
  -o parking_links.json
```

### How to tell if StubHub blocked venue discovery

Check:

- `venue_discovery.attempts`: list of requests made to discover venues (url/status/content-type/venues_extracted)
- If you see challenge-like results (e.g. HTML responses, status 202/403/429, `venues_extracted=0`), auto venue discovery likely failed and the tool will use Excel fallback (if enabled).

---

## Output format

The response includes:

- `venues_resolved`: how many venues were used (from seed discovery and/or Excel fallback)
- `events_generated`: number of rows generated
- `data`: list of rows, each containing:
  - `event_url`
  - `parking_url`
  - `event_name`
  - `event_date`
  - `venue`

---

## Export location

When `export_json=true`, the tool writes the full response JSON under:

- `python-src/storage/search_results/parking_links_<timestamp>.json`

