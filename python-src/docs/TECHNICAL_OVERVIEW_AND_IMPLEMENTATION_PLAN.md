# Event Parking Discovery & Scraping Pipeline  
Technical Overview & Implementation Plan

---

## 1. Project Objective

We are building a Discovery Pipeline for event ticketing platforms focused on:

- Discovering events per venue
- Generating parking pass page links per event
- Later scraping parking pass data (parking names + competitor prices)
- Enabling automated discovery of new events over time

This is a multi-stage production pipeline designed for **scalability, stability, and long-term automation** — not just a simple scraper.

---

## 2. Functional Scope

### Phase 1 — Event Discovery Pipeline

**Input**

- Excel sheet (or structured input) containing venues

**Responsibilities**

- Parse venue list
- Discover events for each venue
- Navigate to event-specific parking pass pages
- Generate structured event-level parking URLs
- Store structured output (Database + optional CSV export)

**Output Structure**

- Venue
- Event Name
- Event Date
- Event URL
- Parking Pass URL

### Phase 2 — Parking Pass Scraper

**Responsibilities**

- Visit each parking pass page
- Extract:
  - Parking lot names
  - Competitor pricing
  - Availability (if applicable)
- Handle:
  - Pagination
  - Dynamic content (JavaScript)
  - Rate limits
  - Anti-bot protections
- Store structured output in database

### Phase 3 — Automated Event Monitoring

**Responsibilities**

- Schedule periodic checks per venue
- Detect newly added events
- Avoid duplicates
- Maintain incremental updates
- Log changes and failures
- Implement health monitoring

---

## 3. Timeline (Full-Time Estimate)

| Phase | Estimated Duration |
|-------|--------------------|
| Discovery + Link Generation | 5–7 days |
| Parking Pass Scraper (with anti-bot handling) | 10–15 days |
| Automated Monitoring + Testing | 5–8 days |
| **Total Estimated Time** | **3–4 weeks** |

This includes: Error handling, Logging, Retry mechanisms, Stability testing, Anti-bot tuning, Edge case handling.

---

## 4. Key Technical Challenges

- Dynamic JavaScript-heavy pages
- Rapidly changing inventory & pricing
- Anti-bot systems (Akamai, rate limiting, fingerprinting)
- IP blocking risks
- Session & cookie management
- Long-term maintainability

This must be treated as a **production system**, not a one-off script.

---

## 5. Proposed Tech Stack

**Core Language**

- Python 3.11+

**Scraping Layer**

- **Primary:** Selenium (optimize existing) **OR** Playwright (recommended upgrade for improved stability & stealth)
- **Hybrid:** Requests + BeautifulSoup for non-JS endpoints; network interception where possible to reduce browser overhead

**Anti-Bot Handling**

- Rotating Residential Proxies (external provider)
- Proxy pool management
- Randomized delays
- Sticky sessions where required
- Cookie/session persistence
- Exponential backoff strategy
- User-Agent rotation
- TLS/header consistency

**Data Processing**

- Pandas (Excel ingestion & transformation)
- OpenPyXL (Excel export)
- Pydantic (data validation models)

**Database**

- **Recommended:** PostgreSQL, SQLAlchemy ORM  
- **Alternative:** MongoDB (if flexible schema required)

**Task Scheduling**

- **Recommended:** Celery + Redis (scalable async workers)
- **Alternative:** APScheduler, Cron-based scheduling
- **Future:** Apache Airflow (if pipeline grows significantly)

**Logging & Monitoring**

- Python logging (structured logging)
- Rotating log files
- Error classification
- Success rate tracking
- Proxy health monitoring
- Optional: Sentry (error monitoring)

**Deployment**

- Dockerized service
- Linux server (VPS or cloud VM)
- CI/CD via GitHub Actions
- Environment-based configuration (.env)

---

## 6. High-Level Architecture

```
Venue Input (Excel)
        ↓
Discovery Engine
        ↓
Event Link Generator
        ↓
Database Storage
        ↓
Parking Scraper
        ↓
Structured Output
        ↓
Monitoring Scheduler
```

System should be **modular**:

- `discovery/`
- `scraper/`
- `anti_bot/`
- `database/`
- `scheduler/`
- `monitoring/`

---

## 7. Optimization of Existing Selenium System

Since there is already a Selenium scraper in place, we should:

- Profile execution time
- Replace fixed sleeps with explicit waits
- Introduce structured retries
- Improve proxy rotation logic
- Add monitoring & recovery mechanisms
- Reduce browser instantiation overhead
- Migrate portions to hybrid model where possible

---

## 8. Quality Requirements

The system must ensure:

- Stable long-running execution
- Conservative anti-bot behavior
- Minimal IP burn rate
- Clean modular architecture
- Clear logging & observability
- Scalability for future expansion

We are building **infrastructure**, not scripts.

---

## 9. Final Notes

This is a ticketing-platform system operating in a **high-sensitivity environment**.

**Key success factors:**

- Stability over speed
- Conservative anti-bot strategy
- Clean architecture
- Monitoring-first mindset
- Modular extensibility

The goal is **long-term operational reliability**.

---

## 10. Implementation Status (This Codebase)

This repository implements the above plan for **StubHub** as the first ticketing platform.

| Plan section | Status | Where |
|--------------|--------|--------|
| **Phase 1 – Event Discovery** | ✅ Implemented | `discovery/` (venue_parser, stubhub_discovery); `GET /ticketing/discovery/run`; Phase 1 CSV + DB |
| **Phase 2 – Parking Scraper** | ✅ Implemented | `scraper/stubhub_parking.py`, Playwright cluster; `GET /ticketing/parking/extract`; Phase 2 CSV + DB |
| **Phase 3 – Monitoring** | ✅ Implemented | Phase 3 diff + scheduler; `GET /ticketing/monitoring/run`, `.../scheduler` |
| **Tech stack** | ✅ Aligned | Python 3.11+, Playwright, httpx, Pandas/OpenPyXL, PostgreSQL (SQLAlchemy), config-driven scheduling, Loguru |
| **Modular layout** | ✅ | `discovery/`, `scraper/`, `anti_bot/`, `database/`, `monitoring/`; scheduler in app |
| **Quality** | ✅ | Retries, timeouts, dedupe, stealth, proxy support, structured logging |
| **Docker / CI/CD** | ⏳ Optional | Not yet in repo |

**Quick reference**

- **How to run:** See [PIPELINE_FLOW.md](../PIPELINE_FLOW.md) for endpoints and execution order.
- **Plan vs code mapping:** See [CROSS_CHECK_TECHNICAL_PLAN_AND_REUSABLE_COMPONENTS.md](CROSS_CHECK_TECHNICAL_PLAN_AND_REUSABLE_COMPONENTS.md) for requirement-by-requirement alignment and reusable components (pricing, normalization, export shaping).
