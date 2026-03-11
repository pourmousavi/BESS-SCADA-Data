# BESS SCADA Data Web App — Implementation Plan

## Architecture Overview

**Hosting:** HuggingFace Spaces (Docker SDK)
**Backend:** FastAPI (Python)
**Frontend:** HTML/CSS/JS served by FastAPI (lightweight, no React build step needed)
**Data Processing:** Polars (memory-efficient, faster than pandas for large CSVs)
**Charts:** Plotly.js (interactive, zoom/pan, rendered client-side)
**Analytics:** Lightweight SQLite logging (ephemeral but rebuilt; optionally push to free Supabase)
**BESS List:** Static JSON file in repo, manually updated monthly

## Data Flow

```
Student selects: State → BESS → Date
         ↓
Frontend sends request to FastAPI: GET /api/data?duid=XXXX&date=2026-03-01
         ↓
Backend constructs AEMO URL:
  - Current: https://www.nemweb.com.au/REPORTS/Current/FPPDAILY/
  - Archive: https://nemweb.com.au/Reports/Archive/FPPDAILY/
         ↓
Backend downloads ZIP from AEMO (~20-50 MB compressed)
         ↓
Backend extracts CSV in-memory (streaming, never full file to disk)
         ↓
Backend filters rows: FPP_UNITID == selected DUID
  Result: ~21,600 rows (~1-2 MB) for one BESS for one day
         ↓
Backend returns JSON (for chart/table display) or CSV/Parquet (for download)
         ↓
Frontend renders interactive chart + paginated table
```

## Project Structure

```
BESS-SCADA-Data/
├── Dockerfile                  # HuggingFace Spaces Docker config
├── requirements.txt            # Python dependencies
├── README.md                   # HuggingFace Spaces metadata
├── app/
│   ├── main.py                 # FastAPI application entry point
│   ├── config.py               # Constants, URLs, settings
│   ├── data/
│   │   ├── bess_list.json      # Static BESS list by state (updated monthly)
│   │   └── quality_flags.json  # MW_QUALITY_FLAG descriptions
│   ├── services/
│   │   ├── aemo_fetcher.py     # Download & extract ZIP from AEMO
│   │   ├── data_processor.py   # Filter, transform CSV data using Polars
│   │   └── analytics.py        # Log requests (IP, timestamp, BESS, date)
│   ├── routers/
│   │   ├── api.py              # REST API endpoints
│   │   └── pages.py            # Serve HTML pages
│   └── static/
│       ├── index.html          # Main single-page app
│       ├── css/
│       │   └── style.css       # Styling
│       └── js/
│           └── app.js          # Frontend logic (fetch, render chart/table)
```

## Implementation Steps

### Step 1: Project Setup & Docker Configuration
- Initialize project structure
- Create `Dockerfile` for HuggingFace Spaces (Python 3.11 slim, expose port 7860)
- Create `requirements.txt`: fastapi, uvicorn, polars, httpx, pyarrow
- Create HuggingFace Spaces `README.md` with metadata (sdk: docker)

### Step 2: Static BESS List
- Parse the AEMO NEM Generation Information Excel manually
- Create `bess_list.json` with structure:
  ```json
  {
    "NSW": [
      {"duid": "BESS1", "name": "Battery Name", "capacity_mw": 100, "region": "NSW1"}
    ],
    "VIC": [...],
    "QLD": [...],
    "SA": [...],
    "TAS": [...]
  }
  ```
- Create `quality_flags.json`:
  ```json
  {
    "0": {"label": "Good", "description": "Good quality data", "color": "green"},
    "1": {"label": "Bad", "description": "Sustained communication failure or manual override", "color": "red"},
    "-1": {"label": "N/A", "description": "Not applicable", "color": "gray"}
  }
  ```

### Step 3: AEMO Data Fetcher Service
- `aemo_fetcher.py`:
  - Given a date, construct the correct AEMO URL (Current vs Archive)
  - Download ZIP using `httpx` with 30-second timeout
  - Extract CSV from ZIP in-memory using `zipfile` module
  - Return raw CSV bytes/stream
  - Error handling: timeout → retry once → user-friendly error
  - Need to investigate the exact filename pattern in the ZIP to construct URLs correctly

### Step 4: Data Processor Service
- `data_processor.py`:
  - Read CSV bytes with Polars (lazy mode for memory efficiency)
  - Filter by `FPP_UNITID == selected_duid`
  - Select only relevant columns: INTERVAL_DATETIME, MEASUREMENT_DATETIME, FPP_UNITID, MEASURED_MW, MW_QUALITY_FLAG
  - Return as:
    - Polars DataFrame (for JSON serialization to frontend)
    - CSV bytes (for CSV download)
    - Parquet bytes (for Parquet download)
  - Compute summary statistics: min, max, mean MW, % good quality, % bad quality

### Step 5: Analytics Service
- `analytics.py`:
  - On each data request, log to SQLite (`/tmp/analytics.db`):
    - Timestamp
    - IP address (from request headers, respecting X-Forwarded-For)
    - Selected BESS (DUID)
    - Selected date
    - Download format (view/csv/parquet)
  - Provide endpoint to view analytics (protected by simple query param token)
  - Note: SQLite on ephemeral storage will be lost on restart — acceptable for basic tracking.
    Could optionally push to free Supabase (500 MB free) for persistence.

### Step 6: FastAPI Backend Endpoints
- `GET /` — Serve main HTML page
- `GET /api/bess` — Return BESS list grouped by state
- `GET /api/data?duid={duid}&date={YYYY-MM-DD}` — Return filtered SCADA data as JSON
  - Response includes: data rows, summary stats, quality flag breakdown
- `GET /api/download/csv?duid={duid}&date={YYYY-MM-DD}` — Download as CSV
- `GET /api/download/parquet?duid={duid}&date={YYYY-MM-DD}` — Download as Parquet
- `GET /api/analytics?token={secret}` — View usage analytics (admin only)

### Step 7: Frontend (Single Page App)
- **State selector:** Dropdown for Australian state (NSW, VIC, QLD, SA, TAS)
- **BESS selector:** Populated dynamically based on selected state
- **Date picker:** Single date (max 1 day), with validation (not future dates, not before data availability)
- **Load button:** Fetches data from backend
- **Loading state:** Spinner with message "Fetching data from AEMO... this may take 10-15 seconds"
- **Chart:** Plotly.js time-series chart of MEASURED_MW vs MEASUREMENT_DATETIME
  - Color-coded by MW_QUALITY_FLAG (green=good, red=bad, gray=N/A)
  - Interactive zoom, pan, hover tooltips
- **Data quality banner:**
  - "X% of measurements are good quality, Y% flagged as bad quality (communication failure or manual override)"
- **Data table:** Paginated table showing all rows (virtual scrolling for performance)
- **Download buttons:** CSV and Parquet download buttons
- **Summary stats panel:** Min, Max, Mean, Std Dev of MEASURED_MW

### Step 8: Error Handling & UX
- AEMO server unavailable → "AEMO data source is temporarily unavailable. Please try again in a few minutes."
- No data for selected BESS/date → "No SCADA data found for [BESS name] on [date]. This unit may not have been operational on this date."
- Large response handling → Stream response, show progress
- Rate limiting → Simple in-memory rate limit (10 requests/minute per IP) to prevent abuse

### Step 9: Deployment to HuggingFace Spaces
- Create HuggingFace Space with Docker SDK
- Push code to HuggingFace repo
- Configure Space settings
- Test with sample BESS queries

### Step 10: Documentation & BESS List Maintenance
- Document how to update `bess_list.json` when AEMO publishes new generation info
- Provide a helper script to parse the AEMO Excel and generate the JSON

## Key Technical Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Polars over Pandas | Polars | 2-10x faster, much lower memory usage, lazy evaluation |
| FastAPI over Flask | FastAPI | Async support (critical for downloading from AEMO), auto-docs, modern |
| Plotly.js over server-side charts | Plotly.js | Client-side rendering reduces server load, interactive |
| Single HTML page over React | Vanilla JS | No build step, simpler deployment, fewer dependencies |
| Parquet for download | PyArrow | Parquet is 5-10x smaller than CSV, native to Python data science workflows |
| httpx over requests | httpx | Async support, connection pooling, timeout handling |

## Risks & Mitigations

| Risk | Mitigation |
|------|------------|
| AEMO changes URL structure | Configurable URLs in `config.py`, easy to update |
| HF Space sleeps after inactivity | Acceptable for academic use; wakes in ~30 seconds |
| 16 GB RAM exceeded | On-demand fetch + Polars lazy mode keeps memory under 500 MB per request |
| AEMO rate-limits our requests | Cache recently fetched data in `/tmp` (ephemeral but helps during active sessions) |
| BESS list becomes stale | Monthly manual update process documented; helper script provided |
