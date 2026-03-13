---
title: NEM Battery SCADA Data Explorer
emoji: 🔋
colorFrom: blue
colorTo: green
sdk: docker
pinned: false
license: mit
short_description: Explore 4-second BESS SCADA & 5-min dispatch data from the Australian NEM
---

# 🔋 NEM Battery SCADA & Energy Data Explorer

An interactive web app for exploring high-frequency power and energy storage data for Battery Energy Storage Systems (BESS) in Australia's National Electricity Market (NEM). Data is fetched live from AEMO NEMWEB on every request — nothing is stored on the server.

---

## Datasets

### 4-second SCADA — `PUBLIC_NEXT_DAY_FPPMW`

- **Source:** [AEMO NEMWEB FPPDAILY](https://www.nemweb.com.au/REPORTS/Current/FPPDAILY/)
- **Available from:** 28 February 2025 (FPP scheme commencement)
- **Granularity:** one row per 4-second grid slot

| Column | Description |
|--------|-------------|
| `INTERVAL_DATETIME` | Target 4-second grid slot (YYYY/MM/DD HH:MM:SS) |
| `MEASUREMENT_DATETIME` | Actual SCADA recording timestamp (may differ slightly from interval) |
| `MEASURED_MW` | Instantaneous power (MW). Positive = discharging, negative = charging |
| `MW_QUALITY_FLAG` | Data quality: **0** Good · **1** Substituted · **2** Bad · **3** Manual override |

### 5-minute Dispatch — `DISPATCH_UNIT_SOLUTION`

- **Source:** [AEMO NEMWEB Next_Day_Dispatch](https://nemweb.com.au/Reports/Current/Next_Day_Dispatch/)
- **Available from:** 11 February 2025
- **Granularity:** one row per 5-minute dispatch interval

| Column | Description |
|--------|-------------|
| `SETTLEMENTDATE` | End of the 5-minute interval (AEST) |
| `INITIALMW` | Actual power output (MW) at interval start — the dispatch baseline |
| `INITIAL_ENERGY_STORAGE` | State of Energy (SoE) at interval start (MWh) |
| `ENERGY_STORAGE` | Target SoE at interval end (MWh), accounting for scheduled dispatch |

---

## Features

- **Interactive charts** — zoomable/pannable Plotly.js time-series for both datasets
- **Quality flag visualisation** — colour-coded SCADA points (green / orange / red / grey)
- **Summary statistics** — min, max, mean, std dev, and flag breakdown per query
- **Downloads** — CSV and Parquet for both datasets (Parquet recommended for Python users)
- **Live BESS list** — fetched from AEMO NEM Generation Information (24 h cache), with fallback to bundled JSON
- **26 BESS units** across NSW, VIC, QLD, SA

---

## Tech Stack

| Layer | Technology |
|-------|------------|
| Backend | [FastAPI](https://fastapi.tiangolo.com/) + [Uvicorn](https://www.uvicorn.org/) (Python 3.11) |
| Data processing | [Polars](https://pola.rs/) |
| Remote ZIP access | [remotezip](https://github.com/gtsystem/python-remotezip) (HTTP Range requests for archive bundles) |
| Frontend | Vanilla HTML / CSS / JavaScript |
| Charts | [Plotly.js](https://plotly.com/javascript/) |
| Deployment | Docker → [HuggingFace Spaces](https://huggingface.co/spaces) |

---

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/bess` | BESS units grouped by state |
| `GET` | `/api/data` | 4-second SCADA data (JSON) |
| `GET` | `/api/download/csv` | SCADA data as CSV |
| `GET` | `/api/download/parquet` | SCADA data as Parquet |
| `GET` | `/api/energy-data` | 5-minute dispatch data (JSON) |
| `GET` | `/api/download/energy-csv` | Dispatch data as CSV |
| `GET` | `/api/download/energy-parquet` | Dispatch data as Parquet |
| `GET` | `/api/quality-flags` | `MW_QUALITY_FLAG` code descriptions |
| `GET` | `/api/info` | App metadata (data start dates, timing estimates) |
| `GET` | `/api/analytics` | Request analytics (requires `?token=`) |

**Common query parameters:** `duid=HORNSDALE_PWR1&date=2025-06-01`

---

## Running Locally

```bash
# 1. Clone
git clone https://github.com/pourmousavi/BESS-SCADA-Data.git
cd BESS-SCADA-Data

# 2. Install dependencies
pip install -r requirements.txt

# 3. Run
uvicorn app.main:app --reload --port 8000
```

Then open [http://localhost:8000](http://localhost:8000).

**Optional environment variable:**

| Variable | Default | Description |
|----------|---------|-------------|
| `ANALYTICS_TOKEN` | `changeme` | Token for the `/api/analytics` admin endpoint |

### Docker

```bash
docker build -t bess-explorer .
docker run -p 7860:7860 bess-explorer
```

---

## Project Structure

```
├── app/
│   ├── main.py                  # FastAPI app & static file serving
│   ├── config.py                # AEMO URLs, date constants, timeouts
│   ├── routers/api.py           # All REST endpoints
│   ├── services/
│   │   ├── aemo_fetcher.py      # Downloads FPPDAILY ZIPs from NEMWEB
│   │   ├── dispatch_fetcher.py  # Downloads Next_Day_Dispatch ZIPs
│   │   ├── data_processor.py    # Polars: filter & transform SCADA CSV
│   │   ├── dispatch_processor.py# Polars: filter & transform dispatch CSV
│   │   ├── gen_info_fetcher.py  # Fetches live BESS list from AEMO XLSX
│   │   └── analytics.py         # SQLite request logging
│   ├── data/
│   │   ├── bess_list.json       # Fallback static BESS list
│   │   └── quality_flags.json   # MW_QUALITY_FLAG descriptions
│   └── static/                  # Frontend (HTML + CSS + JS)
├── Dockerfile
└── requirements.txt
```

---

## Data Notes

- **Trading day boundary:** NEM days run 04:00–04:00 AEST, so overnight queries span two calendar dates.
- **FPPDAILY archive format:** Files before 11 Jan 2026 are in monthly/weekly bundle ZIPs (accessed via HTTP Range requests); newer files are individual daily ZIPs.
- **Nested ZIPs:** FPPDAILY archives contain outer ZIP → inner ZIP → CSV. All extraction is done in-memory.
- **FPPMW half-day files:** From March 2025, each NEM day is split into two 12-hour CSV segments published the following day.
- **No data storage:** All data is streamed from AEMO NEMWEB per request. No user data is retained.

---

## Disclaimer

Data is retrieved as-is from [AEMO NEMWEB](https://www.aemo.com.au) and is subject to change, correction, or unavailability without notice. This tool is provided for informational and research purposes only. Not affiliated with AEMO.
**Data source 1:** [AEMO NEMWEB FPPDAILY Current](https://www.nemweb.com.au/REPORTS/Current/FPPDAILY/)
**Data source 2:** [AEMO NEMWEB FPPDAILY Archive](https://www.nemweb.com.au/REPORTS/ARCHIVE/FPPDAILY/)
**Data source 3:** [AEMO NEMWEB Next Day Dispatch Current](https://nemweb.com.au/Reports/Current/Next_Day_Dispatch/)
**Data source 4:** [AEMO NEMWEB Next Day Dispatch Archive](https://www.nemweb.com.au/REPORTS/ARCHIVE/Next_Day_Dispatch/)
