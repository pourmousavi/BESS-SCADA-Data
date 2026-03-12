import os
from datetime import date

# ── FPPDAILY (4-second SCADA) ────────────────────────────────────────────────
AEMO_CURRENT_URL = "https://www.nemweb.com.au/REPORTS/Current/FPPDAILY/"
AEMO_ARCHIVE_URL = "https://nemweb.com.au/Reports/Archive/FPPDAILY/"

# Data availability: first FPPMW SCADA bundle is PUBLIC_NEXT_DAY_FPPMW_20250228.zip
DATA_START_DATE = date(2025, 2, 28)

# From this date onward, data is served as individual daily ZIPs from the
# Current directory (faster).  Before this date, large archive bundles are
# used (slower).  This cutover is also sent to the frontend so both sides
# classify requests consistently.
FPPMW_CUTOVER_DATE = date(2026, 1, 11)

# ── Next_Day_Dispatch (5-minute energy storage) ──────────────────────────────
DISPATCH_CURRENT_URL = "https://nemweb.com.au/Reports/Current/Next_Day_Dispatch/"
DISPATCH_ARCHIVE_URL = "https://www.nemweb.com.au/REPORTS/ARCHIVE/Next_Day_Dispatch/"

# Dispatch data available from 11 Feb 2025 (INITIAL_ENERGY_STORAGE column).
DISPATCH_START_DATE = date(2025, 2, 11)

# ── Request limits ────────────────────────────────────────────────────────────
MAX_DAYS_PER_REQUEST = 1

# ── HTTP timeouts (seconds) ───────────────────────────────────────────────────
AEMO_CONNECT_TIMEOUT = 15
AEMO_READ_TIMEOUT = 60

# ── Analytics ─────────────────────────────────────────────────────────────────
ANALYTICS_TOKEN = os.environ.get("ANALYTICS_TOKEN", "changeme")
ANALYTICS_DB_PATH = "/tmp/analytics.db"

# ── Timing estimates ──────────────────────────────────────────────────────────
# Minimum successful samples before the learned p75 estimate replaces the default.
TIMING_MIN_SAMPLES = 5

# SCADA (FPPDAILY) defaults
TIMING_DEFAULT_CURRENT_SEC = 120   # ~2 min  — daily file from Current dir
TIMING_DEFAULT_ARCHIVE_SEC  = 480  # ~8 min  — archive bundle (remotezip)

# Dispatch (Next_Day_Dispatch) defaults
TIMING_DEFAULT_DISPATCH_CURRENT_SEC = 60   # ~1 min  — daily file from Current dir
TIMING_DEFAULT_DISPATCH_ARCHIVE_SEC  = 300  # ~5 min  — monthly archive (remotezip)

# ── Column lists ──────────────────────────────────────────────────────────────
# Columns to keep from the SCADA CSV (unit identifier used only for filtering)
REQUIRED_COLUMNS = [
    "INTERVAL_DATETIME",
    "MEASUREMENT_DATETIME",
    "MEASURED_MW",
    "MW_QUALITY_FLAG",
]

# Columns to keep from the dispatch UNIT_SOLUTION CSV
DISPATCH_COLUMNS = [
    "SETTLEMENTDATE",
    "INITIALMW",
    "INITIAL_ENERGY_STORAGE",
    "ENERGY_STORAGE",
]
