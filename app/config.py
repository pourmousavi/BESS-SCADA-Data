import os
from datetime import date

# ── FPPDAILY (4-second SCADA) ────────────────────────────────────────────────
AEMO_CURRENT_URL = "https://www.nemweb.com.au/REPORTS/Current/FPPDAILY/"
AEMO_ARCHIVE_URL = "https://nemweb.com.au/Reports/Archive/FPPDAILY/"

# Data availability: AEMO has rolled off the early Era 1 archive bundles.
# The oldest archive bundle currently on NEMWEB is PUBLIC_NEXT_DAY_FPP_20250430.zip
# whose first inner file is publication 30 Apr 2025 = settlement 29 Apr 2025
# (the first day of Era 2 in our era model). FPPMW Format-1 bundles start the
# same day. So 29 Apr 2025 is the earliest settlement date for which any
# FPPDAILY data can still be fetched.
#
# The FPP scheme itself commenced on 28 Feb 2025, but the Era 1 monthly
# bundles (Dec 2024 – 28 Apr 2025) have all been rolled off the archive.
DATA_START_DATE = date(2025, 4, 29)

# From this date onward, data is served as individual daily ZIPs from the
# Current directory (faster).  Before this date, large archive bundles are
# used (slower).  This cutover is also sent to the frontend so both sides
# classify requests consistently.
FPPMW_CUTOVER_DATE = date(2026, 1, 11)

# ── Next_Day_Dispatch (5-minute energy storage) ──────────────────────────────
DISPATCH_CURRENT_URL = "https://nemweb.com.au/Reports/Current/Next_Day_Dispatch/"
DISPATCH_ARCHIVE_URL = "https://www.nemweb.com.au/REPORTS/ARCHIVE/Next_Day_Dispatch/"

# Dispatch data: the oldest archive bundle on NEMWEB is
# PUBLIC_NEXT_DAY_DISPATCH_20250401.zip (covering settlement dates
# 1 Apr 2025 – 30 Apr 2025). The INITIAL_ENERGY_STORAGE column has existed
# since 11 Feb 2025, but earlier monthly bundles have been rolled off.
DISPATCH_START_DATE = date(2025, 4, 1)

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
