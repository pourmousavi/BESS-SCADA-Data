import os
from datetime import date

# AEMO NEMWEB URLs
AEMO_CURRENT_URL = "https://www.nemweb.com.au/REPORTS/Current/FPPDAILY/"
AEMO_ARCHIVE_URL = "https://nemweb.com.au/Reports/Archive/FPPDAILY/"

# Data availability: first FPPMW SCADA bundle is PUBLIC_NEXT_DAY_FPPMW_20250228.zip
DATA_START_DATE = date(2025, 2, 28)

# From this date onward, data is served as individual daily ZIPs from the
# Current directory (faster).  Before this date, large archive bundles are
# used (slower).  This cutover is also sent to the frontend so both sides
# classify requests consistently.
FPPMW_CUTOVER_DATE = date(2026, 1, 11)

# Request limits
MAX_DAYS_PER_REQUEST = 1

# HTTP timeouts (seconds)
AEMO_CONNECT_TIMEOUT = 15
AEMO_READ_TIMEOUT = 60

# Analytics
ANALYTICS_TOKEN = os.environ.get("ANALYTICS_TOKEN", "changeme")
ANALYTICS_DB_PATH = "/tmp/analytics.db"

# Timing estimates: minimum successful samples before learned estimate is used
# instead of the hardcoded default.
TIMING_MIN_SAMPLES = 5
TIMING_DEFAULT_CURRENT_SEC = 120   # ~2 min default for current-dir files
TIMING_DEFAULT_ARCHIVE_SEC  = 480  # ~8 min default for archive bundles

# Columns to keep from the raw CSV (unit identifier used only for filtering,
# not included in output)
REQUIRED_COLUMNS = [
    "INTERVAL_DATETIME",
    "MEASUREMENT_DATETIME",
    "MEASURED_MW",
    "MW_QUALITY_FLAG",
]
