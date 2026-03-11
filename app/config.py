import os
from datetime import date

# AEMO NEMWEB URLs
AEMO_CURRENT_URL = "https://www.nemweb.com.au/REPORTS/Current/FPPDAILY/"
AEMO_ARCHIVE_URL = "https://nemweb.com.au/Reports/Archive/FPPDAILY/"

# Data availability: first FPPMW SCADA bundle is PUBLIC_NEXT_DAY_FPPMW_20250228.zip
DATA_START_DATE = date(2025, 2, 28)

# Request limits
MAX_DAYS_PER_REQUEST = 1

# HTTP timeouts (seconds)
AEMO_CONNECT_TIMEOUT = 15
AEMO_READ_TIMEOUT = 60

# Analytics
ANALYTICS_TOKEN = os.environ.get("ANALYTICS_TOKEN", "changeme")
ANALYTICS_DB_PATH = "/tmp/analytics.db"

# Columns to keep from the raw CSV (unit identifier used only for filtering,
# not included in output)
REQUIRED_COLUMNS = [
    "INTERVAL_DATETIME",
    "MEASUREMENT_DATETIME",
    "MEASURED_MW",
    "MW_QUALITY_FLAG",
]
