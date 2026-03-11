import os
from datetime import date

# AEMO NEMWEB URLs
AEMO_CURRENT_URL = "https://www.nemweb.com.au/REPORTS/Current/FPPDAILY/"
AEMO_ARCHIVE_URL = "https://nemweb.com.au/Reports/Archive/FPPDAILY/"

# Data availability: FPP scheme started December 2024
DATA_START_DATE = date(2024, 12, 9)

# Request limits
MAX_DAYS_PER_REQUEST = 1

# HTTP timeouts (seconds)
AEMO_CONNECT_TIMEOUT = 15
AEMO_READ_TIMEOUT = 60

# Analytics
ANALYTICS_TOKEN = os.environ.get("ANALYTICS_TOKEN", "changeme")
ANALYTICS_DB_PATH = "/tmp/analytics.db"

# Columns to keep from the raw CSV
REQUIRED_COLUMNS = [
    "INTERVAL_DATETIME",
    "MEASUREMENT_DATETIME",
    "FPP_UNITID",
    "MEASURED_MW",
    "MW_QUALITY_FLAG",
]
