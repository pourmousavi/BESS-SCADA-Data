"""
Fetches and parses the AEMO NEM Generation Information XLSX.

Downloads the spreadsheet from AEMO, reads the "Generator Information" sheet,
and returns all in-service battery storage units grouped by NEM region/state.

The spreadsheet is cached in memory for 24 hours to avoid re-downloading on
every request.  On any failure the caller receives the cached data (if any) or
the static fallback bess_list.json.
"""
import io
import logging
from datetime import datetime, timedelta
from pathlib import Path

import httpx
import openpyxl

logger = logging.getLogger(__name__)

XLSX_URL = (
    "https://www.aemo.com.au/-/media/files/electricity/nem/planning_and_forecasting"
    "/generation_information/2026/nem-generation-information-jan-2026.xlsx"
    "?rev=1f6bccf827284f9fb6d6f3ae56ed3fe9&sc_lang=en"
)

SHEET_NAME = "Generator Information"

# Map NEM region codes to the state keys used in the UI
REGION_TO_STATE: dict[str, str] = {
    "QLD1": "QLD",
    "NSW1": "NSW",
    "VIC1": "VIC",
    "SA1":  "SA",
    "TAS1": "TAS",
}

BATTERY_TECH = "Battery Storage"
IN_SERVICE   = "In Service"

# Column names to search for (case-insensitive substring match as fallback)
_COL_DUID       = "DUID"
_COL_NAME       = "Station Name"
_COL_TECH       = "Technology Type"
_COL_STATUS     = "Commitment Status"
_COL_REGION     = "Region"
_COL_CAPACITY   = "Reg Cap"          # "Reg Cap (MW)" — matched as prefix

CACHE_TTL = timedelta(hours=24)

# In-memory cache: (parsed_result, fetched_at)
_cache: tuple[dict, datetime] | None = None


def _find_col(headers: list[str], target: str) -> int | None:
    """
    Return the 0-based index of the column whose header matches target.
    Tries exact match first, then case-insensitive prefix match.
    """
    target_lower = target.lower()
    for i, h in enumerate(headers):
        if h == target:
            return i
    for i, h in enumerate(headers):
        if h.lower().startswith(target_lower):
            return i
    return None


def _parse_xlsx(xlsx_bytes: bytes) -> dict[str, list[dict]]:
    """
    Parse the XLSX bytes and return BESS list grouped by state.

    Returns:
        { "QLD": [...], "NSW": [...], "VIC": [...], "SA": [...], "TAS": [...] }
        Each entry: { "duid": str, "name": str, "capacity_mw": float|None, "region": str }
    """
    wb = openpyxl.load_workbook(io.BytesIO(xlsx_bytes), read_only=True, data_only=True)

    if SHEET_NAME not in wb.sheetnames:
        available = ", ".join(wb.sheetnames)
        raise ValueError(f"Sheet '{SHEET_NAME}' not found. Available: {available}")

    ws = wb[SHEET_NAME]
    rows = list(ws.iter_rows(values_only=True))

    if len(rows) < 4:
        raise ValueError("Sheet has fewer than 4 rows; expected headers on row 4.")

    # Row 4 (0-indexed: rows[3]) contains the column headers.
    # Rows 1–3 are title/metadata rows that must be skipped.
    raw_headers = [str(c).strip() if c is not None else "" for c in rows[3]]

    col_duid     = _find_col(raw_headers, _COL_DUID)
    col_name     = _find_col(raw_headers, _COL_NAME)
    col_tech     = _find_col(raw_headers, _COL_TECH)
    col_status   = _find_col(raw_headers, _COL_STATUS)
    col_region   = _find_col(raw_headers, _COL_REGION)
    col_capacity = _find_col(raw_headers, _COL_CAPACITY)

    missing = [
        name for name, idx in [
            (_COL_DUID, col_duid), (_COL_TECH, col_tech),
            (_COL_STATUS, col_status), (_COL_REGION, col_region),
        ] if idx is None
    ]
    if missing:
        raise ValueError(
            f"Required columns not found: {missing}. "
            f"Headers seen: {raw_headers[:20]}"
        )

    result: dict[str, list[dict]] = {s: [] for s in REGION_TO_STATE.values()}

    for row in rows[4:]:
        def cell(idx: int | None) -> str:
            if idx is None or idx >= len(row):
                return ""
            v = row[idx]
            return str(v).strip() if v is not None else ""

        tech   = cell(col_tech)
        status = cell(col_status)
        region = cell(col_region)

        if tech != BATTERY_TECH:
            continue
        if status != IN_SERVICE:
            continue
        state = REGION_TO_STATE.get(region)
        if state is None:
            continue

        duid = cell(col_duid)
        if not duid:
            continue

        name = cell(col_name) or duid

        capacity_mw: float | None = None
        if col_capacity is not None:
            raw_cap = row[col_capacity] if col_capacity < len(row) else None
            if raw_cap is not None:
                try:
                    capacity_mw = float(raw_cap)
                except (ValueError, TypeError):
                    pass

        result[state].append({
            "duid": duid,
            "name": name,
            "capacity_mw": capacity_mw,
            "region": region,
        })

    total = sum(len(v) for v in result.values())
    logger.info(
        "Parsed XLSX: %d in-service battery storage units across %d states",
        total, sum(1 for v in result.values() if v),
    )
    return result


async def fetch_bess_list() -> dict[str, list[dict]]:
    """
    Return the BESS list from the AEMO generation information XLSX.

    Uses a 24-hour in-memory cache.  Falls back to the static bess_list.json
    if the download or parse fails and no cached data is available.
    """
    global _cache

    now = datetime.utcnow()

    # Return cached data if still fresh
    if _cache is not None:
        parsed, fetched_at = _cache
        if now - fetched_at < CACHE_TTL:
            logger.debug("Returning cached BESS list (age %s)", now - fetched_at)
            return parsed

    # Attempt download
    try:
        logger.info("Downloading AEMO generation information XLSX from %s", XLSX_URL)
        async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
            resp = await client.get(XLSX_URL)
            resp.raise_for_status()

        parsed = _parse_xlsx(resp.content)
        _cache = (parsed, now)
        return parsed

    except Exception as exc:
        logger.warning("Failed to fetch/parse AEMO gen info XLSX: %s", exc)

        # Return stale cache rather than nothing
        if _cache is not None:
            logger.warning("Returning stale BESS list cache (age %s)", now - _cache[1])
            return _cache[0]

        # Last resort: return static fallback file
        logger.warning("Falling back to static bess_list.json")
        import json
        fallback = Path(__file__).parent.parent / "data" / "bess_list.json"
        return json.loads(fallback.read_text())
