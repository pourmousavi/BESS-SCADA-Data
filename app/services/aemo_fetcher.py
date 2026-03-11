"""
Downloads and extracts FPPDAILY ZIP files from AEMO NEMWEB.

File naming conventions on NEMWEB:

Current directory (https://www.nemweb.com.au/REPORTS/Current/FPPDAILY/):
  PUBLIC_NEXT_DAY_FPP_YYYYMMDD_<16-digit-suffix>.zip
  One file per day, contains the single day's CSV.

Archive directory (https://nemweb.com.au/Reports/Archive/FPPDAILY/):
  PUBLIC_NEXT_DAY_FPP_YYYYMMDD.zip
  Each ZIP bundles multiple days of CSVs. The YYYYMMDD in the filename is the
  START date of the bundle, NOT an end-of-month date.
  Example: PUBLIC_NEXT_DAY_FPP_20250228.zip covers Feb 28 – Mar 30 2025;
           PUBLIC_NEXT_DAY_FPP_20250331.zip covers Mar 31 2025 onwards.
  To find data for a given date, download the archive whose start date is the
  largest date that is <= the target date.

Timezone note:
  INTERVAL_DATETIME / MEASUREMENT_DATETIME inside CSVs are in UTC (or NEM time
  which is UTC+10 without DST).  An Australian market day D runs from
  D-1 14:00 UTC to D 13:30 UTC in five-minute intervals.  The CSV file itself
  is named with the Australian market date, so file selection by date is
  correct without timezone conversion.
"""
import io
import logging
import re
import zipfile
from datetime import date, datetime

import httpx

from app.config import (
    AEMO_ARCHIVE_URL,
    AEMO_CONNECT_TIMEOUT,
    AEMO_CURRENT_URL,
    AEMO_READ_TIMEOUT,
    DATA_START_DATE,
)

logger = logging.getLogger(__name__)

# Mimic a browser to avoid being blocked by NEMWEB
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}


class AEMOFetchError(Exception):
    """Raised when data cannot be retrieved from AEMO."""
    pass


def _is_current(target_date: date) -> bool:
    """
    Files within the last ~7 days live in /Current/; older files in /Archive/.
    We use 9 days to allow for UTC vs AEST timezone differences and publication lag.
    """
    return (date.today() - target_date).days <= 9


async def _list_directory(base_url: str, client: httpx.AsyncClient) -> list[str]:
    """Fetch the HTML directory listing and return all ZIP filenames."""
    logger.info("Listing directory: %s", base_url)
    try:
        resp = await client.get(
            base_url,
            timeout=httpx.Timeout(AEMO_CONNECT_TIMEOUT, read=AEMO_READ_TIMEOUT),
            follow_redirects=True,
            headers=_HEADERS,
        )
        resp.raise_for_status()
    except httpx.TimeoutException:
        raise AEMOFetchError("AEMO server timed out. Please try again in a few minutes.")
    except httpx.HTTPStatusError as e:
        raise AEMOFetchError(f"AEMO server returned error {e.response.status_code}.")

    filenames = []
    for href in re.findall(r'href=["\']([^"\']+)["\']', resp.text, re.IGNORECASE):
        name = href.rstrip("/").split("/")[-1]
        if name.lower().endswith(".zip"):
            filenames.append(name)

    logger.info("Found %d ZIP files at %s", len(filenames), base_url)
    if filenames:
        logger.info("Sample filenames: %s", filenames[:5])
    return filenames


def _extract_zip_date(filename: str) -> date | None:
    """
    Extract the YYYYMMDD date embedded in a NEMWEB ZIP filename.

    Handles both:
      PUBLIC_NEXT_DAY_FPP_20260224_1234567890123456.zip  (Current, with suffix)
      PUBLIC_NEXT_DAY_FPP_20260228.zip                   (Archive, no suffix)
    """
    # Match 8 consecutive digits that appear after an underscore and before
    # either another underscore, a dot, or end-of-string.
    m = re.search(r'_(\d{8})(?:[_.]|$)', filename)
    if m:
        try:
            return datetime.strptime(m.group(1), "%Y%m%d").date()
        except ValueError:
            return None
    return None


def _date_str(target_date: date) -> str:
    """Return date as YYYYMMDD string."""
    return target_date.strftime("%Y%m%d")


async def _find_file_in(
    filenames: list[str],
    target_date: date,
    date_str: str,
    url: str,
) -> tuple[str, str] | None:
    """
    Given a directory listing, return (filename, url) for the best ZIP to use
    for target_date, or None if nothing suitable is found.

    Strategy:
      1. Exact date match — filename contains YYYYMMDD of the target date.
         Covers Current files (PUBLIC_NEXT_DAY_FPP_20260224_XXXXXXXXXXXXXXXX.zip)
         and any archive file whose start date happens to equal target_date.
      2. Archive bundle match — find the ZIP whose embedded date is the largest
         date that is still <= target_date.  That bundle covers target_date.
    """
    # 1. Exact match (e.g. Current dir or lucky archive hit)
    exact = [f for f in filenames if date_str in f]
    if exact:
        logger.info("Exact match at %s: %s", url, exact[0])
        return exact[0], url

    # 2. Archive bundle: largest start-date <= target_date
    candidates: list[tuple[date, str]] = []
    for f in filenames:
        zip_date = _extract_zip_date(f)
        if zip_date is not None and zip_date <= target_date:
            candidates.append((zip_date, f))

    if candidates:
        candidates.sort(reverse=True)  # descending by date
        best_date, best_file = candidates[0]
        logger.info(
            "Archive bundle match at %s: %s (bundle start %s covers %s)",
            url, best_file, best_date, target_date,
        )
        return best_file, url

    return None


async def _find_file(
    target_date: date, date_str: str, base_url: str, client: httpx.AsyncClient
) -> tuple[str, str]:
    """
    Search Current then Archive for a ZIP covering target_date.
    Returns (filename, base_url_of_that_file).
    Raises AEMOFetchError if nothing is found.
    """
    # Always try Current first, then Archive, regardless of age.
    # This handles publication lag and UTC/AEST edge cases.
    urls_to_try: list[str] = []
    if base_url == AEMO_CURRENT_URL:
        urls_to_try = [AEMO_CURRENT_URL, AEMO_ARCHIVE_URL]
    else:
        urls_to_try = [AEMO_ARCHIVE_URL, AEMO_CURRENT_URL]

    last_filenames: list[str] = []
    for url in urls_to_try:
        try:
            filenames = await _list_directory(url, client)
        except AEMOFetchError as e:
            logger.warning("Could not list %s: %s", url, e)
            continue

        last_filenames = filenames
        result = await _find_file_in(filenames, target_date, date_str, url)
        if result:
            return result

    logger.error(
        "No file found for %s. Last directory listing sample: %s",
        date_str, last_filenames[:10],
    )
    raise AEMOFetchError(
        f"No FPPDAILY data found for {target_date.strftime('%d %B %Y')}. "
        "The file may not yet be published by AEMO."
    )


async def fetch_csv_for_date(target_date: date, duid: str) -> bytes:
    """
    Download FPPDAILY data for target_date and return raw CSV bytes.
    Raises AEMOFetchError if unavailable.
    """
    if target_date < DATA_START_DATE:
        raise AEMOFetchError(
            f"Data is only available from {DATA_START_DATE.strftime('%d %B %Y')} "
            "when the FPP scheme commenced."
        )
    if target_date >= date.today():
        raise AEMOFetchError("Cannot request data for today or future dates.")

    base_url = AEMO_CURRENT_URL if _is_current(target_date) else AEMO_ARCHIVE_URL
    date_str = _date_str(target_date)

    async with httpx.AsyncClient() as client:
        filename, found_url = await _find_file(target_date, date_str, base_url, client)
        zip_url = found_url + filename

        logger.info("Downloading: %s", zip_url)
        try:
            resp = await client.get(
                zip_url,
                timeout=httpx.Timeout(AEMO_CONNECT_TIMEOUT, read=AEMO_READ_TIMEOUT),
                follow_redirects=True,
                headers=_HEADERS,
            )
            resp.raise_for_status()
        except httpx.TimeoutException:
            raise AEMOFetchError(
                "AEMO server timed out while downloading the data file. "
                "The file may be large. Please try again."
            )
        except httpx.HTTPStatusError as e:
            raise AEMOFetchError(
                f"Could not download data file (HTTP {e.response.status_code})."
            )

        # Extract the correct daily CSV from the ZIP.
        # Archive ZIPs contain multiple CSVs (one per day); match by date_str.
        # Current ZIPs contain a single CSV.
        try:
            zip_bytes = io.BytesIO(resp.content)
            with zipfile.ZipFile(zip_bytes) as zf:
                all_entries = zf.namelist()
                all_csvs = [n for n in all_entries if n.lower().endswith(".csv")]

                if not all_csvs:
                    raise AEMOFetchError("ZIP file contained no CSV files.")

                # Prefer the CSV whose name contains the target date
                daily_csvs = [n for n in all_csvs if date_str in n]
                if daily_csvs:
                    csv_name = daily_csvs[0]
                elif len(all_csvs) == 1:
                    # Single-CSV ZIP (Current) — use it directly
                    csv_name = all_csvs[0]
                else:
                    # Multi-CSV archive but no name match — log and use newest by name
                    all_csvs_sorted = sorted(all_csvs, reverse=True)
                    logger.warning(
                        "No CSV matching %s in %s; available: %s",
                        date_str, filename, all_csvs_sorted[:5],
                    )
                    raise AEMOFetchError(
                        f"No data file found for {target_date.strftime('%d %B %Y')} "
                        f"inside the archive ZIP. Available dates: "
                        + ", ".join(
                            m.group(1)
                            for n in all_csvs_sorted[:5]
                            for m in [re.search(r'(\d{8})', n)]
                            if m
                        )
                    )

                logger.info("Extracting CSV: %s", csv_name)
                csv_bytes = zf.read(csv_name)

        except zipfile.BadZipFile:
            raise AEMOFetchError(
                "Downloaded file appears to be corrupt. Please try again."
            )

        return csv_bytes
