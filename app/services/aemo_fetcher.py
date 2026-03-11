"""
Downloads and extracts FPPDAILY ZIP files from AEMO NEMWEB.
"""
import io
import logging
import re
import zipfile
from datetime import date

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
    """Files within the last ~7 days are in /Current/, older in /Archive/."""
    return (date.today() - target_date).days <= 7


async def _list_directory(base_url: str, client: httpx.AsyncClient) -> list[str]:
    """Fetch the HTML directory listing and extract all ZIP filenames."""
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


def _date_str(target_date: date) -> str:
    """Return date as YYYYMMDD string."""
    return target_date.strftime("%Y%m%d")


async def _find_file(
    target_date: date, date_str: str, base_url: str, client: httpx.AsyncClient
) -> tuple[list[str], str]:
    """
    Try multiple locations to find a ZIP for the target date.

    Handles two archive structures:
      1. Daily ZIPs: PUBLIC_FPPDAILY_D_YYYYMMDD_xxx.zip  → match by YYYYMMDD
      2. Monthly ZIPs: PUBLIC_FPPDAILY_YYYYMM_xxx.zip    → match by YYYYMM,
         then search inside the ZIP for the correct daily CSV

    Returns (matching_filenames, base_url_where_found).
    """
    month_str = target_date.strftime("%Y%m")

    urls_to_try: list[str] = []
    if base_url == AEMO_CURRENT_URL:
        urls_to_try = [
            AEMO_CURRENT_URL,
            AEMO_ARCHIVE_URL,
            AEMO_ARCHIVE_URL + month_str + "/",
        ]
    else:
        urls_to_try = [
            AEMO_ARCHIVE_URL,
            AEMO_ARCHIVE_URL + month_str + "/",
        ]

    for url in urls_to_try:
        try:
            filenames = await _list_directory(url, client)
        except AEMOFetchError as e:
            logger.warning("Could not list %s: %s", url, e)
            continue

        # Try exact daily match first
        matching = [f for f in filenames if date_str in f]
        if matching:
            logger.info("Found daily match at %s: %s", url, matching[0])
            return matching, url

        # Try monthly match (archive stores one ZIP per month containing all daily CSVs)
        monthly = [f for f in filenames if month_str in f and date_str not in f]
        if monthly:
            logger.info(
                "No daily match; found monthly archive at %s: %s", url, monthly[0]
            )
            return monthly, url

    logger.error(
        "No file found for date %s in any of: %s", date_str, urls_to_try
    )
    return [], base_url


async def fetch_csv_for_date(target_date: date, duid: str) -> bytes:
    """
    Download FPPDAILY data for target_date and return raw CSV bytes.
    Raises AEMOFetchError if unavailable.
    """
    if target_date < DATA_START_DATE:
        raise AEMOFetchError(
            f"Data is only available from {DATA_START_DATE.strftime('%d %B %Y')} "
            f"when the FPP scheme commenced."
        )
    if target_date >= date.today():
        raise AEMOFetchError("Cannot request data for today or future dates.")

    base_url = AEMO_CURRENT_URL if _is_current(target_date) else AEMO_ARCHIVE_URL
    date_str = _date_str(target_date)

    async with httpx.AsyncClient() as client:
        matching, found_url = await _find_file(target_date, date_str, base_url, client)
        if not matching:
            raise AEMOFetchError(
                f"No FPPDAILY data found for {target_date.strftime('%d %B %Y')}. "
                f"The file may not yet be published or the date may not have data."
            )

        filename = matching[0]
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
                "The file may be very large. Please try again."
            )
        except httpx.HTTPStatusError as e:
            raise AEMOFetchError(
                f"Could not download data file (HTTP {e.response.status_code})."
            )

        # Extract CSV from ZIP.
        # If this is a monthly archive ZIP, find the CSV matching the specific date.
        try:
            zip_bytes = io.BytesIO(resp.content)
            with zipfile.ZipFile(zip_bytes) as zf:
                all_csvs = [n for n in zf.namelist() if n.lower().endswith(".csv")]
                if not all_csvs:
                    raise AEMOFetchError("ZIP file contained no CSV files.")

                # Prefer the CSV that matches the target date; fall back to first
                daily_csvs = [n for n in all_csvs if date_str in n]
                csv_name = daily_csvs[0] if daily_csvs else all_csvs[0]
                logger.info("Extracting CSV: %s", csv_name)
                csv_bytes = zf.read(csv_name)
        except zipfile.BadZipFile:
            raise AEMOFetchError("Downloaded file appears to be corrupt. Please try again.")

        return csv_bytes
