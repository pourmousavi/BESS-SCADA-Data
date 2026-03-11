"""
Downloads and extracts FPPDAILY ZIP files from AEMO NEMWEB.

File naming conventions on NEMWEB:

  Up to 10 Jan 2026 — FPP format:
    PUBLIC_NEXT_DAY_FPP_YYYYMMDD[_<suffix>].zip
    The ZIP contains the CSV directly.

  From 11 Jan 2026 — FPPMW format (nested ZIP):
    PUBLIC_NEXT_DAY_FPPMW_YYYYMMDD[_<suffix>].zip
    The outer ZIP contains an inner ZIP, which contains the CSV.
    Both are named with the FPPMW prefix.

Current directory (https://www.nemweb.com.au/REPORTS/Current/FPPDAILY/):
  Individual daily files (FPP or FPPMW), one per market day.

Archive directory (https://nemweb.com.au/Reports/Archive/FPPDAILY/):
  PUBLIC_NEXT_DAY_FPP_YYYYMMDD.zip — multi-day bundles (one per month).
    The YYYYMMDD is the START date of the bundle.  Find data for a given
    date by picking the bundle whose start date is the largest date <= target.
  PUBLIC_NEXT_DAY_FPPMW_YYYYMMDD.zip — individual daily files (FPPMW format),
    only valid for an exact date match.

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

    Handles both naming conventions and both formats:
      PUBLIC_NEXT_DAY_FPP_20260110_1234567890123456.zip  (Current, FPP, with suffix)
      PUBLIC_NEXT_DAY_FPPMW_20260224_1234567890123456.zip (Current, FPPMW, with suffix)
      PUBLIC_NEXT_DAY_FPP_20250228.zip                   (Archive bundle, no suffix)
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
         Covers Current files (FPP or FPPMW naming) and any archive file
         whose start date happens to equal target_date.
      2. Archive bundle match — find the ZIP whose embedded date is the largest
         date that is still <= target_date.  That bundle covers target_date.
    """
    # 1. Exact match (e.g. Current dir or lucky archive hit)
    exact = [f for f in filenames if date_str in f]
    if exact:
        # From 11 Jan 2026 AEMO publishes FPPMW (metered) files alongside older
        # FPP files in the same directory.  Always prefer FPPMW over FPP so we
        # get the correct metered-data file, not a related forecast/other file.
        fppmw_exact = [f for f in exact if "FPPMW" in f]
        chosen = fppmw_exact[0] if fppmw_exact else exact[0]
        logger.info("Exact match at %s: %s", url, chosen)
        return chosen, url

    # 2. Archive bundle: largest start-date <= target_date.
    #    ONLY consider PUBLIC_NEXT_DAY_FPP_*.zip files (multi-day bundles).
    #    PUBLIC_NEXT_DAY_FPPMW_*.zip files are individual daily files — they
    #    only contain data for their own date, so they must not be used as
    #    a bundle for a different date.
    fpp_bundles = [f for f in filenames if "FPPMW" not in f]
    candidates: list[tuple[date, str]] = []
    for f in fpp_bundles:
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
    # Current directory keeps a long history (observed: 180+ files going back
    # months), so always try it first — it has single-file exact matches which
    # are faster and more reliable than hunting through archive bundles.
    # Fall back to Archive for older dates not yet deleted from Current.
    urls_to_try = [AEMO_CURRENT_URL, AEMO_ARCHIVE_URL]

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
        # FPP format  (up to Jan 10 2026): CSV lives directly inside the ZIP.
        # FPPMW format (from Jan 11 2026): outer ZIP wraps an inner ZIP which
        #   contains the CSV (NEMWEB nested-ZIP convention).
        # Archive FPP ZIPs contain multiple CSVs (one per day); match by date.
        try:
            zip_bytes = io.BytesIO(resp.content)
            with zipfile.ZipFile(zip_bytes) as outer_zf:
                all_entries = outer_zf.namelist()
                all_csvs = [n for n in all_entries if n.lower().endswith(".csv")]

                # FPPMW nested-ZIP: outer contains an inner ZIP, not a CSV.
                inner_zf = None
                if not all_csvs:
                    inner_zips = [n for n in all_entries if n.lower().endswith(".zip")]
                    if not inner_zips:
                        raise AEMOFetchError("ZIP file contained no CSV files.")
                    logger.info("Nested ZIP (FPPMW format); opening: %s", inner_zips[0])
                    inner_zf = zipfile.ZipFile(io.BytesIO(outer_zf.read(inner_zips[0])))
                    all_entries = inner_zf.namelist()
                    all_csvs = [n for n in all_entries if n.lower().endswith(".csv")]
                    if not all_csvs:
                        inner_zf.close()
                        raise AEMOFetchError("Inner ZIP contained no CSV files.")

                active_zf = inner_zf if inner_zf is not None else outer_zf
                try:
                    # Prefer the CSV whose name contains the target date
                    daily_csvs = [n for n in all_csvs if date_str in n]
                    if daily_csvs:
                        csv_name = daily_csvs[0]
                    elif len(all_csvs) == 1:
                        # Single-CSV ZIP (Current) — use it directly
                        csv_name = all_csvs[0]
                    else:
                        # Multi-CSV archive but no name match — report available dates
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
                    csv_bytes = active_zf.read(csv_name)
                finally:
                    if inner_zf is not None:
                        inner_zf.close()

        except zipfile.BadZipFile:
            raise AEMOFetchError(
                "Downloaded file appears to be corrupt. Please try again."
            )

        return csv_bytes
