"""
Downloads and extracts FPPDAILY ZIP files from AEMO NEMWEB.

File naming conventions on NEMWEB:

  Up to 10 Jan 2026 — FPP format:
    PUBLIC_NEXT_DAY_FPP_YYYYMMDD[_<suffix>].zip
    ZIP contains the daily CSV directly.

  From 11 Jan 2026 — FPPMW format (nested ZIP):
    PUBLIC_NEXT_DAY_FPPMW_YYYYMMDD[_<suffix>].zip
    Outer ZIP → one or more inner ZIPs → CSV(s).
    The NEM market day (04:00–04:00 AEST) is split into two 12-hour halves.
    The second half (16:00 AEST → 04:00 AEST next calendar day) is typically
    published as a separate inner ZIP whose filename carries the next calendar
    date.  All inner ZIPs must be opened and concatenated; the data_processor
    applies the authoritative [04:00 AEST D, 04:00 AEST D+1) boundary filter.
    All AEMO NEMWEB timestamps are in AEST (UTC+10), no daylight saving.

Current directory (https://www.nemweb.com.au/REPORTS/Current/FPPDAILY/):
  Individual daily files (FPP or FPPMW naming), one per market day.
  Keeps a long rolling history (180+ files observed).

Archive directory (https://nemweb.com.au/Reports/Archive/FPPDAILY/):

  FPP monthly bundles (data up to ~Mar 2025):
    PUBLIC_NEXT_DAY_FPP_YYYYMMDD.zip
    YYYYMMDD = bundle start date.  Contains daily CSVs directly.
    Select by largest start-date <= target.

  FPPMW monthly bundles (Mar 2025 – Jan 2026):
    PUBLIC_NEXT_DAY_FPPMW_YYYYMMDD.zip
    YYYYMMDD = last day of the PREVIOUS month
      e.g. 20250228 → bundle covering all of March 2025
           20251231 → bundle covering all of January 2026
    Contains per-day inner ZIPs; each inner ZIP contains the daily CSV.
    These files are several GB — HTTP Range requests (remotezip) are used
    to fetch only the target day's inner ZIP without downloading the whole
    bundle.

  Individual FPPMW daily archive copies (recent dates, 2026+):
    PUBLIC_NEXT_DAY_FPPMW_YYYYMMDD.zip where YYYYMMDD is NOT an end-of-month
    date.  Treated as individual daily files, not bundles.
"""
import asyncio
import io
import logging
import re
import zipfile
from datetime import date, datetime, timedelta

import httpx
from remotezip import RemoteZip

from app.config import (
    AEMO_ARCHIVE_URL,
    AEMO_CONNECT_TIMEOUT,
    AEMO_CURRENT_URL,
    AEMO_READ_TIMEOUT,
    DATA_START_DATE,
)

logger = logging.getLogger(__name__)

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
    return (date.today() - target_date).days <= 9


def _is_month_end(d: date) -> bool:
    """True if d is the last calendar day of its month."""
    return (d + timedelta(days=1)).month != d.month


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
    """Extract the YYYYMMDD date embedded in a NEMWEB ZIP filename."""
    m = re.search(r'_(\d{8})(?:[_.]|$)', filename)
    if m:
        try:
            return datetime.strptime(m.group(1), "%Y%m%d").date()
        except ValueError:
            return None
    return None


def _date_str(target_date: date) -> str:
    return target_date.strftime("%Y%m%d")


async def _find_file_in(
    filenames: list[str],
    target_date: date,
    date_str: str,
    url: str,
) -> tuple[str, str, str] | None:
    """
    Return (filename, url, kind) for the best ZIP to use, or None.

    kind values
    -----------
    "fppmw_daily"    Individual FPPMW file (Current or non-bundle Archive).
                     Structure: outer ZIP → inner ZIP → CSV.
    "fppmw_monthly"  FPPMW monthly archive bundle (multi-GB).
                     Structure: outer ZIP → per-day inner ZIPs → CSV.
                     Uses HTTP Range requests to avoid full download.
    "fpp_bundle"     FPP monthly archive bundle.
                     Structure: outer ZIP → per-day CSVs directly.
    """
    # Only FPPMW files are used. FPP files lack MEASUREMENT_DATETIME,
    # MEASURED_MW, and MW_QUALITY_FLAG and cannot supply SCADA data.

    # 1. Exact date match — FPPMW files only
    fppmw_exact = [f for f in filenames if "FPPMW" in f and date_str in f]
    if fppmw_exact:
        chosen = fppmw_exact[0]
        logger.info("Exact match at %s: %s", url, chosen)
        return chosen, url, "fppmw_daily"

    # 2. Archive bundle match: FPPMW files whose embedded date is an
    #    end-of-month date (= last day of the previous month convention).
    #    Non-month-end FPPMW dates are individual daily archive copies —
    #    they only contain data for their own date, so skip them here.
    candidates: list[tuple[date, str]] = []
    for f in filenames:
        if "FPPMW" not in f:
            continue
        zip_date = _extract_zip_date(f)
        if zip_date is None or zip_date > target_date:
            continue
        if not _is_month_end(zip_date):
            continue
        candidates.append((zip_date, f))

    if candidates:
        candidates.sort(reverse=True)  # latest bundle first
        best_date, best_file = candidates[0]
        logger.info(
            "Bundle match at %s: %s (start %s covers %s)",
            url, best_file, best_date, target_date,
        )
        return best_file, url, "fppmw_monthly"

    return None


async def _find_file(
    target_date: date, date_str: str, client: httpx.AsyncClient
) -> tuple[str, str, str]:
    """
    Search Current then Archive for a ZIP covering target_date.
    Returns (filename, base_url, kind).
    """
    last_filenames: list[str] = []
    for url in [AEMO_CURRENT_URL, AEMO_ARCHIVE_URL]:
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


def _extract_csv_from_zip(
    zip_content: bytes,
    filename: str,
    date_str: str,
    target_date: date,
) -> bytes:
    """
    Extract the daily CSV bytes from a downloaded ZIP.

    fpp_bundle:   outer ZIP contains per-day CSVs directly (filter by date_str).
    fppmw_daily:  outer ZIP → one or more inner ZIPs → CSV(s).
                  The second 12-hour half of a NEM market day (16:00–04:00 AEST)
                  is often packaged in a separate inner ZIP whose name carries the
                  next calendar date.  We therefore open ALL inner ZIPs and
                  concatenate every CSV found — the data_processor applies the
                  authoritative [04:00 AEST D, 04:00 AEST D+1) boundary filter.
    """
    zip_bytes = io.BytesIO(zip_content)
    with zipfile.ZipFile(zip_bytes) as outer_zf:
        all_entries = outer_zf.namelist()
        all_csvs = sorted(n for n in all_entries if n.lower().endswith(".csv"))

        if all_csvs:
            # fpp_bundle: outer ZIP contains per-day CSVs directly.
            # Filter to the target date so we don't pick up neighbouring days.
            daily_csvs = sorted(n for n in all_csvs if date_str in n)
            if not daily_csvs:
                if len(all_csvs) == 1:
                    daily_csvs = all_csvs
                else:
                    all_csvs_sorted = sorted(all_csvs, reverse=True)
                    logger.warning(
                        "No CSV matching %s in %s; available: %s",
                        date_str, filename, all_csvs_sorted[:5],
                    )
                    raise AEMOFetchError(
                        f"No data file found for {target_date.strftime('%d %B %Y')} "
                        "inside the archive ZIP. Available dates: "
                        + ", ".join(
                            m.group(1)
                            for n in all_csvs_sorted[:5]
                            for m in [re.search(r'(\d{8})', n)]
                            if m
                        )
                    )
            logger.info("Extracting CSV(s): %s", daily_csvs)
            return b"".join(outer_zf.read(name) for name in daily_csvs)

        # No direct CSVs — FPPMW daily: outer ZIP wraps one or more inner ZIPs.
        # Each inner ZIP typically holds one 12-hour half of the market day;
        # read ALL of them to capture the full 24-hour period.
        inner_zips = sorted(n for n in all_entries if n.lower().endswith(".zip"))
        if not inner_zips:
            raise AEMOFetchError("ZIP file contained no CSV files or inner ZIPs.")

        all_csv_bytes: list[bytes] = []
        for inner_zip_name in inner_zips:
            logger.info("Opening inner ZIP: %s", inner_zip_name)
            with zipfile.ZipFile(io.BytesIO(outer_zf.read(inner_zip_name))) as inner_zf:
                inner_csvs = sorted(
                    n for n in inner_zf.namelist() if n.lower().endswith(".csv")
                )
                logger.info("Extracting from %s: %s", inner_zip_name, inner_csvs)
                for csv_name in inner_csvs:
                    all_csv_bytes.append(inner_zf.read(csv_name))

        if not all_csv_bytes:
            raise AEMOFetchError("Inner ZIP(s) contained no CSV files.")
        return b"".join(all_csv_bytes)


async def _fetch_fppmw_monthly_csv(bundle_url: str, date_str: str) -> bytes:
    """
    Extract one day's CSV from a large FPPMW monthly archive bundle.

    Uses HTTP Range requests via remotezip so only the ZIP central directory
    and the specific daily ZIP entry are transferred — not the full multi-GB
    bundle.

    Bundle structure:
      PUBLIC_NEXT_DAY_FPPMW_YYYYMMDD.zip          ← several GB
        PUBLIC_NEXT_DAY_FPPMW_YYYYMMDD_XXXXXXXX.ZIP  ← daily ZIP (~MB)
          PUBLIC_NEXT_DAY_FPPMW_YYYYMMDD_XXXXXXXX.CSV
    """
    def _sync_extract() -> bytes:
        logger.info("Opening FPPMW monthly bundle via HTTP Range: %s", bundle_url)
        with RemoteZip(bundle_url, headers=_HEADERS) as rz:
            daily_zips = [
                name for name in rz.namelist()
                if date_str in name and name.upper().endswith(".ZIP")
            ]
            if not daily_zips:
                raise AEMOFetchError(
                    f"No entry found for {date_str} in the FPPMW monthly archive."
                )
            daily_zip_name = daily_zips[0]
            logger.info("Downloading daily ZIP entry from bundle: %s", daily_zip_name)
            daily_zip_bytes = io.BytesIO(rz.read(daily_zip_name))

        with zipfile.ZipFile(daily_zip_bytes) as daily_zf:
            csvs = sorted(n for n in daily_zf.namelist() if n.lower().endswith(".csv"))
            if not csvs:
                raise AEMOFetchError("Daily ZIP from monthly bundle contains no CSV.")
            logger.info("Extracting CSV(s): %s", csvs)
            return b"".join(daily_zf.read(name) for name in csvs)

    try:
        return await asyncio.to_thread(_sync_extract)
    except AEMOFetchError:
        raise
    except Exception as exc:
        raise AEMOFetchError(
            f"Failed to read FPPMW monthly archive: {exc}"
        ) from exc


async def fetch_csv_for_date(
    target_date: date,
    duid: str,
    *,
    skip_future_check: bool = False,
) -> bytes:
    """
    Download FPPDAILY data for target_date and return raw CSV bytes.
    Raises AEMOFetchError if unavailable.

    skip_future_check: set True when fetching D+1 for 24-hour coverage;
    suppresses the "today or future" guard so the call can proceed if the
    next-day file happens to be published already.
    """
    if target_date < DATA_START_DATE:
        raise AEMOFetchError(
            f"Data is only available from {DATA_START_DATE.strftime('%d %B %Y')} "
            "when the FPP scheme commenced."
        )
    if not skip_future_check and target_date >= date.today():
        raise AEMOFetchError("Cannot request data for today or future dates.")

    date_str = _date_str(target_date)

    async with httpx.AsyncClient() as client:
        filename, found_url, kind = await _find_file(target_date, date_str, client)
        zip_url = found_url + filename

        # FPPMW monthly bundles are several GB — extract via HTTP Range only.
        if kind == "fppmw_monthly":
            return await _fetch_fppmw_monthly_csv(zip_url, date_str)

        # fpp_bundle and fppmw_daily: download the (daily-sized) ZIP file.
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

        try:
            return _extract_csv_from_zip(resp.content, filename, date_str, target_date)
        except zipfile.BadZipFile:
            raise AEMOFetchError(
                "Downloaded file appears to be corrupt. Please try again."
            )
