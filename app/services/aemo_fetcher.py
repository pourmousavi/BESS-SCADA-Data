"""
Downloads and extracts FPPDAILY ZIP files from AEMO NEMWEB.

File naming conventions on NEMWEB:

  Up to 10 Jan 2026 — FPP format:
    PUBLIC_NEXT_DAY_FPP_YYYYMMDD[_<suffix>].zip
    ZIP contains the daily CSV directly.

  From 11 Jan 2026 (Current dir) and from ~Sep 2025 (Archive) — FPPMW format:
    Each NEM market day (04:00–04:00 AEST) is split into two 12-hour halves,
    each published as a separate outer ZIP.  CRITICAL: the YYYYMMDD embedded in
    FPPMW filenames is the PUBLICATION date (D+1), not the settlement date D.

      Format-1 — first half (04:00–16:00 AEST, published next calendar day):
        PUBLIC_NEXT_DAY_FPPMW_YYYYMMDD[_<seq>].zip
        YYYYMMDD = D+1 (publication date)

      Format-2 — second half (16:00–04:00 AEST, also published on D+1):
        PUBLIC_NEXT_DAY_FPPMW_2_YYYYMMDD[HHMMSS][_<seq>].zip
        YYYYMMDD = D+1 (same publication date as first half)

    To fetch all data for settlement date D, search for pub_date_str = D+1.
    This captures both Format-1 and Format-2 files in a single substring search.

    Inner ZIP structure (each outer ZIP):
      PUBLIC_NEXT_DAY_FPPMW_*.zip   ← outer ZIP (~MB)
        PUBLIC_NEXT_DAY_FPPMW_*.ZIP ← inner ZIP
          PUBLIC_NEXT_DAY_FPPMW_*.CSV

Current directory (https://www.nemweb.com.au/REPORTS/Current/FPPDAILY/):
  Individual daily files: PUBLIC_NEXT_DAY_FPPMW_YYYYMMDD_<seq>.zip
  YYYYMMDD = publication date (D+1).  Found by exact pub_date_str match.

Archive directory (https://nemweb.com.au/Reports/Archive/FPPDAILY/):

  FPP monthly bundles (data up to ~Mar 2025):
    PUBLIC_NEXT_DAY_FPP_YYYYMMDD.zip
    YYYYMMDD = bundle start date.  Contains daily CSVs directly.
    Select by largest start-date <= target.

  FPPMW bundles (Mar 2025 – Jan 2026) — end with _YYYYMMDD.zip (no seq suffix):
    Format-1 monthly: PUBLIC_NEXT_DAY_FPPMW_YYYYMMDD.zip
      YYYYMMDD = publication date of the first entry in the bundle.
      e.g. 20250228 → bundle start = 1 Mar 2025 pub (= settlement 28 Feb)
    Format-1 weekly:  PUBLIC_NEXT_DAY_FPPMW_YYYYMMDD.zip  (Aug 2025+)
    Format-2 weekly:  PUBLIC_NEXT_DAY_FPPMW_2_YYYYMMDD.zip (Sep 2025+)
      Both cover the same pub-date range as their corresponding Format-1 bundle.

    Bundle selection: largest bundle_date <= pub_date, separately for each format.
    Inner ZIP search: use pub_date_str (D+1) to find the target day's inner ZIP.
    HTTP Range requests (remotezip) are used to avoid downloading the full bundle.
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


def _is_fppmw_bundle(filename: str) -> bool:
    """
    True if filename is an FPPMW bundle (monthly or weekly archive).

    Bundles end with _YYYYMMDD.zip — no sequence number or timestamp suffix
    after the 8-digit date.  Individual daily files always have a suffix
    (_<seq> or _YYYYMMDDHHMMSS_<seq>) after the date.
    """
    return bool(re.search(r'FPPMW.*_\d{8}\.zip$', filename, re.IGNORECASE))


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
    except httpx.HTTPError as e:
        raise AEMOFetchError(f"Network error fetching directory listing: {e}")

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


async def _find_files_in(
    filenames: list[str],
    target_date: date,
    pub_date_str: str,
    url: str,
) -> list[tuple[str, str, str]]:
    """
    Return a list of (filename, url, kind) for every ZIP that covers target_date.

    FPPMW files use the PUBLICATION date (D+1) in their filename, not the
    settlement date D.  pub_date_str must therefore be _date_str(target_date + 1).

    Two file styles exist:

      Individual daily files (Current dir, and recent archive copies):
        PUBLIC_NEXT_DAY_FPPMW_YYYYMMDD_<seq>.zip         ← Format-1
        PUBLIC_NEXT_DAY_FPPMW_2_YYYYMMDDHHMMSS_<seq>.zip ← Format-2
        Both embed pub_date_str in their name → one substring search finds both.

      Archive bundles (monthly Mar 2025 – Jan 2026, weekly Aug 2025+):
        PUBLIC_NEXT_DAY_FPPMW_YYYYMMDD.zip      ← Format-1 bundle
        PUBLIC_NEXT_DAY_FPPMW_2_YYYYMMDD.zip    ← Format-2 bundle
        Bundles end with _YYYYMMDD.zip (no suffix after date).
        YYYYMMDD = publication date of the first entry in the bundle.
        Select the latest bundle whose date <= pub_date, independently for
        each format, so both 12-hour halves are retrieved when separate
        Format-2 bundles exist (Sep 2025+).

    kind values
    -----------
    "fppmw_daily"    Individual FPPMW file (Current dir or archive copy).
                     Structure: outer ZIP → inner ZIP(s) → CSV(s).
    "fppmw_monthly"  FPPMW archive bundle (monthly or weekly).
                     Structure: outer ZIP → per-pub-date inner ZIPs → CSV.
                     Uses HTTP Range requests to avoid full download.
    """
    # Only FPPMW files are used. FPP files lack MEASUREMENT_DATETIME,
    # MEASURED_MW, and MW_QUALITY_FLAG and cannot supply SCADA data.

    pub_date = target_date + timedelta(days=1)

    # 1. Exact daily match: non-bundle FPPMW files containing pub_date_str.
    #    Both Format-1 (_YYYYMMDD_<seq>) and Format-2 (_2_YYYYMMDDHHMMSS_<seq>)
    #    embed pub_date_str, so one substring search captures both halves.
    fppmw_exact = sorted(
        f for f in filenames
        if "FPPMW" in f and pub_date_str in f and not _is_fppmw_bundle(f)
    )
    if fppmw_exact:
        logger.info("Exact match(es) at %s: %s", url, fppmw_exact)
        return [(f, url, "fppmw_daily") for f in fppmw_exact]

    # 2. Archive bundle match: find the best Format-1 and Format-2 bundle
    #    independently.  A bundle's date is the pub date of its first entry,
    #    so select the latest bundle_date <= pub_date for each format.
    f1_candidates: list[tuple[date, str]] = []
    f2_candidates: list[tuple[date, str]] = []
    for f in filenames:
        if not _is_fppmw_bundle(f):
            continue
        zip_date = _extract_zip_date(f)
        if zip_date is None or zip_date > pub_date:
            continue
        if re.search(r'FPPMW_2_', f, re.IGNORECASE):
            f2_candidates.append((zip_date, f))
        else:
            f1_candidates.append((zip_date, f))

    results: list[tuple[str, str, str]] = []
    if f1_candidates:
        f1_candidates.sort(reverse=True)
        best_f1 = f1_candidates[0][1]
        logger.info("Format-1 bundle match at %s: %s (pub_date %s)", url, best_f1, pub_date_str)
        results.append((best_f1, url, "fppmw_monthly"))
    if f2_candidates:
        f2_candidates.sort(reverse=True)
        best_f2 = f2_candidates[0][1]
        logger.info("Format-2 bundle match at %s: %s (pub_date %s)", url, best_f2, pub_date_str)
        results.append((best_f2, url, "fppmw_monthly"))

    return results


async def _find_files(
    target_date: date, client: httpx.AsyncClient
) -> tuple[list[tuple[str, str, str]], str]:
    """
    Search Current then Archive for all ZIPs covering target_date.
    Returns (list of (filename, base_url, kind), pub_date_str).
    pub_date_str is the publication date string (target_date + 1 day).
    """
    pub_date_str = _date_str(target_date + timedelta(days=1))
    last_filenames: list[str] = []
    for url in [AEMO_CURRENT_URL, AEMO_ARCHIVE_URL]:
        try:
            filenames = await _list_directory(url, client)
        except AEMOFetchError as e:
            logger.warning("Could not list %s: %s", url, e)
            continue

        last_filenames = filenames
        results = await _find_files_in(filenames, target_date, pub_date_str, url)
        if results:
            return results, pub_date_str

    logger.error(
        "No file found for %s (pub_date_str=%s). Last directory listing sample: %s",
        target_date, pub_date_str, last_filenames[:10],
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


async def _fetch_fppmw_monthly_csv(bundle_url: str, pub_date_str: str) -> bytes:
    """
    Extract one NEM market day's CSV data from an FPPMW archive bundle.

    Uses HTTP Range requests via remotezip so only the ZIP central directory
    and the required daily ZIP entries are transferred — not the full bundle.

    pub_date_str must be the PUBLICATION date (settlement date + 1 day),
    because FPPMW inner ZIPs embed the publication date in their names,
    not the settlement date.

    Bundle structure (Format-1 only, e.g. monthly Apr–Aug 2025):
      PUBLIC_NEXT_DAY_FPPMW_YYYYMMDD.zip
        PUBLIC_NEXT_DAY_FPPMW_<pub_date>_XXXXXXXX.ZIP   ← first-half inner ZIP
          PUBLIC_NEXT_DAY_FPPMW_<pub_date>_XXXXXXXX.CSV

    Bundle structure (Format-1 + Format-2, Sep 2025+):
      PUBLIC_NEXT_DAY_FPPMW_YYYYMMDD.zip
        PUBLIC_NEXT_DAY_FPPMW_<pub_date>_XXXXXXXX.ZIP           ← first half
      PUBLIC_NEXT_DAY_FPPMW_2_YYYYMMDD.zip
        PUBLIC_NEXT_DAY_FPPMW_2_<pub_date>HHMMSS_XXXXXXXX.ZIP  ← second half

    A single pub_date_str substring search finds all matching inner ZIPs;
    all are extracted and their CSVs concatenated to cover the half-day in
    this bundle (the other half is handled by the sibling bundle call).
    """
    def _sync_extract() -> bytes:
        logger.info("Opening FPPMW archive bundle via HTTP Range: %s", bundle_url)
        with RemoteZip(bundle_url, headers=_HEADERS) as rz:
            all_names = rz.namelist()
            daily_zips = [
                name for name in all_names
                if name.upper().endswith(".ZIP") and pub_date_str in name
            ]
            if not daily_zips:
                raise AEMOFetchError(
                    f"No entry found for pub_date {pub_date_str} in the FPPMW archive bundle."
                )
            logger.info(
                "Downloading %d inner ZIP(s) from bundle: %s", len(daily_zips), daily_zips
            )

            all_csv_bytes: list[bytes] = []
            for daily_zip_name in sorted(daily_zips):  # sort: first half before second
                daily_zip_bytes = io.BytesIO(rz.read(daily_zip_name))
                with zipfile.ZipFile(daily_zip_bytes) as daily_zf:
                    csvs = sorted(
                        n for n in daily_zf.namelist() if n.lower().endswith(".csv")
                    )
                    if not csvs:
                        logger.warning(
                            "Inner ZIP %s from monthly bundle contains no CSV; skipping.",
                            daily_zip_name,
                        )
                        continue
                    logger.info("Extracting CSV(s) from %s: %s", daily_zip_name, csvs)
                    for csv_name in csvs:
                        all_csv_bytes.append(daily_zf.read(csv_name))

        if not all_csv_bytes:
            raise AEMOFetchError(
                "All inner ZIPs from monthly bundle contained no CSV files."
            )
        return b"".join(all_csv_bytes)

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
    Download ALL FPPDAILY files for target_date and return concatenated CSV bytes.

    FPPMW filenames embed the PUBLICATION date (D+1), not the settlement date D.
    _find_files searches using pub_date_str = D+1, finding both Format-1 and
    Format-2 files.  All matches are downloaded and their CSV bytes concatenated.
    The data_processor applies the [04:00 AEST D, 04:00 AEST D+1) boundary.

    skip_future_check: set True to bypass the future-date guard.
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
        file_list, pub_date_str = await _find_files(target_date, client)

        csv_parts: list[bytes] = []
        for filename, found_url, kind in file_list:
            zip_url = found_url + filename

            # FPPMW archive bundles: extract via HTTP Range using pub_date_str
            # to locate the target day's inner ZIP(s) within the bundle.
            if kind == "fppmw_monthly":
                csv_parts.append(await _fetch_fppmw_monthly_csv(zip_url, pub_date_str))
                continue

            # fppmw_daily: download the (daily-sized) ZIP file.
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
            except httpx.HTTPError as e:
                raise AEMOFetchError(
                    f"Network error while downloading data file: {e}"
                )

            try:
                csv_parts.append(
                    _extract_csv_from_zip(resp.content, filename, date_str, target_date)
                )
            except zipfile.BadZipFile:
                raise AEMOFetchError(
                    "Downloaded file appears to be corrupt. Please try again."
                )

        return b"".join(csv_parts)
