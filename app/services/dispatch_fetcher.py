"""
Downloads and extracts Next_Day_Dispatch ZIP files from AEMO NEMWEB.

File naming conventions:

  Current directory (https://nemweb.com.au/Reports/Current/Next_Day_Dispatch/):
    Individual daily files: PUBLIC_NEXT_DAY_DISPATCH_YYYYMMDD_<16-digit-seq>.zip
    YYYYMMDD = settlement date (same day as the data — NOT publication date D+1).
    Each ZIP contains one CSV (or an inner ZIP wrapping one CSV).

  Archive directory (https://www.nemweb.com.au/REPORTS/ARCHIVE/Next_Day_Dispatch/):
    Monthly bundles: PUBLIC_NEXT_DAY_DISPATCH_YYYYMMDD.zip
    YYYYMMDD = first day of the calendar month covered by the bundle.
    Each bundle contains one CSV per settlement day.
    Detected by the absence of a suffix after the 8-digit date (no _<seq>).

Strategy: try Current first for all dates >= DISPATCH_START_DATE.
          Fall back to Archive if not found in Current.
          Use HTTP Range requests (remotezip) for archive bundles to avoid
          downloading the full monthly file.
"""
import asyncio
import io
import logging
import re
import zipfile
from datetime import date, datetime

import httpx
from remotezip import RemoteZip

from app.config import (
    AEMO_CONNECT_TIMEOUT,
    AEMO_READ_TIMEOUT,
    DISPATCH_ARCHIVE_URL,
    DISPATCH_CURRENT_URL,
    DISPATCH_START_DATE,
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


class DispatchFetchError(Exception):
    """Raised when dispatch data cannot be retrieved from AEMO."""
    pass


def _date_str(d: date) -> str:
    return d.strftime("%Y%m%d")


def _is_dispatch_bundle(filename: str) -> bool:
    """
    True if filename is a monthly archive bundle.
    Bundles end with _YYYYMMDD.zip — no suffix after the 8-digit date.
    Daily files always carry a long sequence number after the date, e.g.
    PUBLIC_NEXT_DAY_DISPATCH_20260311_0000000507452407.zip
    """
    return bool(re.search(r'DISPATCH.*_\d{8}\.zip$', filename, re.IGNORECASE))


def _extract_zip_date(filename: str) -> date | None:
    m = re.search(r'_(\d{8})(?:[_.]|$)', filename)
    if m:
        try:
            return datetime.strptime(m.group(1), "%Y%m%d").date()
        except ValueError:
            return None
    return None


async def _list_directory(base_url: str, client: httpx.AsyncClient) -> list[str]:
    """Fetch HTML directory listing and return all ZIP filenames found."""
    logger.info("Listing dispatch directory: %s", base_url)
    try:
        resp = await client.get(
            base_url,
            timeout=httpx.Timeout(AEMO_CONNECT_TIMEOUT, read=AEMO_READ_TIMEOUT),
            follow_redirects=True,
            headers=_HEADERS,
        )
        resp.raise_for_status()
    except httpx.TimeoutException:
        raise DispatchFetchError(
            "AEMO server timed out while listing dispatch directory. Try again."
        )
    except httpx.HTTPStatusError as e:
        raise DispatchFetchError(
            f"AEMO server returned HTTP {e.response.status_code} for dispatch directory."
        )
    except httpx.HTTPError as e:
        raise DispatchFetchError(f"Network error listing dispatch directory: {e}")

    filenames: list[str] = []
    for href in re.findall(r'href=["\']([^"\']+)["\']', resp.text, re.IGNORECASE):
        name = href.rstrip("/").split("/")[-1]
        if name.lower().endswith(".zip") and "DISPATCH" in name.upper():
            filenames.append(name)
    logger.info("Found %d dispatch ZIPs at %s", len(filenames), base_url)
    if filenames:
        logger.info("Sample: %s", filenames[:5])
    return filenames


def _find_current_file(filenames: list[str], date_str: str) -> str | None:
    """Return the daily dispatch ZIP for date_str, or None if not found."""
    matches = sorted(
        f for f in filenames
        if date_str in f and not _is_dispatch_bundle(f)
    )
    if matches:
        logger.info("Dispatch current match: %s", matches[-1])
        return matches[-1]  # take latest if multiple (higher seq = more recent publish)
    return None


def _find_archive_bundle(filenames: list[str], target_date: date) -> str | None:
    """
    Find the best monthly archive bundle covering target_date.
    Select the bundle with the largest start-date <= target_date.
    """
    candidates: list[tuple[date, str]] = []
    for f in filenames:
        if not _is_dispatch_bundle(f):
            continue
        d = _extract_zip_date(f)
        if d is None or d > target_date:
            continue
        candidates.append((d, f))
    if not candidates:
        return None
    candidates.sort(reverse=True)
    best = candidates[0][1]
    logger.info("Dispatch archive bundle selected: %s", best)
    return best


def _extract_csv_from_zip_bytes(zip_content: bytes, target_date: date) -> bytes:
    """
    Extract the settlement-day CSV from a downloaded daily dispatch ZIP.

    Handles two structures:
      1. Direct: outer ZIP → CSV file(s)
      2. Nested: outer ZIP → inner ZIP → CSV file(s)
    """
    date_str = _date_str(target_date)
    with zipfile.ZipFile(io.BytesIO(zip_content)) as outer_zf:
        entries = outer_zf.namelist()

        # Case 1: CSVs directly inside the outer ZIP
        csvs = sorted(n for n in entries if n.lower().endswith(".csv"))
        if csvs:
            target_csvs = [n for n in csvs if date_str in n] or csvs
            logger.info("Extracting dispatch CSV(s) directly: %s", target_csvs)
            return b"".join(outer_zf.read(n) for n in target_csvs)

        # Case 2: Inner ZIPs containing CSVs
        inner_zips = sorted(n for n in entries if n.lower().endswith(".zip"))
        if inner_zips:
            parts: list[bytes] = []
            for iz in inner_zips:
                logger.info("Opening inner ZIP: %s", iz)
                with zipfile.ZipFile(io.BytesIO(outer_zf.read(iz))) as izf:
                    inner_csvs = sorted(
                        n for n in izf.namelist() if n.lower().endswith(".csv")
                    )
                    for cn in inner_csvs:
                        parts.append(izf.read(cn))
            if parts:
                return b"".join(parts)

        raise DispatchFetchError(
            "Daily dispatch ZIP contained no CSV files or inner ZIPs."
        )


async def _extract_csv_from_archive(bundle_url: str, target_date: date) -> bytes:
    """
    Extract the target day's data from a monthly archive bundle using
    HTTP Range requests (remotezip) to avoid downloading the full bundle.

    The archive ZIP contains per-day entries whose names include the
    settlement date in YYYYMMDD format.  Handles both direct CSVs and
    nested inner ZIPs inside the bundle.
    """
    date_str = _date_str(target_date)

    def _sync() -> bytes:
        logger.info(
            "Opening dispatch archive via HTTP Range: %s (looking for %s)",
            bundle_url, date_str,
        )
        with RemoteZip(bundle_url, headers=_HEADERS) as rz:
            names = rz.namelist()

            # Prefer direct CSV entries for the target date
            daily_csvs = sorted(
                n for n in names if date_str in n and n.lower().endswith(".csv")
            )
            if daily_csvs:
                logger.info("Extracting from archive: %s", daily_csvs)
                return b"".join(rz.read(n) for n in daily_csvs)

            # Fall back to inner ZIPs containing the target date
            daily_zips = sorted(
                n for n in names if date_str in n and n.lower().endswith(".zip")
            )
            if daily_zips:
                parts: list[bytes] = []
                for zip_name in daily_zips:
                    logger.info("Extracting inner ZIP from archive: %s", zip_name)
                    with zipfile.ZipFile(io.BytesIO(rz.read(zip_name))) as izf:
                        for csv_name in sorted(
                            n for n in izf.namelist() if n.lower().endswith(".csv")
                        ):
                            parts.append(izf.read(csv_name))
                if parts:
                    return b"".join(parts)

            # Last resort: if only one CSV in bundle (single-month single-file)
            all_csvs = sorted(n for n in names if n.lower().endswith(".csv"))
            if len(all_csvs) == 1:
                logger.warning(
                    "Single CSV in archive bundle (no date match found for %s); "
                    "using it anyway: %s",
                    date_str, all_csvs[0],
                )
                return rz.read(all_csvs[0])

            available = sorted(
                m.group(1)
                for n in names[:20]
                for m in [re.search(r'(\d{8})', n)]
                if m
            )
            raise DispatchFetchError(
                f"No dispatch data for {target_date.strftime('%d %B %Y')} "
                f"found in the archive bundle. "
                f"Available dates (sample): {', '.join(available[:10])}"
            )

    try:
        return await asyncio.to_thread(_sync)
    except DispatchFetchError:
        raise
    except Exception as exc:
        raise DispatchFetchError(
            f"Failed to read dispatch archive bundle: {exc}"
        ) from exc


async def fetch_dispatch_csv_for_date(
    target_date: date,
) -> tuple[bytes, str]:
    """
    Download the Next_Day_Dispatch CSV for the given settlement date.

    Returns (csv_bytes, source_type) where source_type is
    'dispatch_current' (found in Current dir) or 'dispatch_archive'
    (found in Archive).

    Raises DispatchFetchError if data cannot be retrieved.
    """
    if target_date < DISPATCH_START_DATE:
        raise DispatchFetchError(
            f"Dispatch energy data is only available from "
            f"{DISPATCH_START_DATE.strftime('%d %B %Y')}."
        )
    if target_date >= date.today():
        raise DispatchFetchError(
            "Cannot request dispatch data for today or future dates."
        )

    date_str = _date_str(target_date)

    async with httpx.AsyncClient() as client:
        # ── 1. Try Current directory ─────────────────────────────────────────
        try:
            filenames = await _list_directory(DISPATCH_CURRENT_URL, client)
            filename = _find_current_file(filenames, date_str)
            if filename:
                zip_url = DISPATCH_CURRENT_URL + filename
                logger.info("Downloading dispatch (current): %s", zip_url)
                resp = await client.get(
                    zip_url,
                    timeout=httpx.Timeout(AEMO_CONNECT_TIMEOUT, read=AEMO_READ_TIMEOUT),
                    follow_redirects=True,
                    headers=_HEADERS,
                )
                resp.raise_for_status()
                csv_bytes = _extract_csv_from_zip_bytes(resp.content, target_date)
                return csv_bytes, "dispatch_current"
        except DispatchFetchError as e:
            logger.warning("Dispatch current dir failed: %s", e)
        except httpx.TimeoutException:
            logger.warning("Dispatch current dir timed out for %s", target_date)
        except httpx.HTTPStatusError as e:
            logger.warning(
                "Dispatch current dir HTTP %s for %s",
                e.response.status_code, target_date,
            )
        except Exception as e:
            logger.warning("Dispatch current dir unexpected error: %s", e)

        # ── 2. Fall back to Archive ──────────────────────────────────────────
        try:
            filenames = await _list_directory(DISPATCH_ARCHIVE_URL, client)
            bundle = _find_archive_bundle(filenames, target_date)
            if bundle:
                bundle_url = DISPATCH_ARCHIVE_URL + bundle
                csv_bytes = await _extract_csv_from_archive(bundle_url, target_date)
                return csv_bytes, "dispatch_archive"
        except DispatchFetchError as e:
            logger.warning("Dispatch archive dir failed: %s", e)
        except Exception as e:
            logger.warning("Dispatch archive dir unexpected error: %s", e)

    raise DispatchFetchError(
        f"No dispatch energy data found for {target_date.strftime('%d %B %Y')}. "
        "The file may not yet be published by AEMO, or the date may be outside "
        "the available range."
    )
