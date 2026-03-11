"""
Downloads and extracts FPPDAILY ZIP files from AEMO NEMWEB.
"""
import io
import zipfile
from datetime import date, datetime, timedelta

import httpx

from app.config import (
    AEMO_ARCHIVE_URL,
    AEMO_CONNECT_TIMEOUT,
    AEMO_CURRENT_URL,
    AEMO_READ_TIMEOUT,
    DATA_START_DATE,
)


class AEMOFetchError(Exception):
    """Raised when data cannot be retrieved from AEMO."""
    pass


def _is_current(target_date: date) -> bool:
    """Files within the last ~7 days are in /Current/, older in /Archive/."""
    return (date.today() - target_date).days <= 7


async def _list_directory(base_url: str, client: httpx.AsyncClient) -> list[str]:
    """Fetch the HTML directory listing and extract filenames."""
    try:
        resp = await client.get(
            base_url,
            timeout=httpx.Timeout(AEMO_CONNECT_TIMEOUT, read=AEMO_READ_TIMEOUT),
            follow_redirects=True,
        )
        resp.raise_for_status()
    except httpx.TimeoutException:
        raise AEMOFetchError("AEMO server timed out. Please try again in a few minutes.")
    except httpx.HTTPStatusError as e:
        raise AEMOFetchError(f"AEMO server returned error {e.response.status_code}.")

    # Extract href links from directory listing (simple parse, no BeautifulSoup needed)
    filenames = []
    for line in resp.text.splitlines():
        if 'href="' in line and ".zip" in line.lower():
            start = line.index('href="') + 6
            end = line.index('"', start)
            href = line[start:end]
            if href.lower().endswith(".zip"):
                filenames.append(href.split("/")[-1])
    return filenames


def _date_str(target_date: date) -> str:
    """Return date as YYYYMMDD string."""
    return target_date.strftime("%Y%m%d")


async def fetch_csv_for_date(target_date: date, duid: str) -> bytes:
    """
    Download FPPDAILY ZIP for target_date from AEMO and return raw CSV bytes.
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
        # List the directory to find the correct filename
        try:
            filenames = await _list_directory(base_url, client)
        except AEMOFetchError:
            # Fallback: try archive if current failed
            if base_url == AEMO_CURRENT_URL:
                filenames = await _list_directory(AEMO_ARCHIVE_URL, client)
                base_url = AEMO_ARCHIVE_URL
            else:
                raise

        # Find a file matching the date
        matching = [f for f in filenames if date_str in f]
        if not matching:
            raise AEMOFetchError(
                f"No FPPDAILY data found for {target_date.strftime('%d %B %Y')}. "
                f"The file may not yet be published or the date may not have data."
            )

        # Use the first matching file
        filename = matching[0]
        zip_url = base_url + filename

        try:
            resp = await client.get(
                zip_url,
                timeout=httpx.Timeout(AEMO_CONNECT_TIMEOUT, read=AEMO_READ_TIMEOUT),
                follow_redirects=True,
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

        # Extract CSV from ZIP (may contain multiple files; pick the relevant one)
        try:
            zip_bytes = io.BytesIO(resp.content)
            with zipfile.ZipFile(zip_bytes) as zf:
                csv_names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
                if not csv_names:
                    raise AEMOFetchError("ZIP file contained no CSV files.")
                # Pick the first (usually only one) CSV
                csv_bytes = zf.read(csv_names[0])
        except zipfile.BadZipFile:
            raise AEMOFetchError("Downloaded file appears to be corrupt. Please try again.")

        return csv_bytes
