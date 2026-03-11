"""
REST API endpoints for BESS SCADA data.
"""
import json
import logging
from datetime import date, datetime, timedelta
from pathlib import Path

logger = logging.getLogger(__name__)

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import Response

from app.config import ANALYTICS_TOKEN, DATA_START_DATE, MAX_DAYS_PER_REQUEST
from app.services.aemo_fetcher import AEMOFetchError, fetch_csv_for_date
from app.services.analytics import get_stats, log_request
from app.services.gen_info_fetcher import fetch_bess_list
from app.services.data_processor import (
    DataProcessingError,
    compute_summary,
    filter_and_process,
    to_csv_bytes,
    to_json_records,
    to_parquet_bytes,
)

router = APIRouter(prefix="/api")

DATA_DIR = Path(__file__).parent.parent / "data"


def _get_ip(request: Request) -> str:
    forwarded = request.headers.get("X-Forwarded-For")
    if forwarded:
        return forwarded.split(",")[0].strip()
    return request.client.host if request.client else "unknown"


def _parse_date(date_str: str) -> date:
    try:
        return datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD.")


@router.get("/bess")
async def get_bess_list():
    """Return in-service battery storage units grouped by state, sourced from the
    AEMO NEM Generation Information XLSX (cached 24 h; falls back to static JSON)."""
    return await fetch_bess_list()


@router.get("/quality-flags")
def get_quality_flags():
    """Return MW_QUALITY_FLAG descriptions."""
    flags_file = DATA_DIR / "quality_flags.json"
    return json.loads(flags_file.read_text())


async def _fetch_day_pair(target_date: date, duid: str) -> tuple[bytes, bytes | None]:
    """
    Fetch CSV bytes for target_date and, if available, for target_date + 1 day.

    The NEM market day (04:00 UTC → 04:00 UTC next day) spans two calendar-day
    files, so both are needed for complete 24-hour coverage.  The next-day file
    is fetched with skip_future_check=True (it may be today's date) and any
    error is silently swallowed — partial data is acceptable.
    """
    import asyncio as _asyncio
    csv_bytes = await fetch_csv_for_date(target_date, duid)

    next_date = target_date + timedelta(days=1)
    csv_next: bytes | None = None
    try:
        csv_next = await fetch_csv_for_date(next_date, duid, skip_future_check=True)
    except Exception as exc:
        logger.debug("Next-day CSV not available for %s (%s): %s", duid, next_date, exc)

    return csv_bytes, csv_next


@router.get("/data")
async def get_data(
    request: Request,
    duid: str = Query(..., description="BESS DUID identifier"),
    date: str = Query(..., description="Date in YYYY-MM-DD format"),
):
    """
    Fetch and return filtered SCADA data as JSON (for display).
    Returns up to 5000 rows for charting; use /download endpoints for full data.
    """
    target_date = _parse_date(date)
    ip = _get_ip(request)

    try:
        csv_bytes, csv_next = await _fetch_day_pair(target_date, duid)
        df = filter_and_process(csv_bytes, duid, target_date, csv_next)
        summary = compute_summary(df)
        records = to_json_records(df)
        log_request(ip, duid, date, "view")
        return {
            "duid": duid,
            "date": date,
            "total_rows": len(df),
            "displayed_rows": len(records),
            "summary": summary,
            "data": records,
        }
    except AEMOFetchError as e:
        raise HTTPException(status_code=503, detail=str(e))
    except DataProcessingError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.get("/download/csv")
async def download_csv(
    request: Request,
    duid: str = Query(...),
    date: str = Query(...),
):
    """Download full filtered data as CSV."""
    target_date = _parse_date(date)
    ip = _get_ip(request)

    try:
        csv_bytes, csv_next = await _fetch_day_pair(target_date, duid)
        df = filter_and_process(csv_bytes, duid, target_date, csv_next)
        output = to_csv_bytes(df)
        log_request(ip, duid, date, "download_csv")
        filename = f"BESS_SCADA_{duid}_{date}.csv"
        return Response(
            content=output,
            media_type="text/csv",
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )
    except AEMOFetchError as e:
        raise HTTPException(status_code=503, detail=str(e))
    except DataProcessingError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.get("/download/parquet")
async def download_parquet(
    request: Request,
    duid: str = Query(...),
    date: str = Query(...),
):
    """Download full filtered data as Parquet."""
    target_date = _parse_date(date)
    ip = _get_ip(request)

    try:
        csv_bytes, csv_next = await _fetch_day_pair(target_date, duid)
        df = filter_and_process(csv_bytes, duid, target_date, csv_next)
        output = to_parquet_bytes(df)
        log_request(ip, duid, date, "download_parquet")
        filename = f"BESS_SCADA_{duid}_{date}.parquet"
        return Response(
            content=output,
            media_type="application/octet-stream",
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )
    except AEMOFetchError as e:
        raise HTTPException(status_code=503, detail=str(e))
    except DataProcessingError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.get("/analytics")
def analytics(token: str = Query(...)):
    """Admin-only analytics endpoint."""
    if token != ANALYTICS_TOKEN:
        raise HTTPException(status_code=403, detail="Invalid token.")
    return get_stats()


@router.get("/info")
def info():
    """Return app metadata for the frontend."""
    return {
        "data_start_date": DATA_START_DATE.isoformat(),
        "max_days_per_request": MAX_DAYS_PER_REQUEST,
    }
