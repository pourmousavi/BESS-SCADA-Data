"""
REST API endpoints for BESS SCADA data and dispatch energy data.
"""
import json
import logging
import time
from datetime import date, datetime
from pathlib import Path

logger = logging.getLogger(__name__)

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import Response

from app.config import (
    ANALYTICS_TOKEN,
    DATA_START_DATE,
    DISPATCH_START_DATE,
    FPPMW_CUTOVER_DATE,
    MAX_DAYS_PER_REQUEST,
)
from app.services.aemo_fetcher import AEMOFetchError, fetch_csv_for_date
from app.services.analytics import get_stats, get_timing_estimate, log_request
from app.services.dispatch_fetcher import DispatchFetchError, fetch_dispatch_csv_for_date
from app.services.dispatch_processor import (
    DispatchProcessingError,
    compute_dispatch_summary,
    filter_and_process_dispatch,
    to_csv_bytes as dispatch_to_csv_bytes,
    to_json_records as dispatch_to_json_records,
    to_parquet_bytes as dispatch_to_parquet_bytes,
)
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
        raise HTTPException(
            status_code=400, detail="Invalid date format. Use YYYY-MM-DD."
        )


def _source_type(target_date: date) -> str:
    """'current' for SCADA dates from the Current directory, 'archive' otherwise."""
    return "current" if target_date >= FPPMW_CUTOVER_DATE else "archive"


# ── BESS list ─────────────────────────────────────────────────────────────────

@router.get("/bess")
async def get_bess_list():
    """Return in-service battery storage units grouped by state."""
    return await fetch_bess_list()


# ── Quality flags ─────────────────────────────────────────────────────────────

@router.get("/quality-flags")
def get_quality_flags():
    """Return MW_QUALITY_FLAG descriptions."""
    flags_file = DATA_DIR / "quality_flags.json"
    return json.loads(flags_file.read_text())


# ── SCADA data ────────────────────────────────────────────────────────────────

async def _fetch_day_csv(target_date: date, duid: str) -> bytes:
    return await fetch_csv_for_date(target_date, duid)


@router.get("/data")
async def get_data(
    request: Request,
    duid: str = Query(..., description="BESS DUID identifier"),
    date: str = Query(..., description="Date in YYYY-MM-DD format"),
):
    """Fetch and return filtered 4-second SCADA data as JSON."""
    target_date = _parse_date(date)
    ip = _get_ip(request)
    src = _source_type(target_date)

    t0 = time.monotonic()
    try:
        csv_bytes = await _fetch_day_csv(target_date, duid)
        df = filter_and_process(csv_bytes, duid, target_date)
        duration_ms = int((time.monotonic() - t0) * 1000)
        summary = compute_summary(df)
        records = to_json_records(df)
        log_request(ip, duid, date, "view", duration_ms=duration_ms, source_type=src)
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
    """Download full filtered SCADA data as CSV."""
    target_date = _parse_date(date)
    ip = _get_ip(request)
    src = _source_type(target_date)

    t0 = time.monotonic()
    try:
        csv_bytes = await _fetch_day_csv(target_date, duid)
        df = filter_and_process(csv_bytes, duid, target_date)
        duration_ms = int((time.monotonic() - t0) * 1000)
        output = to_csv_bytes(df)
        log_request(ip, duid, date, "download_csv", duration_ms=duration_ms, source_type=src)
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
    """Download full filtered SCADA data as Parquet."""
    target_date = _parse_date(date)
    ip = _get_ip(request)
    src = _source_type(target_date)

    t0 = time.monotonic()
    try:
        csv_bytes = await _fetch_day_csv(target_date, duid)
        df = filter_and_process(csv_bytes, duid, target_date)
        duration_ms = int((time.monotonic() - t0) * 1000)
        output = to_parquet_bytes(df)
        log_request(ip, duid, date, "download_parquet", duration_ms=duration_ms, source_type=src)
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


# ── Dispatch / energy data ────────────────────────────────────────────────────

@router.get("/energy-data")
async def get_energy_data(
    request: Request,
    duid: str = Query(..., description="BESS DUID identifier"),
    date: str = Query(..., description="Date in YYYY-MM-DD format"),
):
    """Fetch and return filtered 5-minute dispatch energy data as JSON."""
    target_date = _parse_date(date)
    ip = _get_ip(request)

    t0 = time.monotonic()
    try:
        csv_bytes, src = await fetch_dispatch_csv_for_date(target_date)
        df = filter_and_process_dispatch(csv_bytes, duid, target_date)
        duration_ms = int((time.monotonic() - t0) * 1000)
        summary = compute_dispatch_summary(df)
        records = dispatch_to_json_records(df)
        log_request(ip, duid, date, "view", duration_ms=duration_ms, source_type=src)
        return {
            "duid": duid,
            "date": date,
            "total_rows": len(df),
            "summary": summary,
            "data": records,
        }
    except DispatchFetchError as e:
        raise HTTPException(status_code=503, detail=str(e))
    except DispatchProcessingError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.get("/download/energy-csv")
async def download_energy_csv(
    request: Request,
    duid: str = Query(...),
    date: str = Query(...),
):
    """Download full filtered dispatch energy data as CSV."""
    target_date = _parse_date(date)
    ip = _get_ip(request)

    t0 = time.monotonic()
    try:
        csv_bytes, src = await fetch_dispatch_csv_for_date(target_date)
        df = filter_and_process_dispatch(csv_bytes, duid, target_date)
        duration_ms = int((time.monotonic() - t0) * 1000)
        output = dispatch_to_csv_bytes(df)
        log_request(ip, duid, date, "download_energy_csv",
                    duration_ms=duration_ms, source_type=src)
        filename = f"BESS_Energy_{duid}_{date}.csv"
        return Response(
            content=output,
            media_type="text/csv",
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )
    except DispatchFetchError as e:
        raise HTTPException(status_code=503, detail=str(e))
    except DispatchProcessingError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.get("/download/energy-parquet")
async def download_energy_parquet(
    request: Request,
    duid: str = Query(...),
    date: str = Query(...),
):
    """Download full filtered dispatch energy data as Parquet."""
    target_date = _parse_date(date)
    ip = _get_ip(request)

    t0 = time.monotonic()
    try:
        csv_bytes, src = await fetch_dispatch_csv_for_date(target_date)
        df = filter_and_process_dispatch(csv_bytes, duid, target_date)
        duration_ms = int((time.monotonic() - t0) * 1000)
        output = dispatch_to_parquet_bytes(df)
        log_request(ip, duid, date, "download_energy_parquet",
                    duration_ms=duration_ms, source_type=src)
        filename = f"BESS_Energy_{duid}_{date}.parquet"
        return Response(
            content=output,
            media_type="application/octet-stream",
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )
    except DispatchFetchError as e:
        raise HTTPException(status_code=503, detail=str(e))
    except DispatchProcessingError as e:
        raise HTTPException(status_code=404, detail=str(e))


# ── Admin ─────────────────────────────────────────────────────────────────────

@router.get("/analytics")
def analytics(token: str = Query(...)):
    """Admin-only analytics endpoint."""
    if token != ANALYTICS_TOKEN:
        raise HTTPException(status_code=403, detail="Invalid token.")
    return get_stats()


# ── App info ──────────────────────────────────────────────────────────────────

@router.get("/info")
def info():
    """Return app metadata and timing estimates for the frontend."""
    return {
        "data_start_date":     DATA_START_DATE.isoformat(),
        "dispatch_start_date": DISPATCH_START_DATE.isoformat(),
        "cutover_date":        FPPMW_CUTOVER_DATE.isoformat(),
        "max_days_per_request": MAX_DAYS_PER_REQUEST,
        "estimates": {
            "current":          get_timing_estimate("current"),
            "archive":          get_timing_estimate("archive"),
            "dispatch_current": get_timing_estimate("dispatch_current"),
            "dispatch_archive": get_timing_estimate("dispatch_archive"),
        },
    }
