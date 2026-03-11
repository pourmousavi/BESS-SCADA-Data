"""
Filters and transforms raw AEMO FPPDAILY CSV bytes using Polars.
"""
import io
from datetime import date, datetime, time as dtime, timedelta

import polars as pl
import pyarrow.parquet as pq
import pyarrow as pa

from app.config import REQUIRED_COLUMNS


class DataProcessingError(Exception):
    pass


# NEM market day boundary: 04:00 UTC
_DAY_START_HOUR = 4


def _parse_aemo_csv(csv_bytes: bytes) -> pl.DataFrame:
    """
    AEMO CSV files have a non-standard header format:
      - "C,..." rows: comment / metadata (skipped)
      - "I,..." row:  column headers (format: I, TABLE, SUBTABLE, col1, col2, …)
      - "D,..." rows: data
    """
    text = csv_bytes.decode("utf-8", errors="replace")
    lines = text.splitlines()

    header = None
    data_lines = []

    for line in lines:
        if not line.strip():
            continue
        if line.startswith("I,"):
            parts = line.split(",")
            # Strip the leading "I, TABLE_NAME, SUBTABLE" — columns start at index 3
            if len(parts) > 3:
                header = parts[3:]
        elif line.startswith("D,"):
            parts = line.split(",")
            if len(parts) > 3:
                data_lines.append(parts[3:])

    if header is None:
        raise DataProcessingError("Could not find column header row in CSV file.")
    if not data_lines:
        raise DataProcessingError("CSV file contained no data rows.")

    n_cols = len(header)
    padded = []
    for row in data_lines:
        if len(row) >= n_cols:
            padded.append(row[:n_cols])
        else:
            padded.append(row + [""] * (n_cols - len(row)))

    csv_content = ",".join(header) + "\n" + "\n".join(",".join(r) for r in padded)
    return pl.read_csv(io.StringIO(csv_content), infer_schema_length=None)


def _find_duid_col(df: pl.DataFrame) -> str:
    """
    Return the column name used as the unit identifier.

    FPP format (up to Jan 10 2026) uses 'FPP_UNITID'.
    FPPMW format (from Jan 11 2026 / Mar 2025 archive) uses 'DUID'.
    """
    for candidate in ("FPP_UNITID", "DUID"):
        if candidate in df.columns:
            return candidate
    raise DataProcessingError(
        "Cannot find unit identifier column (expected 'FPP_UNITID' or 'DUID'). "
        "AEMO may have changed the file format."
    )


def _parse_and_filter_duid(csv_bytes: bytes, duid: str) -> pl.DataFrame:
    """Parse CSV bytes, filter to the given DUID, return raw (uncast) DataFrame."""
    df = _parse_aemo_csv(csv_bytes)

    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise DataProcessingError(
            f"CSV missing expected columns: {missing}. "
            "AEMO may have changed the file format."
        )

    duid_col = _find_duid_col(df)
    return df.filter(pl.col(duid_col) == duid).select(REQUIRED_COLUMNS)


def filter_and_process(
    csv_bytes: bytes,
    duid: str,
    target_date: date,
    csv_bytes_next: bytes | None = None,
) -> pl.DataFrame:
    """
    Parse and filter CSV data to the requested DUID, covering the full NEM
    market day: 04:00 UTC on target_date → 04:00 UTC on target_date + 1 day.

    csv_bytes_next: optional CSV for target_date + 1 day, kept as a safety
    net for older single-CSV files.  Since Jan 2026 FPPMW daily ZIPs may
    already include both 12-hour halves; any overlap is removed by dedup.
    """
    df = _parse_and_filter_duid(csv_bytes, duid)

    if csv_bytes_next is not None:
        try:
            df_next = _parse_and_filter_duid(csv_bytes_next, duid)
            df = pl.concat([df, df_next])
        except Exception as exc:
            # Non-fatal: proceed with partial data if next-day file is
            # unavailable or malformed.
            import logging
            logging.getLogger(__name__).warning(
                "Could not merge next-day CSV; using partial data: %s", exc
            )

    if df.is_empty():
        raise DataProcessingError(
            f"No data found for DUID '{duid}' on this date. "
            "This unit may not have been operational or eligible for FPP on this date."
        )

    # Cast to proper types
    df = df.with_columns([
        pl.col("INTERVAL_DATETIME").str.to_datetime(
            format="%Y/%m/%d %H:%M:%S", strict=False
        ),
        pl.col("MEASUREMENT_DATETIME").str.to_datetime(
            format="%Y/%m/%d %H:%M:%S", strict=False
        ),
        pl.col("MEASURED_MW").cast(pl.Float64, strict=False),
        pl.col("MW_QUALITY_FLAG").cast(pl.Int32, strict=False),
    ])

    # Deduplicate on MEASUREMENT_DATETIME (boundary rows may appear in both
    # the D and D+1 files) then sort chronologically.  No time-boundary
    # filter is applied — the raw CSV contents define the coverage window.
    return (
        df.unique(subset=["MEASUREMENT_DATETIME"], keep="first")
          .sort("MEASUREMENT_DATETIME")
    )


def compute_summary(df: pl.DataFrame) -> dict:
    """Compute summary statistics for the filtered data."""
    mw = df["MEASURED_MW"].drop_nulls()
    flags = df["MW_QUALITY_FLAG"].value_counts().sort("MW_QUALITY_FLAG")
    total = len(df)

    flag_breakdown = {}
    for row in flags.iter_rows(named=True):
        flag = str(row["MW_QUALITY_FLAG"])
        count = row["count"]
        flag_breakdown[flag] = {
            "count": count,
            "pct": round(100 * count / total, 1) if total > 0 else 0,
        }

    return {
        "total_rows": total,
        "min_mw": round(float(mw.min()), 3) if len(mw) > 0 else None,
        "max_mw": round(float(mw.max()), 3) if len(mw) > 0 else None,
        "mean_mw": round(float(mw.mean()), 3) if len(mw) > 0 else None,
        "std_mw": round(float(mw.std()), 3) if len(mw) > 0 else None,
        "flag_breakdown": flag_breakdown,
    }


def to_csv_bytes(df: pl.DataFrame) -> bytes:
    """Serialize DataFrame to CSV bytes with space-separated datetimes (no T)."""
    out = df.with_columns([
        pl.col("INTERVAL_DATETIME").dt.strftime("%Y-%m-%d %H:%M:%S"),
        pl.col("MEASUREMENT_DATETIME").dt.strftime("%Y-%m-%d %H:%M:%S"),
    ])
    return out.write_csv().encode("utf-8")


def to_parquet_bytes(df: pl.DataFrame) -> bytes:
    """Serialize DataFrame to Parquet bytes."""
    buf = io.BytesIO()
    df.write_parquet(buf)
    return buf.getvalue()


def to_json_records(df: pl.DataFrame, max_rows: int = 25_000) -> list[dict]:
    """
    Return data as a list of dicts for JSON response.
    Datetimes formatted as 'YYYY-MM-DD HH:MM:SS' (no T, no microseconds).
    Default cap of 25 000 rows covers a full 24-hour NEM day at 4-second
    resolution (21 600 rows) with headroom.
    """
    display_df = df.head(max_rows).with_columns([
        pl.col("INTERVAL_DATETIME").dt.strftime("%Y-%m-%d %H:%M:%S"),
        pl.col("MEASUREMENT_DATETIME").dt.strftime("%Y-%m-%d %H:%M:%S"),
    ])
    return display_df.to_dicts()
