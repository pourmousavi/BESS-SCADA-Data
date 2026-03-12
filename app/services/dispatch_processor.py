"""
Filters and transforms raw AEMO Next_Day_Dispatch CSV bytes using Polars.

The dispatch CSV uses the same AEMO "C/I/D" row-prefix format as FPPDAILY but
contains multiple tables (PRICE, UNIT_SOLUTION, INTERCONNECTION, …).  We
extract only the UNIT_SOLUTION table, which holds per-DUID per-interval data.

Expected columns (from the UNIT_SOLUTION I-row header):
  SETTLEMENTDATE          — end of 5-minute dispatch interval (AEST)
  DUID                    — Dispatchable Unit ID (used for filtering only)
  INITIALMW               — initial MW at the start of the interval
  INITIAL_ENERGY_STORAGE  — energy stored at the start of the interval (MWh)
  ENERGY_STORAGE          — energy stored at the end of the interval (MWh)

NEM trading-day window applied: SETTLEMENTDATE > 04:00 AEST D
                                AND SETTLEMENTDATE <= 04:00 AEST D+1
(SETTLEMENTDATE is an end-of-interval timestamp, so the first interval of the
 trading day has SETTLEMENTDATE = 04:05 and the last = 04:00 next day.)
"""
import io
import logging
from datetime import date, datetime, time as dtime, timedelta

import polars as pl

from app.config import DISPATCH_COLUMNS

logger = logging.getLogger(__name__)

_DAY_START_HOUR = 4          # NEM market day starts at 04:00 AEST
_TARGET_TABLE   = "UNIT_SOLUTION"


class DispatchProcessingError(Exception):
    pass


# ── CSV parsing ───────────────────────────────────────────────────────────────

def _segment_to_df(header: list[str], data_lines: list[list[str]]) -> pl.DataFrame:
    """Build a Polars DataFrame from one parsed segment (same as data_processor)."""
    n_cols = len(header)
    padded = []
    for row in data_lines:
        if len(row) >= n_cols:
            padded.append(row[:n_cols])
        else:
            padded.append(row + [""] * (n_cols - len(row)))
    csv_content = (
        ",".join(header) + "\n"
        + "\n".join(",".join(r) for r in padded)
    )
    return pl.read_csv(io.StringIO(csv_content), infer_schema_length=None)


def _parse_dispatch_csv(csv_bytes: bytes) -> pl.DataFrame:
    """
    Parse AEMO dispatch CSV, extracting only the UNIT_SOLUTION table rows.

    AEMO CSV structure:
      C,...          comment / metadata rows  (skipped)
      I,DISPATCH,UNIT_SOLUTION,4,SETTLEMENTDATE,RUNNO,DUID,...   header
      D,DISPATCH,UNIT_SOLUTION,4,2026/03/11 00:05:00,...          data
      I,DISPATCH,PRICE,4,...                                       other table (ignored)
      D,DISPATCH,PRICE,4,...                                       other table (ignored)

    The version number at parts[3] (e.g. "4") becomes the first DataFrame
    column; this is harmless since we select by column name, not position.

    Multiple UNIT_SOLUTION segments can appear when csv_bytes is the
    concatenation of several daily files; each is parsed independently and
    then concatenated via diagonal concat.
    """
    text = csv_bytes.decode("utf-8", errors="replace")
    lines = text.splitlines()

    segments: list[tuple[list[str], list[list[str]]]] = []
    current_header: list[str] | None = None
    current_data:   list[list[str]] = []

    for line in lines:
        if not line.strip():
            continue
        parts = line.split(",")
        if not parts:
            continue

        tag = parts[0].upper()

        if tag == "I":
            if len(parts) > 3 and parts[2].strip().upper() == _TARGET_TABLE:
                # Flush any accumulated UNIT_SOLUTION data before starting new segment
                if current_header is not None and current_data:
                    segments.append((current_header, current_data))
                    current_data = []
                current_header = parts[3:]   # [version, col1, col2, …]

        elif tag == "D":
            # Only accept D rows that belong to UNIT_SOLUTION
            if (
                current_header is not None
                and len(parts) > 3
                and parts[2].strip().upper() == _TARGET_TABLE
            ):
                current_data.append(parts[3:])

    # Flush final segment
    if current_header is not None and current_data:
        segments.append((current_header, current_data))

    if not segments:
        raise DispatchProcessingError(
            f"No {_TARGET_TABLE} table found in dispatch CSV. "
            "AEMO may have changed the file format."
        )

    if len(segments) == 1:
        return _segment_to_df(*segments[0])

    dfs = [_segment_to_df(h, d) for h, d in segments]
    logger.debug("Dispatch: %d CSV segments, concatenating.", len(dfs))
    try:
        return pl.concat(dfs, how="diagonal")
    except Exception as exc:
        logger.warning("Dispatch diagonal concat failed (%s); using first segment.", exc)
        return dfs[0]


# ── Main filter / process function ───────────────────────────────────────────

def filter_and_process_dispatch(
    csv_bytes: bytes,
    duid: str,
    target_date: date,
) -> pl.DataFrame:
    """
    Parse raw dispatch CSV, filter to the requested DUID, and apply the
    NEM market-day window: 04:00 AEST target_date < SETTLEMENTDATE <= 04:00 AEST D+1.

    Returns a DataFrame with columns SETTLEMENTDATE, INITIALMW,
    INITIAL_ENERGY_STORAGE (if present), ENERGY_STORAGE (if present),
    sorted by SETTLEMENTDATE.
    """
    df = _parse_dispatch_csv(csv_bytes)
    logger.info("Dispatch raw rows: %d, columns: %s", len(df), df.columns)

    # Find DUID column (always expected in UNIT_SOLUTION)
    if "DUID" not in df.columns:
        raise DispatchProcessingError(
            "DUID column not found in dispatch UNIT_SOLUTION table."
        )
    df = df.filter(pl.col("DUID") == duid)
    logger.info("After DUID filter (%s): %d rows", duid, len(df))

    if df.is_empty():
        raise DispatchProcessingError(
            f"No dispatch data found for DUID '{duid}' on this date. "
            "This unit may not have been operational on this date, or "
            "may not report energy storage."
        )

    # Check mandatory column
    if "SETTLEMENTDATE" not in df.columns:
        raise DispatchProcessingError(
            "SETTLEMENTDATE column not found in dispatch data."
        )

    # Cast SETTLEMENTDATE to datetime
    df = df.with_columns(
        pl.col("SETTLEMENTDATE").str.to_datetime(
            format="%Y/%m/%d %H:%M:%S", strict=False
        )
    )

    # Cast numeric columns where present
    cast_cols = {"INITIALMW", "INITIAL_ENERGY_STORAGE", "ENERGY_STORAGE"}
    for col in cast_cols:
        if col in df.columns:
            df = df.with_columns(pl.col(col).cast(pl.Float64, strict=False))

    # Apply NEM trading-day window.
    # SETTLEMENTDATE is end-of-interval, so:
    #   first interval of day D → 04:05 AEST (> 04:00 D)
    #   last  interval of day D → 04:00 AEST D+1 (included)
    day_start = datetime.combine(target_date,          dtime(_DAY_START_HOUR, 0, 0))
    day_end   = datetime.combine(target_date + timedelta(days=1), dtime(_DAY_START_HOUR, 0, 0))
    df = df.filter(
        (pl.col("SETTLEMENTDATE") >  day_start) &
        (pl.col("SETTLEMENTDATE") <= day_end)
    )
    logger.info(
        "After date-window filter (%s, %s]: %d rows",
        day_start.strftime("%Y-%m-%d %H:%M"),
        day_end.strftime("%Y-%m-%d %H:%M"),
        len(df),
    )

    if df.is_empty():
        raise DispatchProcessingError(
            f"No dispatch data found for DUID '{duid}' within the NEM market day "
            f"({day_start.strftime('%d %b %Y %H:%M')}–"
            f"{day_end.strftime('%d %b %Y %H:%M')} AEST)."
        )

    # Keep only the output columns that exist in this file
    keep = ["SETTLEMENTDATE"] + [c for c in DISPATCH_COLUMNS[1:] if c in df.columns]
    df = df.select(keep)

    return (
        df.unique(subset=["SETTLEMENTDATE"], keep="first")
          .sort("SETTLEMENTDATE")
    )


# ── Summary ───────────────────────────────────────────────────────────────────

def compute_dispatch_summary(df: pl.DataFrame) -> dict:
    """Compute summary statistics for the filtered dispatch data."""
    def _safe(val) -> float | None:
        return round(float(val), 2) if val is not None else None

    result: dict = {"total_rows": len(df)}

    if "INITIALMW" in df.columns:
        mw = df["INITIALMW"].fill_nan(None).drop_nulls()
        result["min_mw"]  = _safe(mw.min()  if len(mw) > 0 else None)
        result["max_mw"]  = _safe(mw.max()  if len(mw) > 0 else None)
        result["mean_mw"] = _safe(mw.mean() if len(mw) > 0 else None)

    for col, prefix in [
        ("INITIAL_ENERGY_STORAGE", "init_mwh"),
        ("ENERGY_STORAGE",         "end_mwh"),
    ]:
        if col in df.columns:
            vals = df[col].fill_nan(None).drop_nulls()
            if len(vals) > 0:
                result[f"min_{prefix}"]  = _safe(vals.min())
                result[f"max_{prefix}"]  = _safe(vals.max())
                result[f"mean_{prefix}"] = _safe(vals.mean())

    return result


# ── Serialisers ───────────────────────────────────────────────────────────────

def to_csv_bytes(df: pl.DataFrame) -> bytes:
    """Serialize DataFrame to CSV bytes with space-separated datetimes."""
    out = df.with_columns(
        pl.col("SETTLEMENTDATE").dt.strftime("%Y-%m-%d %H:%M:%S")
    )
    return out.write_csv().encode("utf-8")


def to_parquet_bytes(df: pl.DataFrame) -> bytes:
    """Serialize DataFrame to Parquet bytes."""
    buf = io.BytesIO()
    df.write_parquet(buf)
    return buf.getvalue()


def to_json_records(df: pl.DataFrame) -> list[dict]:
    """Return data as a list of dicts suitable for JSON response."""
    display_df = df.with_columns(
        pl.col("SETTLEMENTDATE").dt.strftime("%Y-%m-%d %H:%M:%S")
    )
    # Replace float NaN with null for JSON serialisation
    for col in ["INITIALMW", "INITIAL_ENERGY_STORAGE", "ENERGY_STORAGE"]:
        if col in display_df.columns:
            display_df = display_df.with_columns(pl.col(col).fill_nan(None))
    return display_df.to_dicts()
