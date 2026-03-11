"""
Filters and transforms raw AEMO FPPDAILY CSV bytes using Polars.
"""
import io

import polars as pl
import pyarrow.parquet as pq
import pyarrow as pa

from app.config import REQUIRED_COLUMNS


class DataProcessingError(Exception):
    pass


def _parse_aemo_csv(csv_bytes: bytes) -> pl.DataFrame:
    """
    AEMO CSV files have a non-standard header format:
      - First row: "C,..." (comment/metadata)
      - Second row: "I,..." (table name/column header info)
      - Data rows: "D,..." (data)
    We need to skip the metadata rows and parse only data rows.
    """
    text = csv_bytes.decode("utf-8", errors="replace")
    lines = text.splitlines()

    header = None
    data_lines = []

    for line in lines:
        if not line.strip():
            continue
        if line.startswith("I,"):
            # Column header row — strip the leading "I," and use remaining fields
            parts = line.split(",")
            # Format: I, TABLE_NAME, VERSION, col1, col2, ...
            # The actual column names start at index 3 (after I, table, version)
            if len(parts) > 3:
                header = parts[3:]
        elif line.startswith("D,"):
            # Data row — strip leading "D," and the table/version fields
            parts = line.split(",")
            if len(parts) > 3:
                data_lines.append(parts[3:])

    if header is None:
        raise DataProcessingError("Could not find column header row in CSV file.")
    if not data_lines:
        raise DataProcessingError("CSV file contained no data rows.")

    # Build a simple CSV string for Polars to parse
    # Pad/trim rows to match header length
    n_cols = len(header)
    padded = []
    for row in data_lines:
        if len(row) >= n_cols:
            padded.append(row[:n_cols])
        else:
            padded.append(row + [""] * (n_cols - len(row)))

    csv_content = ",".join(header) + "\n" + "\n".join(",".join(r) for r in padded)
    df = pl.read_csv(io.StringIO(csv_content), infer_schema_length=1000)
    return df


def filter_and_process(csv_bytes: bytes, duid: str) -> pl.DataFrame:
    """
    Parse raw CSV bytes, filter to the requested DUID, return cleaned DataFrame.
    """
    df = _parse_aemo_csv(csv_bytes)

    # Check required columns exist
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise DataProcessingError(
            f"CSV missing expected columns: {missing}. "
            f"AEMO may have changed the file format."
        )

    # Filter to selected DUID
    df = df.filter(pl.col("FPP_UNITID") == duid).select(REQUIRED_COLUMNS)

    if df.is_empty():
        raise DataProcessingError(
            f"No data found for DUID '{duid}' on this date. "
            f"This unit may not have been operational or eligible for FPP on this date."
        )

    # Cast types
    df = df.with_columns([
        pl.col("INTERVAL_DATETIME").str.to_datetime(format="%Y/%m/%d %H:%M:%S", strict=False),
        pl.col("MEASUREMENT_DATETIME").str.to_datetime(format="%Y/%m/%d %H:%M:%S", strict=False),
        pl.col("MEASURED_MW").cast(pl.Float64, strict=False),
        pl.col("MW_QUALITY_FLAG").cast(pl.Int32, strict=False),
    ]).sort("MEASUREMENT_DATETIME")

    return df


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
    """Serialize DataFrame to CSV bytes."""
    return df.write_csv().encode("utf-8")


def to_parquet_bytes(df: pl.DataFrame) -> bytes:
    """Serialize DataFrame to Parquet bytes."""
    buf = io.BytesIO()
    df.write_parquet(buf)
    return buf.getvalue()


def to_json_records(df: pl.DataFrame, max_rows: int = 5000) -> list[dict]:
    """
    Return data as a list of dicts for JSON response.
    Truncates to max_rows for display (full data available via download).
    Datetimes serialized as ISO strings.
    """
    display_df = df.head(max_rows).with_columns([
        pl.col("INTERVAL_DATETIME").dt.strftime("%Y-%m-%dT%H:%M:%S"),
        pl.col("MEASUREMENT_DATETIME").dt.strftime("%Y-%m-%dT%H:%M:%S"),
    ])
    return display_df.to_dicts()
