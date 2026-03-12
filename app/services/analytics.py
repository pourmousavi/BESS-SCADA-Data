"""
Lightweight SQLite-based analytics logger.
Tracks request counts, IPs, selected parameters, and end-to-end durations.
"""
import sqlite3
from datetime import datetime

from app.config import (
    ANALYTICS_DB_PATH,
    TIMING_DEFAULT_ARCHIVE_SEC,
    TIMING_DEFAULT_CURRENT_SEC,
    TIMING_MIN_SAMPLES,
)


def _get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(ANALYTICS_DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    """Create the analytics table and migrate any missing columns."""
    with _get_conn() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS requests (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp   TEXT    NOT NULL,
                ip          TEXT,
                duid        TEXT,
                date        TEXT,
                action      TEXT,
                duration_ms INTEGER,
                source_type TEXT
            )
        """)
        # Migrate pre-existing tables that lack the new columns.
        existing = {row[1] for row in conn.execute("PRAGMA table_info(requests)")}
        if "duration_ms" not in existing:
            conn.execute("ALTER TABLE requests ADD COLUMN duration_ms INTEGER")
        if "source_type" not in existing:
            conn.execute("ALTER TABLE requests ADD COLUMN source_type TEXT")
        conn.commit()


def log_request(
    ip: str,
    duid: str,
    date: str,
    action: str,
    *,
    duration_ms: int | None = None,
    source_type: str | None = None,
) -> None:
    """Log a single request, optionally with timing and data-source type."""
    try:
        with _get_conn() as conn:
            conn.execute(
                """
                INSERT INTO requests
                    (timestamp, ip, duid, date, action, duration_ms, source_type)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (datetime.utcnow().isoformat(), ip, duid, date, action,
                 duration_ms, source_type),
            )
            conn.commit()
    except Exception:
        pass  # Never let analytics failures break the main flow


def get_timing_estimate(source_type: str) -> dict:
    """
    Return a p75 wait-time estimate in seconds for the given source_type
    ('current' or 'archive'), derived from the last 100 successful view
    requests that recorded a duration.

    Falls back to a hardcoded default when fewer than TIMING_MIN_SAMPLES
    records are available.
    """
    default_sec = (
        TIMING_DEFAULT_CURRENT_SEC
        if source_type == "current"
        else TIMING_DEFAULT_ARCHIVE_SEC
    )
    try:
        with _get_conn() as conn:
            rows = conn.execute(
                """
                SELECT duration_ms FROM requests
                WHERE source_type = ?
                  AND duration_ms IS NOT NULL
                  AND action = 'view'
                ORDER BY id DESC
                LIMIT 100
                """,
                (source_type,),
            ).fetchall()

        durations = sorted(row[0] for row in rows)
        n = len(durations)
        if n < TIMING_MIN_SAMPLES:
            return {"seconds": default_sec, "sample_count": n, "is_default": True}

        p75_idx = int(0.75 * (n - 1))
        p75_sec = round(durations[p75_idx] / 1000)
        return {"seconds": p75_sec, "sample_count": n, "is_default": False}

    except Exception:
        return {"seconds": default_sec, "sample_count": 0, "is_default": True}


def get_stats() -> dict:
    """Return summary analytics."""
    try:
        with _get_conn() as conn:
            total = conn.execute("SELECT COUNT(*) FROM requests").fetchone()[0]
            by_action = conn.execute(
                "SELECT action, COUNT(*) as cnt FROM requests GROUP BY action ORDER BY cnt DESC"
            ).fetchall()
            by_duid = conn.execute(
                "SELECT duid, COUNT(*) as cnt FROM requests GROUP BY duid ORDER BY cnt DESC LIMIT 20"
            ).fetchall()
            by_ip = conn.execute(
                "SELECT ip, COUNT(*) as cnt FROM requests GROUP BY ip ORDER BY cnt DESC LIMIT 50"
            ).fetchall()
            recent = conn.execute(
                """
                SELECT timestamp, ip, duid, date, action, duration_ms, source_type
                FROM requests ORDER BY id DESC LIMIT 100
                """
            ).fetchall()

        return {
            "total_requests": total,
            "by_action": [dict(r) for r in by_action],
            "by_duid": [dict(r) for r in by_duid],
            "by_ip": [dict(r) for r in by_ip],
            "recent": [dict(r) for r in recent],
        }
    except Exception as e:
        return {"error": str(e)}
