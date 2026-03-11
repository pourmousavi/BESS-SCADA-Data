"""
Lightweight SQLite-based analytics logger.
Tracks request counts, IPs, and selected parameters.
"""
import sqlite3
from datetime import datetime

from app.config import ANALYTICS_DB_PATH


def _get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(ANALYTICS_DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    """Create the analytics table if it doesn't exist."""
    with _get_conn() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS requests (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                ip TEXT,
                duid TEXT,
                date TEXT,
                action TEXT
            )
        """)
        conn.commit()


def log_request(ip: str, duid: str, date: str, action: str) -> None:
    """Log a single request."""
    try:
        with _get_conn() as conn:
            conn.execute(
                "INSERT INTO requests (timestamp, ip, duid, date, action) VALUES (?, ?, ?, ?, ?)",
                (datetime.utcnow().isoformat(), ip, duid, date, action),
            )
            conn.commit()
    except Exception:
        pass  # Never let analytics failures break the main flow


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
                "SELECT timestamp, ip, duid, date, action FROM requests ORDER BY id DESC LIMIT 100"
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
