#!/usr/bin/env python3
"""
Refresh the bundled BESS list snapshot.

Downloads the AEMO Generation Information XLSX, parses it into the same
shape served by /api/bess, and writes two files into app/data/:

  bess_list.json      — parsed snapshot used as runtime fallback when the
                        live AEMO fetch is blocked (e.g. WAF 403 from the
                        HuggingFace egress IP).
  aemo_gen_info.xlsx  — the raw XLSX bytes, so gen_info_fetcher.py can also
                        fall back to fetching it from raw.githubusercontent.com
                        and parse a fresher copy at runtime.

Run from the repo root:
    python scripts/refresh_bess_list.py

Intended to be invoked by a scheduled GitHub Action (see
.github/workflows/refresh_bess_list.yml).
"""
from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import httpx

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from app.services.gen_info_fetcher import XLSX_URL, _BROWSER_HEADERS, _parse_xlsx

DATA_DIR = REPO_ROOT / "app" / "data"
JSON_OUT = DATA_DIR / "bess_list.json"
XLSX_OUT = DATA_DIR / "aemo_gen_info.xlsx"
META_OUT = DATA_DIR / "bess_list_meta.json"


def main() -> int:
    print(f"Downloading {XLSX_URL}")
    with httpx.Client(timeout=60, follow_redirects=True) as client:
        resp = client.get(XLSX_URL, headers=_BROWSER_HEADERS)
        resp.raise_for_status()
    xlsx_bytes = resp.content
    print(f"Got {len(xlsx_bytes):,} bytes")

    parsed = _parse_xlsx(xlsx_bytes)
    total = sum(len(v) for v in parsed.values())
    print(f"Parsed {total} BESS units across {sum(1 for v in parsed.values() if v)} states")
    if total == 0:
        print("ERROR: parsed XLSX yielded zero BESS units — refusing to overwrite snapshot")
        return 1

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    XLSX_OUT.write_bytes(xlsx_bytes)
    JSON_OUT.write_text(json.dumps(parsed, indent=2, sort_keys=True) + "\n")
    META_OUT.write_text(json.dumps({
        "fetched_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "source_url": XLSX_URL,
        "xlsx_bytes": len(xlsx_bytes),
        "unit_count": total,
    }, indent=2) + "\n")

    print(f"Wrote {JSON_OUT.relative_to(REPO_ROOT)}")
    print(f"Wrote {XLSX_OUT.relative_to(REPO_ROOT)}")
    print(f"Wrote {META_OUT.relative_to(REPO_ROOT)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
