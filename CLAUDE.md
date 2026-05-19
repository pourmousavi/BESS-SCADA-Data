# CLAUDE.md — BESS-SCADA-Data project guide

A FastAPI app that streams BESS SCADA (4-second power) and dispatch (5-minute
energy) data live from AEMO NEMWEB. Deployed as a Docker Space on HuggingFace.
Nothing is stored server-side; every request fetches fresh from AEMO.

## Where things live

```
app/
  main.py                       FastAPI app, static mounting, cache-busted index
  config.py                     AEMO URLs, start dates, timeouts, column lists
  routers/api.py                All HTTP endpoints
  services/
    aemo_fetcher.py             FPPDAILY ZIP download (Current + Archive)
    dispatch_fetcher.py         Next_Day_Dispatch ZIP download
    data_processor.py           Polars: parse + filter SCADA CSV
    dispatch_processor.py       Polars: parse + filter dispatch CSV
    gen_info_fetcher.py         AEMO XLSX → BESS list (live → bundled snapshot)
    analytics.py                SQLite request logging (/tmp/analytics.db)
  data/
    bess_list.json              Snapshot, refreshed daily by CI workflow
    bess_list_meta.json         Snapshot timestamp + provenance
    quality_flags.json          MW_QUALITY_FLAG descriptions
  static/                       index.html, app.js, style.css (UI)
scripts/refresh_bess_list.py    Standalone refresher invoked by CI
.github/workflows/
  sync_to_hf.yml                Master push → HF Space deploy (filters binaries)
  refresh_bess_list.yml         Daily cron → regenerates bess_list.json on master
```

## Deploy flow

1. Push to `master` → `sync_to_hf.yml` runs.
2. The workflow rewrites local history (in the runner only) to strip
   `app/data/aemo_gen_info.xlsx` from every commit. **HuggingFace rejects
   any pushed commit that contains a non-Xet binary file**, including
   historical commits — this filter step exists because of that policy.
3. Force-pushes the filtered history to the HF Space's `main` branch.
4. HF rebuilds the Docker image and swaps the container (~30–60 s).

Origin `master` is never rewritten by the deploy. Only HF's `main` is.

## Cache-busting

`app/main.py` reads `app/static/index.html` once at startup, substitutes
`__VERSION__` with `int(time.time())`, and serves the result from memory.
Every container restart = new version stamp = browsers refetch JS/CSS.
**Keep the `__VERSION__` placeholders on the `app.js` and `style.css`
`<script>` / `<link>` tags in index.html** — removing them silently
reintroduces the cache-staleness bug.

## AEMO data quirks

- **Three FPPMW format eras** — see `aemo_fetcher.py` docstring.
  - Era 1 (Dec 2024 – 28 Apr 2025): single daily file, settlement date in filename.
  - Era 2 (29 Apr – 10 Sep 2025): single 12-hour file (04:00–16:00 only), publication date in filename.
  - Era 3 (11 Sep 2025+): two files per day (FPPMW first half, FPPMW_2 second half).
  Era logic lives in `_era()` and `_inner_zip_date_str()`.
- **Rolling archive** — AEMO removes old monthly bundles. `DATA_START_DATE`
  and `DISPATCH_START_DATE` must be updated when bundles roll off. As of
  2026-05: SCADA from 29 Apr 2025, dispatch from 1 Apr 2025. Verify against
  the actual NEMWEB Archive directory listings, not memory.
- **AEMO WAF blocks datacentre IPs** on `www.aemo.com.au` (returns 403 to
  the HF egress range). The Generation Information XLSX fetch is wrapped
  in browser-style headers AND backed by the daily-refreshed
  `bess_list.json` snapshot. The snapshot's CI runner is on GitHub-hosted
  infrastructure, which AEMO doesn't block.
- **NEM market day boundary is 04:00 AEST**, not midnight. `data_processor.py`
  filters strictly to `[04:00 AEST D, 04:00 AEST D+1)`. Era 1 callers must
  request both D and D+1 calendar files to cover one market day.

## Critical API contracts

- `/api/bess` returns `{states, source, fetched_at, warnings}` —
  callers must read `.states` for the unit list. **Breaking this shape
  breaks the frontend dropdown** (and any external scripts). If you must
  change it, update `app.js:init()` in lockstep.
- `/api/data` and `/api/energy-data` return `{duid, date, total_rows, summary, data, warnings?}`.
- All endpoints raise `HTTPException(404)` for "no data for this DUID/date"
  and `HTTPException(503)` for upstream AEMO failures. Don't change these
  status codes — the frontend distinguishes them.

## Running locally

```bash
pip install -r requirements.txt
uvicorn app.main:app --reload --port 8000
# then open http://localhost:8000
```

## Things that have bitten us before

- `bess_list.json` once contained fictional DUIDs (e.g. `NESBESS1`,
  `HORNSDALE_PWR1`) that don't exist in real SCADA data. The daily
  workflow guarantees this can't happen now, but verify any manual edit
  against `scripts/refresh_bess_list.py` output.
- Browser cache served old `app.js` with new `/api/bess` shape →
  nonsense state-dropdown options. The `__VERSION__` cache-bust prevents
  recurrence; do not remove it.
- HuggingFace rejects pushed commits with binary files anywhere in
  history, not just current tree. Don't reintroduce binary commits even
  if you plan to delete them in the next commit.

---

CLAUDE.md skill to append to any coding project


# CLAUDE.md — 12-rule template

These rules apply to every task in this project unless explicitly overridden.
Bias: caution over speed on non-trivial work. Use judgment on trivial tasks.

## Rule 1 — Think Before Coding
State assumptions explicitly. If uncertain, ask rather than guess.
Present multiple interpretations when ambiguity exists.
Push back when a simpler approach exists.
Stop when confused. Name what's unclear.

## Rule 2 — Simplicity First
Minimum code that solves the problem. Nothing speculative.
No features beyond what was asked. No abstractions for single-use code.
Test: would a senior engineer say this is overcomplicated? If yes, simplify.

## Rule 3 — Surgical Changes
Touch only what you must. Clean up only your own mess.
Don't "improve" adjacent code, comments, or formatting.
Don't refactor what isn't broken. Match existing style.

## Rule 4 — Goal-Driven Execution
Define success criteria. Loop until verified.
Don't follow steps. Define success and iterate.
Strong success criteria let you loop independently.

## Rule 5 — Use the model only for judgment calls
Use me for: classification, drafting, summarization, extraction.
Do NOT use me for: routing, retries, deterministic transforms.
If code can answer, code answers.

## Rule 6 — Token budgets are not advisory
Per-task: 4,000 tokens. Per-session: 30,000 tokens.
If approaching budget, summarize and start fresh.
Surface the breach. Do not silently overrun.

## Rule 7 — Surface conflicts, don't average them
If two patterns contradict, pick one (more recent / more tested).
Explain why. Flag the other for cleanup.
Don't blend conflicting patterns.

## Rule 8 — Read before you write
Before adding code, read exports, immediate callers, shared utilities.
"Looks orthogonal" is dangerous. If unsure why code is structured a way, ask.

## Rule 9 — Tests verify intent, not just behavior
Tests must encode WHY behavior matters, not just WHAT it does.
A test that can't fail when business logic changes is wrong.

## Rule 10 — Checkpoint after every significant step
Summarize what was done, what's verified, what's left.
Don't continue from a state you can't describe back.
If you lose track, stop and restate.

## Rule 11 — Match the codebase's conventions, even if you disagree
Conformance > taste inside the codebase.
If you genuinely think a convention is harmful, surface it. Don't fork silently.

## Rule 12 — Fail loud
"Completed" is wrong if anything was skipped silently.
"Tests pass" is wrong if any were skipped.
Default to surfacing uncertainty, not hiding it.
