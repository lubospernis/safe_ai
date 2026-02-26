# safe-ai — ECB SAFE Microdata Pipeline

Automated ingestion pipeline for the [ECB Survey on the Access to Finance of Enterprises (SAFE)](https://www.ecb.europa.eu/stats/ecb_surveys/safe/html/data.en.html) microdata.

---

## Architecture

```
GitHub Actions (monthly cron / manual)
  └─ ingestion/ingest.py
        ├─ HEAD request → ETag / Last-Modified
        ├─ Watermark check  ← MotherDuck raw.ingestion_log
        ├─ (404 fallback)   → scrape ECB data page for new .zip URL
        ├─ Download + unzip
        ├─ Load CSVs        → MotherDuck raw.safe_microdata
        └─ Write run log    → MotherDuck raw.ingestion_log
```

---

## Repository structure

```
.
├── ingestion/
│   ├── ingest.py        # Main pipeline script
│   └── config.yaml      # Runtime configuration (URLs, DB names)
├── .github/
│   └── workflows/
│       └── ingest.yml   # Monthly cron + manual trigger
├── requirements.txt
└── README.md
```

---

## Quick start

### Prerequisites

| Tool | Version |
|------|---------|
| Python | 3.11+ |
| pip | latest |
| MotherDuck account | — |

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Set the MotherDuck token

```bash
export MOTHERDUCK_TOKEN="your_token_here"
```

### 3. Run the ingestion

```bash
python ingestion/ingest.py
```

Logs are written to stdout in structured format:

```
2026-02-26T06:00:01Z  INFO     safe_ingestion  HEAD https://…/ecb.SAFE_microdata.zip
2026-02-26T06:00:02Z  INFO     safe_ingestion  ETag="abc123"  Last-Modified=…
2026-02-26T06:00:02Z  INFO     safe_ingestion  Watermark — prev_etag=None …
2026-02-26T06:00:07Z  INFO     safe_ingestion  Download complete — 12.3 MiB
2026-02-26T06:00:15Z  INFO     safe_ingestion  Total rows inserted: 142857
2026-02-26T06:00:15Z  INFO     safe_ingestion  Done (success) — 142857 rows in 13.8s
```

---

## Configuration (`ingestion/config.yaml`)

| Key | Description |
|-----|-------------|
| `ingestion.zip_url` | Primary URL for the microdata zip. Updated automatically on 404 fallback. |
| `ingestion.fallback_scrape_url` | ECB data page to scrape when the primary URL returns 404. |
| `ingestion.request_timeout_seconds` | HTTP timeout in seconds (default: 60). |
| `ingestion.user_agent` | User-Agent header sent with every request. |
| `motherduck.database` | MotherDuck database name (default: `safe_ai`). |
| `motherduck.schema` | Landing schema (default: `raw`). |
| `motherduck.table` | Microdata table (default: `safe_microdata`). |
| `motherduck.log_table` | Run log table (default: `ingestion_log`). |

---

## MotherDuck schema

### `raw.safe_microdata`

Created dynamically from the CSV schema on the first successful run.
An extra column `_ingested_at TIMESTAMPTZ` is appended to every row.

### `raw.ingestion_log`

| Column | Type | Description |
|--------|------|-------------|
| `run_id` | VARCHAR | UUID for this run |
| `run_at` | TIMESTAMPTZ | UTC start time |
| `url_used` | VARCHAR | Actual URL that was (attempted to be) fetched |
| `etag` | VARCHAR | ETag from HTTP response headers |
| `last_modified` | VARCHAR | Last-Modified from HTTP response headers |
| `status` | VARCHAR | `success` \| `skipped` \| `error` |
| `rows_loaded` | BIGINT | Rows inserted this run (0 if skipped) |
| `error_message` | VARCHAR | Exception message on failure |
| `duration_s` | DOUBLE | Wall-clock seconds for the run |

---

## GitHub Actions

The workflow (`.github/workflows/ingest.yml`) runs:

- **Automatically** — 1st of every month at 06:00 UTC.
- **Manually** — via "Run workflow" in the Actions tab, with optional inputs:
  - `zip_url_override` — one-shot URL override (does not persist to the repo).
  - `dry_run` — prints intended actions without writing to MotherDuck.

### Required secret

Add `MOTHERDUCK_TOKEN` as a repository secret:
**Settings → Secrets and variables → Actions → New repository secret**

### Permissions

The workflow needs `contents: write` (already set in the YAML) to commit an
updated `config.yaml` when the primary URL returns 404 and a new URL is discovered.

---

## Watermark logic

The pipeline avoids re-downloading unchanged data by comparing the `ETag` (preferred)
or `Last-Modified` HTTP response header against the last successful run stored in
`raw.ingestion_log`.

| Scenario | Behaviour |
|----------|-----------|
| ETag matches previous success | Skip download, log as `skipped` |
| Last-Modified matches (no ETag) | Skip download, log as `skipped` |
| Neither header present | Download to be safe |
| No previous successful run | Download unconditionally |

---

## Extending to later stages

`raw.safe_microdata` is the landing zone. Downstream stages (cleaning,
harmonisation, analysis) should read from this table and write to separate
schemas (e.g. `staging`, `mart`).
