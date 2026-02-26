"""
ECB SAFE Microdata — Ingestion Engine
======================================
Stage 1 of the safe-ai pipeline.

Behaviour
---------
1. Perform a HEAD request against the configured zip URL to fetch ETag /
   Last-Modified headers.
2. Compare those headers against the watermark stored in MotherDuck
   (raw.ingestion_log).  Skip if unchanged.
3. If the HEAD returns 404, scrape the ECB data page for any .zip link,
   persist the new URL back to config.yaml, then continue.
4. On new data: stream-download the zip, extract every CSV, load into
   MotherDuck raw.safe_microdata (append + _ingested_at column).
5. Write a run record to raw.ingestion_log regardless of outcome.

Environment
-----------
  MOTHERDUCK_TOKEN   Required — MotherDuck service token.
"""

from __future__ import annotations

import csv
import io
import logging
import os
import sys
import tempfile
import time
import uuid
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
from urllib.parse import urljoin, urlparse

import duckdb
import requests
import yaml
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
    stream=sys.stdout,
)
log = logging.getLogger("safe_ingestion")

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

CONFIG_PATH = Path(__file__).parent / "config.yaml"

# ---------------------------------------------------------------------------
# Config helpers
# ---------------------------------------------------------------------------


def load_config() -> dict:
    """Load YAML config from disk."""
    with CONFIG_PATH.open("r", encoding="utf-8") as fh:
        return yaml.safe_load(fh)


def save_zip_url(url: str) -> None:
    """Persist an updated zip_url back to config.yaml in-place."""
    cfg = load_config()
    cfg["ingestion"]["zip_url"] = url
    with CONFIG_PATH.open("w", encoding="utf-8") as fh:
        yaml.dump(cfg, fh, default_flow_style=False, allow_unicode=True)
    log.info("config.yaml updated with new zip_url: %s", url)


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------


def build_session(user_agent: str, timeout: int) -> requests.Session:
    session = requests.Session()
    session.headers.update({"User-Agent": user_agent})
    session.request_timeout = timeout  # stored for callers to reuse
    return session


def head_request(
    session: requests.Session, url: str, timeout: int
) -> requests.Response:
    """Execute a HEAD request.  Raises requests.HTTPError on 4xx/5xx != 404."""
    resp = session.head(url, timeout=timeout, allow_redirects=True)
    if resp.status_code != 404:
        resp.raise_for_status()
    return resp


def scrape_zip_url(
    session: requests.Session, scrape_url: str, timeout: int
) -> str:
    """
    Scrape *scrape_url* and return the first href ending in '.zip'.
    The href is made absolute relative to *scrape_url*.
    Raises RuntimeError if nothing is found.
    """
    log.info("Scraping fallback page for .zip link: %s", scrape_url)
    resp = session.get(scrape_url, timeout=timeout)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")
    for tag in soup.find_all("a", href=True):
        href: str = tag["href"]
        if href.lower().endswith(".zip"):
            absolute = urljoin(scrape_url, href)
            log.info("Found zip link on fallback page: %s", absolute)
            return absolute

    raise RuntimeError(
        f"No .zip link found while scraping fallback page: {scrape_url}"
    )


def download_zip(
    session: requests.Session, url: str, timeout: int, dest: Path
) -> None:
    """Stream-download *url* to *dest* on disk."""
    log.info("Downloading zip from %s …", url)
    with session.get(url, timeout=timeout, stream=True) as resp:
        resp.raise_for_status()
        with dest.open("wb") as fh:
            for chunk in resp.iter_content(chunk_size=1 << 20):  # 1 MiB
                fh.write(chunk)
    size_mb = dest.stat().st_size / (1 << 20)
    log.info("Download complete — %.1f MiB saved to %s", size_mb, dest)


# ---------------------------------------------------------------------------
# MotherDuck / DuckDB helpers
# ---------------------------------------------------------------------------


def connect_motherduck(token: str, database: str) -> duckdb.DuckDBPyConnection:
    """Open an authenticated connection to MotherDuck."""
    conn_str = f"md:{database}?motherduck_token={token}"
    log.info("Connecting to MotherDuck database '%s' …", database)
    conn = duckdb.connect(conn_str)
    log.info("Connected.")
    return conn


def ensure_schema_and_tables(conn: duckdb.DuckDBPyConnection, cfg: dict) -> None:
    """Create the raw schema and both tables if they don't already exist."""
    schema = cfg["motherduck"]["schema"]
    data_table = cfg["motherduck"]["table"]
    log_table = cfg["motherduck"]["log_table"]

    conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")

    # ingestion_log — one row per pipeline run
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {schema}.{log_table} (
            run_id          VARCHAR     NOT NULL,
            run_at          TIMESTAMPTZ NOT NULL,
            url_used        VARCHAR     NOT NULL,
            etag            VARCHAR,
            last_modified   VARCHAR,
            status          VARCHAR     NOT NULL,   -- 'success' | 'skipped' | 'error'
            rows_loaded     BIGINT,
            error_message   VARCHAR,
            duration_s      DOUBLE
        )
        """
    )

    # safe_microdata — raw landing table; schema is inferred from CSV headers
    # We create a minimal shell; columns are added at load time via INSERT SELECT.
    # The table is created on first real load (see load_csvs_into_motherduck).
    log.debug("Schema and log table verified.")


def get_watermark(
    conn: duckdb.DuckDBPyConnection, schema: str, log_table: str
) -> tuple[Optional[str], Optional[str]]:
    """
    Return the (etag, last_modified) from the most recent successful run.
    Both values may be None if no successful run exists yet.
    """
    result = conn.execute(
        f"""
        SELECT etag, last_modified
        FROM {schema}.{log_table}
        WHERE status = 'success'
        ORDER BY run_at DESC
        LIMIT 1
        """
    ).fetchone()
    if result is None:
        return None, None
    return result[0], result[1]


def load_csvs_into_motherduck(
    conn: duckdb.DuckDBPyConnection,
    zip_path: Path,
    schema: str,
    table: str,
    ingested_at: datetime,
) -> int:
    """
    Extract every .csv inside *zip_path* and load all rows into
    {schema}.{table}, appending a _ingested_at column.

    Returns the total number of rows inserted.
    """
    total_rows = 0
    ingested_at_str = ingested_at.isoformat()

    with zipfile.ZipFile(zip_path, "r") as zf:
        csv_names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
        if not csv_names:
            raise RuntimeError("No CSV files found inside the downloaded zip.")

        log.info("Found %d CSV file(s) inside zip: %s", len(csv_names), csv_names)

        with tempfile.TemporaryDirectory() as tmp_dir:
            zf.extractall(tmp_dir)

            for csv_name in csv_names:
                csv_path = Path(tmp_dir) / csv_name
                log.info("Loading %s …", csv_name)

                # Use DuckDB's read_csv_auto for flexible schema inference.
                # We register the CSV as a view, then INSERT into the target table
                # (creating it on the first CSV if needed).
                view_name = f"_tmp_csv_{uuid.uuid4().hex[:8]}"
                conn.execute(
                    f"""
                    CREATE OR REPLACE TEMP VIEW {view_name} AS
                    SELECT *, TIMESTAMPTZ '{ingested_at_str}' AS _ingested_at
                    FROM read_csv_auto('{csv_path.as_posix()}',
                                       header=true,
                                       ignore_errors=true)
                    """
                )

                # Check if the target table exists; create from first CSV if not.
                table_exists = conn.execute(
                    f"""
                    SELECT COUNT(*) FROM information_schema.tables
                    WHERE table_schema = '{schema}' AND table_name = '{table}'
                    """
                ).fetchone()[0]

                if not table_exists:
                    log.info("Creating table %s.%s from first CSV schema.", schema, table)
                    conn.execute(
                        f"CREATE TABLE {schema}.{table} AS SELECT * FROM {view_name} LIMIT 0"
                    )

                conn.execute(
                    f"INSERT INTO {schema}.{table} SELECT * FROM {view_name}"
                )

                rows_in_file = conn.execute(
                    f"SELECT COUNT(*) FROM {view_name}"
                ).fetchone()[0]
                log.info("  → %d rows loaded from %s", rows_in_file, csv_name)
                total_rows += rows_in_file

                conn.execute(f"DROP VIEW IF EXISTS {view_name}")

    log.info("Total rows inserted: %d", total_rows)
    return total_rows


def write_log(
    conn: duckdb.DuckDBPyConnection,
    schema: str,
    log_table: str,
    *,
    run_id: str,
    run_at: datetime,
    url_used: str,
    etag: Optional[str],
    last_modified: Optional[str],
    status: str,
    rows_loaded: Optional[int] = None,
    error_message: Optional[str] = None,
    duration_s: Optional[float] = None,
) -> None:
    conn.execute(
        f"""
        INSERT INTO {schema}.{log_table}
            (run_id, run_at, url_used, etag, last_modified,
             status, rows_loaded, error_message, duration_s)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            run_id,
            run_at.isoformat(),
            url_used,
            etag,
            last_modified,
            status,
            rows_loaded,
            error_message,
            duration_s,
        ],
    )
    log.info("Run logged to %s.%s — status=%s", schema, log_table, status)


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------


def run() -> None:  # noqa: C901 (complexity is intentional — one clear flow)
    t0 = time.monotonic()
    run_id = str(uuid.uuid4())
    run_at = datetime.now(tz=timezone.utc)

    log.info("=" * 60)
    log.info("ECB SAFE Ingestion — run_id=%s", run_id)
    log.info("=" * 60)

    # ------------------------------------------------------------------
    # 0. Load config and environment
    # ------------------------------------------------------------------
    cfg = load_config()
    ing = cfg["ingestion"]
    md_cfg = cfg["motherduck"]

    token = os.environ.get("MOTHERDUCK_TOKEN", "").strip()
    if not token:
        log.critical("MOTHERDUCK_TOKEN environment variable is not set. Aborting.")
        sys.exit(1)

    zip_url: str = ing["zip_url"]
    timeout: int = int(ing["request_timeout_seconds"])
    session = build_session(ing["user_agent"], timeout)

    schema = md_cfg["schema"]
    table = md_cfg["table"]
    log_table = md_cfg["log_table"]

    # ------------------------------------------------------------------
    # 1. Connect to MotherDuck and verify DDL
    # ------------------------------------------------------------------
    conn = connect_motherduck(token, md_cfg["database"])
    ensure_schema_and_tables(conn, cfg)

    # ------------------------------------------------------------------
    # 2. HEAD request (with 404 → scrape fallback)
    # ------------------------------------------------------------------
    etag: Optional[str] = None
    last_modified: Optional[str] = None

    try:
        log.info("HEAD %s", zip_url)
        resp = head_request(session, zip_url, timeout)

        if resp.status_code == 404:
            log.warning("Primary URL returned 404 — falling back to scrape.")
            zip_url = scrape_zip_url(session, ing["fallback_scrape_url"], timeout)
            save_zip_url(zip_url)
            # Re-issue HEAD against the discovered URL
            log.info("HEAD %s (fallback)", zip_url)
            resp = head_request(session, zip_url, timeout)
            resp.raise_for_status()

        etag = resp.headers.get("ETag") or None
        last_modified = resp.headers.get("Last-Modified") or None
        log.info("ETag=%s  Last-Modified=%s", etag, last_modified)

    except Exception as exc:
        duration_s = time.monotonic() - t0
        log.exception("HEAD request failed: %s", exc)
        write_log(
            conn,
            schema,
            log_table,
            run_id=run_id,
            run_at=run_at,
            url_used=zip_url,
            etag=None,
            last_modified=None,
            status="error",
            error_message=str(exc),
            duration_s=duration_s,
        )
        conn.close()
        sys.exit(1)

    # ------------------------------------------------------------------
    # 3. Watermark comparison
    # ------------------------------------------------------------------
    prev_etag, prev_last_modified = get_watermark(conn, schema, log_table)
    log.info("Watermark — prev_etag=%s  prev_last_modified=%s", prev_etag, prev_last_modified)

    def _is_unchanged() -> bool:
        if etag and prev_etag:
            return etag == prev_etag
        if last_modified and prev_last_modified:
            return last_modified == prev_last_modified
        # No headers at all → cannot determine; treat as changed to be safe.
        return False

    if _is_unchanged():
        duration_s = time.monotonic() - t0
        log.info("Data is unchanged — skipping download.")
        write_log(
            conn,
            schema,
            log_table,
            run_id=run_id,
            run_at=run_at,
            url_used=zip_url,
            etag=etag,
            last_modified=last_modified,
            status="skipped",
            rows_loaded=0,
            duration_s=duration_s,
        )
        conn.close()
        log.info("Done (skipped) — %.1fs", duration_s)
        return

    # ------------------------------------------------------------------
    # 4. Download, extract, and load
    # ------------------------------------------------------------------
    rows_loaded: int = 0
    try:
        with tempfile.TemporaryDirectory() as tmp_dir:
            zip_dest = Path(tmp_dir) / "safe_microdata.zip"
            download_zip(session, zip_url, timeout, zip_dest)

            ingested_at = datetime.now(tz=timezone.utc)
            rows_loaded = load_csvs_into_motherduck(
                conn, zip_dest, schema, table, ingested_at
            )

    except Exception as exc:
        duration_s = time.monotonic() - t0
        log.exception("Data load failed: %s", exc)
        write_log(
            conn,
            schema,
            log_table,
            run_id=run_id,
            run_at=run_at,
            url_used=zip_url,
            etag=etag,
            last_modified=last_modified,
            status="error",
            rows_loaded=rows_loaded,
            error_message=str(exc),
            duration_s=duration_s,
        )
        conn.close()
        sys.exit(1)

    # ------------------------------------------------------------------
    # 5. Log success
    # ------------------------------------------------------------------
    duration_s = time.monotonic() - t0
    write_log(
        conn,
        schema,
        log_table,
        run_id=run_id,
        run_at=run_at,
        url_used=zip_url,
        etag=etag,
        last_modified=last_modified,
        status="success",
        rows_loaded=rows_loaded,
        duration_s=duration_s,
    )
    conn.close()
    log.info("Done (success) — %d rows in %.1fs", rows_loaded, duration_s)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    run()
