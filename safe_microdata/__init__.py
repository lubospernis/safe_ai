"""
ECB SAFE Microdata — dlt source
================================
Downloads the ECB SAFE microdata zip, extracts every CSV, and yields rows
for the dlt pipeline to load into MotherDuck.

Incremental behaviour
---------------------
The ETag (or Last-Modified if no ETag) from the HEAD response is stored in
dlt pipeline state.  If the header is unchanged from the last successful run
the resource exits early and yields nothing, so dlt performs no load.

Config (via .dlt/config.toml  [sources.safe_microdata_source])
--------------------------------------------------------------
  zip_url              Primary download URL for the zip file.
  fallback_scrape_url  Page to scrape for a .zip link if zip_url returns 404.
  request_timeout      HTTP timeout in seconds (default 60).
"""

from __future__ import annotations

import csv
import logging
import tempfile
import zipfile
from pathlib import Path
from typing import Iterator
from urllib.parse import urljoin

import dlt
import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

def _make_session() -> requests.Session:
    session = requests.Session()
    session.headers["User-Agent"] = (
        "safe-ai-dlt/1.0 (research pipeline; contact: lubos.pernis@gmail.com)"
    )
    return session


def _scrape_zip_url(session: requests.Session, scrape_url: str, timeout: int) -> str:
    """Scrape *scrape_url* and return the first absolute .zip href found."""
    logger.info("Scraping fallback page: %s", scrape_url)
    resp = session.get(scrape_url, timeout=timeout)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    for tag in soup.find_all("a", href=True):
        href: str = tag["href"]
        if href.lower().endswith(".zip"):
            url = urljoin(scrape_url, href)
            logger.info("Found zip link: %s", url)
            return url
    raise RuntimeError(f"No .zip link found on fallback page: {scrape_url}")


# ---------------------------------------------------------------------------
# dlt source / resource
# ---------------------------------------------------------------------------

@dlt.source
def safe_microdata_source(
    zip_url: str = dlt.config.value,
    fallback_scrape_url: str = dlt.config.value,
    request_timeout: int = dlt.config.value,
) -> dlt.sources.DltSource:
    """dlt source for the ECB SAFE microdata zip."""
    return _safe_microdata(zip_url, fallback_scrape_url, request_timeout)


@dlt.resource(
    name="safe_microdata",
    write_disposition="replace",
)
def _safe_microdata(
    zip_url: str,
    fallback_scrape_url: str,
    request_timeout: int,
) -> Iterator[dict]:
    session = _make_session()
    state = dlt.current.source_state()

    # ------------------------------------------------------------------ #
    # 1. HEAD request — resolve URL and fetch cache headers               #
    # ------------------------------------------------------------------ #
    logger.info("HEAD %s", zip_url)
    resp = session.head(zip_url, timeout=request_timeout, allow_redirects=True)

    if resp.status_code == 404:
        logger.warning("Primary URL returned 404 — scraping fallback page.")
        zip_url = _scrape_zip_url(session, fallback_scrape_url, request_timeout)
        resp = session.head(zip_url, timeout=request_timeout, allow_redirects=True)
        resp.raise_for_status()
    else:
        resp.raise_for_status()

    etag: str | None = resp.headers.get("ETag")
    last_modified: str | None = resp.headers.get("Last-Modified")
    logger.info("ETag=%s  Last-Modified=%s", etag, last_modified)

    # ------------------------------------------------------------------ #
    # 2. Watermark check — skip if nothing has changed                    #
    # ------------------------------------------------------------------ #
    if etag and etag == state.get("etag"):
        logger.info("ETag unchanged — no new data, skipping load.")
        return
    if not etag and last_modified and last_modified == state.get("last_modified"):
        logger.info("Last-Modified unchanged — no new data, skipping load.")
        return

    # ------------------------------------------------------------------ #
    # 3. Download zip and extract CSVs                                    #
    # ------------------------------------------------------------------ #
    rows: list[dict] = []

    with tempfile.TemporaryDirectory() as tmp_dir:
        zip_path = Path(tmp_dir) / "safe_microdata.zip"

        logger.info("Downloading zip from %s …", zip_url)
        with session.get(zip_url, timeout=request_timeout, stream=True) as dl:
            dl.raise_for_status()
            with zip_path.open("wb") as fh:
                for chunk in dl.iter_content(chunk_size=1 << 20):
                    fh.write(chunk)
        size_mb = zip_path.stat().st_size / (1 << 20)
        logger.info("Download complete — %.1f MiB", size_mb)

        with zipfile.ZipFile(zip_path, "r") as zf:
            csv_names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
            if not csv_names:
                raise RuntimeError("No CSV files found inside the downloaded zip.")
            logger.info("Extracting %d CSV file(s): %s", len(csv_names), csv_names)
            zf.extractall(tmp_dir)

        for csv_name in csv_names:
            csv_path = Path(tmp_dir) / csv_name
            logger.info("Reading %s …", csv_name)
            with csv_path.open("r", encoding="utf-8", errors="replace") as fh:
                for row in csv.DictReader(fh):
                    rows.append(dict(row))

    logger.info("Yielding %d rows to dlt.", len(rows))

    # ------------------------------------------------------------------ #
    # 4. Yield rows — outside the temp dir context so it is already gone  #
    # ------------------------------------------------------------------ #
    yield from rows

    # ------------------------------------------------------------------ #
    # 5. Persist watermark after successful yield                         #
    # ------------------------------------------------------------------ #
    if etag:
        state["etag"] = etag
    if last_modified:
        state["last_modified"] = last_modified
    logger.info("State updated — etag=%s  last_modified=%s", etag, last_modified)
