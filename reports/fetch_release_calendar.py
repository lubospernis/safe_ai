"""
Fetch the next ECB SAFE dataset release date from the ECB statistical release
calendar and store it in MotherDuck main_safe.ref_safe__release_calendar.

Source: https://www.ecb.europa.eu/press/calendars/statscal/html/index.en.html
The page lists <dt>DD/MM/YYYY HH:MM CET</dt><dd>...(Dataset: SAFE)...
Reference period: QX YYYY...</dd> pairs for every dataset ECB publishes.

Soft-fails: if the SAFE row can't be found (page structure changed, or ECB
paused publishing dates), the previous stored value is left untouched and a
warning is printed — CI treats this the same as the existing annex-fetch
failure path (see .github/workflows/safe_microdata.yml).

Usage:
  python reports/fetch_release_calendar.py

Required env: MOTHERDUCK_TOKEN
"""

import os
import re
import sys
import time
from datetime import date, datetime

import duckdb
import requests
from bs4 import BeautifulSoup

STATSCAL_URL = "https://www.ecb.europa.eu/press/calendars/statscal/html/index.en.html"

_DDL = """
    CREATE TABLE IF NOT EXISTS main_safe.ref_safe__release_calendar (
        dataset              TEXT PRIMARY KEY,
        next_release_date    DATE,
        next_release_time_cet TEXT,
        reference_period     TEXT,
        fetched_at           TIMESTAMP
    )
"""

_UPSERT = """
    INSERT OR REPLACE INTO main_safe.ref_safe__release_calendar (
        dataset, next_release_date, next_release_time_cet, reference_period, fetched_at
    ) VALUES (?, ?, ?, ?, ?)
"""


def fetch_safe_release() -> dict | None:
    """Scrape the ECB stats calendar and return the next SAFE release info, or None."""
    resp = None
    last_error: Exception | None = None
    for attempt in range(3):
        try:
            resp = requests.get(STATSCAL_URL, timeout=30, headers={"User-Agent": "Mozilla/5.0"})
            resp.raise_for_status()
            last_error = None
            break
        except Exception as e:
            last_error = e
            resp = None
            if attempt < 2:
                time.sleep(2 ** attempt)
    if resp is None:
        print(f"  WARNING: could not fetch ECB stats calendar: {last_error}")
        return None

    soup = BeautifulSoup(resp.text, "lxml")
    for dt_el in soup.find_all("dt"):
        dd_el = dt_el.find_next_sibling("dd")
        if not dd_el:
            continue
        dd_text = dd_el.get_text(" ", strip=True)
        if "Dataset: SAFE)" not in dd_text:
            continue

        dt_text = dt_el.get_text(" ", strip=True)
        date_m = re.search(r"(\d{2})/(\d{2})/(\d{4})\s+(\d{2}:\d{2})\s*(CET|CEST)?", dt_text)
        if not date_m:
            print(f"  WARNING: found SAFE row but couldn't parse date from {dt_text!r}")
            return None
        day, month, year, time_str, tz = date_m.groups()
        release_date = date(int(year), int(month), int(day))
        time_cet = f"{time_str} {tz or 'CET'}"

        period_m = re.search(r"Reference period:\s*([^\n<]+?)(?:\s*Includes|\s*$)", dd_text)
        reference_period = period_m.group(1).strip() if period_m else ""

        return {
            "next_release_date": release_date,
            "next_release_time_cet": time_cet,
            "reference_period": reference_period,
        }

    print("  WARNING: no 'Dataset: SAFE' row found on the ECB stats calendar page")
    return None


def main() -> None:
    result = fetch_safe_release()
    if result is None:
        print("Release calendar fetch failed — leaving previous stored value in place.")
        sys.exit(1)

    token = os.environ["MOTHERDUCK_TOKEN"]
    con = None
    for attempt in range(3):
        try:
            con = duckdb.connect(f"md:my_db?motherduck_token={token}")
            break
        except Exception:
            if attempt < 2:
                time.sleep(2 ** attempt)
            else:
                raise
    con.execute(_DDL)
    con.execute(_UPSERT, [
        "SAFE",
        result["next_release_date"],
        result["next_release_time_cet"],
        result["reference_period"],
        datetime.utcnow(),
    ])
    print(
        f"Release calendar updated: SAFE next release "
        f"{result['next_release_date'].isoformat()} {result['next_release_time_cet']} "
        f"(reference period {result['reference_period']})"
    )


if __name__ == "__main__":
    main()
