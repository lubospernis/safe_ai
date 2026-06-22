"""
ECB SAFE Microdata — dlt pipeline entry point (Oracle on-prem)
==============================================================
Run locally:
    python pipeline.py

The pipeline reads config from .dlt/config.toml and Oracle credentials from
.dlt/secrets.toml (or the DESTINATION__SQLALCHEMY__CREDENTIALS env var).

Dependencies:
    pip install "dlt[sqlalchemy]" python-oracledb beautifulsoup4 requests

Oracle schema
-------------
All rows land in the schema/Oracle user specified by dataset_name.  dlt will
CREATE the table on first run with write_disposition="replace", meaning the
table is dropped and recreated on every successful pipeline execution.  Switch
to "append" in __init__.py if you want incremental loads without truncation.
"""

import logging

import dlt

from safe_microdata import safe_microdata_source

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)


def main() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="safe_microdata",
        destination="sqlalchemy",   # Oracle via python-oracledb / cx_Oracle
        dataset_name="SAFE_RAW",    # Oracle schema (must exist and be granted to the DB user)
    )

    load_info = pipeline.run(safe_microdata_source())
    print(load_info)


if __name__ == "__main__":
    main()
