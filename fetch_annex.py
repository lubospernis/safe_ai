"""
Fetch the ECB SAFE annex XLSX via dlt and load into MotherDuck main_safe.ref_safe__annex.

Run locally:
    python fetch_annex.py

The dlt pipeline uses destination=motherduck and dataset_name=main_safe, so the
resource named "ref_safe__annex" lands in main_safe.ref_safe__annex.

Exits 0 on success, non-zero on failure — CI catches the exit code and opens a
GitHub Issue (see .github/workflows/safe_microdata.yml).
"""

import logging

import dlt

from safe_microdata import safe_annex_source

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)


def main() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="safe_annex",
        destination="motherduck",
        dataset_name="main_safe",
    )

    load_info = pipeline.run(safe_annex_source())
    print(load_info)


if __name__ == "__main__":
    main()
