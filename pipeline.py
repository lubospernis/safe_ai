"""
ECB SAFE Microdata — dlt pipeline entry point
==============================================
Run locally:
    python pipeline.py

The pipeline reads config from .dlt/config.toml and credentials from
.dlt/secrets.toml (or environment variables — see README).
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
        destination="motherduck",
        dataset_name="raw",
    )

    load_info = pipeline.run(safe_microdata_source())
    print(load_info)


if __name__ == "__main__":
    main()
