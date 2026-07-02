"""
Standalone helper to append the most recent run snapshot to run_log.json.

run_report.py now appends to run_log.json directly during report generation,
so this script is only needed if run_report.py was interrupted before writing,
or for manual back-fills.

Usage:
  env/bin/python3 reports/append_run_log.py  [--run-log path/to/run_log.json]
"""

import json
from pathlib import Path

OUTPUT_DIR = Path(__file__).parent / "output"
RUN_LOG = OUTPUT_DIR / "run_log.json"


def append_run_log(cost: dict | None = None) -> dict:
    """Append one entry to run_log.json. Returns the entry written.

    If cost dict is provided, appends it directly.
    If cost is None, no-ops (run_report.py is responsible for writing).
    """
    if cost is None:
        raise ValueError("cost dict required — run_report.py writes run_log.json automatically")

    existing: list = json.loads(RUN_LOG.read_text()) if RUN_LOG.exists() else []
    # Avoid duplicate: if last entry matches, skip
    if existing and existing[-1].get("run_date") == cost.get("run_date") and \
            existing[-1].get("wave_number") == cost.get("wave_number"):
        return existing[-1]

    existing.append(cost)
    RUN_LOG.write_text(json.dumps(existing, indent=2, ensure_ascii=False))
    return cost


def main() -> None:
    print("append_run_log.py: run_report.py now writes run_log.json directly.")
    print(f"Current entries in {RUN_LOG}: {len(json.loads(RUN_LOG.read_text())) if RUN_LOG.exists() else 0}")


if __name__ == "__main__":
    main()
