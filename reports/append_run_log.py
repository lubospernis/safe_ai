"""
Append the current run's cost data to reports/output/run_log.json.

Reads reports/output/cost_tracker.json (written by run_report.py) and appends
one entry to run_log.json, creating the file if it doesn't exist yet.

Run after every report or adhoc run to maintain a persistent local history:
  env/bin/python3 reports/append_run_log.py

Also called by write_run_manifest.py so CI runs update the log automatically.
"""

import json
from datetime import datetime, timezone
from pathlib import Path

OUTPUT_DIR = Path(__file__).parent / "output"
RUN_LOG = OUTPUT_DIR / "run_log.json"
COST_TRACKER = OUTPUT_DIR / "cost_tracker.json"


def append_run_log(cost: dict | None = None) -> dict:
    """Append one entry to run_log.json. Returns the entry written.

    If cost is None, reads cost_tracker.json from disk.
    """
    if cost is None:
        cost = json.loads(COST_TRACKER.read_text())

    now_str = cost.get("run_date", "") + "T" + cost.get("run_time", "")
    wave = cost["wave_number"]
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    run_id = f"{wave}_{ts}"

    entry = {
        "run_id":            run_id,
        "run_type":          cost.get("run_type", "main"),
        "wave_number":       wave,
        "run_date":          cost.get("run_date", ""),
        "run_time":          cost.get("run_time", ""),
        "total_cost_usd":    cost["total_cost_usd"],
        "input_tokens":      cost["input_tokens"],
        "output_tokens":     cost["output_tokens"],
        "cache_read_tokens": cost.get("cache_read_tokens", 0),
        "n_sections":        cost.get("n_sections", 0),
        "duration_seconds":  cost.get("duration_seconds", 0.0),
        "model_sonnet":      cost.get("model_sonnet", ""),
        "model_mistral":     cost.get("model_mistral", ""),
    }

    existing: list = json.loads(RUN_LOG.read_text()) if RUN_LOG.exists() else []
    existing.append(entry)
    RUN_LOG.write_text(json.dumps(existing, indent=2, ensure_ascii=False))
    return entry


def main() -> None:
    entry = append_run_log()
    print(
        f"Run log updated: {entry['run_id']}  "
        f"type={entry['run_type']}  "
        f"cost=${entry['total_cost_usd']:.4f}  "
        f"wave={entry['wave_number']}"
    )
    print(f"  → {RUN_LOG}  ({len(json.loads(RUN_LOG.read_text()))} entries total)")


if __name__ == "__main__":
    main()
