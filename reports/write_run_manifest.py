"""
Write run manifest to main_safe.ref_safe__run_log after each report CI run.

Reads:
  reports/output/run_log.json        — written by run_report.py (last entry = current run)
  reports/output/quality_scores.json — written by quality_check.py

Usage:
  python reports/write_run_manifest.py

Required env: MOTHERDUCK_TOKEN

Schema (ref_safe__run_log):
  run_id, wave_number, run_date, run_time, run_type, git_sha
  total_cost_usd, input_tokens, output_tokens, cache_read_tokens
  cost_by_model     JSON  -- {"claude-sonnet-4-6": {calls, input_tokens, output_tokens, usd}, ...}
  n_sections, duration_seconds, grounding_warning_count
  quality_readability, quality_substance, quality_coherence, quality_sign_convention
  quality_verdict, quality_reason
  adhoc_grounding, adhoc_coverage, adhoc_readability, adhoc_chart_alignment, adhoc_verdict
"""

import json
import os
import subprocess
from datetime import date, datetime
from pathlib import Path

import duckdb

OUTPUT_DIR = Path(__file__).parent / "output"

_DDL = """
    CREATE OR REPLACE TABLE main_safe.ref_safe__run_log (
        run_id                  TEXT PRIMARY KEY,
        wave_number             INTEGER,
        run_date                DATE,
        run_time                TEXT,
        run_type                TEXT,
        git_sha                 TEXT,
        total_cost_usd          FLOAT,
        input_tokens            INTEGER,
        output_tokens           INTEGER,
        cache_read_tokens       INTEGER,
        cost_by_model           JSON,
        n_sections              INTEGER,
        duration_seconds        FLOAT,
        grounding_warning_count INTEGER,
        quality_readability     FLOAT,
        quality_substance       FLOAT,
        quality_coherence       FLOAT,
        quality_sign_convention FLOAT,
        quality_verdict         TEXT,
        quality_reason          TEXT,
        adhoc_grounding         FLOAT,
        adhoc_coverage          FLOAT,
        adhoc_readability       FLOAT,
        adhoc_chart_alignment   FLOAT,
        adhoc_verdict           TEXT
    )
"""

_INSERT = """
    INSERT OR REPLACE INTO main_safe.ref_safe__run_log (
        run_id, wave_number, run_date, run_time, run_type, git_sha,
        total_cost_usd, input_tokens, output_tokens, cache_read_tokens,
        cost_by_model, n_sections, duration_seconds, grounding_warning_count,
        quality_readability, quality_substance, quality_coherence, quality_sign_convention,
        quality_verdict, quality_reason,
        adhoc_grounding, adhoc_coverage, adhoc_readability, adhoc_chart_alignment, adhoc_verdict
    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
"""


def main() -> None:
    run_log_path = OUTPUT_DIR / "run_log.json"
    all_runs = json.loads(run_log_path.read_text())
    cost = all_runs[-1]  # last entry = current run

    quality_path = OUTPUT_DIR / "quality_scores.json"
    quality = (
        json.loads(quality_path.read_text())
        if quality_path.exists()
        else {"readability": None, "substance": None, "coherence": None,
              "sign_convention": None, "verdict": "unknown", "reason": "quality_scores.json missing"}
    )
    adhoc_q = quality.get("adhoc", {})

    wave = cost["wave_number"]
    sha = subprocess.check_output(["git", "rev-parse", "--short", "HEAD"]).decode().strip()
    now = datetime.utcnow()
    run_id = f"{wave}_{now.strftime('%Y%m%d_%H%M%S')}"

    token = os.environ["MOTHERDUCK_TOKEN"]
    con = duckdb.connect(f"md:my_db?motherduck_token={token}")

    # Drop and recreate with the canonical schema on first run after schema change.
    # Safe because old rows weren't load-bearing (wave 38 test runs only).
    existing_cols = {
        row[0]
        for row in con.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = 'main_safe' AND table_name = 'ref_safe__run_log'"
        ).fetchall()
    }
    target_cols = {
        "run_id", "wave_number", "run_date", "run_time", "run_type", "git_sha",
        "total_cost_usd", "input_tokens", "output_tokens", "cache_read_tokens",
        "cost_by_model", "n_sections", "duration_seconds", "grounding_warning_count",
        "quality_readability", "quality_substance", "quality_coherence", "quality_sign_convention",
        "quality_verdict", "quality_reason",
        "adhoc_grounding", "adhoc_coverage", "adhoc_readability", "adhoc_chart_alignment", "adhoc_verdict",
    }
    if not existing_cols >= target_cols:
        # Schema mismatch — recreate the table (old data was not important)
        print("  Schema mismatch — recreating ref_safe__run_log with canonical schema")
        con.execute("DROP TABLE IF EXISTS main_safe.ref_safe__run_log")
        con.execute(_DDL)
    else:
        con.execute("CREATE TABLE IF NOT EXISTS main_safe.ref_safe__run_log (run_id TEXT PRIMARY KEY)")

    con.execute(_INSERT, [
        run_id,
        wave,
        date.today(),
        now.strftime("%H:%M:%S"),
        cost.get("run_type", "main"),
        sha,
        cost["total_cost_usd"],
        cost["input_tokens"],
        cost["output_tokens"],
        cost["cache_read_tokens"],
        json.dumps(cost.get("cost_by_model", {})),
        cost["n_sections"],
        cost["duration_seconds"],
        cost.get("grounding_warning_count", 0),
        quality["readability"],
        quality["substance"],
        quality["coherence"],
        quality["sign_convention"],
        quality["verdict"],
        quality.get("reason", ""),
        adhoc_q.get("grounding"),
        adhoc_q.get("coverage"),
        adhoc_q.get("readability"),
        adhoc_q.get("chart_alignment"),
        adhoc_q.get("verdict"),
    ])

    print(
        f"Run manifest written: {run_id}  "
        f"cost=${cost['total_cost_usd']:.3f}  "
        f"verdict={quality['verdict']}"
    )


if __name__ == "__main__":
    main()
