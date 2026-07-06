"""
Write run manifest to main_safe.ref_safe__run_log after each report CI run.

Reads:
  reports/output/run_log.json        — written by run_report.py (last entry = current run)
  reports/output/quality_scores.json — written by quality_check.py

Usage:
  python reports/write_run_manifest.py

Required env: MOTHERDUCK_TOKEN
"""

import json
import os
import subprocess
from datetime import date, datetime
from pathlib import Path

import duckdb

OUTPUT_DIR = Path(__file__).parent / "output"


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
    run_id = f"{wave}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"

    token = os.environ["MOTHERDUCK_TOKEN"]
    con = duckdb.connect(f"md:my_db?motherduck_token={token}")

    con.execute("""
        CREATE TABLE IF NOT EXISTS main_safe.ref_safe__run_log (
            run_id            TEXT PRIMARY KEY,
            wave_number       INTEGER,
            run_date          DATE,
            git_sha           TEXT,
            model_sonnet      TEXT,
            model_mistral     TEXT,
            total_cost_usd    FLOAT,
            input_tokens      INTEGER,
            output_tokens     INTEGER,
            cache_read_tokens INTEGER,
            quality_readability     FLOAT,
            quality_substance       FLOAT,
            quality_coherence       FLOAT,
            quality_sign_convention FLOAT,
            quality_verdict   TEXT,
            quality_reason    TEXT,
            n_sections        INTEGER,
            duration_seconds  FLOAT,
            adhoc_grounding         FLOAT,
            adhoc_coverage          FLOAT,
            adhoc_readability       FLOAT,
            adhoc_chart_alignment   FLOAT,
            adhoc_verdict           TEXT
        )
    """)

    con.execute("""
        INSERT OR REPLACE INTO main_safe.ref_safe__run_log VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, [
        run_id,
        wave,
        date.today(),
        sha,
        cost["model_sonnet"],
        cost["model_mistral"],
        cost["total_cost_usd"],
        cost["input_tokens"],
        cost["output_tokens"],
        cost["cache_read_tokens"],
        quality["readability"],
        quality["substance"],
        quality["coherence"],
        quality["sign_convention"],
        quality["verdict"],
        quality.get("reason", ""),
        cost["n_sections"],
        cost["duration_seconds"],
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
