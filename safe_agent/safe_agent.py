"""SAFE Agent — main orchestration entry point."""

from __future__ import annotations

import os
import sys
from pathlib import Path

from query_marts import fetch_report_data, to_json_payload
from fetch_ecb_report import fetch_ecb_report
from llm_loop import run_llm_loop
from render_report import render
from send_email import send_report_email


_PAGES_URL = os.environ.get(
    "PAGES_URL",
    "https://lubospernis.github.io/safe_ai/",
)
_RECIPIENTS_ENV = os.environ.get("REPORT_RECIPIENTS", "lubos.pernis@gmail.com")


def _all_mart_columns() -> list[str]:
    """Return a flat list of all column names across all mart tables for gap analysis."""
    from query_marts import _connect
    con = _connect()
    tables = [
        "main_safe.mart_safe__financing_conditions",
        "main_safe.mart_safe__business_situation",
        "main_safe.mart_safe__q0b_pressingness",
        "main_safe.mart_safe__outlook",
        "main_safe.mart_safe__expectations",
    ]
    cols = []
    for t in tables:
        rows = con.execute(f"DESCRIBE {t}").fetchall()
        cols.extend(f"{t}.{r[0]}" for r in rows)
    con.close()
    return cols


def main() -> None:
    print("[safe_agent] starting")

    # 1. Query MotherDuck
    print("[safe_agent] querying MotherDuck marts...")
    data = fetch_report_data(country_code="SK", n_waves=10)
    data_json = to_json_payload(data)
    print(f"[safe_agent] latest wave: {data.latest_wave} ({data.latest_period_label})")

    # 2. Fetch ECB reference PDF
    print("[safe_agent] fetching ECB SAFE report PDF...")
    try:
        ecb_report = fetch_ecb_report()
    except Exception as exc:
        print(f"[safe_agent] WARNING: could not fetch ECB PDF ({exc}); proceeding without reference.")
        ecb_report = {"full_text": "", "slovakia_text": "", "euroarea_text": "", "source_url": ""}

    # 3. LLM loop
    print("[safe_agent] running LLM draft/judge/rewrite loop...")
    mart_columns = _all_mart_columns()
    result = run_llm_loop(data_json, ecb_report, mart_columns)
    print(
        f"[safe_agent] LLM loop complete: {result['iterations']} iteration(s), "
        f"final score {result['final_score']:.2f}"
    )

    # 4. Render HTML
    print("[safe_agent] rendering HTML report...")
    out_path = render(
        result=result,
        data_json=data_json,
        wave_label=data.latest_period_label,
        ecb_source_url=ecb_report.get("source_url", ""),
    )
    print(f"[safe_agent] report written to {out_path}")

    # 5. Send email
    recipients = [r.strip() for r in _RECIPIENTS_ENV.split(",") if r.strip()]
    send_report_email(
        recipients=recipients,
        wave_label=data.latest_period_label,
        executive_summary=result["executive_summary"],
        pages_url=_PAGES_URL,
        final_score=result["final_score"],
        iterations=result["iterations"],
    )

    print("[safe_agent] done.")


if __name__ == "__main__":
    sys.path.insert(0, str(Path(__file__).parent))
    main()
