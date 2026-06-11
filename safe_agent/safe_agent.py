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


def _mart_schema_summary() -> str:
    """Return column-level schema of mart_safe__slovakia_kpis for the gap-finder prompt.

    The gap finder needs to know what KPIs the pipeline *already* covers so it
    doesn't hallucinate gaps.  We use the KPI mart (not raw marts) because it
    contains exactly the series the agent surfaces — nothing ambiguous.
    """
    from query_marts import _connect
    con = _connect()
    kpi_table = "main_safe.mart_safe__slovakia_kpis"
    try:
        cols = con.execute(f"DESCRIBE {kpi_table}").fetchall()
        # col tuple: (name, type, null, key, default, extra)
        lines = [f"mart_safe__slovakia_kpis — pre-selected KPIs for Slovakia SMEs:"]
        for col in cols:
            lines.append(f"  {col[0]}  ({col[1]})")
    except Exception as exc:
        lines = [f"[schema_summary] could not describe {kpi_table}: {exc}"]
    con.close()
    return "\n".join(lines)


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
    mart_schema = _mart_schema_summary()
    result = run_llm_loop(data_json, ecb_report, mart_schema)
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
