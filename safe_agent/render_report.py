"""Render the final HTML report to docs/index.html using Jinja2."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from jinja2 import Environment, FileSystemLoader

_TEMPLATES_DIR = Path(__file__).parent.parent / "templates"
_DOCS_DIR = Path(__file__).parent.parent / "docs"


def _strip_fences(text: str) -> str:
    text = text.strip()
    if text.startswith("```"):
        lines = text.split("\n")
        if lines[-1].strip() == "```":
            lines = lines[1:-1]
        else:
            lines = lines[1:]
        text = "\n".join(lines)
    return text.strip()


def _get(rows: list, wave: int, key: str, **filters) -> float | None:
    for r in rows:
        if r.get("wave_number") != wave:
            continue
        if all(r.get(k) == v for k, v in filters.items()):
            val = r.get(key)
            return round(float(val), 1) if val is not None else None
    return None


def _wave_labels(rows: list, country_code: str) -> tuple[list[int], list[str]]:
    waves = sorted({r["wave_number"] for r in rows if r.get("country_code") == country_code})
    labels = []
    seen: dict[int, str] = {}
    for r in rows:
        w = r["wave_number"]
        if w not in seen and r.get("country_code") == country_code:
            seen[w] = r.get("survey_period_label") or str(w)
    labels = [seen.get(w, str(w)) for w in waves]
    return waves, labels


def _build_chart_datasets(data_json: dict, country_code: str = "SK") -> dict:
    charts = {}

    # --- Financing conditions: 4 most policy-relevant sub-items ---
    fin = [r for r in data_json.get("financing_conditions", []) if r.get("country_code") == country_code]
    if fin:
        waves, labels = _wave_labels(fin, country_code)
        # Priority sub-items — match by keyword
        priority_keywords = [
            ("Interest rates", "Level of interest rates"),
            ("Loan availability", "Bank loans (excl. overdraft"),
            ("Non-interest costs", "Non-interest financing costs"),
            ("Credit lines", "Credit line, bank overdraft"),
        ]
        datasets = []
        for chart_label, keyword in priority_keywords:
            matched = next((r["sub_item_label"] for r in fin if keyword.lower() in r["sub_item_label"].lower()), None)
            if matched:
                vals = [_get(fin, w, "net_balance_wtd", sub_item_label=matched) for w in waves]
                if any(v is not None for v in vals):
                    datasets.append({"label": chart_label, "data": vals})
        charts["financing"] = {"labels": labels, "datasets": datasets}

    # --- Loan applications: grouped bar (latest wave) ---
    loans = [r for r in data_json.get("loan_applications", []) if r.get("country_code") == country_code]
    if loans:
        waves, wave_labels_list = _wave_labels(loans, country_code)
        instruments = sorted({r["instrument_label"] for r in loans if r.get("instrument_label")})
        app_series, disc_series, rej_series = [], [], []
        for ins in instruments:
            app_series.append(_get(loans, waves[-1], "application_rate_wtd", instrument_label=ins))
            disc_series.append(_get(loans, waves[-1], "discouragement_rate_wtd", instrument_label=ins))
            rej_series.append(_get(loans, waves[-1], "rejection_rate_wtd", instrument_label=ins))

        # Also build trend lines for bank loans over time
        trend_datasets = []
        for metric, label in [("application_rate_wtd", "Application rate"), ("discouragement_rate_wtd", "Discouragement rate"), ("rejection_rate_wtd", "Rejection rate")]:
            vals = [_get(loans, w, metric, instrument_label="Bank loan (excl. overdraft and credit lines)") for w in waves]
            if any(v is not None for v in vals):
                trend_datasets.append({"label": label, "data": vals})

        charts["loan_applications"] = {
            "labels": wave_labels_list,
            "bar": {
                "labels": instruments,
                "datasets": [
                    {"label": "Application rate %", "data": app_series},
                    {"label": "Discouragement rate %", "data": disc_series},
                    {"label": "Rejection rate %", "data": rej_series},
                ],
            },
            "trend": {"labels": wave_labels_list, "datasets": trend_datasets},
        }

    # --- Business situation: two focused charts ---
    biz = [r for r in data_json.get("business_situation", []) if r.get("country_code") == country_code]
    if biz:
        waves, labels = _wave_labels(biz, country_code)
        # Chart 1: performance (turnover, profit, employment, investment)
        perf_items = ["Turnover", "Profit", "Number of employees", "Investments in property, plant or equipment"]
        perf_datasets = []
        for sub in perf_items:
            # match case-insensitively / partial
            matching = {r["sub_item_label"] for r in biz if sub.lower() in r["sub_item_label"].lower()}
            for m in sorted(matching):
                vals = [_get(biz, w, "net_balance_wtd", sub_item_label=m) for w in waves]
                if any(v is not None for v in vals):
                    perf_datasets.append({"label": m, "data": vals})
                    break
        charts["business_performance"] = {"labels": labels, "datasets": perf_datasets}
        # Chart 2: cost pressures
        cost_items = ["Labour costs", "Other costs", "Interest expenses"]
        cost_datasets = []
        for sub in cost_items:
            matching = {r["sub_item_label"] for r in biz if sub.lower() in r["sub_item_label"].lower()}
            for m in sorted(matching):
                vals = [_get(biz, w, "net_balance_wtd", sub_item_label=m) for w in waves]
                if any(v is not None for v in vals):
                    cost_datasets.append({"label": m, "data": vals})
                    break
        charts["business_costs"] = {"labels": labels, "datasets": cost_datasets}
        # Keep "business" as alias for performance for backward compat with any old placeholders
        charts["business"] = charts["business_performance"]

    # --- Pressing problems: horizontal bar, latest wave, deduplicated ---
    press = [r for r in data_json.get("pressing_problems", []) if r.get("country_code") == country_code]
    if press:
        latest = max(r["wave_number"] for r in press)
        latest_rows = [r for r in press if r["wave_number"] == latest]
        # Deduplicate by problem_id — prefer 6m, already done in query but guard here too
        seen_ids: set = set()
        deduped = []
        for r in sorted(latest_rows, key=lambda x: 1 if x.get("reference_period") == "6m" else 2):
            pid = r.get("problem_id")
            if pid not in seen_ids:
                deduped.append(r)
                seen_ids.add(pid)
        deduped.sort(key=lambda r: r.get("avg_pressingness_wtd") or 0, reverse=True)
        charts["pressingness"] = {
            "labels": [r["problem_label"] for r in deduped],
            "sk": [round(r["avg_pressingness_wtd"] or 0, 2) for r in deduped],
        }

    # --- Outlook: net balance trend lines ---
    outlook = [r for r in data_json.get("outlook", []) if r.get("country_code") == country_code]
    if outlook:
        waves, labels = _wave_labels(outlook, country_code)
        sub_items = sorted({r["sub_item_label"] for r in outlook})
        datasets = []
        for sub in sub_items:
            vals = [_get(outlook, w, "net_balance_wtd", sub_item_label=sub) for w in waves]
            if any(v is not None for v in vals):
                datasets.append({"label": sub, "data": vals})
        charts["outlook"] = {"labels": labels, "datasets": datasets}

    # --- Expectations: bar chart of latest-wave mean_wtd by sub_item ---
    exp = [r for r in data_json.get("expectations", []) if r.get("country_code") == country_code]
    if exp:
        latest = max(r["wave_number"] for r in exp)
        latest_exp = [r for r in exp if r["wave_number"] == latest]
        # Group by question for separate series
        questions = sorted({r.get("question_label", "") for r in latest_exp})
        datasets = []
        all_labels: list[str] = []
        all_vals: list[float | None] = []
        for q in questions:
            q_rows = sorted([r for r in latest_exp if r.get("question_label") == q],
                            key=lambda r: r.get("sub_item_label", ""))
            for r in q_rows:
                lbl = r.get("sub_item_label") or r.get("question_label", "")
                val = r.get("mean_wtd")
                all_labels.append(lbl)
                all_vals.append(round(float(val), 2) if val is not None else None)
        charts["expectations"] = {
            "labels": all_labels,
            "datasets": [{"label": "Expected change (%)", "data": all_vals}],
        }

    return charts


def render(
    result: dict,
    data_json: dict,
    wave_label: str,
    ecb_source_url: str,
    country_code: str = "SK",
) -> Path:
    _DOCS_DIR.mkdir(exist_ok=True)

    env = Environment(loader=FileSystemLoader(str(_TEMPLATES_DIR)), autoescape=False)
    template = env.get_template("report.html.j2")

    charts = _build_chart_datasets(data_json, country_code)

    report_body = _strip_fences(result["report_html"])
    gap_section = _strip_fences(result["gap_section"])

    # Serialise — replace NaN/Inf with null so JSON is valid
    charts_json = json.dumps(charts, allow_nan=False, default=lambda x: None)

    html = template.render(
        report_body=report_body,
        gap_section=gap_section,
        wave_label=wave_label,
        generated_at=datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        iterations=result["iterations"],
        final_score=round(result["final_score"], 2),
        ecb_source_url=ecb_source_url,
        charts_json=charts_json,
    )

    out = _DOCS_DIR / "index.html"
    out.write_text(html, encoding="utf-8")
    print(f"[render] written to {out}")
    return out
