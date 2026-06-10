"""Render the final HTML report to docs/index.html using Jinja2."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from jinja2 import Environment, FileSystemLoader

_TEMPLATES_DIR = Path(__file__).parent.parent / "templates"
_DOCS_DIR = Path(__file__).parent.parent / "docs"


def _build_chart_datasets(data_json: dict, country_code: str = "SK") -> dict:
    """Transform mart data into Chart.js-ready datasets."""
    charts = {}

    # Financing conditions — net balance by sub_item for SK vs EA
    fin = data_json.get("financing_conditions", [])
    if fin:
        waves = sorted({r["wave_number"] for r in fin})
        labels = []
        for w in waves:
            rows = [r for r in fin if r["wave_number"] == w]
            if rows:
                labels.append(rows[0].get("survey_period_label") or str(w))
            else:
                labels.append(str(w))
        labels = list(dict.fromkeys(labels))  # deduplicate keeping order

        sub_items = sorted({r["sub_item_label"] for r in fin if r["country_code"] == country_code})
        financing_datasets = []
        for sub in sub_items:
            sk_vals = []
            ea_vals = []
            for w in waves:
                sk_row = next(
                    (r for r in fin if r["wave_number"] == w and r["country_code"] == country_code and r["sub_item_label"] == sub),
                    None,
                )
                ea_row = next(
                    (r for r in fin if r["wave_number"] == w and r["country_code"] == "EA" and r["sub_item_label"] == sub),
                    None,
                )
                sk_vals.append(round(sk_row["net_balance_wtd"], 1) if sk_row and sk_row["net_balance_wtd"] is not None else None)
                ea_vals.append(round(ea_row["net_balance_wtd"], 1) if ea_row and ea_row["net_balance_wtd"] is not None else None)
            financing_datasets.append({"label": f"SK — {sub}", "data": sk_vals, "country": "SK"})
            financing_datasets.append({"label": f"EA — {sub}", "data": ea_vals, "country": "EA", "borderDash": [5, 5]})

        charts["financing"] = {"labels": labels, "datasets": financing_datasets}

    # Business situation — net balance
    biz = data_json.get("business_situation", [])
    if biz:
        waves = sorted({r["wave_number"] for r in biz})
        labels = list(dict.fromkeys(
            next((r["survey_period_label"] for r in biz if r["wave_number"] == w), str(w))
            for w in waves
        ))
        sub_items = sorted({r["sub_item_label"] for r in biz if r["country_code"] == country_code})
        biz_datasets = []
        for sub in sub_items:
            sk_vals = [
                round(next((r["net_balance_wtd"] for r in biz if r["wave_number"] == w and r["country_code"] == country_code and r["sub_item_label"] == sub), None) or 0, 1)
                for w in waves
            ]
            biz_datasets.append({"label": sub, "data": sk_vals})
        charts["business"] = {"labels": labels, "datasets": biz_datasets}

    # Pressing problems — latest wave bar chart
    press = data_json.get("pressing_problems", [])
    if press:
        latest = max(r["wave_number"] for r in press)
        sk_press = [r for r in press if r["wave_number"] == latest and r["country_code"] == country_code]
        sk_press.sort(key=lambda r: r.get("avg_pressingness_wtd") or 0, reverse=True)
        charts["pressingness"] = {
            "labels": [r["problem_label"] for r in sk_press],
            "sk": [round(r["avg_pressingness_wtd"] or 0, 2) for r in sk_press],
            "ea": [
                round(
                    next((r2["avg_pressingness_wtd"] for r2 in press if r2["wave_number"] == latest and r2["country_code"] == "EA" and r2["problem_id"] == r["problem_id"]), 0) or 0,
                    2,
                )
                for r in sk_press
            ],
        }

    return charts


def render(
    result: dict,
    data_json: dict,
    wave_label: str,
    ecb_source_url: str,
    country_code: str = "SK",
) -> Path:
    """Render docs/index.html and return its path."""
    _DOCS_DIR.mkdir(exist_ok=True)

    env = Environment(loader=FileSystemLoader(str(_TEMPLATES_DIR)), autoescape=False)
    template = env.get_template("report.html.j2")

    charts = _build_chart_datasets(data_json, country_code)

    html = template.render(
        report_body=result["report_html"],
        gap_section=result["gap_section"],
        wave_label=wave_label,
        generated_at=datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        iterations=result["iterations"],
        final_score=round(result["final_score"], 2),
        ecb_source_url=ecb_source_url,
        charts_json=json.dumps(charts),
    )

    out = _DOCS_DIR / "index.html"
    out.write_text(html, encoding="utf-8")
    print(f"[render] written to {out}")
    return out
