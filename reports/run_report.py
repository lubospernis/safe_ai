"""
SAFE Survey Report Generator — config-driven multi-section orchestrator.

Loops over SECTIONS in config.py:
  1. Fetch data for all sections via MotherDuck (prod) or local dev.duckdb (dev)
  2. Parallel Haiku interest checks — returns interesting flag, chart_type, best_panel
  3. For interesting sections: build chart + generate Sonnet bullets
  4. Generate executive summary (Sonnet prose) from all rendered sections
  5. Build collapsible question annex from annex.csv
  6. Assemble single multi-section HTML report

Usage:
  python run_report.py           # prod (MotherDuck, requires MOTHERDUCK_TOKEN)
  python run_report.py --dev     # local dev.duckdb, no token needed

Required environment variables:
  MOTHERDUCK_TOKEN   — MotherDuck service token (prod only)
  ANTHROPIC_API_KEY  — Anthropic API key
"""

import argparse
import base64
import csv
import io
import json
import os
import textwrap
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
from pathlib import Path

import anthropic
import duckdb
import matplotlib
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
import pandas as pd

matplotlib.use("Agg")

from config import SECTIONS  # noqa: E402  (local import after matplotlib setup)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

SQL_DIR = Path(__file__).parent / "sql"
OUTPUT_DIR = Path(__file__).parent / "output"
ANNEX_CSV = Path(__file__).parent.parent / "collateral" / "annex.csv"
OUTPUT_DIR.mkdir(exist_ok=True)

COUNTRIES = {"SK": "Slovakia", "EA": "Euro Area", "DE": "Germany"}
COUNTRY_COLORS = {"SK": "#bd4e35", "EA": "#0777b3", "DE": "#e18727"}
COUNTRY_ORDER = ["SK", "EA", "DE"]

# Dev DB: local DuckDB built by `dbt run --target dev`, no token needed.
# dbt dev target uses schema main_safe_safe (dbt appends the profile name).
DEV_DB_PATH = Path(__file__).parent.parent / "dev.duckdb"
DEV_SCHEMA = "main_safe_safe"
PROD_SCHEMA = "main_safe"

ROUTED_FOOTNOTE = (
    "<p class=\"footnote\">* Only firms that have used or applied for this type of financing "
    "in the past are asked this question. A lower n relative to the total sample is by design "
    "and does not indicate a data quality issue — see ECB SAFE methodology for details.</p>"
)


# ---------------------------------------------------------------------------
# 1. Fetch all sections
# ---------------------------------------------------------------------------

def _get_connection(dev: bool) -> duckdb.DuckDBPyConnection:
    if dev:
        return duckdb.connect(str(DEV_DB_PATH))
    motherduck_token = os.environ["MOTHERDUCK_TOKEN"]
    return duckdb.connect(f"md:my_db?motherduck_token={motherduck_token}")


def fetch_all(dev: bool = False) -> dict[str, pd.DataFrame]:
    schema = DEV_SCHEMA if dev else PROD_SCHEMA
    con = _get_connection(dev)
    results = {}
    for sec in SECTIONS:
        sql = (SQL_DIR / sec["sql_file"]).read_text()
        # Rewrite schema reference so dev and prod SQLs stay identical
        sql = sql.replace(f"{PROD_SCHEMA}.", f"{schema}.")
        results[sec["id"]] = con.execute(sql).df()
    con.close()
    return results


# ---------------------------------------------------------------------------
# 2. Interest check (Haiku, parallel)
# ---------------------------------------------------------------------------

INTEREST_SYSTEM = textwrap.dedent("""
    You are a statistical analyst reviewing survey time series data.
    Decide if the data is interesting enough to include in a report, and how to best visualise it.

    A section is INTERESTING if, across any country, there is:
    - A swing of >=6pp (or >=0.5 score units for pressingness) between any two consecutive waves, OR
    - A clear monotonic trend (same direction 3+ consecutive waves), OR
    - A notable divergence between countries (>=8pp gap in the latest wave).

    A section is NOT INTERESTING if all series are flat or too noisy with no direction.

    chart_type:
    - "line" for time series where trends matter (net balances, rates)
    - "bar" for snapshot comparisons where absolute level matters more than trend
      (e.g. pressingness scores across multiple problems)

    best_panel: the single most interesting panel value (from panel_col) NOT already in
    pinned_panels. Must have non-null data for SK, EA, and DE in the latest wave.
    Return null if no such panel exists or if panel_col is null.

    Respond with JSON only:
    {"interesting": true/false, "chart_type": "line" or "bar", "best_panel": "x" or null, "reason": "one sentence"}
""").strip()


def _check_one(sec: dict, df: pd.DataFrame, client: anthropic.Anthropic) -> dict:
    lines = [f"Section: {sec['title']}"]
    panel_col = sec["panel_col"]
    value_col = sec["value_col"]
    series_col = sec["series_col"]

    if panel_col and panel_col in df.columns:
        for panel_val, grp in df.groupby(panel_col):
            label_col = sec.get("panel_label_col", panel_col)
            label = grp[label_col].iloc[0] if label_col in grp.columns else str(panel_val)
            lines.append(f"  panel={panel_val} ({label}):")
            for country in COUNTRY_ORDER:
                cdf = grp[grp[series_col] == country].sort_values("wave_number")
                if cdf.empty:
                    continue
                vals = " ".join(f"w{r['wave_number']}:{r[value_col]:+.2f}" for _, r in cdf.iterrows())
                lines.append(f"    {country}: {vals}")
    else:
        for country in COUNTRY_ORDER:
            cdf = df[df[series_col] == country].sort_values("wave_number")
            if cdf.empty:
                continue
            vals = " ".join(f"w{r['wave_number']}:{r[value_col]:+.2f}" for _, r in cdf.iterrows())
            lines.append(f"  {country}: {vals}")

    lines.append(f"pinned_panels: {sec['pinned_panels']}")

    msg = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=120,
        system=INTEREST_SYSTEM,
        messages=[{"role": "user", "content": "\n".join(lines)}],
    )
    raw = msg.content[0].text.strip()
    try:
        result = json.loads(raw)
    except json.JSONDecodeError:
        result = {"interesting": True, "chart_type": "line", "best_panel": None, "reason": "parse error"}
    result["section_id"] = sec["id"]
    return result


def check_all_interest(sections: list[dict], data: dict[str, pd.DataFrame]) -> dict[str, dict]:
    client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
    results = {}
    with ThreadPoolExecutor(max_workers=5) as pool:
        futures = {
            pool.submit(_check_one, sec, data[sec["id"]], client): sec["id"]
            for sec in sections
            if not sec["always_include"]
        }
        for future in as_completed(futures):
            r = future.result()
            results[r["section_id"]] = r
    # always_include sections get a default result
    for sec in sections:
        if sec["always_include"]:
            results[sec["id"]] = {
                "interesting": True,
                "chart_type": "line",
                "best_panel": None,
                "reason": "always included",
                "section_id": sec["id"],
            }
    return results


# ---------------------------------------------------------------------------
# 3. Build chart
# ---------------------------------------------------------------------------

def _select_panels(sec: dict, df: pd.DataFrame, best_panel) -> list:
    """Return ordered list of panel values to plot, capped at max_panels."""
    panel_col = sec["panel_col"]
    if not panel_col:
        return [None]

    pinned = list(sec["pinned_panels"])
    all_panels = sorted(df[panel_col].dropna().unique().tolist(), key=str)

    # Add best_panel if valid and not already pinned
    if best_panel is not None and str(best_panel) not in [str(p) for p in pinned]:
        # Verify no NA gap for SK/EA/DE in latest wave
        latest = df["wave_number"].max()
        value_col = sec["value_col"]
        panel_data = df[(df[panel_col].astype(str) == str(best_panel)) & (df["wave_number"] == latest)]
        countries_present = set(panel_data[panel_data[value_col].notna()]["country_code"].tolist())
        if {"SK", "EA", "DE"}.issubset(countries_present):
            pinned.append(best_panel)

    # Fill remaining slots with other panels (in order) if below max_panels
    for p in all_panels:
        if len(pinned) >= sec["max_panels"]:
            break
        if str(p) not in [str(x) for x in pinned]:
            pinned.append(p)

    return pinned[: sec["max_panels"]]


def build_chart(sec: dict, df: pd.DataFrame, chart_type: str, best_panel) -> bytes:
    panels = _select_panels(sec, df, best_panel)
    n_panels = len(panels)
    panel_col = sec["panel_col"]
    panel_label_col = sec.get("panel_label_col", panel_col)
    value_col = sec["value_col"]
    series_col = sec["series_col"]

    ncols = min(n_panels, 2)
    nrows = (n_panels + 1) // 2

    fig, axes = plt.subplots(nrows, ncols, figsize=(6.5 * ncols, 4.2 * nrows))
    if n_panels == 1:
        axes_flat = [axes]
    else:
        axes_flat = list(np.array(axes).flatten())

    fig.subplots_adjust(top=0.84, hspace=0.6, wspace=0.35, bottom=0.18)

    waves = sorted(df["wave_number"].unique())
    wave_labels = (
        df[["wave_number", "survey_period_label"]]
        .drop_duplicates()
        .sort_values("wave_number")
        .set_index("wave_number")["survey_period_label"]
    )
    xtick_labels = [wave_labels[w] for w in waves]

    handles, legend_labels = [], []

    for ax, panel_val in zip(axes_flat, panels):
        if panel_col and panel_val is not None:
            sub_df = df[df[panel_col].astype(str) == str(panel_val)]
            label_val = sub_df[panel_label_col].iloc[0] if not sub_df.empty and panel_label_col in sub_df else str(panel_val)
        else:
            sub_df = df
            label_val = sec["title"]

        if chart_type == "bar":
            latest_wave = df["wave_number"].max()
            bar_df = sub_df[sub_df["wave_number"] == latest_wave]
            x = np.arange(len(COUNTRY_ORDER))
            width = 0.55
            for i, country in enumerate(COUNTRY_ORDER):
                cdf = bar_df[bar_df[series_col] == country]
                val = cdf[value_col].iloc[0] if not cdf.empty else 0
                bar = ax.bar(x[i], val, width, color=COUNTRY_COLORS[country], label=COUNTRIES[country])
                if panel_val == panels[0]:
                    handles.append(bar)
                    legend_labels.append(COUNTRIES[country])
            ax.set_xticks(x)
            ax.set_xticklabels([COUNTRIES[c] for c in COUNTRY_ORDER], fontsize=8)
            ax.yaxis.set_major_formatter(mticker.FormatStrFormatter("%.1f"))
        else:
            # Line chart
            for country in COUNTRY_ORDER:
                cdf = sub_df[sub_df[series_col] == country].sort_values("wave_number")
                if cdf.empty:
                    continue
                line, = ax.plot(
                    cdf["wave_number"],
                    cdf[value_col],
                    label=COUNTRIES[country],
                    color=COUNTRY_COLORS[country],
                    linewidth=2,
                    marker="o",
                    markersize=4,
                )
                if panel_val == panels[0]:
                    handles.append(line)
                    legend_labels.append(COUNTRIES[country])
            ax.axhline(0, color="#adadad", linewidth=0.8, linestyle="--")
            ax.set_xticks(waves)
            ax.set_xticklabels(xtick_labels, rotation=40, ha="right", fontsize=7)
            ax.yaxis.set_major_formatter(mticker.FormatStrFormatter("%+.0f"))

        ax.set_title(label_val, fontsize=9, color="#231f20", pad=6)
        ax.tick_params(axis="y", labelsize=8)
        ax.set_ylabel(value_col.replace("_", " "), fontsize=7, color="#6a6a6a")
        ax.spines[["top", "right"]].set_visible(False)
        ax.set_facecolor("#f8f8f8")

    # Hide unused axes
    for ax in axes_flat[n_panels:]:
        ax.set_visible(False)

    # Legend below all panels
    fig.legend(
        handles, legend_labels,
        loc="lower center",
        bbox_to_anchor=(0.5, 0.01),
        ncol=len(COUNTRY_ORDER),
        fontsize=9,
        frameon=False,
    )

    fig.patch.set_facecolor("#f8f8f8")

    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=150, bbox_inches="tight")
    plt.close(fig)
    buf.seek(0)
    return buf.read()


# ---------------------------------------------------------------------------
# 3b. Custom chart: financing gap (grouped bars + gap line)
# ---------------------------------------------------------------------------

def build_financing_gap_chart(sec: dict, df: pd.DataFrame) -> bytes:
    """
    For the financing_gap section: grouped bars showing need_nb and availability_nb
    per country per wave, with financing_gap_wtd overlaid as a line.
    One subplot per sub_item (pinned panel only, since chart is already complex).
    """
    panels = sec["pinned_panels"][:1]  # one panel keeps it readable
    panel_col = sec["panel_col"]
    panel_label_col = sec.get("panel_label_col", panel_col)

    fig, ax = plt.subplots(1, 1, figsize=(9, 4.5))
    fig.subplots_adjust(top=0.82, bottom=0.22, left=0.09, right=0.97)

    panel_val = panels[0]
    sub_df = df[df[panel_col].astype(str) == str(panel_val)].copy()
    label_val = sub_df[panel_label_col].iloc[0] if not sub_df.empty else str(panel_val)

    waves = sorted(sub_df["wave_number"].unique())
    wave_labels = (
        sub_df[["wave_number", "survey_period_label"]]
        .drop_duplicates()
        .sort_values("wave_number")
        .set_index("wave_number")["survey_period_label"]
    )

    n_waves = len(waves)
    n_countries = len(COUNTRY_ORDER)
    group_width = 0.7
    bar_width = group_width / (n_countries * 2)  # 2 bars per country (need + avail)
    group_gap = 1.0

    bar_handles = []
    bar_labels_legend = []
    line_handles = []
    line_labels_legend = []

    # Colour scheme: need = solid country colour, availability = lighter hatched version
    NEED_HATCH = ""
    AVAIL_HATCH = "//"

    for c_idx, country in enumerate(COUNTRY_ORDER):
        cdf = sub_df[sub_df["country_code"] == country].sort_values("wave_number")
        if cdf.empty:
            continue

        base_color = COUNTRY_COLORS[country]
        # Lighter version for availability bars
        import matplotlib.colors as mcolors
        rgb = mcolors.to_rgb(base_color)
        light_color = tuple(min(1.0, v + 0.35) for v in rgb)

        for w_idx, wave in enumerate(waves):
            row = cdf[cdf["wave_number"] == wave]
            if row.empty:
                continue
            need_val = row["need_nb"].iloc[0]
            avail_val = row["availability_nb"].iloc[0]

            x_center = w_idx * group_gap
            # Offset within wave group: (country pair index) × (2 bar widths + small gap)
            pair_offset = (c_idx - n_countries / 2 + 0.5) * (2 * bar_width + 0.02)
            x_need = x_center + pair_offset
            x_avail = x_center + pair_offset + bar_width

            b1 = ax.bar(x_need, need_val, bar_width, color=base_color,
                        hatch=NEED_HATCH, edgecolor="white", linewidth=0.5, zorder=2)
            b2 = ax.bar(x_avail, avail_val, bar_width, color=light_color,
                        hatch=AVAIL_HATCH, edgecolor=base_color, linewidth=0.5, zorder=2)

            if w_idx == 0:
                bar_handles.extend([b1, b2])
                bar_labels_legend.extend([
                    f"{COUNTRIES[country]} — need",
                    f"{COUNTRIES[country]} — availability",
                ])

        # Gap line: plotted at wave group x centres
        x_points = [w_idx * group_gap for w_idx, wave in enumerate(waves)
                    if not cdf[cdf["wave_number"] == wave].empty]
        gap_vals = [cdf[cdf["wave_number"] == wave]["financing_gap_wtd"].iloc[0]
                    for w_idx, wave in enumerate(waves)
                    if not cdf[cdf["wave_number"] == wave].empty]
        line, = ax.plot(x_points, gap_vals, color=base_color, linewidth=2.2,
                        marker="D", markersize=5, linestyle="--", zorder=3)
        line_handles.append(line)
        line_labels_legend.append(f"{COUNTRIES[country]} — gap (need−avail)")

    ax.axhline(0, color="#adadad", linewidth=0.8, linestyle="--", zorder=1)
    ax.set_xticks([w_idx * group_gap for w_idx in range(n_waves)])
    ax.set_xticklabels([wave_labels[w] for w in waves], rotation=35, ha="right", fontsize=8)
    ax.yaxis.set_major_formatter(mticker.FormatStrFormatter("%+.0f"))
    ax.tick_params(axis="y", labelsize=8)
    ax.set_ylabel("Net balance (pp)", fontsize=7, color="#6a6a6a")
    ax.set_title(f"{label_val} — financing need (bars) vs availability (hatched bars); gap (dashed line)", fontsize=9)
    ax.spines[["top", "right"]].set_visible(False)
    ax.set_facecolor("#f8f8f8")
    fig.patch.set_facecolor("#f8f8f8")

    # Two-row legend: bars on top row, gap lines below
    all_handles = bar_handles + line_handles
    all_labels = bar_labels_legend + line_labels_legend
    fig.legend(all_handles, all_labels,
               loc="lower center", bbox_to_anchor=(0.5, 0.0),
               ncol=3, fontsize=7.5, frameon=False)

    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=150, bbox_inches="tight")
    plt.close(fig)
    buf.seek(0)
    return buf.read()


# ---------------------------------------------------------------------------
# 4. Generate bullets (Sonnet, per section)
# ---------------------------------------------------------------------------

BULLET_TEMPLATE = textwrap.dedent("""
    You are an ECB analyst writing concise bullets for a SAFE survey report focused on Slovakia.
    Write at most 3 bullet points about the latest wave results for this section.

    Sign convention for this section:
    {sign_note}

    Language rules:
    - Frame as firm behaviour: "a net 26% of firms reported an increase in interest rates"
      NOT "the net balance widened to 26pp"
    - For net balances: compare to prior wave: "compared with a net X% in the previous quarter"
    - Include sample size for SK and EA where available: "a net 26% of firms (n=80)..."
    - One sentence per bullet, max ~25 words. No headers, no preamble. Bullets start with "•".

    Focus:
    {focus}
""").strip()


def _fmt_financing_gap(d: pd.DataFrame, wave_label: str) -> str:
    """Format financing gap data rows for the bullet prompt (need + avail + gap per country)."""
    rows = [f"{wave_label}:"]
    for _, r in d.iterrows():
        country = r["country_code"]
        need = f"{r['need_nb']:+.1f}" if pd.notna(r.get("need_nb")) else "n/a"
        avail = f"{r['availability_nb']:+.1f}" if pd.notna(r.get("availability_nb")) else "n/a"
        gap = f"{r['financing_gap_wtd']:+.1f}" if pd.notna(r.get("financing_gap_wtd")) else "n/a"
        n_need = int(r["n_respondents_need"]) if pd.notna(r.get("n_respondents_need")) else "?"
        rows.append(f"  {country} | need={need}pp (n={n_need}) | availability={avail}pp | gap={gap}pp")
    return "\n".join(rows)


def get_bullets(sec: dict, df: pd.DataFrame) -> list[str]:
    latest_wave = df["wave_number"].max()
    waves_sorted = sorted(df["wave_number"].unique())
    prev_wave = waves_sorted[-2] if len(waves_sorted) >= 2 else latest_wave

    latest = df[df["wave_number"] == latest_wave]
    prev = df[df["wave_number"] == prev_wave]
    value_col = sec["value_col"]

    # Financing gap section gets a richer prompt with need/avail decomposition
    if sec["id"] == "financing_gap":
        user_msg = (
            _fmt_financing_gap(latest, f"Wave {latest_wave} (latest)")
            + "\n\n"
            + _fmt_financing_gap(prev, f"Wave {prev_wave} (previous)")
        )
    else:
        def fmt(d: pd.DataFrame) -> str:
            rows = []
            for _, r in d.iterrows():
                n_part = f" | n={r['n_respondents']}" if r.get("country_code") in ("SK", "EA") and "n_respondents" in r else ""
                val_str = f"{r[value_col]:+.2f}" if pd.notna(r[value_col]) else "n/a"
                panel_col = sec["panel_col"]
                panel_label_col = sec.get("panel_label_col", panel_col)
                panel_part = f" | {r[panel_label_col]}" if panel_label_col and panel_label_col in r.index else ""
                rows.append(f"  {r['country_code']}{panel_part} | {value_col}={val_str}{n_part}")
            return "\n".join(rows)

        user_msg = (
            f"Wave {latest_wave} (latest):\n{fmt(latest)}\n\n"
            f"Wave {prev_wave} (previous):\n{fmt(prev)}"
        )

    system_prompt = BULLET_TEMPLATE.format(
        sign_note=sec["sign_note"],
        focus=sec["focus"],
    )

    client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
    msg = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=220,
        system=system_prompt,
        messages=[{"role": "user", "content": user_msg}],
    )
    raw = msg.content[0].text.strip()
    bullets = [line.strip() for line in raw.splitlines() if line.strip().startswith("•")]
    return bullets[:3]


# ---------------------------------------------------------------------------
# 5. Executive summary (Sonnet prose)
# ---------------------------------------------------------------------------

EXEC_SUMMARY_SYSTEM = textwrap.dedent("""
    You are a senior ECB economist writing an executive summary for a technical director
    reviewing the latest ECB SAFE survey results for Slovakia.

    The summary should:
    - Open with the single most important finding for Slovakia in this wave
    - Cover financing conditions, access to finance, and business situation where notable
    - Compare Slovakia to the euro area average; highlight any significant divergence
    - Use precise language: "a net X% of firms reported..." not "the index rose to X"
    - Positive net balance = net tightening/rising (adverse for firms unless stated otherwise)
    - Negative net balance = net easing/falling (favourable for firms unless stated otherwise)
    - Tone: concise, analytical, no hedging. Flowing prose — no bullet points, no headers.
    - Max 130 words.
""").strip()


def get_exec_summary(rendered_sections: list[dict]) -> str:
    lines = ["Below are the key findings per topic from the latest wave:\n"]
    for s in rendered_sections:
        lines.append(f"## {s['title']}")
        lines.append(f"Sign convention: {s['sign_note']}")
        for b in s["bullets"]:
            lines.append(f"  {b}")
        lines.append("")

    client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
    msg = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=300,
        system=EXEC_SUMMARY_SYSTEM,
        messages=[{"role": "user", "content": "\n".join(lines)}],
    )
    return msg.content[0].text.strip()


# ---------------------------------------------------------------------------
# 6. Collapsible question annex
# ---------------------------------------------------------------------------

# Questions to include, grouped into display blocks
ANNEX_GROUPS = [
    ("Business situation", ["Q0b", "Q2"]),
    ("Financing needs &amp; availability", ["Q4", "Q5", "Q9"]),
    ("Credit supply factors", ["Q11"]),
    ("Financing conditions &amp; terms", ["Q10", "Q23"]),
    ("Financing applications", ["Q7A", "Q7B", "Q6A"]),
    ("Outlook &amp; expectations", ["Q31", "Q33", "Q34"]),
]
ANNEX_Q_IDS = {q for _, qs in ANNEX_GROUPS for q in qs}


def _clean_question_text(text: str) -> str:
    """Strip the 'Qxx/Qxx_g1.' or 'Qxx.' prefix from question text."""
    import re
    # Remove patterns like "Q0b/Q0b_g1. " or "Q10. " or "Q7A/Q7A_g1. "
    text = re.sub(r'^[A-Za-z0-9]+(?:/[A-Za-z0-9_]+)*\.\s*', '', text)
    # Also remove leading "Looking ahead, " style if it came from Q34 which has no prefix
    return text.strip()


def build_annex_html(annex_csv_path: Path) -> str:
    # Read annex and extract one question text per question ID (round 30 = col index 7)
    q_texts: dict[str, tuple[str, str]] = {}  # q_id -> (sample, text)
    try:
        with open(annex_csv_path, newline="", encoding="utf-8-sig") as f:
            reader = csv.reader(f)
            rows = list(reader)
        for row in rows[1:]:
            if len(row) > 7 and row[1] == "question":
                q_id = row[2].strip()
                # Normalise: Q0b -> Q0b, Q7A -> Q7A  (keep case)
                # Check against ANNEX_Q_IDS case-insensitively
                matched = next((k for k in ANNEX_Q_IDS if k.lower() == q_id.lower()), None)
                if matched and matched not in q_texts:
                    sample = row[4].strip()
                    text = row[7].strip()
                    if text:
                        q_texts[matched] = (sample, _clean_question_text(text))
    except FileNotFoundError:
        return ""

    # Build HTML groups
    group_rows = []
    for group_label, q_ids in ANNEX_GROUPS:
        first = True
        for q_id in q_ids:
            entry = q_texts.get(q_id)
            if not entry:
                continue
            sample, text = entry
            sample_badge = (
                '<span class="badge-ecb">ECB module</span>'
                if "ECB" in sample
                else '<span class="badge-common">Common</span>'
            )
            group_cell = f'<td class="group-cell">{group_label}</td>' if first else '<td class="group-cell"></td>'
            first = False
            group_rows.append(
                f"    <tr>{group_cell}"
                f'<td><strong>{q_id}</strong></td>'
                f"<td>{text[:200]}{'…' if len(text) > 200 else ''}</td>"
                f"<td>{sample_badge}</td></tr>"
            )

    if not group_rows:
        return ""

    rows_html = "\n".join(group_rows)
    return textwrap.dedent(f"""
<details>
  <summary>Survey questions collected on a 3-month basis ({len(q_texts)} questions)</summary>
  <table>
    <thead>
      <tr><th>Topic</th><th>ID</th><th>Question</th><th>Module</th></tr>
    </thead>
    <tbody>
{rows_html}
    </tbody>
  </table>
</details>
""").strip()


# ---------------------------------------------------------------------------
# 7. Build HTML
# ---------------------------------------------------------------------------

HTML_PAGE = textwrap.dedent("""
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<style>
  body        {{ font-family: Arial, sans-serif; background: #f4f4f4; color: #231f20;
                 max-width: 960px; margin: 40px auto; padding: 0 24px; }}
  h1          {{ font-size: 22px; font-weight: bold; margin-bottom: 4px; }}
  .meta       {{ color: #6a6a6a; font-size: 13px; margin-bottom: 20px; }}
  section     {{ background: #fff; border: 1px solid #e0e0e0; border-radius: 6px;
                 padding: 24px 28px; margin-bottom: 28px; }}
  h2          {{ font-size: 16px; font-weight: bold; margin: 0 0 12px 0; color: #231f20; }}
  ul          {{ padding-left: 20px; margin: 0 0 16px 0; }}
  li          {{ margin-bottom: 6px; font-size: 13.5px; line-height: 1.5; }}
  img         {{ width: 100%; margin-top: 8px; }}
  .footnote   {{ font-size: 11px; color: #888; margin-top: 10px; line-height: 1.4; }}
  .footer     {{ color: #adadad; font-size: 11px; margin-top: 32px; text-align: center; }}

  /* Executive summary */
  #exec-summary               {{ background: #eef4fb; border-left: 4px solid #0777b3; }}
  #exec-summary h2            {{ color: #0777b3; }}
  #exec-summary p             {{ font-size: 14px; line-height: 1.8; margin: 0; }}

  /* Collapsible annex */
  details     {{ background: #fff; border: 1px solid #e0e0e0; border-radius: 6px;
                 padding: 14px 22px; margin-bottom: 20px; }}
  summary     {{ font-weight: bold; font-size: 13px; cursor: pointer; color: #555;
                 user-select: none; }}
  summary:hover {{ color: #231f20; }}
  details table           {{ width: 100%; border-collapse: collapse; margin-top: 12px;
                             font-size: 12px; }}
  details td, details th  {{ padding: 5px 8px; border-bottom: 1px solid #f0f0f0;
                             vertical-align: top; }}
  details th              {{ font-weight: bold; background: #f8f8f8; text-align: left; }}
  .group-cell             {{ color: #888; font-style: italic; white-space: nowrap; }}
  .badge-common           {{ background: #e8f4e8; color: #2d7a00; padding: 1px 6px;
                             border-radius: 3px; font-size: 11px; }}
  .badge-ecb              {{ background: #eef4fb; color: #0777b3; padding: 1px 6px;
                             border-radius: 3px; font-size: 11px; }}
</style>
</head>
<body>
<h1>ECB SAFE Survey — Automatic Report</h1>
<p class="meta">Slovakia · Euro Area · Germany &nbsp;|&nbsp; Generated {date}</p>
{annex}
{exec_summary_section}
{sections}
<p class="footer">Source: ECB SAFE microdata. Net balance = % reporting increase minus % reporting decrease.
Positive = tightening/rising (adverse for firms unless noted). Negative = easing/falling.</p>
</body>
</html>
""").strip()

SECTION_TMPL = textwrap.dedent("""
<section>
  <h2>{title}</h2>
  <ul>
{bullets}
  </ul>
{footnote}  <img src="data:image/png;base64,{chart_b64}" alt="{title} chart">
</section>
""").strip()

EXEC_SUMMARY_TMPL = textwrap.dedent("""
<section id="exec-summary">
  <h2>Executive Summary</h2>
  <p>{text}</p>
</section>
""").strip()


def build_html(
    rendered_sections: list[dict],
    annex_html: str,
    exec_summary: str,
) -> str:
    section_html = "\n\n".join(
        SECTION_TMPL.format(
            title=s["title"],
            bullets="\n".join(f"    <li>{b.lstrip('• ').strip()}</li>" for b in s["bullets"]),
            footnote=ROUTED_FOOTNOTE + "\n" if s.get("routed") else "",
            chart_b64=base64.b64encode(s["chart_png"]).decode(),
        )
        for s in rendered_sections
    )
    exec_section = EXEC_SUMMARY_TMPL.format(text=exec_summary) if exec_summary else ""
    return HTML_PAGE.format(
        date=date.today().strftime("%d %b %Y"),
        annex=annex_html,
        exec_summary_section=exec_section,
        sections=section_html,
    )


# ---------------------------------------------------------------------------
# 8. Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dev", action="store_true",
                        help="Use local dev.duckdb instead of MotherDuck (no MOTHERDUCK_TOKEN needed)")
    args = parser.parse_args()

    if args.dev:
        print(f"[DEV] Using local DuckDB: {DEV_DB_PATH}")
    else:
        print("[PROD] Using MotherDuck")

    print("Fetching data for all sections...")
    data = fetch_all(dev=args.dev)
    for sid, df in data.items():
        print(f"  {sid}: {len(df)} rows")

    print("Running interest checks (parallel)...")
    interest = check_all_interest(SECTIONS, data)
    for sid, r in interest.items():
        flag = "✓" if r["interesting"] else "✗"
        print(f"  {flag} {sid}: {r['reason']} [chart={r['chart_type']}, best_panel={r['best_panel']}]")

    rendered = []
    for sec in SECTIONS:
        sid = sec["id"]
        r = interest[sid]
        if not r["interesting"]:
            print(f"  Skipping {sid} (not interesting)")
            continue

        print(f"  Building chart for {sid}...")
        if sid == "financing_gap":
            chart_png = build_financing_gap_chart(sec, data[sid])
        else:
            chart_png = build_chart(sec, data[sid], r["chart_type"], r["best_panel"])

        print(f"  Generating bullets for {sid}...")
        bullets = get_bullets(sec, data[sid])
        for b in bullets:
            print(f"    {b}")

        rendered.append({
            "title": sec["title"],
            "bullets": bullets,
            "chart_png": chart_png,
            "sign_note": sec["sign_note"],
            "routed": sec.get("routed", False),
        })

    print("Generating executive summary...")
    exec_summary = get_exec_summary(rendered) if rendered else ""
    print(f"  {exec_summary[:120]}...")

    print("Building question annex...")
    annex_html = build_annex_html(ANNEX_CSV)

    print("Assembling HTML...")
    html = build_html(rendered, annex_html, exec_summary)

    out_path = OUTPUT_DIR / "report_latest.html"
    out_path.write_text(html, encoding="utf-8")
    print(f"Saved → {out_path}")
    print("Done.")


if __name__ == "__main__":
    main()
