"""
SAFE Survey Report Generator — config-driven multi-section orchestrator.

Loops over SECTIONS in config.py:
  1. Fetch data for all sections via MotherDuck
  2. Parallel Haiku interest checks — returns interesting flag, chart_type, best_panel
  3. For interesting sections: build chart + generate Sonnet bullets
  4. Assemble single multi-section HTML report

Required environment variables:
  MOTHERDUCK_TOKEN   — MotherDuck service token
  ANTHROPIC_API_KEY  — Anthropic API key
"""

import base64
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
OUTPUT_DIR.mkdir(exist_ok=True)

COUNTRIES = {"SK": "Slovakia", "EA": "Euro Area", "DE": "Germany"}
COUNTRY_COLORS = {"SK": "#bd4e35", "EA": "#0777b3", "DE": "#e18727"}
COUNTRY_ORDER = ["SK", "EA", "DE"]

MOTHERDUCK_TOKEN = os.environ["MOTHERDUCK_TOKEN"]
ANTHROPIC_API_KEY = os.environ["ANTHROPIC_API_KEY"]


# ---------------------------------------------------------------------------
# 1. Fetch all sections
# ---------------------------------------------------------------------------

def fetch_all() -> dict[str, pd.DataFrame]:
    con = duckdb.connect(f"md:my_db?motherduck_token={MOTHERDUCK_TOKEN}")
    results = {}
    for sec in SECTIONS:
        sql = (SQL_DIR / sec["sql_file"]).read_text()
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
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
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


def get_bullets(sec: dict, df: pd.DataFrame) -> list[str]:
    latest_wave = df["wave_number"].max()
    waves_sorted = sorted(df["wave_number"].unique())
    prev_wave = waves_sorted[-2] if len(waves_sorted) >= 2 else latest_wave

    latest = df[df["wave_number"] == latest_wave]
    prev = df[df["wave_number"] == prev_wave]
    value_col = sec["value_col"]

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

    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
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
# 5. Build HTML
# ---------------------------------------------------------------------------

HTML_PAGE = textwrap.dedent("""
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<style>
  body    {{ font-family: Arial, sans-serif; background: #f4f4f4; color: #231f20;
             max-width: 960px; margin: 40px auto; padding: 0 24px; }}
  h1      {{ font-size: 22px; font-weight: bold; margin-bottom: 4px; }}
  .meta   {{ color: #6a6a6a; font-size: 13px; margin-bottom: 36px; }}
  section {{ background: #fff; border: 1px solid #e0e0e0; border-radius: 6px;
             padding: 24px 28px; margin-bottom: 28px; }}
  h2      {{ font-size: 16px; font-weight: bold; margin: 0 0 12px 0; color: #231f20; }}
  ul      {{ padding-left: 20px; margin: 0 0 16px 0; }}
  li      {{ margin-bottom: 6px; font-size: 13.5px; line-height: 1.5; }}
  img     {{ width: 100%; margin-top: 8px; }}
  .footer {{ color: #adadad; font-size: 11px; margin-top: 32px; text-align: center; }}
</style>
</head>
<body>
<h1>ECB SAFE Survey — Automatic Report</h1>
<p class="meta">Slovakia · Euro Area · Germany &nbsp;|&nbsp; Generated {date}</p>
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
  <img src="data:image/png;base64,{chart_b64}" alt="{title} chart">
</section>
""").strip()


def build_html(rendered_sections: list[dict]) -> str:
    section_html = "\n\n".join(
        SECTION_TMPL.format(
            title=s["title"],
            bullets="\n".join(f"    <li>{b.lstrip('• ').strip()}</li>" for b in s["bullets"]),
            chart_b64=base64.b64encode(s["chart_png"]).decode(),
        )
        for s in rendered_sections
    )
    return HTML_PAGE.format(date=date.today().strftime("%d %b %Y"), sections=section_html)


# ---------------------------------------------------------------------------
# 6. Main
# ---------------------------------------------------------------------------

def main() -> None:
    print("Fetching data for all sections...")
    data = fetch_all()
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
        chart_png = build_chart(sec, data[sid], r["chart_type"], r["best_panel"])

        print(f"  Generating bullets for {sid}...")
        bullets = get_bullets(sec, data[sid])
        for b in bullets:
            print(f"    {b}")

        rendered.append({"title": sec["title"], "bullets": bullets, "chart_png": chart_png})

    print("Assembling HTML...")
    html = build_html(rendered)

    out_path = OUTPUT_DIR / "report_latest.html"
    out_path.write_text(html, encoding="utf-8")
    print(f"Saved → {out_path}")
    print("Done.")


if __name__ == "__main__":
    main()
