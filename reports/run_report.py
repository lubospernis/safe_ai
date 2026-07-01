"""
SAFE Survey Report Generator — config-driven multi-section orchestrator.

Loops over SECTIONS in config.py:
  1. Fetch data for all sections via MotherDuck (prod) or local dev.duckdb (dev)
  2. Parallel Haiku interest checks — returns interesting flag, chart_type, best_panel
  3. For interesting sections: build chart + generate Sonnet bullets
  4. Generate executive summary (Sonnet prose) from all rendered sections
  5. Build collapsible question annex from MotherDuck ref_safe__annex
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
import io
import json
import os
import re
import textwrap
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
from pathlib import Path

import yaml
import anthropic
import duckdb
from json_repair import repair_json
from dotenv import load_dotenv
from mistralai import Mistral
import matplotlib
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
import pandas as pd

matplotlib.use("Agg")

# NBS brand: Arial everywhere in charts
matplotlib.rcParams.update({
    "font.family": "Arial",
    "font.size": 9,
    "axes.labelsize": 8,
    "xtick.labelsize": 9,
    "ytick.labelsize": 9,
    "legend.fontsize": 9,
    "figure.facecolor": "#f4f4f4",
    "axes.facecolor": "#f4f4f4",
})

load_dotenv(Path(__file__).parent.parent / ".env")

from config import SECTIONS  # noqa: E402  (local import after matplotlib setup)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

SQL_DIR = Path(__file__).parent / "sql"
OUTPUT_DIR = Path(__file__).parent / "output"
MARTS_SCHEMA_YML = Path(__file__).parent.parent / "dbt_project" / "models" / "marts" / "schema.yml"
OUTPUT_DIR.mkdir(exist_ok=True)

COUNTRIES = {"SK": "Slovakia", "EA": "Euro Area", "DE": "Germany"}
# NBS brand palette: primary blues + supplementary accent colours
NBS_NAVY   = "#2B5291"
NBS_BLUE   = "#0086DE"
NBS_TEAL   = "#008C7A"
NBS_BURG   = "#A63559"
NBS_ORANGE = "#FF7430"
NBS_YELLOW = "#FAB937"
NBS_GREY   = "#D2DBE0"
NBS_TEXT   = "#231f20"

COUNTRY_COLORS = {"SK": NBS_NAVY, "EA": NBS_BLUE, "DE": NBS_TEAL}
COUNTRY_ORDER = ["SK", "EA", "DE"]

# Dev DB: local DuckDB built by `dbt run --target dev`, no token needed.
# dbt dev target uses schema main_safe_safe (dbt appends the profile name).
DEV_DB_PATH = Path(__file__).parent.parent / "dev.duckdb"
DEV_SCHEMA = "main_safe_safe"
PROD_SCHEMA = "main_safe"

# ---------------------------------------------------------------------------
# Cost tracking
# ---------------------------------------------------------------------------

_PRICE = {
    "claude-sonnet-4-6":     {"input": 3.00,  "output": 15.00},
    "mistral-small-latest":  {"input": 0.10,  "output": 0.30},
    "mistral-medium-latest": {"input": 0.40,  "output": 2.00},
}


class _Usage:
    """Minimal usage container for non-Anthropic calls (Mistral has different field names)."""
    def __init__(self, input_tokens: int, output_tokens: int):
        self.input_tokens = input_tokens
        self.output_tokens = output_tokens
        self.cache_creation_input_tokens = 0
        self.cache_read_input_tokens = 0


def _track_cost(tracker: dict, model: str, usage) -> None:
    """Accept an Anthropic usage object (supports cache_creation/cache_read fields)."""
    p = _PRICE.get(model, {"input": 0.0, "output": 0.0})
    # input_tokens is already ONLY non-cached tokens — cache fields are additive, not overlapping
    normal_in   = getattr(usage, "input_tokens", 0) or 0
    cache_write = getattr(usage, "cache_creation_input_tokens", 0) or 0
    cache_read  = getattr(usage, "cache_read_input_tokens", 0) or 0
    output_tok  = getattr(usage, "output_tokens", 0) or 0
    usd = (
        normal_in   * p["input"]          +
        cache_write * p["input"] * 1.25   +
        cache_read  * p["input"] * 0.10   +
        output_tok  * p["output"]
    ) / 1_000_000
    total_in = normal_in + cache_write + cache_read
    tracker["input_tokens"] += total_in
    tracker["output_tokens"] += output_tok
    tracker["usd"] += usd
    tracker["calls"] += 1
    m = tracker["by_model"].setdefault(model, {"calls": 0, "input": 0, "output": 0, "usd": 0.0,
                                               "cache_write": 0, "cache_read": 0})
    m["calls"] += 1
    m["input"] += total_in
    m["output"] += output_tok
    m["usd"] += usd
    m["cache_write"] += cache_write
    m["cache_read"] += cache_read


# Artwork shown alongside the exec summary — update each quarter.
# User fills in the correct webumenia.sk values below.
ARTWORK = {
    "page_url": "https://www.webumenia.sk/dielo/SVK:SNG.IM_127",
    "img_url":  "https://www.webumenia.sk/dielo/nahlad/SVK:SNG.IM_127/600",
    "title":    "Július Koller — Pre každú príležitosť... osviežujúci národný podnik. (UFO) (1978)",
}


def _fetch_painting_inner_html() -> str:
    """Fetch the quarterly artwork; return inner <img>+<span> HTML (no outer wrapper).
    Returns empty string on any network failure so the report still builds.
    """
    import requests as _requests
    try:
        resp = _requests.get(ARTWORK["img_url"], timeout=10, headers={"User-Agent": "Mozilla/5.0"})
        resp.raise_for_status()
        b64 = base64.b64encode(resp.content).decode()
        ct = resp.headers.get("Content-Type", "image/jpeg").split(";")[0].strip()
        title = ARTWORK["title"]
        page_url = ARTWORK["page_url"]
        return (
            f'<a href="{page_url}" target="_blank" title="{title}">'
            f'<img src="data:{ct};base64,{b64}" alt="{title}" '
            f'style="width:100%;border-radius:4px;border:1px solid #e0e0e0;display:block;">'
            f'</a>'
            f'<span style="font-size:9px;color:#aaa;display:block;margin-top:4px;'
            f'text-align:right;font-style:italic;line-height:1.3;">'
            f'<a href="{page_url}" target="_blank" style="color:#aaa;text-decoration:none;">'
            f'{title}</a></span>'
        )
    except Exception as e:
        print(f"  Warning: could not fetch painting ({e}) — skipping thumbnail")
        return ""


ROUTED_FOOTNOTE = (
    "<p class=\"footnote\">* Only firms that have used or applied for this type of financing "
    "in the past are asked this question. A lower n relative to the total sample is by design "
    "and does not indicate a data quality issue — see ECB SAFE methodology for details.</p>"
)

MISSINGNESS_FOOTNOTE = (
    "<p class=\"footnote\">† Observations with fewer than 10 valid responses in a given "
    "wave × country × sub-item cell are excluded from the chart; gaps in series indicate "
    "insufficient data for that period.</p>"
)

# ---------------------------------------------------------------------------
# Slovak UI strings
# ---------------------------------------------------------------------------

_SK_UI = {
    "lang":            "sk",
    "title":           "ECB SAFE Survey — Vlna {wave} · Slovensko",
    "h1":              "ECB SAFE Survey — Vlna {wave}",
    "meta":            "Slovensko · Eurozóna · Nemecko &nbsp;|&nbsp; Vygenerované {date}",
    "exec_h2":         "Zhrnutie",
    "toc_title":       "Obsah",
    "group_financing": "Podmienky financovania",
    "group_economic":  "Ekonomická situácia firiem",
    "footer": (
        "Zdroj: ECB SAFE mikrodáta. Čistá bilancia = % podnikov hlásiacich nárast "
        "mínus % podnikov hlásiacich pokles. Kladná hodnota = sprísnenie / rast "
        "(nepriaznivé pre firmy, ak nie je uvedené inak). Záporná hodnota = uvoľnenie / pokles."
    ),
    "footnote_routed": (
        "<p class=\"footnote\">* Túto otázku dostávajú iba firmy, ktoré v minulosti využili "
        "alebo žiadali o daný typ financovania. Nižší počet respondentov oproti celkovej vzorke "
        "je zámerný a neindikuje problém s kvalitou dát — pozri metodológiu ECB SAFE.</p>"
    ),
    "footnote_missing": (
        "<p class=\"footnote\">† Bunky s menej ako 10 platnými odpoveďami v danej kombinácii "
        "vlny × krajiny × položky sú vynechané z grafu; medzery v sérii indikujú nedostatok dát "
        "pre dané obdobie.</p>"
    ),
    "footnote_agentic": (
        "<p class=\"footnote\">🤖 Táto sekcia obsahuje dáta získané AI agentom priamym "
        "dopytovaním databázy SAFE počas generovania správy.</p>\n"
    ),
}

# ---------------------------------------------------------------------------
# Tool-use: query_mart
# ---------------------------------------------------------------------------

ALLOWED_MART_TABLES = {
    "mart_safe__financing_conditions", "mart_safe__financing_purpose",
    "mart_safe__slovakia_kpis", "mart_safe__business_problems",
    "mart_safe__financing_factors", "mart_safe__loan_applications",
    "mart_safe__business_situation", "mart_safe__outlook",
    "mart_safe__availability_expectations", "mart_safe__expectations",
    "mart_safe__survey_participants", "mart_safe__question_coverage",
    "mart_safe__adhoc_responses",
    "int_safe__core_questions_long",
}
_WRITE_RE = re.compile(
    r"\b(INSERT|UPDATE|DELETE|DROP|CREATE|COPY|TRUNCATE|ALTER)\b", re.IGNORECASE
)
MAX_TOOL_ROWS = 30
MAX_TOOL_TURNS = 2

QUERY_MART_TOOL = {
    "name": "query_mart",
    "description": (
        "Execute a read-only DuckDB SELECT against the SAFE mart tables. "
        "Only call this when: (1) you need data before wave 30 (use int_safe__core_questions_long), "
        "(2) you need a sub_item or column not present in the provided section data, or "
        "(3) you need to verify a historical extreme (e.g. highest since wave X). "
        "Do NOT use this to discover table or column names — see the schema catalogue in the system prompt. "
        "Always use fully-qualified names: main_safe.mart_safe__<name>. "
        "For mart_safe__financing_purpose and mart_safe__business_problems, always add "
        "AND reference_period = '3m'. Only SELECT is permitted. "
        "Prefer filling in a query template from the system prompt over writing SQL from scratch."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "sql": {
                "type": "string",
                "description": (
                    "A SELECT query. Must reference main_safe.mart_safe__* or "
                    "main_safe.int_safe__core_questions_long only."
                ),
            }
        },
        "required": ["sql"],
    },
}


def _run_query_tool(sql: str, con, schema: str) -> str:
    """Validate and execute a tool-use SQL query. Returns markdown table or error string."""
    if _WRITE_RE.search(sql):
        return "ERROR: only SELECT queries are permitted."
    referenced = set(re.findall(r"(mart_safe__\w+|int_safe__\w+)", sql))
    disallowed = referenced - ALLOWED_MART_TABLES
    if disallowed:
        return f"ERROR: table(s) not in whitelist: {disallowed}"
    sql = sql.replace(f"{PROD_SCHEMA}.", f"{schema}.")
    try:
        df = con.execute(sql).df()
    except Exception as e:
        return f"ERROR executing query: {e}"
    if df.empty:
        return "Query returned 0 rows."
    truncated = len(df) > MAX_TOOL_ROWS
    result = df.head(MAX_TOOL_ROWS).to_markdown(index=False)
    if truncated:
        result += f"\n\n_(truncated to {MAX_TOOL_ROWS} rows)_"
    return result


MART_QUERY_TEMPLATES = textwrap.dedent("""
    Query templates — fill in the UPPER_CASE placeholders, do not change the rest:

    -- Historical trend for a net-balance mart (financing_conditions, business_situation,
    -- outlook, availability_expectations, financing_factors):
    SELECT wave_number, country_code, net_balance_wtd, n_respondents
    FROM main_safe.mart_safe__MART_NAME
    WHERE country_code IN ('SK', 'EA', 'DE')
      AND firm_size = 'all'
      AND sub_item = 'SUB_ITEM_CODE'
    ORDER BY wave_number, country_code;

    -- Loan application / rejection rates:
    SELECT wave_number, country_code, application_rate_wtd, rejection_rate_wtd,
           discouragement_rate_wtd, financing_gap_wtd
    FROM main_safe.mart_safe__loan_applications
    WHERE country_code IN ('SK', 'EA', 'DE')
      AND firm_size = 'all'
      AND sub_item = 'a'
    ORDER BY wave_number, country_code;

    -- Business problems pressingness (note: reference_period filter required):
    SELECT wave_number, country_code, problem_label, avg_pressingness_wtd
    FROM main_safe.mart_safe__business_problems
    WHERE country_code IN ('SK', 'EA', 'DE')
      AND firm_size = 'all'
      AND reference_period = '3m'
    ORDER BY wave_number, avg_pressingness_wtd DESC;

    -- Financing purpose (note: reference_period filter required):
    SELECT wave_number, country_code, purpose_label, pct_cited_wtd
    FROM main_safe.mart_safe__financing_purpose
    WHERE country_code IN ('SK', 'EA', 'DE')
      AND firm_size = 'all'
      AND reference_period = '3m'
    ORDER BY wave_number, country_code, purpose_id;

    -- Expectations (Q31 mean / Q33 net balance / Q34 pct):
    SELECT wave_number, country_code, question_id, sub_item_label,
           mean_wtd, net_balance_wtd, pct_upside_wtd, pct_downside_wtd
    FROM main_safe.mart_safe__expectations
    WHERE country_code IN ('SK', 'EA', 'DE')
      AND firm_size = 'all'
      AND question_id = 'QUESTION_ID'
    ORDER BY wave_number, country_code;

    -- Historical microdata (pre-wave-30 only):
    SELECT wave_number, country_code,
           AVG(CASE WHEN response_3m IN (1,2,3) THEN 1.0 ELSE 0.0 END) AS pct_relevant
    FROM main_safe.int_safe__core_questions_long
    WHERE country_code IN ('SK', 'EA')
      AND question_id = 'QUESTION_ID'
      AND sub_item = 'SUB_ITEM_CODE'
      AND wave_number < 30
      AND is_nonresponse = false
    GROUP BY wave_number, country_code
    ORDER BY wave_number;
""").strip()


def build_mart_catalogue(con, schema: str) -> str:
    """Build compact mart catalogue from schema.yml, verified against live DB."""
    with open(MARTS_SCHEMA_YML) as f:
        dbt_schema = yaml.safe_load(f)

    lines = [
        "Available mart tables (all contain only 3m reference period data, waves 30+).",
        "Default filters: WHERE firm_size = 'all' AND country_code IN ('SK','EA','DE').",
        "EXCEPTION: mart_safe__financing_purpose and mart_safe__business_problems keep",
        "  both periods — always add: AND reference_period = '3m' for those two tables.",
        "",
    ]

    for model in dbt_schema.get("models", []):
        name = model["name"]
        if not name.startswith("mart_safe__"):
            continue
        full_name = f"{schema}.{name}"
        try:
            con.execute(f"SELECT 1 FROM {full_name} LIMIT 1")
        except Exception:
            continue
        cols = [c["name"] for c in model.get("columns", [])]
        lines.append(full_name)
        for i in range(0, len(cols), 6):
            lines.append("  " + ", ".join(cols[i:i + 6]))
        lines.append("")

    lines += [
        "main_safe.int_safe__core_questions_long",
        "  permid, wave_number, country_code, question_id, sub_item, response_raw,",
        "  response_rec, response_3m, weight_common, is_nonresponse, employee_band_code, is_sme",
        "  (26M rows — use ONLY for pre-wave-30 history or raw microdata drill-downs)",
    ]

    return "\n".join(lines)


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


def _mistral_client() -> Mistral:
    return Mistral(api_key=os.environ["MISTRAL_API_KEY"])


def _check_one(sec: dict, df: pd.DataFrame) -> dict:
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

    client = _mistral_client()
    resp = client.chat.complete(
        model="mistral-small-latest",
        max_tokens=120,
        messages=[
            {"role": "system", "content": INTEREST_SYSTEM},
            {"role": "user", "content": "\n".join(lines)},
        ],
    )
    raw = resp.choices[0].message.content.strip()
    try:
        result = json.loads(repair_json(raw))
    except Exception:
        result = {"interesting": True, "chart_type": "line", "best_panel": None, "reason": "parse error"}
    result["section_id"] = sec["id"]
    if resp.usage:
        result["_usage"] = {"input": resp.usage.prompt_tokens, "output": resp.usage.completion_tokens}
    return result


def check_all_interest(
    sections: list[dict],
    data: dict[str, pd.DataFrame],
    cost_tracker: dict,
) -> dict[str, dict]:
    results = {}
    with ThreadPoolExecutor(max_workers=5) as pool:
        futures = {
            pool.submit(_check_one, sec, data[sec["id"]]): sec["id"]
            for sec in sections
            if not sec["always_include"]
        }
        for future in as_completed(futures):
            r = future.result()
            if "_usage" in r:
                u = r.pop("_usage")
                _track_cost(cost_tracker, "mistral-small-latest", _Usage(u["input"], u["output"]))
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


def _nbs_style_ax(ax, chart_type: str, waves=None, xtick_labels=None) -> None:
    """Apply NBS visual style to a single axes."""
    ax.set_facecolor("#f4f4f4")
    # All spines off — zero line and horizontal grid carry the reference
    for spine in ax.spines.values():
        spine.set_visible(False)
    # No tick marks, only labels
    ax.tick_params(which="both", length=0, colors=NBS_TEXT)
    ax.title.set_fontsize(10)
    ax.title.set_color(NBS_TEXT)
    # Horizontal grid only — solid, very light
    ax.yaxis.grid(True, color="#D2DBE0", linewidth=0.6, linestyle="-", zorder=0)
    ax.xaxis.grid(False)
    ax.set_axisbelow(True)
    # Zero baseline — thin dark line
    ax.axhline(0, color="#9aa5ad", linewidth=0.8, zorder=1)
    if chart_type == "line" and waves is not None:
        ax.set_xticks(waves)
        ax.set_xticklabels(xtick_labels or [], rotation=35, ha="right", fontsize=8)
        ax.yaxis.set_major_formatter(mticker.FormatStrFormatter("%+.0f"))
    else:
        ax.yaxis.set_major_formatter(mticker.FormatStrFormatter("%.1f"))


def build_chart(sec: dict, df: pd.DataFrame, chart_type: str, best_panel) -> bytes:
    panels = _select_panels(sec, df, best_panel)
    n_panels = len(panels)
    panel_col = sec["panel_col"]
    panel_label_col = sec.get("panel_label_col", panel_col)
    value_col = sec["value_col"]
    series_col = sec["series_col"]

    ncols = min(n_panels, 2)
    nrows = (n_panels + 1) // 2

    # Sizing: single-panel compact; multi-panel capped at 4.5" wide per column
    if n_panels == 1:
        fig_w, fig_h = 5.0, 3.2
    else:
        fig_w, fig_h = 4.5 * ncols, 3.2 * nrows

    # Share y-axis for bar charts so adjacent panels are directly comparable
    sharey_mode = "row" if (chart_type == "bar" and n_panels > 1) else False
    fig, axes = plt.subplots(nrows, ncols, figsize=(fig_w, fig_h), sharey=sharey_mode)
    if n_panels == 1:
        axes_flat = [axes]
    else:
        axes_flat = list(np.array(axes).flatten())

    fig.subplots_adjust(top=0.86, hspace=0.70, wspace=0.30, bottom=0.22)
    fig.patch.set_facecolor("#f4f4f4")

    waves = sorted(df["wave_number"].unique())
    wave_labels = (
        df[["wave_number", "survey_period_label"]]
        .drop_duplicates(subset=["wave_number"])
        .sort_values("wave_number")
        .set_index("wave_number")["survey_period_label"]
    )
    xtick_labels = [str(wave_labels[w]) for w in waves]

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
            width = 0.52
            for i, country in enumerate(COUNTRY_ORDER):
                cdf = bar_df[bar_df[series_col] == country]
                val = cdf[value_col].iloc[0] if not cdf.empty else 0
                bar = ax.bar(x[i], val, width, color=COUNTRY_COLORS[country],
                             edgecolor="none", zorder=2)
                if panel_val == panels[0]:
                    handles.append(bar)
                    legend_labels.append(COUNTRIES[country])
            ax.set_xticks(x)
            ax.set_xticklabels([COUNTRIES[c] for c in COUNTRY_ORDER], fontsize=8)
            ax.axhline(0, color="#D2DBE0", linewidth=0.9, linestyle="-", zorder=0)
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

        ax.set_title(label_val, fontsize=9, pad=6)
        ax.set_ylabel("")
        _nbs_style_ax(ax, chart_type,
                      waves=(waves if chart_type == "line" else None),
                      xtick_labels=(xtick_labels if chart_type == "line" else None))

    # Hide unused axes
    for ax in axes_flat[n_panels:]:
        ax.set_visible(False)

    # Legend below all panels — square markers, NBS style
    fig.legend(
        handles, legend_labels,
        loc="lower center",
        bbox_to_anchor=(0.5, 0.01),
        ncol=len(COUNTRY_ORDER),
        fontsize=9,
        frameon=False,
        handlelength=1.0,
        handleheight=0.8,
    )

    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=150, bbox_inches="tight", facecolor="#f4f4f4")
    plt.close(fig)
    buf.seek(0)
    return buf.read()


# ---------------------------------------------------------------------------
# 3b. Custom chart: financing gap (grouped bars + gap line)
# ---------------------------------------------------------------------------

def _financing_gap_bars(df: pd.DataFrame) -> bytes:
    """
    Chart 1: grouped bars (need/availability) + gap line for bank loans (sub_item='a'),
    SK, EA, DE, last 4 waves.
    """
    import matplotlib.colors as mcolors

    sub_df = df[df["sub_item"] == "a"].copy()
    label_val = sub_df["sub_item_label"].iloc[0] if not sub_df.empty else "Bank loans"

    waves = sorted(sub_df["wave_number"].unique())
    wave_labels = (
        sub_df[["wave_number", "survey_period_label"]]
        .drop_duplicates(subset=["wave_number"]).sort_values("wave_number")
        .set_index("wave_number")["survey_period_label"]
    )

    fig, ax = plt.subplots(1, 1, figsize=(7.5, 3.8))
    fig.patch.set_facecolor("#f4f4f4")
    fig.subplots_adjust(top=0.82, bottom=0.26, left=0.09, right=0.97)

    n_countries = len(COUNTRY_ORDER)
    group_gap = 1.0
    bar_width = 0.7 / (n_countries * 2)
    bar_handles, bar_labels_leg, line_handles, line_labels_leg = [], [], [], []

    for c_idx, country in enumerate(COUNTRY_ORDER):
        cdf = sub_df[sub_df["country_code"] == country].sort_values("wave_number")
        if cdf.empty:
            continue
        base_color = COUNTRY_COLORS[country]
        rgb = mcolors.to_rgb(base_color)
        light_color = tuple(min(1.0, v + 0.30) for v in rgb)

        for w_idx, wave in enumerate(waves):
            row = cdf[cdf["wave_number"] == wave]
            if row.empty:
                continue
            pair_offset = (c_idx - n_countries / 2 + 0.5) * (2 * bar_width + 0.02)
            x_center = w_idx * group_gap
            b1 = ax.bar(x_center + pair_offset, row["need_nb"].iloc[0], bar_width,
                        color=base_color, edgecolor="white", linewidth=0.5, zorder=2)
            b2 = ax.bar(x_center + pair_offset + bar_width, row["availability_nb"].iloc[0], bar_width,
                        color=light_color, hatch="//", edgecolor=base_color, linewidth=0.5, zorder=2)
            if w_idx == 0:
                bar_handles += [b1, b2]
                bar_labels_leg += [f"{COUNTRIES[country]} — need", f"{COUNTRIES[country]} — availability"]

        x_pts = [i * group_gap for i, w in enumerate(waves) if not cdf[cdf["wave_number"] == w].empty]
        gap_vals = [cdf[cdf["wave_number"] == w]["financing_gap_wtd"].iloc[0] for w in waves
                    if not cdf[cdf["wave_number"] == w].empty]
        line, = ax.plot(x_pts, gap_vals, color=base_color, linewidth=2.0,
                        marker="D", markersize=4, linestyle="--", zorder=3)
        line_handles.append(line)
        line_labels_leg.append(f"{COUNTRIES[country]} — gap")

    ax.set_facecolor("#f4f4f4")
    for spine in ax.spines.values():
        spine.set_visible(False)
    ax.tick_params(which="both", length=0, colors=NBS_TEXT)
    ax.yaxis.grid(True, color="#D2DBE0", linewidth=0.6, linestyle="-", zorder=0)
    ax.xaxis.grid(False)
    ax.set_axisbelow(True)
    ax.axhline(0, color="#9aa5ad", linewidth=0.8, zorder=1)
    ax.set_xticks([i * group_gap for i in range(len(waves))])
    ax.set_xticklabels([str(wave_labels[w]) for w in waves], rotation=35, ha="right", fontsize=8)
    ax.yaxis.set_major_formatter(mticker.FormatStrFormatter("%+.0f"))
    ax.set_ylabel("")
    ax.set_title(f"{label_val} — need vs availability (bars); financing gap (dashed)", fontsize=9)
    fig.legend(bar_handles + line_handles, bar_labels_leg + line_labels_leg,
               loc="lower center", bbox_to_anchor=(0.5, 0.0), ncol=3, fontsize=7.5, frameon=False)

    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=150, bbox_inches="tight", facecolor="#f4f4f4")
    plt.close(fig)
    buf.seek(0)
    return buf.read()


# Instrument colours for the SK breakdown chart
INSTRUMENT_COLORS = {
    "a": "#bd4e35", "b": "#0777b3", "f": "#e18727", "g": "#5a9e6f", "h": "#7b5ea7",
}
INSTRUMENT_LABELS = {
    "a": "Bank loans", "b": "Trade credit", "f": "Credit lines",
    "g": "Leasing/hire-purchase", "h": "Other loans",
}


def _financing_gap_sk_instruments(df_sk: pd.DataFrame) -> bytes:
    """
    Chart 2: financing gap for Slovakia by instrument — bar chart, latest wave only.
    Line chart was dropped because not all instruments are asked every wave, causing
    visually confusing interrupted lines.
    """
    latest_wave = df_sk["wave_number"].max()
    bar_df = df_sk[df_sk["wave_number"] == latest_wave].copy()

    instruments = [s for s in INSTRUMENT_COLORS if not bar_df[bar_df["sub_item"] == s].empty]
    x = np.arange(len(instruments))
    vals, colors, labels = [], [], []
    for sub_item in instruments:
        row = bar_df[bar_df["sub_item"] == sub_item]
        vals.append(row["financing_gap_wtd"].iloc[0] if not row.empty else 0)
        colors.append(INSTRUMENT_COLORS[sub_item])
        labels.append(
            row["sub_item_label"].iloc[0] if ("sub_item_label" in row.columns and not row.empty)
            else INSTRUMENT_LABELS.get(sub_item, sub_item)
        )

    fig, ax = plt.subplots(1, 1, figsize=(5.5, 3.2))
    fig.patch.set_facecolor("#f4f4f4")
    fig.subplots_adjust(top=0.86, bottom=0.30, left=0.10, right=0.97)

    ax.bar(x, vals, 0.55, color=colors, edgecolor="none", zorder=2)
    ax.set_xticks(x)
    ax.set_xticklabels(labels, fontsize=8, rotation=25, ha="right")
    ax.set_facecolor("#f4f4f4")
    for spine in ax.spines.values():
        spine.set_visible(False)
    ax.tick_params(which="both", length=0, colors=NBS_TEXT)
    ax.yaxis.grid(True, color="#D2DBE0", linewidth=0.6, linestyle="-", zorder=0)
    ax.xaxis.grid(False)
    ax.set_axisbelow(True)
    ax.axhline(0, color="#9aa5ad", linewidth=0.8, zorder=1)
    ax.yaxis.set_major_formatter(mticker.FormatStrFormatter("%+.0f"))
    ax.set_ylabel("")
    ax.set_title("Slovakia — financing gap by instrument (latest wave)", fontsize=9)

    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=150, bbox_inches="tight", facecolor="#f4f4f4")
    plt.close(fig)
    buf.seek(0)
    return buf.read()


def build_financing_gap_chart(sec: dict, df: pd.DataFrame) -> bytes:
    """
    Returns a single PNG with two stacked charts:
    - Top: grouped bar chart for bank loans (SK/EA/DE), need vs availability vs gap
    - Bottom: SK-only gap line chart by instrument type
    """
    df_main = df[df["chart_type"] == "main"]
    df_sk = df[df["chart_type"] == "sk_all"]

    png1 = _financing_gap_bars(df_main)
    png2 = _financing_gap_sk_instruments(df_sk)

    # Stack vertically into a single PNG
    from PIL import Image
    img1 = Image.open(io.BytesIO(png1))
    img2 = Image.open(io.BytesIO(png2))
    combined = Image.new("RGB", (max(img1.width, img2.width), img1.height + img2.height), (248, 248, 248))
    combined.paste(img1, (0, 0))
    combined.paste(img2, (0, img1.height))
    buf = io.BytesIO()
    combined.save(buf, format="PNG")
    buf.seek(0)
    return buf.read()


# ---------------------------------------------------------------------------
# 4. Generate bullets (Sonnet, per section)
# ---------------------------------------------------------------------------

SECTION_CONTENT_SYSTEM = textwrap.dedent("""
    You are an ECB analyst writing content for a SAFE survey report focused on Slovakia.
    Return a JSON object with exactly two fields:

    "finding": A single declarative headline (max 12 words) summarising the most notable
      finding for Slovakia. Use active voice, name the direction. Do NOT mention question
      codes (Q10, Q5, etc.). Example: "Net tightening in interest rates reported by Slovak firms"

    "bullets": A list of 3 strings, each a bullet point (starting with "•") about the latest
      wave. Rules:
      - Frame as firm behaviour: "a net 26% of firms reported..." NOT "the net balance rose"
      - Compare to prior wave: "compared with a net X% in the previous quarter"
      - Include sample size for SK and EA where available: "a net 26% of firms (n=80)..."
      - One sentence per bullet, max ~25 words.
      - When citing a rate (application_rate, discouragement_rate, rejection_rate), always
        give n_respondents in parentheses immediately after the rate.
      - Avoid dramatic language: never write "surged", "collapsed", or "plummeted".
        Write "rose from X% to Y%" instead.
      - For ambiguous metrics, briefly define what the question asked in plain language
        (one embedded clause is enough — see the survey question text provided below).

    CRITICAL prose rule for net balances:
      A net balance value already encodes direction. Always use the ABSOLUTE value in prose
      and express direction through words only:
        ✓ "a net 5% of firms expected availability to deteriorate" (net_balance_wtd = -5.16)
        ✓ "a net 12% of firms reported tightening in interest rates" (net_balance_wtd = +12)
        ✗ "a net -5% of firms expected deterioration" — double-negative, never write this
        ✗ "a net +12% reported adverse conditions" — drop the + sign, use a direction word
      Use + or - ONLY when comparing two numbers showing a change (e.g. "from -3pp to +2pp").

    Wave-over-wave delta (Δ) usage:
      The latest-wave data includes a pre-computed Δ column showing the change from the
      previous wave. Always use it to state direction of change in at least one bullet.
      Format: "rose from X% (wave N−1) to Y% (wave N)" or "eased by Z pp vs the prior quarter".
      Examples:
        ✓ "Interest rates tightened for a net 12% of Slovak firms (n=80), easing 1.8 pp from
           the previous quarter's net 14% — but still above the EA's net 8%."
        ✓ "Slovak firms' selling price expectations edged up to +4.3% (Δ +0.3 pp vs wave 37),
           outpacing the EA median of +3.6%."
        ✗ "The net balance was 12 in wave 38 and 14 in wave 37." — never list raw wave numbers;
           always translate into direction language.

    Available mart tables and columns:
    {schema_catalogue}

    Query templates (fill in UPPER_CASE placeholders only):
    {query_templates}

    When to call query_mart — only these 3 cases justify a tool call:
    1. You need data from before wave 30 (use int_safe__core_questions_long)
    2. You need a sub_item or column NOT present in the section data provided above
    3. You need to verify a historical extreme (e.g. "is this the highest since wave X?")

    Do NOT call query_mart to:
    - Discover table or column names (the catalogue above is complete and current)
    - Recalculate or reformat data already in the provided section data
    - Fetch the same wave/country/sub_item combinations already shown to you
    - Explore what tables exist (they are all listed above)

    If none of the 3 cases apply, write your JSON response immediately.
    Cite only numbers from the provided data or a tool result you actually ran.
{historical_context}
    Return valid JSON only — no markdown fences, no commentary.
""").strip()


def _fmt_data_for_prompt(sec: dict, df: pd.DataFrame) -> str:
    """Serialize latest + previous wave data for the LLM prompt."""
    latest_wave = df["wave_number"].max()
    waves_sorted = sorted(df["wave_number"].unique())
    prev_wave = waves_sorted[-2] if len(waves_sorted) >= 2 else latest_wave
    latest = df[df["wave_number"] == latest_wave]
    prev = df[df["wave_number"] == prev_wave]

    if sec["id"] == "financing_gap":
        def fmt_gap(d: pd.DataFrame, label: str) -> str:
            rows = [f"{label}:"]
            main_rows = d[d.get("chart_type", "main") == "main"] if "chart_type" in d.columns else d
            by_inst = main_rows.groupby("sub_item")
            for sub_item, grp in sorted(by_inst, key=lambda x: x[0]):
                inst_label = grp["sub_item_label"].iloc[0] if not grp.empty else sub_item
                rows.append(f"  [{inst_label}]")
                sk_gap = ea_gap = None
                for _, r in grp.iterrows():
                    need = f"{r['need_nb']:+.1f}" if pd.notna(r.get("need_nb")) else "n/a"
                    avail = f"{r['availability_nb']:+.1f}" if pd.notna(r.get("availability_nb")) else "n/a"
                    gap_val = r.get("financing_gap_wtd")
                    gap = f"{gap_val:+.1f}" if pd.notna(gap_val) else "n/a"
                    n = int(r["n_respondents_need"]) if pd.notna(r.get("n_respondents_need")) else "?"
                    rows.append(f"    {r['country_code']} | need={need}pp (n={n}) | avail={avail}pp | gap={gap}pp")
                    if r["country_code"] == "SK" and pd.notna(gap_val):
                        sk_gap = gap_val
                    if r["country_code"] == "EA" and pd.notna(gap_val):
                        ea_gap = gap_val
                # Pre-compute SK vs EA comparison to prevent sign errors
                if sk_gap is not None and ea_gap is not None:
                    diff = sk_gap - ea_gap
                    if diff > 0:
                        comparison = f"SK gap is {diff:+.1f}pp HIGHER than EA → SK MORE stressed than EA"
                    elif diff < 0:
                        comparison = f"SK gap is {diff:+.1f}pp LOWER than EA → SK LESS stressed than EA"
                    else:
                        comparison = "SK gap equals EA gap"
                    rows.append(f"    ↳ Comparison: {comparison}")
            return "\n".join(rows)
        return fmt_gap(latest, f"Wave {latest_wave} (latest)") + "\n\n" + fmt_gap(prev, f"Wave {prev_wave} (previous)")

    value_col = sec["value_col"]
    panel_col = sec["panel_col"]
    panel_label_col = sec.get("panel_label_col", panel_col)

    # Build a lookup of prev-wave values keyed by (country_code, panel_value) for delta computation
    def _prev_key(r) -> tuple:
        panel_val = r[panel_col] if panel_col and panel_col in r.index else None
        return (r["country_code"], panel_val)

    prev_vals: dict[tuple, float] = {}
    for _, r in prev.iterrows():
        if pd.notna(r.get(value_col)):
            prev_vals[_prev_key(r)] = float(r[value_col])

    def fmt_latest_with_delta(d: pd.DataFrame, label: str) -> str:
        rows = [f"{label}:"]
        for _, r in d.iterrows():
            n_part = f" | n={r['n_respondents']}" if r.get("country_code") in ("SK", "EA") and "n_respondents" in r else ""
            val = r[value_col]
            val_str = f"{val:+.2f}" if pd.notna(val) else "n/a"
            panel_part = f" | {r[panel_label_col]}" if panel_label_col and panel_label_col in r.index else ""

            # Pre-compute wave-over-wave delta where possible
            delta_str = ""
            if pd.notna(val):
                prev_v = prev_vals.get(_prev_key(r))
                if prev_v is not None:
                    delta = float(val) - prev_v
                    delta_str = f" | Δ={delta:+.2f} vs wave {prev_wave}"

            rows.append(f"  {r['country_code']}{panel_part} | {value_col}={val_str}{delta_str}{n_part}")
        return "\n".join(rows)

    def fmt_prev(d: pd.DataFrame, label: str) -> str:
        rows = [f"{label}:"]
        for _, r in d.iterrows():
            n_part = f" | n={r['n_respondents']}" if r.get("country_code") in ("SK", "EA") and "n_respondents" in r else ""
            val_str = f"{r[value_col]:+.2f}" if pd.notna(r[value_col]) else "n/a"
            panel_part = f" | {r[panel_label_col]}" if panel_label_col and panel_label_col in r.index else ""
            rows.append(f"  {r['country_code']}{panel_part} | {value_col}={val_str}{n_part}")
        return "\n".join(rows)

    return fmt_latest_with_delta(latest, f"Wave {latest_wave} (latest)") + "\n\n" + fmt_prev(prev, f"Wave {prev_wave} (previous)")


def _sme_divergence_note(df: pd.DataFrame, value_col: str, panel_col: str | None, threshold: float = 30.0) -> str:
    """
    Return a divergence note string if SK SMEs differ from SK all-firms by >= threshold units
    in the latest wave. Injected into the LLM bullet prompt so it can surface the divergence.
    """
    if "firm_size" not in df.columns:
        return ""
    latest = df["wave_number"].max()
    all_sk = df[(df["firm_size"] == "all") & (df["wave_number"] == latest) & (df["country_code"] == "SK")]
    sme_sk = df[(df["firm_size"] == "sme") & (df["wave_number"] == latest) & (df["country_code"] == "SK")]
    if all_sk.empty or sme_sk.empty:
        return ""

    notes = []
    panels = all_sk[panel_col].dropna().unique() if panel_col and panel_col in all_sk.columns else [None]
    for panel in panels:
        if panel is not None:
            a_vals = all_sk[all_sk[panel_col] == panel][value_col]
            s_vals = sme_sk[sme_sk[panel_col] == panel][value_col]
        else:
            a_vals = all_sk[value_col]
            s_vals = sme_sk[value_col]
        a = a_vals.mean() if not a_vals.empty else float("nan")
        s = s_vals.mean() if not s_vals.empty else float("nan")
        if pd.notna(a) and pd.notna(s) and abs(a - s) >= threshold:
            panel_tag = f" [{panel}]" if panel is not None else ""
            sign = "higher" if s > a else "lower"
            notes.append(
                f"(SME divergence{panel_tag}: SK all-firms={a:+.1f}, SK SMEs={s:+.1f} — "
                f"SMEs are {abs(a - s):.0f} units {sign}; consider calling this out)"
            )
    return " ".join(notes)


def _parse_section_response(raw: str, sec: dict) -> dict:
    """Parse Sonnet JSON response into {"finding", "bullets"}.

    Robust to prose before/after the JSON block (Claude often reasons aloud when
    tools are active before emitting the final JSON).
    """
    # Primary: find a JSON object containing both keys anywhere in the response
    match = re.search(r'\{.*?"finding".*?"bullets".*?\}', raw, re.DOTALL)
    if not match:
        # Secondary: strip fences and find any JSON object
        stripped = raw.strip().removeprefix("```json").removeprefix("```").removesuffix("```").strip()
        match = re.search(r'\{.*\}', stripped, re.DOTALL)
        raw = stripped
    try:
        parsed = json.loads(match.group() if match else raw)
        finding = str(parsed.get("finding", sec["title"]))
        raw_bullets = parsed.get("bullets", [])
        if isinstance(raw_bullets, list):
            bullets = [str(b).strip().lstrip("•- ") for b in raw_bullets if str(b).strip()][:3]
        else:
            bullets = [b.strip().lstrip("•- ") for b in str(raw_bullets).splitlines() if b.strip()][:3]
    except (json.JSONDecodeError, AttributeError, TypeError):
        finding = sec["title"]
        # Filter out structural/markdown lines so prose reasoning doesn't become bullets
        bullets = [
            line.strip().lstrip("•- ") for line in raw.splitlines()
            if line.strip() and not line.strip().startswith(("```", "{", "}", "*", "#"))
        ][:3]
    return {"finding": finding, "bullets": bullets}


def get_section_content_agentic(
    sec: dict,
    df: pd.DataFrame,
    tool_con,
    schema: str,
    mart_catalogue: str,
    cost_tracker: dict,
    question_texts: dict[str, str] | None = None,
    client: anthropic.Anthropic | None = None,
    historical_context: str = "",
) -> dict:
    """Return {"finding": str, "bullets": [str, ...]} via an agentic Sonnet loop.

    Claude may call query_mart 0–3 times to fetch additional context before producing
    its final JSON response. All tool queries are validated (read-only + whitelist)
    before execution.
    """
    # System prompt is identical across all sections (schema_catalogue and query_templates
    # are the only variable parts, and they don't change between sections). The
    # cache_control block tells Anthropic to cache this prefix — subsequent section calls
    # pay only 10% of normal input price for these tokens.
    # historical_context is pre-rendered as "\n\n## Historical context...\n..." or ""
    # so the format slot is always safe — empty string leaves no extra blank lines
    system_prompt = SECTION_CONTENT_SYSTEM.format(
        schema_catalogue=mart_catalogue,
        query_templates=MART_QUERY_TEMPLATES,
        historical_context=f"\n{historical_context}" if historical_context else "",
    )
    cached_system = [{"type": "text", "text": system_prompt,
                      "cache_control": {"type": "ephemeral"}}]

    # Section-specific context goes in the user message (not the system prompt) so the
    # cached system block stays byte-for-byte identical across all section calls.
    base_data = _fmt_data_for_prompt(sec, df)
    divergence = _sme_divergence_note(df, sec["value_col"], sec.get("panel_col"))

    # Inject question text from annex so Claude can explain ambiguous metrics in plain language
    q_text_block = ""
    if question_texts:
        q_ids = sec.get("question_ids", [])
        found = [(qid.upper(), question_texts.get(qid.lower(), "")) for qid in q_ids]
        found = [(qid, txt) for qid, txt in found if txt]
        if found:
            lines = ["## Survey question text (use to write plain-language explanations)"]
            for qid, txt in found:
                lines.append(f"{qid}: {txt}")
            q_text_block = "\n".join(lines) + "\n\n"

    section_header = (
        f"Sign convention: {sec['sign_note']}\n"
        f"Focus: {sec['focus']}\n\n"
    )
    initial_msg = q_text_block + section_header + base_data + (f"\n\nSME divergence check:\n{divergence}" if divergence else "")

    if client is None:
        client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
    messages = [{"role": "user", "content": initial_msg}]
    tool_calls_made = 0

    for turn in range(MAX_TOOL_TURNS):
        response = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=600,
            system=cached_system,
            tools=[QUERY_MART_TOOL],
            messages=messages,
            extra_headers={"anthropic-beta": "prompt-caching-2024-07-31"},
        )

        _track_cost(cost_tracker, "claude-sonnet-4-6", response.usage)

        if response.stop_reason != "tool_use":
            # Final text response — extract and parse
            text_blocks = [b.text for b in response.content if hasattr(b, "text")]
            raw = text_blocks[-1] if text_blocks else ""
            result = _parse_section_response(raw, sec)
            result["tool_calls"] = tool_calls_made
            return result

        # Process tool calls
        messages.append({"role": "assistant", "content": response.content})
        tool_results = []
        for block in response.content:
            if block.type != "tool_use":
                continue
            sql = block.input.get("sql", "")
            print(f"    [tool_use] query_mart called (turn {turn + 1}): {sql[:120]!r}")
            result = _run_query_tool(sql, tool_con, schema)
            tool_calls_made += 1
            tool_results.append({
                "type": "tool_result",
                "tool_use_id": block.id,
                "content": result,
            })
        messages.append({"role": "user", "content": tool_results})

    # Exceeded MAX_TOOL_TURNS — request a final answer without tools
    messages.append({
        "role": "user",
        "content": "Please now return your final JSON response with finding and bullets.",
    })
    final = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=600,
        system=cached_system,
        messages=messages,
        extra_headers={"anthropic-beta": "prompt-caching-2024-07-31"},
    )
    _track_cost(cost_tracker, "claude-sonnet-4-6", final.usage)
    text_blocks = [b.text for b in final.content if hasattr(b, "text")]
    raw = text_blocks[-1] if text_blocks else ""
    result = _parse_section_response(raw, sec)
    result["tool_calls"] = tool_calls_made
    return result


# ---------------------------------------------------------------------------
# 5. Executive summary (Sonnet, bullet points)
# ---------------------------------------------------------------------------

EXEC_CROSS_SECTION_SYSTEM = textwrap.dedent("""
    You are a senior ECB economist. You will receive section-by-section findings from the
    latest ECB SAFE survey for Slovakia.

    Your sole task: identify 2–3 cross-cutting tensions or themes that span multiple sections.
    These are patterns that no single section captures alone — e.g.:
    - A disconnect between two financing instruments ("bank loans eased while credit lines remained tight")
    - A cost-revenue squeeze ("labour costs rose while turnover fell, squeezing margins")
    - A divergence between current conditions and forward expectations
    - A contrast between Slovakia and the Euro Area that is consistent across topics

    Rules:
    - Write exactly 2–3 short plain-English bullet points. No headers, no numbers, no markdown.
    - Only name a tension if it is actually visible in the findings given — do not invent themes.
    - If fewer than 2 genuine tensions exist, write only those you see.
    - These bullets are internal notes for a second analyst — they will NOT appear verbatim in the report.
""").strip()

EXEC_SUMMARY_SYSTEM = textwrap.dedent("""
    You are an economist writing an executive summary of the latest ECB SAFE survey results
    for Slovakia. You will receive:
    1. Section-by-section findings from the report
    2. Cross-cutting themes identified by a first-pass analyst

    Your task: write EXACTLY 3–4 bullets. No more. Cover BOTH financing conditions AND
    the economic situation of firms. Prioritise the most striking or cross-cutting findings
    and ruthlessly drop the rest.

    FORMAT — every bullet must follow this exact structure:
      **Short label (2–4 words):** One concise sentence of explanation.

    Examples of well-formed bullets:
      **Interest rate tightening:** Slovak firms reported a net increase in bank loan interest costs,
        with the tightening more pronounced than in the Euro Area average.
      **Margin squeeze:** Labour costs continued to rise while turnover declined, compressing
        operating margins for Slovak SMEs — the sharpest divergence in three waves.
      **Credit access stable:** Despite tighter terms, actual credit availability remained broadly
        unchanged and access-to-finance was rated the least pressing business obstacle.

    Requirements:
    - At least one bullet must be anchored on a cross-cutting theme from the first-pass analyst.
    - Include a number when it sharpens the story — net balance, % change, wave comparison.
      Numbers are allowed and encouraged where they add real intel. Omit them only when the
      direction alone is the point.
    - Do NOT include the section_id or any section name in the bullet text itself.
      The section_id belongs only in the JSON "section_id" field, never in the bullet string.

    Return a JSON array only — no markdown fences, no commentary:
    [
      {"bullet": "**Label:** explanation", "section_id": "bank_loan_terms"},
      ...
    ]

    Valid section_id values (use exactly as written):
    bank_loan_terms, financing_gap, loan_applications, availability_expectations,
    financing_purpose, financing_factors, business_situation, outlook,
    expectations_quantitative, expectations_risk, business_problems, adhoc_spotlight

    For cross-cutting bullets spanning multiple sections, use the most relevant section_id.
    No leading bullet character inside the bullet text.

    Special rule for adhoc_spotlight: if you include a bullet for that section, prefix the
    bullet text with the 🔍 emoji, e.g. "🔍 **AI Peer Estimates:** Slovak firms estimated..."
""").strip()

SO_WHAT_SYSTEM = textwrap.dedent("""
    You are an editorial analyst reviewing bullet points from an ECB SAFE survey section.
    Your task: for each bullet, add a brief "so what" implication clause if the bullet is
    purely descriptive — i.e. it states what happened but not why it matters for Slovak firms.

    Rules:
    - Add ONE embedded implication clause per bullet that needs it (a subordinate clause or
      "—" dash phrase). Keep the original wording; just extend it.
    - Do NOT add any numbers that are not already in the bullet.
    - Do NOT change bullets that already have an implication (e.g. already say "suggesting",
      "putting pressure on", "compressing", "signalling", "indicating", etc.).
    - Do NOT change the finding headline — only revise the bullets array.
    - Return valid JSON only — no markdown fences:
      {"finding": "<original finding unchanged>", "bullets": ["revised bullet 1", ...]}
""").strip()


def get_exec_summary(
    rendered_sections: list[dict], cost_tracker: dict
) -> list[dict]:
    """Two-pass exec summary. Returns list of {bullet, section_id} dicts."""
    section_ids = {s["section_id"] for s in rendered_sections}

    # Build the shared input block (same for both passes)
    lines = ["Section findings:\n"]
    for s in rendered_sections:
        lines.append(f"## {s['title']} [section_id: {s['section_id']}]")
        if s.get("sign_note"):
            lines.append(f"Sign convention: {s['sign_note']}")
        for b in s["bullets"]:
            lines.append(f"  {b}")
        lines.append("")
    section_text = "\n".join(lines)

    client = _mistral_client()

    # Pass 1: identify cross-cutting themes
    resp1 = client.chat.complete(
        model="mistral-small-latest",
        max_tokens=200,
        messages=[
            {"role": "system", "content": EXEC_CROSS_SECTION_SYSTEM},
            {"role": "user", "content": section_text},
        ],
    )
    if resp1.usage:
        _track_cost(cost_tracker, "mistral-small-latest",
                    _Usage(resp1.usage.prompt_tokens, resp1.usage.completion_tokens))
    themes = resp1.choices[0].message.content.strip()

    # Pass 2: write final bullets as JSON with section_id
    user_msg = (
        f"{section_text}\n\n"
        f"Cross-cutting themes identified by first-pass analyst:\n{themes}"
    )
    resp2 = client.chat.complete(
        model="mistral-small-latest",
        max_tokens=500,
        messages=[
            {"role": "system", "content": EXEC_SUMMARY_SYSTEM},
            {"role": "user", "content": user_msg},
        ],
    )
    if resp2.usage:
        _track_cost(cost_tracker, "mistral-small-latest",
                    _Usage(resp2.usage.prompt_tokens, resp2.usage.completion_tokens))

    raw = resp2.choices[0].message.content.strip()
    # Strip markdown fences if model wraps output despite instructions
    raw = re.sub(r"^```(?:json)?\s*", "", raw)
    raw = re.sub(r"\s*```$", "", raw).strip()

    try:
        items = json.loads(repair_json(raw))
        # Validate and normalise: keep only dicts with a non-empty bullet field
        result = []
        for item in items:
            if not isinstance(item, dict):
                continue
            bullet = str(item.get("bullet", "")).strip().lstrip("•- ")
            sid = str(item.get("section_id", "")).strip()
            # Strip 🔍 emoji from non-adhoc bullets (guard against LLM over-applying the rule)
            if sid != "adhoc_spotlight":
                bullet = bullet.lstrip("🔍 ")
            if bullet:
                result.append({"bullet": bullet, "section_id": sid if sid in section_ids else ""})
        return result[:4]
    except Exception:
        # Fallback: treat as plain text, no section links
        plain = [l.strip().lstrip("•- ") for l in raw.splitlines() if l.strip()]
        return [{"bullet": b, "section_id": ""} for b in plain[:4]]


def _add_so_what(content: dict, sec: dict, mistral_client, cost_tracker: dict) -> dict:
    """Add implication clauses to purely-descriptive section bullets via Mistral Small."""
    bullets_text = "\n".join(f"- {b}" for b in content["bullets"])
    user_msg = (
        f"Section: {sec['title']}\n"
        f"Sign convention: {sec['sign_note']}\n\n"
        f"Finding: {content['finding']}\n\n"
        f"Bullets:\n{bullets_text}"
    )
    try:
        resp = mistral_client.chat.complete(
            model="mistral-small-latest",
            max_tokens=400,
            messages=[
                {"role": "system", "content": SO_WHAT_SYSTEM},
                {"role": "user", "content": user_msg},
            ],
        )
        if resp.usage:
            _track_cost(cost_tracker, "mistral-small-latest",
                        _Usage(resp.usage.prompt_tokens, resp.usage.completion_tokens))
        raw = resp.choices[0].message.content.strip()
        raw = re.sub(r"^```(?:json)?\s*", "", raw)
        raw = re.sub(r"\s*```$", "", raw).strip()
        parsed = json.loads(repair_json(raw))
        revised_bullets = parsed.get("bullets", [])
        if revised_bullets and len(revised_bullets) == len(content["bullets"]):
            return {**content, "bullets": [str(b).strip() for b in revised_bullets]}
    except Exception:
        pass  # on any failure, return original unchanged
    return content


def _write_wave_memory(
    wave: int,
    exec_bullets: list[dict],
    rendered: list[dict],
    mistral_client,
    con,
    cost_tracker: dict,
) -> None:
    """Write a 3–4 sentence summary of this wave's notable findings to MotherDuck."""
    bullet_text = " | ".join(b["bullet"] for b in exec_bullets if b.get("bullet"))
    findings = " | ".join(r["finding"] for r in rendered if r.get("finding"))
    prompt = (
        f"Wave {wave} findings for Slovak firms: {findings}. "
        f"Executive summary: {bullet_text}. "
        "Write 3–4 sentences summarising what was most notable for Slovak firms this wave, "
        "for a reader who will see this as historical context in a FUTURE wave's report. "
        "Past tense. Include 2–3 specific numbers. Plain text only, no markdown."
    )
    model = "mistral-small-latest"
    try:
        resp = mistral_client.chat.complete(
            model=model,
            max_tokens=200,
            messages=[{"role": "user", "content": prompt}],
        )
        summary = resp.choices[0].message.content.strip()
        # Strip any accidental markdown formatting
        summary = re.sub(r"\*+", "", summary).strip()
        if resp.usage:
            _track_cost(cost_tracker, model,
                        _Usage(resp.usage.prompt_tokens, resp.usage.completion_tokens))
        con.execute("""
            CREATE TABLE IF NOT EXISTS main_safe.ref_safe__wave_memory (
                wave_number INTEGER PRIMARY KEY,
                run_date    DATE,
                notable_summary TEXT,
                model_id    TEXT
            )
        """)
        con.execute(
            "INSERT OR REPLACE INTO main_safe.ref_safe__wave_memory VALUES (?,?,?,?)",
            [wave, date.today(), summary, model],
        )
        print(f"  Wave memory written for wave {wave}")
    except Exception as e:
        print(f"  Wave memory write failed: {e}")


# ---------------------------------------------------------------------------
# 5b. Adhoc module spotlight
# ---------------------------------------------------------------------------

_MODULE_THEME_FALLBACK: dict[str, str] = {
    "qa1": "Digital transformation",
    "qa1a": "Digital transformation",
    "qa1b": "Digital transformation",
    "qa1c": "Digital transformation",
    "qa2": "Green transition",
    "qa2dec": "Green transition",
    "qa2inc": "Green transition",
    "qa3": "Supply chain resilience",
    "qa4": "AI Adoption",
    "qa5": "Geopolitical Risk",
    "qa6": "Energy Costs",
    "qb1": "AI Adoption Expectations",
    "qb2": "Special Focus",
}

_ECB_FOCUS_INDEX = "https://www.ecb.europa.eu/press/economic-bulletin/focus/"


def detect_adhoc_theme(wave_number: int, con, schema: str, mistral_client=None, cost_tracker: dict | None = None) -> dict | None:
    """Return {module_id, theme_label, question_texts} for the wave's adhoc modules, or None.

    Theme label is derived from annex question text via Mistral Small (2–3 word phrase).
    Falls back to _MODULE_THEME_FALLBACK if Mistral is unavailable or annex has no text.
    """
    try:
        rows = con.execute(f"""
            SELECT module_id, sum(n_firms) as n
            FROM {schema}.mart_safe__adhoc_responses
            WHERE wave_number = {wave_number}
            GROUP BY module_id
            ORDER BY n DESC
        """).fetchall()
    except Exception:
        return None
    if not rows:
        return None

    # Pick the module with the most respondents
    primary_module_id = rows[0][0]

    # Fetch question text from annex table
    question_texts: dict[str, str] = {}
    try:
        annex_cols = con.execute(f"""
            SELECT column_name FROM information_schema.columns
            WHERE table_schema = '{schema}'
              AND table_name = 'ref_safe__annex'
              AND column_name LIKE 'safe_%'
            ORDER BY column_name DESC
        """).fetchall()
        wave_cols = [r[0] for r in annex_cols]
        if wave_cols:
            coalesce_expr = ", ".join(wave_cols)
            ann_rows = con.execute(f"""
                SELECT question_item, COALESCE({coalesce_expr}) as question_text
                FROM {schema}.ref_safe__annex
                WHERE element = 'question'
                  AND UPPER(question_item) LIKE UPPER('{primary_module_id}%')
            """).fetchall()
            for qitem, qtext in ann_rows:
                if qtext:
                    sub = qitem.lower().replace(primary_module_id.lower(), "").strip() or "a"
                    question_texts[sub] = qtext
    except Exception:
        pass

    # Derive theme label from annex text via Mistral Small
    theme_label = _MODULE_THEME_FALLBACK.get(primary_module_id, primary_module_id.upper())
    if mistral_client and question_texts:
        sample_texts = "\n".join(f"- {t}" for t in list(question_texts.values())[:4])
        model = "mistral-small-latest"
        try:
            resp = mistral_client.chat.complete(
                model=model,
                max_tokens=20,
                messages=[{
                    "role": "user",
                    "content": (
                        f"These are ECB SAFE survey questions from module '{primary_module_id}':\n"
                        f"{sample_texts}\n\n"
                        "In 2–4 words, what is the single overarching topic of these questions? "
                        "Reply with the topic only, title case, no punctuation."
                    ),
                }],
            )
            label = resp.choices[0].message.content.strip().rstrip(".")
            if label:
                theme_label = label
            if resp.usage and cost_tracker is not None:
                _track_cost(cost_tracker, model,
                            _Usage(resp.usage.prompt_tokens, resp.usage.completion_tokens))
        except Exception:
            pass  # keep fallback label

    return {
        "module_id": primary_module_id,
        "theme_label": theme_label,
        "question_texts": question_texts,
    }


ADHOC_CONTENT_SYSTEM = textwrap.dedent("""
    You are a concise economic analyst writing a "Special Focus" sidebar for a Slovakia
    SAFE survey report. Your task: write exactly 2 short bullets (≤ 30 words each)
    comparing Slovak firms (SK) to the Euro Area (EA) on the topic indicated.

    RULES — READ CAREFULLY:
    1. Only cite numbers that appear in the data table below. Do NOT invent or estimate.
    2. If SK or EA data is missing, say so explicitly rather than omitting the comparison.
    3. Bullets start with a bolded 3–5 word label: **Label:** sentence.
    4. One sentence per bullet. Plain text only, no markdown headers or lists beyond bullets.
    5. Return valid JSON only (no markdown fences):
       {"finding": "One sentence headline ≤ 20 words", "bullets": ["bullet 1", "bullet 2"]}
    6. The finding must name the theme and the most striking difference (SK vs EA).
       If results are similar, say so.
""").strip()


def _is_continuous(df: pd.DataFrame) -> bool:
    """True if response_raw looks like an open numeric estimate (>10 distinct values per group).

    Categorical adhoc questions (yes/no, 1-4 scale) have few distinct codes.
    Continuous questions (e.g. 'what % of peers invested?') produce many distinct integers.
    """
    if df.empty:
        return False
    max_distinct = df.groupby(["country_code", "sub_item"])["response_raw"].nunique().max()
    return int(max_distinct) > 10


_ADHOC_CHART_AGENT_SYSTEM = textwrap.dedent("""
    You are a data analyst building a chart for an ECB SAFE adhoc survey question.

    You have access to mart_safe__adhoc_responses (columns: wave_number, period_asked,
    module_id, sub_item, country_code, response_raw, n_firms, n_firms_wtd, n_total_wtd, pct_wtd).
    Always use fully-qualified names: main_safe.mart_safe__adhoc_responses.

    Your task:
    1. Call query_mart to fetch chart data for the given module. Filter to wave_number,
       module_id, and country_code IN ('SK', 'EA').
    2. For CONTINUOUS questions (open numeric 0-100 estimates): bucket into 10-unit intervals
       using FLOOR(CAST(response_raw AS INTEGER) / 10) * 10 AS bucket, aggregate pct_wtd.
    3. For CATEGORICAL questions: return response_raw codes with pct_wtd as-is.
    4. After getting the data, return JSON only (no markdown):
       {
         "chart_type": "continuous" or "categorical",
         "rows": [
           {"country_code": "SK", "sub_item": "a", "response_raw": 10, "pct_wtd": 23.5},
           ...
         ]
       }

    Return only the final JSON — no explanation, no markdown fences.
""").strip()


def _build_adhoc_chart_agentic(
    theme: dict,
    wave_number: int,
    schema: str,
    con,
    cost_tracker: dict,
    client: anthropic.Anthropic,
) -> pd.DataFrame | None:
    """Use Claude + query_mart tool to fetch chart-ready data for the adhoc module.
    Returns a DataFrame with columns country_code, sub_item, response_raw, pct_wtd,
    or None on failure.
    """
    is_cont = _is_continuous(
        con.execute(
            f"SELECT country_code, sub_item, response_raw FROM {schema}.mart_safe__adhoc_responses "
            f"WHERE wave_number={wave_number} AND module_id='{theme['module_id']}' "
            f"AND country_code IN ('SK','EA')"
        ).df()
    )
    question_ctx = ""
    if theme.get("question_texts"):
        question_ctx = "\n".join(f"  Sub-item {k}: {v}" for k, v in theme["question_texts"].items())

    user_msg = (
        f"Module: {theme['module_id']} — {theme['theme_label']}\n"
        f"Wave: {wave_number}, Schema: {schema}\n"
        f"Question type: {'CONTINUOUS (open numeric 0-100 estimates)' if is_cont else 'CATEGORICAL'}\n"
        + (f"Question text:\n{question_ctx}\n" if question_ctx else "")
        + "\nFetch the data and return the JSON chart spec."
    )

    messages = [{"role": "user", "content": user_msg}]
    for turn in range(3):
        response = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=800,
            system=_ADHOC_CHART_AGENT_SYSTEM,
            tools=[QUERY_MART_TOOL],
            messages=messages,
        )
        _track_cost(cost_tracker, "claude-sonnet-4-6", response.usage)

        if response.stop_reason != "tool_use":
            # Parse final JSON chart spec
            text_blocks = [b.text for b in response.content if hasattr(b, "text")]
            raw = (text_blocks[-1] if text_blocks else "").strip()
            if raw.startswith("```"):
                raw = raw.split("\n", 1)[-1].rsplit("```", 1)[0].strip()
            try:
                spec = json.loads(raw)
                rows = spec.get("rows", [])
                if not rows:
                    return None
                return pd.DataFrame(rows)
            except Exception as e:
                print(f"  Adhoc chart spec parse failed: {e}")
                return None

        messages.append({"role": "assistant", "content": response.content})
        tool_results = []
        for block in response.content:
            if block.type != "tool_use":
                continue
            sql = block.input.get("sql", "")
            print(f"    [adhoc chart] query_mart: {sql[:100]!r}")
            result = _run_query_tool(sql, con, schema)
            tool_results.append({
                "type": "tool_result",
                "tool_use_id": block.id,
                "content": result,
            })
        messages.append({"role": "user", "content": tool_results})

    return None


def _build_adhoc_chart(df: pd.DataFrame, theme: dict) -> bytes | None:
    """Render NBS-styled bar chart from adhoc chart DataFrame. Returns PNG bytes or None."""
    if df is None or df.empty:
        return None
    try:
        sub_items = sorted(df["sub_item"].unique())
        n_panels = len(sub_items)
        ncols = min(n_panels, 2)
        nrows = (n_panels + 1) // 2
        fig_w = 5.0 if n_panels == 1 else 4.5 * ncols
        fig_h = 3.2 if n_panels == 1 else 3.2 * nrows

        fig, axes = plt.subplots(nrows, ncols, figsize=(fig_w, fig_h))
        axes_flat = [axes] if n_panels == 1 else list(np.array(axes).flatten())
        fig.patch.set_facecolor("#f4f4f4")
        fig.subplots_adjust(top=0.86, hspace=0.70, wspace=0.30, bottom=0.22)

        countries = [c for c in ["SK", "EA"] if c in df["country_code"].values]
        handles, legend_labels_list = [], []

        for ax, sub in zip(axes_flat, sub_items):
            sub_df = df[df["sub_item"] == sub]
            x_vals = sorted(sub_df["response_raw"].unique())
            x = np.arange(len(x_vals))
            width = 0.35

            for i, country in enumerate(countries):
                cdf = sub_df[sub_df["country_code"] == country]
                vals = [
                    cdf[cdf["response_raw"] == v]["pct_wtd"].iloc[0]
                    if not cdf[cdf["response_raw"] == v].empty else 0
                    for v in x_vals
                ]
                offset = (i - len(countries) / 2 + 0.5) * width
                bar = ax.bar(x + offset, vals, width,
                             color=COUNTRY_COLORS.get(country, "#888"),
                             edgecolor="none", zorder=2)
                if sub == sub_items[0]:
                    handles.append(bar)
                    legend_labels_list.append(COUNTRIES.get(country, country))

            ax.set_xticks(x)
            # If values look like 10-unit buckets, label as ranges
            if len(x_vals) > 5 and all(v % 10 == 0 for v in x_vals if isinstance(v, (int, float))):
                ax.set_xticklabels([f"{int(v)}–{int(v)+9}%" for v in x_vals],
                                   rotation=35, ha="right", fontsize=7.5)
            else:
                ax.set_xticklabels([str(v) for v in x_vals], fontsize=8)

            title = (theme.get("question_texts") or {}).get(sub, sub or theme["theme_label"])
            ax.set_title(str(title)[:55], fontsize=8, pad=5)
            _nbs_style_ax(ax, "bar")

        for ax in axes_flat[n_panels:]:
            ax.set_visible(False)

        fig.legend(handles, legend_labels_list, loc="lower center",
                   bbox_to_anchor=(0.5, 0.01), ncol=len(countries),
                   fontsize=9, frameon=False, handlelength=1.0)

        buf = io.BytesIO()
        fig.savefig(buf, format="png", dpi=150, bbox_inches="tight", facecolor="#f4f4f4")
        plt.close(fig)
        buf.seek(0)
        return buf.read()
    except Exception as e:
        print(f"  Adhoc chart render failed: {e}")
        return None


def build_adhoc_spotlight(
    theme: dict,
    wave_number: int,
    con,
    schema: str,
    mistral_client,
    cost_tracker: dict,
    anthropic_client: anthropic.Anthropic | None = None,
) -> dict | None:
    """Generate the adhoc spotlight section. Returns a rendered-section dict or None."""
    sql_template = (SQL_DIR / "adhoc_spotlight.sql").read_text()
    sql = sql_template.format(
        wave_number=wave_number,
        module_id=theme["module_id"],
        schema=schema,
    )
    try:
        df = con.execute(sql).df()
    except Exception as e:
        print(f"  Adhoc spotlight SQL failed: {e}")
        return None
    if df.empty:
        return None

    # Detect whether responses are categorical codes or continuous numeric estimates
    if _is_continuous(df):
        # Collapse distribution to weighted mean + approximate median per country × sub_item
        lines = ["country_code | sub_item | mean_pct | median_pct | n_firms"]
        for (cc, sub), grp in df.groupby(["country_code", "sub_item"]):
            grp = grp.sort_values("response_raw")
            total_w = grp["n_firms_wtd"].sum()
            wtd_mean = (grp["response_raw"] * grp["n_firms_wtd"]).sum() / total_w
            cumw = grp["n_firms_wtd"].cumsum()
            median_val = int(grp[cumw >= total_w / 2]["response_raw"].iloc[0])
            lines.append(
                f"{cc} | {sub} | {wtd_mean:.1f}% | {median_val}% | {int(grp['n_firms'].sum())}"
            )
        data_table = "\n".join(lines)
        data_note = (
            "\n\nNOTE: Each firm gave a numeric % estimate. "
            "The table shows the weighted mean and approximate median of those estimates."
        )
    else:
        # Categorical: list each response code with its share
        pivot_lines = ["country_code | sub_item | response_raw | pct_wtd | n_firms"]
        for _, row in df.iterrows():
            pivot_lines.append(
                f"{row['country_code']} | {row['sub_item']} | {row['response_raw']} "
                f"| {row['pct_wtd']:.1f}% | {int(row['n_firms'])}"
            )
        data_table = "\n".join(pivot_lines)
        data_note = ""

    question_ctx = ""
    if theme.get("question_texts"):
        question_ctx = "\n".join(
            f"  Sub-item {k}: {v}" for k, v in theme["question_texts"].items()
        )
        question_ctx = f"\n\nQuestion text:\n{question_ctx}"

    user_msg = (
        f"Topic: {theme['theme_label']}{question_ctx}\n\n"
        f"Data (wave {wave_number}, SK vs EA):\n{data_table}{data_note}\n\n"
        "Write the finding and 2 bullets as specified."
    )

    model = "mistral-small-latest"
    try:
        resp = mistral_client.chat.complete(
            model=model,
            max_tokens=200,
            messages=[
                {"role": "system", "content": ADHOC_CONTENT_SYSTEM},
                {"role": "user", "content": user_msg},
            ],
        )
        raw = resp.choices[0].message.content.strip()
        if resp.usage:
            _track_cost(cost_tracker, model,
                        _Usage(resp.usage.prompt_tokens, resp.usage.completion_tokens))
        if raw.startswith("```"):
            raw = raw.split("\n", 1)[-1].rsplit("```", 1)[0].strip()
        result = json.loads(repair_json(raw))
    except Exception as e:
        print(f"  Adhoc spotlight generation failed: {e}")
        return None

    # Build chart via agentic SQL query
    chart_png = None
    if anthropic_client is not None:
        try:
            chart_df = _build_adhoc_chart_agentic(
                theme, wave_number, schema, con, cost_tracker, anthropic_client
            )
            chart_png = _build_adhoc_chart(chart_df, theme)
            if chart_png:
                print(f"  Adhoc chart built ({len(chart_df)} rows)")
        except Exception as e:
            print(f"  Adhoc chart skipped: {e}")

    return {
        "section_id": "adhoc_spotlight",
        "title": f"Special Focus: {theme['theme_label']}",
        "finding": result.get("finding", ""),
        "bullets": result.get("bullets", []),
        "chart_png": chart_png,
        "theme_label": theme["theme_label"],
    }


def _find_ecb_focus_article(theme_label: str, mistral_client, cost_tracker: dict) -> str | None:
    """Search ECB Economic Bulletin focus articles for a match. Returns URL or None."""
    import urllib.request
    try:
        req = urllib.request.Request(_ECB_FOCUS_INDEX, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=10) as r:
            html = r.read().decode("utf-8", errors="replace")
    except Exception:
        return None

    # Extract (url, title) pairs from ECB focus article links
    pairs = re.findall(
        r'href="(https://www\.ecb\.europa\.eu/press/economic-bulletin/focus/\d{4}/html/[^"]+)"'
        r'[^>]*>([^<]+)<',
        html,
    )
    # Also try relative URLs
    rel_pairs = re.findall(
        r'href="(/press/economic-bulletin/focus/\d{4}/html/[^"]+)"[^>]*>([^<]+)<',
        html,
    )
    for path, title in rel_pairs:
        pairs.append(("https://www.ecb.europa.eu" + path, title))

    if not pairs:
        return None

    listing = "\n".join(
        f"{i + 1}. {title.strip()} — {url}" for i, (url, title) in enumerate(pairs[:20])
    )
    model = "mistral-small-latest"
    try:
        resp = mistral_client.chat.complete(
            model=model,
            max_tokens=80,
            messages=[{
                "role": "user",
                "content": (
                    f"Theme: {theme_label}\n\nECB Economic Bulletin focus articles:\n{listing}\n\n"
                    "Return JSON only: {\"index\": <1-based number or null if no match>, "
                    "\"confidence\": <0.0-1.0>}\n"
                    "Set confidence=0 if no article clearly matches the theme."
                ),
            }],
        )
        raw = resp.choices[0].message.content.strip()
        if resp.usage:
            _track_cost(cost_tracker, model,
                        _Usage(resp.usage.prompt_tokens, resp.usage.completion_tokens))
        result = json.loads(repair_json(raw))
        idx = result.get("index")
        conf = float(result.get("confidence", 0))
        if idx and conf >= 0.90:
            url, _ = pairs[int(idx) - 1]
            return url
    except Exception:
        pass
    return None


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


def _load_annex_question_texts(con=None) -> dict[str, str]:
    """Return {q_id_lower: cleaned_question_text} for all questions from MotherDuck."""
    if con is None:
        return {}
    try:
        cols_res = con.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = 'main_safe' AND table_name = 'ref_safe__annex' "
            "ORDER BY ordinal_position"
        ).fetchall()
        if not cols_res:
            return {}
        all_cols = [r[0] for r in cols_res]
        try:
            notes_idx = all_cols.index("notes")
        except ValueError:
            notes_idx = 6
        wave_cols = all_cols[notes_idx + 1:]
        element_col = all_cols[1] if len(all_cols) > 1 else "element"
        q_item_col = all_cols[2] if len(all_cols) > 2 else "question_item"

        wave_sel = ", ".join(f'"{c}"' for c in wave_cols)
        rows_md = con.execute(
            f'SELECT "{q_item_col}", {wave_sel} '
            f'FROM main_safe.ref_safe__annex '
            f"WHERE \"{element_col}\" = 'question'"
        ).fetchall()

        texts: dict[str, str] = {}
        for row in rows_md:
            q_id = (row[0] or "").strip().lower()
            if not q_id:
                continue
            text = next((v for v in row[1:] if v and v.strip()), "")
            if text and q_id not in texts:
                texts[q_id] = _clean_question_text(text)

        print(f"  Loaded {len(texts)} question texts from MotherDuck annex table")
        return texts
    except Exception as exc:
        print(f"  Warning: MotherDuck annex table unavailable ({exc})")
        return {}


def build_annex_html(con=None) -> str:
    # Read annex from MotherDuck ref_safe__annex.
    q_texts: dict[str, tuple[str, str]] = {}  # q_id -> (sample, text)

    if con is None:
        return ""
    try:
        cols_res = con.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = 'main_safe' AND table_name = 'ref_safe__annex' "
            "ORDER BY ordinal_position"
        ).fetchall()
        if cols_res:
            all_cols = [r[0] for r in cols_res]
            try:
                notes_idx = all_cols.index("notes")
            except ValueError:
                notes_idx = 6
            wave_cols = all_cols[notes_idx + 1:]
            element_col = all_cols[1] if len(all_cols) > 1 else "element"
            q_item_col = all_cols[2] if len(all_cols) > 2 else "question_item"
            sample_col = all_cols[4] if len(all_cols) > 4 else "sample"
            wave_sel = ", ".join(f'"{c}"' for c in wave_cols)
            rows_md = con.execute(
                f'SELECT "{q_item_col}", "{sample_col}", {wave_sel} '
                f"FROM main_safe.ref_safe__annex "
                f"WHERE \"{element_col}\" = 'question'"
            ).fetchall()
            for row in rows_md:
                q_id_raw = (row[0] or "").strip()
                sample = (row[1] or "").strip()
                matched = next((k for k in ANNEX_Q_IDS if k.lower() == q_id_raw.lower()), None)
                if matched and matched not in q_texts:
                    text = next((v for v in row[2:] if v and v.strip()), "")
                    if text:
                        q_texts[matched] = (sample, _clean_question_text(text.strip()))
    except Exception as exc:
        print(f"  Warning: MotherDuck annex table unavailable for HTML widget ({exc})")
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
# 6. Slovak translation pass (translate-after-render, no re-querying)
# ---------------------------------------------------------------------------

def translate_to_slovak(
    rendered: list[dict],
    exec_bullets: list[dict],
    cost_tracker: dict,
) -> tuple[list[dict], list[dict]]:
    # Send only bullet text for translation; section_id is restored afterwards
    exec_bullet_texts = [item.get("bullet", "") for item in exec_bullets]
    payload = {
        "exec_bullets": exec_bullet_texts,
        "sections": [
            {"id": s["section_id"], "finding": s["finding"], "bullets": s["bullets"]}
            for s in rendered
        ],
    }
    prompt = (
        "Translate the following ECB SAFE survey report content to Slovak. "
        "Keep all numbers, percentages, and proper nouns (Slovakia, Euro Area, Germany, ECB, "
        "SAFE) unchanged. Use formal economic Slovak (not colloquial). "
        "Return valid JSON only — no markdown fences — with exactly the same structure as the input.\n\n"
        + json.dumps(payload, ensure_ascii=False)
    )
    client = _mistral_client()
    resp = client.chat.complete(
        model="mistral-small-latest",
        max_tokens=4000,
        messages=[{"role": "user", "content": prompt}],
    )
    if resp.usage:
        _track_cost(cost_tracker, "mistral-small-latest",
                    _Usage(resp.usage.prompt_tokens, resp.usage.completion_tokens))

    raw = resp.choices[0].message.content.strip()
    if raw.startswith("```"):
        raw = raw.split("\n", 1)[-1].rsplit("```", 1)[0].strip()
    try:
        translated = json.loads(repair_json(raw))
    except Exception:
        print("  [SK] Translation JSON parse failed — falling back to English content")
        return rendered, exec_bullets

    sk_rendered = []
    by_id = {s["id"]: s for s in translated.get("sections", [])}
    for s in rendered:
        t = by_id.get(s["section_id"], {})
        sk_rendered.append({
            **s,
            "finding": t.get("finding", s["finding"]),
            "bullets": t.get("bullets", s["bullets"]),
        })

    # Restore section_id on translated exec bullets
    sk_bullet_texts = translated.get("exec_bullets", exec_bullet_texts)
    sk_exec_bullets = [
        {"bullet": str(text), "section_id": orig.get("section_id", "")}
        for text, orig in zip(sk_bullet_texts, exec_bullets)
    ]
    return sk_rendered, sk_exec_bullets


# ---------------------------------------------------------------------------
# 7. TOC
# ---------------------------------------------------------------------------

# Canonical group order for rendering
GROUP_ORDER = ["Financing Conditions", "Economic Situation of Firms"]


def build_toc(rendered_sections: list[dict], ui: dict | None = None) -> str:
    _ui = ui or {}
    group_labels = {
        "Financing Conditions":        _ui.get("group_financing", "Financing Conditions"),
        "Economic Situation of Firms": _ui.get("group_economic",  "Economic Situation of Firms"),
    }
    toc_title = _ui.get("toc_title", "Contents")

    by_group: dict[str, list[dict]] = {}
    for s in rendered_sections:
        g = s.get("group", "Other")
        by_group.setdefault(g, []).append(s)

    items = []
    for group in GROUP_ORDER:
        secs = by_group.get(group, [])
        if not secs:
            continue
        label = group_labels.get(group, group)
        inner = "\n".join(
            f'        <li><a href="#{s["section_id"]}">{s["finding"]}</a></li>'
            for s in secs
        )
        items.append(f"    <li><strong>{label}</strong>\n      <ul>\n{inner}\n      </ul>\n    </li>")

    # Add adhoc spotlight link if present
    adhoc_s = next((s for s in rendered_sections if s.get("section_id") == "adhoc_spotlight"), None)
    if adhoc_s:
        theme_label = adhoc_s.get("theme_label", "Special Focus")
        items.append(
            f'    <li><a href="#adhoc_spotlight">⭐ Special Focus: {theme_label}</a></li>'
        )

    if not items:
        return ""
    rows = "\n".join(items)
    return textwrap.dedent(f"""
<nav id="toc">
  <p class="toc-title">{toc_title}</p>
  <ul>
{rows}
  </ul>
</nav>
""").strip()


# ---------------------------------------------------------------------------
# 8. Build HTML
# ---------------------------------------------------------------------------

HTML_PAGE = textwrap.dedent("""
<!DOCTYPE html>
<html lang="{lang}">
<head>
<meta charset="UTF-8">
<title>{title_str}</title>
<style>
  /* NBS brand: Sitka Banner for headings, Arial for body */
  body        {{ font-family: Arial, sans-serif; background: #f4f4f4; color: #231f20;
                 max-width: 1200px; margin: 40px auto; padding: 0 24px; }}
  h1          {{ font-family: "Sitka Banner", "Sitka Text", Georgia, serif;
                 font-size: 26px; font-weight: bold; margin-bottom: 4px; color: #2B5291; }}
  .meta       {{ color: #6a6a6a; font-size: 13px; margin-bottom: 20px; }}
  section     {{ background: #fff; border: 1px solid #D2DBE0; border-radius: 6px;
                 padding: 24px 28px; margin-bottom: 20px; }}
  h2          {{ font-family: "Sitka Banner", "Sitka Text", Georgia, serif;
                 font-size: 18px; font-weight: bold; margin: 36px 0 12px 0; color: #2B5291;
                 border-bottom: 2px solid #2B5291; padding-bottom: 6px; }}
  h3          {{ font-family: "Sitka Banner", "Sitka Text", Georgia, serif;
                 font-size: 15px; font-weight: bold; margin: 0 0 4px 0; color: #231f20; }}
  .section-subtitle {{ font-size: 11px; color: #888; margin: 0 0 12px 0; }}
  ul          {{ padding-left: 20px; margin: 0 0 16px 0; }}
  li          {{ margin-bottom: 6px; font-size: 13.5px; line-height: 1.5; }}
  img         {{ width: 100%; margin-top: 8px; }}
  .footnote   {{ font-size: 11px; color: #888; margin-top: 10px; line-height: 1.4; }}
  .footer     {{ color: #adadad; font-size: 11px; margin-top: 32px; text-align: center; }}
  .lang-switch {{ float: right; font-size: 12px; color: #2B5291; text-decoration: none;
                  border: 1px solid #2B5291; border-radius: 4px; padding: 2px 8px;
                  margin-top: 4px; }}
  .lang-switch:hover {{ background: #eef2f9; }}

  /* Exec summary + painting flexbox */
  .exec-flex     {{ display: flex; gap: 24px; align-items: flex-start; margin-bottom: 20px; }}
  .exec-painting {{ flex: 1; min-width: 0; }}
  .exec-summary  {{ flex: 3; min-width: 0; background: #eef2f9;
                    border-left: 4px solid #2B5291; padding: 20px 24px; border-radius: 6px; }}
  .exec-summary h2 {{ font-family: "Sitka Banner", "Sitka Text", Georgia, serif;
                      font-size: 16px; color: #2B5291; border-bottom: none;
                      margin: 0 0 10px 0; padding-bottom: 0; }}
  .exec-summary li {{ font-size: 14px; line-height: 1.7; }}
  .exec-summary li a {{ color: inherit; text-decoration: underline dotted #7a9dc4; }}
  .exec-summary li a:hover {{ text-decoration: underline; color: #2B5291; }}

  /* TOC */
  #toc        {{ background: #fff; border: 1px solid #D2DBE0; border-radius: 6px;
                 padding: 16px 24px; margin-bottom: 20px; font-size: 13px; }}
  .toc-title  {{ font-family: "Sitka Banner", "Sitka Text", Georgia, serif;
                 font-weight: bold; margin: 0 0 8px 0; color: #2B5291; font-size: 13px; }}
  #toc ul     {{ margin: 4px 0; padding-left: 18px; }}
  #toc li     {{ margin-bottom: 3px; }}
  #toc a      {{ color: #0086DE; text-decoration: none; }}
  #toc a:hover {{ text-decoration: underline; }}

  /* Collapsible annex */
  details     {{ background: #fff; border: 1px solid #D2DBE0; border-radius: 6px;
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
  .badge-ecb              {{ background: #eef2f9; color: #2B5291; padding: 1px 6px;
                             border-radius: 3px; font-size: 11px; }}
</style>
</head>
<body>
{lang_switch}<h1>{h1_str}</h1>
<p class="meta">{meta_str}</p>
{annex}
{exec_flex}
{toc}
{sections}
<p class="footer">{footer_str}</p>
</body>
</html>
""").strip()

SECTION_TMPL = textwrap.dedent("""
<section id="{section_id}">
  <h3>{finding}</h3>
  <p class="section-subtitle">{title}</p>
  <ul>
{bullets}
  </ul>
{footnote}{agentic_footnote}  <img src="data:image/png;base64,{chart_b64}" alt="{title} chart">
</section>
""").strip()

_AGENTIC_FOOTNOTE = (
    '<p class="footnote">🤖 This section includes data retrieved by an AI agent '
    'querying the SAFE database directly during report generation.</p>\n'
)


def build_html(
    rendered_sections: list[dict],
    annex_html: str,
    exec_bullets: list[dict],
    toc_html: str,
    painting_inner_html: str = "",
    latest_wave: int = 0,
    ui: dict | None = None,
) -> str:
    _ui = ui or {}
    today = date.today().strftime("%d %b %Y")
    wave_str = str(latest_wave)

    group_labels = {
        "Financing Conditions":        _ui.get("group_financing", "Financing Conditions"),
        "Economic Situation of Firms": _ui.get("group_economic",  "Economic Situation of Firms"),
    }
    fn_routed   = _ui.get("footnote_routed",   ROUTED_FOOTNOTE)
    fn_missing  = _ui.get("footnote_missing",  MISSINGNESS_FOOTNOTE)
    fn_agentic  = _ui.get("footnote_agentic",  _AGENTIC_FOOTNOTE)

    # Separate adhoc spotlight from regular sections
    adhoc_s = next((s for s in rendered_sections if s.get("section_id") == "adhoc_spotlight"), None)
    regular_sections = [s for s in rendered_sections if s.get("section_id") != "adhoc_spotlight"]

    # Group sections and emit h2 group headings between them
    by_group: dict[str, list[dict]] = {}
    for s in regular_sections:
        g = s.get("group", "Other")
        by_group.setdefault(g, []).append(s)

    sections_parts = []
    for group in GROUP_ORDER:
        secs = by_group.get(group, [])
        if not secs:
            continue
        sections_parts.append(f"<h2>{group_labels.get(group, group)}</h2>")
        for s in secs:
            chart_html = (
                f'  <img src="data:image/png;base64,{base64.b64encode(s["chart_png"]).decode()}" alt="{s["title"]} chart">'
                if s.get("chart_png") else ""
            )
            sections_parts.append(
                SECTION_TMPL.format(
                    section_id=s["section_id"],
                    finding=s["finding"],
                    title=s["title"],
                    bullets="\n".join(f"    <li>{b.lstrip('• ').strip()}</li>" for b in s["bullets"]),
                    footnote=(
                        (fn_routed + "\n" if s.get("routed") else "") +
                        (fn_missing + "\n" if s.get("has_missingness_caveat") else "")
                    ),
                    agentic_footnote=fn_agentic if s.get("tool_calls", 0) > 0 else "",
                    chart_b64=base64.b64encode(s["chart_png"]).decode() if s.get("chart_png") else "",
                )
            )

    # Adhoc spotlight section (appended after all groups, collapsible)
    if adhoc_s:
        ecb_link_html = ""
        if adhoc_s.get("ecb_article_url"):
            ecb_link_html = (
                f'  <p class="footnote">Read more: '
                f'<a href="{adhoc_s["ecb_article_url"]}" target="_blank" rel="noopener">'
                f'ECB Economic Bulletin focus article</a></p>\n'
            )
        theme_label = adhoc_s.get("theme_label", "Special Focus")
        spotlight_bullets = "\n".join(
            f"    <li>{b.lstrip('• ').strip()}</li>" for b in adhoc_s.get("bullets", [])
        )
        adhoc_chart_html = ""
        if adhoc_s.get("chart_png"):
            chart_b64 = base64.b64encode(adhoc_s["chart_png"]).decode()
            adhoc_chart_html = (
                f'<img src="data:image/png;base64,{chart_b64}" '
                f'alt="{theme_label} chart" style="max-width:100%;margin:10px 0 16px;">\n'
            )
        spotlight_html = textwrap.dedent(f"""
            <details id="adhoc_spotlight" data-theme="{theme_label}" open>
              <summary>
                <h2>Special Focus: {theme_label}</h2>
              </summary>
              <section>
                <h3>{adhoc_s['finding']}</h3>
                <p class="section-subtitle">{adhoc_s['title']}</p>
            {adhoc_chart_html}    <ul>
            {spotlight_bullets}
                </ul>
            {ecb_link_html}  </section>
            </details>
        """).strip()
        sections_parts.append(spotlight_html)

    # Build exec-flex: painting (1) + exec summary (3)
    exec_h2 = _ui.get("exec_h2", "Executive Summary")
    painting_slot = (
        f'<div class="exec-painting">{painting_inner_html}</div>'
        if painting_inner_html else ""
    )
    # exec_bullets is list[dict{bullet, section_id}]; render with anchor links where available
    exec_bullet_items = []
    for item in exec_bullets:
        if isinstance(item, dict):
            text = item.get("bullet", "").lstrip("• ").strip()
            sid = item.get("section_id", "").strip()
        else:
            text = str(item).lstrip("• ").strip()
            sid = ""
        if not text:
            continue
        # Convert **bold** markdown to <strong> HTML
        text = re.sub(r'\*\*(.+?)\*\*\s*', lambda m: f'<strong>{m.group(1)}</strong>' + (' ' if m.group(1).endswith(':') else ''), text)
        if sid:
            exec_bullet_items.append(f'    <li><a href="#{sid}">{text}</a></li>')
        else:
            exec_bullet_items.append(f"    <li>{text}</li>")
    exec_bullets_html = "\n".join(exec_bullet_items)
    exec_summary_div = (
        f'<div class="exec-summary" id="exec-summary">\n'
        f'  <h2>{exec_h2}</h2>\n'
        f'  <ul>\n{exec_bullets_html}\n  </ul>\n'
        f'</div>'
    ) if exec_bullets else ""
    exec_flex = (
        f'<div class="exec-flex">{painting_slot}{exec_summary_div}</div>'
        if (painting_slot or exec_summary_div) else ""
    )

    is_slovak = _ui.get("lang", "en") == "sk"
    if is_slovak:
        lang_switch = '<a class="lang-switch" href="index.html">🇬🇧 EN</a>\n'
    else:
        lang_switch = '<a class="lang-switch" href="sk.html">🇸🇰 SK</a>\n'

    return HTML_PAGE.format(
        lang=_ui.get("lang", "en"),
        lang_switch=lang_switch,
        title_str=_ui.get("title", "ECB SAFE Survey — Wave {wave} · Slovakia").format(wave=wave_str),
        h1_str=_ui.get("h1", "ECB SAFE Survey — Wave {wave} · Slovakia").format(wave=wave_str),
        meta_str=_ui.get("meta", "Slovakia · Euro Area · Germany &nbsp;|&nbsp; Generated {date}").format(date=today),
        footer_str=_ui.get(
            "footer",
            "Source: ECB SAFE microdata. Net balance = % reporting increase minus % reporting decrease. "
            "Positive = tightening/rising (adverse for firms unless noted). Negative = easing/falling."
        ),
        annex=annex_html,
        exec_flex=exec_flex,
        toc=toc_html,
        sections="\n\n".join(sections_parts),
    )


# ---------------------------------------------------------------------------
# 8. ECB sharpener — post-generation bullet pass
# ---------------------------------------------------------------------------

ECB_SAFE_INDEX = "https://www.ecb.europa.eu/stats/ecb_surveys/safe/html/index.en.html"
ECB_BASE = "https://www.ecb.europa.eu"
_ECB_SHARPEN_MAX_CHARS = 8_000


def _fetch_ecb_context() -> tuple[str, str]:
    """Fetch the latest ECB SAFE publication and return (url, plain_text).

    Returns ("", "") silently on any failure — the sharpener is optional.
    """
    import urllib.request
    try:
        req = urllib.request.Request(ECB_SAFE_INDEX, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=15) as r:
            index_html = r.read().decode("utf-8", errors="replace")
        m = re.search(r'href="(/stats/ecb_surveys/safe/html/ecb\.safe\d{6}\.en\.html)"',
                      index_html)
        if not m:
            return "", ""
        ecb_url = ECB_BASE + m.group(1)
        req2 = urllib.request.Request(ecb_url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req2, timeout=20) as r:
            page_html = r.read().decode("utf-8", errors="replace")
        text = re.sub(r"<[^>]+>", " ", page_html)
        text = re.sub(r"\s+", " ", text).strip()
        return ecb_url, text[:_ECB_SHARPEN_MAX_CHARS]
    except Exception:
        return "", ""


ECB_SHARPEN_SYSTEM = textwrap.dedent("""
    You are an editorial analyst sharpening a Slovakia SAFE survey report against the
    ECB's own published findings for the same wave.

    You will receive:
    1. Section-by-section findings and bullets from the automated report
    2. The ECB publication text

    Your task: revise bullets where a specific improvement is possible — a sharper EA
    comparison supported by the ECB text, a missed context point the ECB highlights, or
    more precise language matching the ECB's own framing.

    Rules:
    - Only revise bullets where improvement is directly supported by the ECB text provided.
      Do NOT invent numbers or comparisons not in the ECB text.
    - Preserve all existing numbers and sign conventions.
    - Keep bullets ≤ 35 words. Do not change the finding headline.
    - Return JSON only — no markdown fences:
      {"section_id": {"finding": "unchanged headline", "bullets": ["revised..."]}, ...}
      Include ONLY sections where you actually changed at least one bullet.
      If no improvements are possible, return an empty JSON object: {}
""").strip()


def _sharpen_with_ecb(
    rendered: list[dict],
    ecb_text: str,
    mistral_client,
    cost_tracker: dict,
) -> list[dict]:
    """Post-generation pass: sharpen bullets against ECB publication. Returns rendered."""
    if not ecb_text or not rendered:
        return rendered
    sections_text = "\n\n".join(
        f"### {r['section_id']}\nFinding: {r['finding']}\n"
        + "\n".join(f"- {b}" for b in r["bullets"])
        for r in rendered
    )
    user_msg = (
        f"## OUR REPORT SECTIONS\n\n{sections_text}\n\n"
        f"---\n\n## ECB PUBLICATION TEXT\n\n{ecb_text}"
    )
    try:
        resp = mistral_client.chat.complete(
            model="mistral-small-latest",
            max_tokens=1500,
            messages=[
                {"role": "system", "content": ECB_SHARPEN_SYSTEM},
                {"role": "user", "content": user_msg},
            ],
        )
        if resp.usage:
            _track_cost(cost_tracker, "mistral-small-latest",
                        _Usage(resp.usage.prompt_tokens, resp.usage.completion_tokens))
        raw = resp.choices[0].message.content.strip()
        raw = re.sub(r"^```(?:json)?\s*", "", raw)
        raw = re.sub(r"\s*```$", "", raw).strip()
        revisions: dict = json.loads(repair_json(raw))
        if not revisions:
            print("  ECB sharpener: no improvements identified")
            return rendered
        updated = []
        for r in rendered:
            sid = r["section_id"]
            if sid in revisions:
                rev = revisions[sid]
                new_bullets = rev.get("bullets", [])
                # Only apply if bullet count matches (safety check)
                if new_bullets and len(new_bullets) == len(r["bullets"]):
                    updated.append({**r, "bullets": [str(b).strip() for b in new_bullets]})
                    print(f"  ECB sharpener: revised {sid}")
                else:
                    updated.append(r)
            else:
                updated.append(r)
        return updated
    except Exception as e:
        print(f"  ECB sharpener failed: {e} — keeping original bullets")
        return rendered


# ---------------------------------------------------------------------------
# 9. Main
# ---------------------------------------------------------------------------

def main() -> None:
    from datetime import datetime as _dt
    _run_start = _dt.now()

    parser = argparse.ArgumentParser()
    parser.add_argument("--dev", action="store_true",
                        help="Use local dev.duckdb instead of MotherDuck (no MOTHERDUCK_TOKEN needed)")
    parser.add_argument("--wave", type=int, default=None,
                        help="Cap data at this wave number for retrospective reports (e.g. --wave 37)")
    args = parser.parse_args()

    if args.dev:
        print(f"[DEV] Using local DuckDB: {DEV_DB_PATH}")
    else:
        print("[PROD] Using MotherDuck")

    print("Fetching data for all sections...")
    data = fetch_all(dev=args.dev)

    # Retrospective mode: drop rows from waves after the target wave
    if args.wave is not None:
        print(f"  [RETROSPECTIVE] Capping data at wave {args.wave}")
        data = {sid: df[df["wave_number"] <= args.wave].copy() for sid, df in data.items()}

    for sid, df in data.items():
        print(f"  {sid}: {len(df)} rows")
    latest_wave = int(max(df["wave_number"].max() for df in data.values()))
    print(f"  Latest wave: {latest_wave}")

    cost_tracker = {"input_tokens": 0, "output_tokens": 0, "usd": 0.0, "calls": 0, "by_model": {}}

    print("Running interest checks (parallel)...")
    interest = check_all_interest(SECTIONS, data, cost_tracker)
    for sid, r in interest.items():
        flag = "✓" if r["interesting"] else "✗"
        print(f"  {flag} {sid}: {r['reason']} [chart={r['chart_type']}, best_panel={r['best_panel']}]")

    schema = DEV_SCHEMA if args.dev else PROD_SCHEMA
    tool_con = _get_connection(args.dev)

    # Build historical context block injected into every section system prompt.
    # Sources: (1) wave memory from MotherDuck, (2) interpretation notes from prior gap analysis.
    historical_context = ""
    if not args.dev:
        try:
            rows = tool_con.execute("""
                SELECT wave_number, notable_summary
                FROM main_safe.ref_safe__wave_memory
                ORDER BY wave_number DESC LIMIT 3
            """).fetchall()
            if rows:
                historical_context = (
                    "\n\n## Historical context (prior waves — for trend awareness only)\n"
                    + "\n".join(f"  Wave {r[0]}: {r[1]}" for r in rows)
                )
        except Exception:
            pass
        interp_path = Path(__file__).parent / "output" / "interpretation_context.md"
        if interp_path.exists():
            interp_text = interp_path.read_text().strip()
            if interp_text:
                historical_context += (
                    "\n\n## Interpretation notes from prior gap analysis\n" + interp_text
                )
        if historical_context:
            print(f"  Loaded historical context ({len(historical_context)} chars)")

    ecb_url, ecb_context = "", ""
    if not args.dev:
        print("Fetching ECB publication for sharpener pass...")
        ecb_url, ecb_context = _fetch_ecb_context()
        if ecb_context:
            print(f"  Fetched {len(ecb_context):,} chars from {ecb_url}")
        else:
            print("  ECB fetch failed or unavailable — sharpener pass skipped")

    print("Building mart schema catalogue...")
    mart_catalogue = build_mart_catalogue(tool_con, schema)

    print("Loading annex question texts...")
    question_texts = _load_annex_question_texts(con=tool_con)
    print(f"  Loaded {len(question_texts)} question texts from annex")

    interesting_sections = [s for s in SECTIONS if interest[s["id"]]["interesting"]]
    skipped = [s["id"] for s in SECTIONS if not interest[s["id"]]["interesting"]]
    for sid in skipped:
        print(f"  Skipping {sid} (not interesting)")

    anthropic_client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
    mistral_client = _mistral_client()
    cost_lock = threading.Lock()

    def _build_section(sec: dict) -> dict:
        sid = sec["id"]
        r = interest[sid]
        # Each thread needs its own DuckDB connection — connections aren't thread-safe
        thread_con = _get_connection(args.dev)
        try:
            print(f"  Building chart for {sid}...")
            if sid == "financing_gap":
                chart_png = build_financing_gap_chart(sec, data[sid])
            elif sid == "bank_loan_terms":
                # Q10: not all terms are asked every wave → interrupted lines look broken.
                # Bar chart (latest wave only) is cleaner and still shows SK vs EA vs DE.
                chart_png = build_chart(sec, data[sid], "bar", r["best_panel"])
            else:
                chart_png = build_chart(sec, data[sid], r["chart_type"], r["best_panel"])

            # Thread-local cost accumulator — merged under lock after the call
            local_tracker = {"input_tokens": 0, "output_tokens": 0, "usd": 0.0, "calls": 0, "by_model": {}}
            print(f"  Generating finding + bullets for {sid}...")
            content = get_section_content_agentic(
                sec, data[sid], thread_con, schema, mart_catalogue,
                local_tracker, question_texts, client=anthropic_client,
                historical_context=historical_context,
            )

            # "So what?" pass — adds implication clauses to purely-descriptive bullets
            print(f"  Adding implications for {sid}...")
            content = _add_so_what(content, sec, mistral_client, local_tracker)

            with cost_lock:
                cost_tracker["input_tokens"] += local_tracker["input_tokens"]
                cost_tracker["output_tokens"] += local_tracker["output_tokens"]
                cost_tracker["usd"] += local_tracker["usd"]
                cost_tracker["calls"] += local_tracker["calls"]
                for model, m in local_tracker["by_model"].items():
                    bm = cost_tracker["by_model"].setdefault(model, {"calls": 0, "input": 0, "output": 0, "usd": 0.0, "cache_write": 0, "cache_read": 0})
                    for k in bm:
                        bm[k] += m.get(k, 0)

            print(f"    [{sid}] finding: {content['finding']}")
            for b in content["bullets"]:
                print(f"    [{sid}] {b}")

            return {
                "section_id": sid,
                "title": sec["title"],
                "group": sec.get("group", "Other"),
                "finding": content["finding"],
                "bullets": content["bullets"],
                "chart_png": chart_png,
                "sign_note": sec["sign_note"],
                "routed": sec.get("routed", False),
                "has_missingness_caveat": sec.get("has_missingness_caveat", False),
                "tool_calls": content.get("tool_calls", 0),
            }
        finally:
            thread_con.close()

    print(f"Generating {len(interesting_sections)} sections (parallel)...")
    rendered_map: dict[str, dict] = {}
    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = {pool.submit(_build_section, sec): sec["id"] for sec in interesting_sections}
        for future in as_completed(futures):
            result = future.result()
            rendered_map[result["section_id"]] = result

    # Restore original SECTIONS order
    rendered = [rendered_map[s["id"]] for s in SECTIONS if s["id"] in rendered_map]

    if ecb_context:
        print("Sharpening bullets against ECB publication...")
        rendered = _sharpen_with_ecb(rendered, ecb_context, mistral_client, cost_tracker)

    # Adhoc spotlight — detect theme, generate section, find ECB article
    # Built BEFORE exec summary so Mistral can synthesise it as a peer section
    adhoc_section: dict | None = None
    adhoc_ecb_url: str | None = None
    print("Checking for adhoc module spotlight...")
    adhoc_theme = detect_adhoc_theme(latest_wave, tool_con, schema, mistral_client, cost_tracker)
    if adhoc_theme:
        print(f"  Adhoc theme detected: {adhoc_theme['theme_label']} ({adhoc_theme['module_id']})")
        adhoc_section = build_adhoc_spotlight(
            adhoc_theme, latest_wave, tool_con, schema, mistral_client, cost_tracker,
            anthropic_client=anthropic_client,
        )
        if adhoc_section:
            rendered.append(adhoc_section)
            print(f"  Adhoc spotlight generated: {adhoc_section['finding']}")
            if not args.dev:
                adhoc_ecb_url = _find_ecb_focus_article(
                    adhoc_theme["theme_label"], mistral_client, cost_tracker
                )
                if adhoc_ecb_url:
                    adhoc_section["ecb_article_url"] = adhoc_ecb_url
                    print(f"  ECB focus article: {adhoc_ecb_url}")
    else:
        print("  No adhoc module data for this wave.")

    print("Generating executive summary (two-pass)...")
    exec_bullets = get_exec_summary(rendered, cost_tracker) if rendered else []
    for item in exec_bullets:
        print(f"  [{item.get('section_id', '?')}] {item.get('bullet', '')}")

    if not args.dev and exec_bullets and rendered:
        print("Writing wave memory...")
        _write_wave_memory(latest_wave, exec_bullets, rendered,
                           mistral_client, tool_con, cost_tracker)

    print("Building TOC...")
    toc_html = build_toc(rendered)

    print("Building question annex...")
    annex_html = build_annex_html(con=tool_con)
    tool_con.close()

    print("Fetching painting thumbnail...")
    painting_inner_html = _fetch_painting_inner_html()

    print("Assembling HTML (EN)...")
    html = build_html(rendered, annex_html, exec_bullets, toc_html, painting_inner_html, latest_wave)

    retro = args.wave is not None
    out_path = OUTPUT_DIR / (f"report_q{latest_wave}.html" if retro else "report_latest.html")
    out_path.write_text(html, encoding="utf-8")
    print(f"Saved → {out_path}")

    print("Translating to Slovak...")
    sk_rendered, sk_exec_bullets = translate_to_slovak(rendered, exec_bullets, cost_tracker)
    sk_toc_html = build_toc(sk_rendered, ui=_SK_UI)
    sk_html = build_html(sk_rendered, annex_html, sk_exec_bullets, sk_toc_html,
                         painting_inner_html, latest_wave, ui=_SK_UI)
    sk_path = OUTPUT_DIR / (f"report_q{latest_wave}_sk.html" if retro else "report_latest_sk.html")
    sk_path.write_text(sk_html, encoding="utf-8")
    print(f"Saved → {sk_path}")

    w = 54
    print(f"\n{'─' * w}\nRun cost estimate")
    for model, m in sorted(cost_tracker["by_model"].items()):
        cache_note = ""
        if m.get("cache_write") or m.get("cache_read"):
            cache_note = f"  (cache write={m['cache_write']:,} read={m['cache_read']:,})"
        print(f"  {model:<28} {m['calls']:>3} calls  "
              f"{m['input']:>7,} in  {m['output']:>5,} out  ${m['usd']:.4f}{cache_note}")
    print(f"  {'─' * (w - 2)}")
    print(f"  {'Total':<28} {cost_tracker['calls']:>3} calls  "
          f"{cost_tracker['input_tokens']:>7,} in  {cost_tracker['output_tokens']:>5,} out  "
          f"${cost_tracker['usd']:.4f}")
    print(f"{'─' * w}")

    cache_read_total = sum(m.get("cache_read", 0) for m in cost_tracker["by_model"].values())
    (OUTPUT_DIR / "cost_tracker.json").write_text(json.dumps({
        "wave_number": latest_wave,
        "total_cost_usd": round(cost_tracker["usd"], 5),
        "input_tokens": cost_tracker["input_tokens"],
        "output_tokens": cost_tracker["output_tokens"],
        "cache_read_tokens": cache_read_total,
        "model_sonnet": "claude-sonnet-4-6",
        "model_mistral": "mistral-small-latest",
        "n_sections": len(rendered),
        "duration_seconds": round((_dt.now() - _run_start).total_seconds(), 1),
    }))
    print("Done.")


if __name__ == "__main__":
    main()
