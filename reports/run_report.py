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
import re
import textwrap
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
from pathlib import Path

import yaml
import anthropic
import duckdb
from dotenv import load_dotenv
from mistralai import Mistral
import matplotlib
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
import pandas as pd

matplotlib.use("Agg")
load_dotenv(Path(__file__).parent.parent / ".env")

from config import SECTIONS  # noqa: E402  (local import after matplotlib setup)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

SQL_DIR = Path(__file__).parent / "sql"
OUTPUT_DIR = Path(__file__).parent / "output"
ANNEX_CSV = Path(__file__).parent.parent / "collateral" / "annex.csv"
MARTS_SCHEMA_YML = Path(__file__).parent.parent / "dbt_project" / "models" / "marts" / "schema.yml"
OUTPUT_DIR.mkdir(exist_ok=True)

COUNTRIES = {"SK": "Slovakia", "EA": "Euro Area", "DE": "Germany"}
COUNTRY_COLORS = {"SK": "#bd4e35", "EA": "#0777b3", "DE": "#e18727"}
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
        result = json.loads(raw)
    except json.JSONDecodeError:
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
        .drop_duplicates().sort_values("wave_number")
        .set_index("wave_number")["survey_period_label"]
    )

    fig, ax = plt.subplots(1, 1, figsize=(9, 4.5))
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
        light_color = tuple(min(1.0, v + 0.35) for v in rgb)

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
        line, = ax.plot(x_pts, gap_vals, color=base_color, linewidth=2.2,
                        marker="D", markersize=5, linestyle="--", zorder=3)
        line_handles.append(line)
        line_labels_leg.append(f"{COUNTRIES[country]} — gap")

    ax.axhline(0, color="#adadad", linewidth=0.8, linestyle="--", zorder=1)
    ax.set_xticks([i * group_gap for i in range(len(waves))])
    ax.set_xticklabels([wave_labels[w] for w in waves], rotation=35, ha="right", fontsize=8)
    ax.yaxis.set_major_formatter(mticker.FormatStrFormatter("%+.0f"))
    ax.tick_params(axis="y", labelsize=8)
    ax.set_ylabel("Net balance (pp)", fontsize=7, color="#6a6a6a")
    ax.set_title(f"{label_val} — need (solid bars) vs availability (hatched); gap (dashed)", fontsize=9)
    ax.spines[["top", "right"]].set_visible(False)
    ax.set_facecolor("#f8f8f8")
    fig.patch.set_facecolor("#f8f8f8")
    fig.legend(bar_handles + line_handles, bar_labels_leg + line_labels_leg,
               loc="lower center", bbox_to_anchor=(0.5, 0.0), ncol=3, fontsize=7.5, frameon=False)

    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=150, bbox_inches="tight")
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
    Chart 2: financing gap (need − availability) for Slovakia by instrument, line chart.
    """
    waves = sorted(df_sk["wave_number"].unique())
    wave_labels = (
        df_sk[["wave_number", "survey_period_label"]]
        .drop_duplicates().sort_values("wave_number")
        .set_index("wave_number")["survey_period_label"]
    )

    fig, ax = plt.subplots(1, 1, figsize=(7, 4.2))
    fig.subplots_adjust(top=0.84, bottom=0.22, left=0.1, right=0.97)

    handles, labels = [], []
    for sub_item, color in INSTRUMENT_COLORS.items():
        idf = df_sk[df_sk["sub_item"] == sub_item].sort_values("wave_number")
        if idf.empty:
            continue
        label = idf["sub_item_label"].iloc[0] if "sub_item_label" in idf.columns else INSTRUMENT_LABELS.get(sub_item, sub_item)
        line, = ax.plot(idf["wave_number"], idf["financing_gap_wtd"],
                        color=color, linewidth=2, marker="o", markersize=4, label=label)
        handles.append(line)
        labels.append(label)

    ax.axhline(0, color="#adadad", linewidth=0.8, linestyle="--")
    ax.set_xticks(waves)
    ax.set_xticklabels([wave_labels[w] for w in waves], rotation=40, ha="right", fontsize=7)
    ax.yaxis.set_major_formatter(mticker.FormatStrFormatter("%+.0f"))
    ax.tick_params(axis="y", labelsize=8)
    ax.set_ylabel("Financing gap (pp)", fontsize=7, color="#6a6a6a")
    ax.set_title("Slovakia — financing gap by instrument (need − availability)", fontsize=9)
    ax.spines[["top", "right"]].set_visible(False)
    ax.set_facecolor("#f8f8f8")
    fig.patch.set_facecolor("#f8f8f8")
    fig.legend(handles, labels, loc="lower center", bbox_to_anchor=(0.5, 0.0),
               ncol=3, fontsize=7.5, frameon=False)

    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=150, bbox_inches="tight")
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

    def fmt(d: pd.DataFrame, label: str) -> str:
        rows = [f"{label}:"]
        for _, r in d.iterrows():
            n_part = f" | n={r['n_respondents']}" if r.get("country_code") in ("SK", "EA") and "n_respondents" in r else ""
            val_str = f"{r[value_col]:+.2f}" if pd.notna(r[value_col]) else "n/a"
            panel_part = f" | {r[panel_label_col]}" if panel_label_col and panel_label_col in r.index else ""
            rows.append(f"  {r['country_code']}{panel_part} | {value_col}={val_str}{n_part}")
        return "\n".join(rows)

    return fmt(latest, f"Wave {latest_wave} (latest)") + "\n\n" + fmt(prev, f"Wave {prev_wave} (previous)")


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
    system_prompt = SECTION_CONTENT_SYSTEM.format(
        schema_catalogue=mart_catalogue,
        query_templates=MART_QUERY_TEMPLATES,
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

EXEC_SUMMARY_SYSTEM = textwrap.dedent("""
    You are an economist writing an executive summary of the latest ECB SAFE survey results
    for Slovakia. You will be given the key findings from each section of the report.

    Your task: read ALL the section findings and distill them into 5–7 bullet points that
    give a balanced picture of BOTH financing conditions AND the economic situation of firms.
    Do not focus only on financing — business situation, pressingness scores, and economic
    outlook findings are equally important and should appear in the summary.

    Pick the most notable finding from each section. If a finding is unremarkable, skip it.
    If something stands out strongly across sections (e.g. a disconnect between tight credit
    lines and easy bank loans, or rising costs alongside falling turnover), synthesise it
    into a single cross-cutting bullet.

    Style rules:
    - Write narrative statements about direction and change, NOT about numbers.
      Good: "Firms reported a net tightening in interest rates on bank loans."
      Good: "Unchanged needs and a marginal decrease in availability led to a wider financing gap."
      Good: "Turnover and profits declined sharply while labour costs continued to rise."
      Good: "Access to finance was perceived as the least pressing business obstacle."
      Bad: "A net 8.3% of firms reported easing in interest rates."
      Bad: "The net balance improved by 5pp to -7.6pp."
    - Only include a number if it is exceptionally striking and the narrative would be misleading without it.
    - Use plain active voice: "Firms reported...", "Slovak firms perceived...", "Applications declined..."
    - Bullets only — no prose, no headers, no leading bullet character.
""").strip()


def get_exec_summary(rendered_sections: list[dict], cost_tracker: dict) -> list[str]:
    lines = ["Below are the key findings per topic from the latest wave:\n"]
    for s in rendered_sections:
        lines.append(f"## {s['title']}")
        lines.append(f"Sign convention: {s['sign_note']}")
        for b in s["bullets"]:
            lines.append(f"  {b}")
        lines.append("")

    client = _mistral_client()
    resp = client.chat.complete(
        model="mistral-small-latest",
        max_tokens=600,
        messages=[
            {"role": "system", "content": EXEC_SUMMARY_SYSTEM},
            {"role": "user", "content": "\n".join(lines)},
        ],
    )
    if resp.usage:
        _track_cost(cost_tracker, "mistral-small-latest",
                    _Usage(resp.usage.prompt_tokens, resp.usage.completion_tokens))
    raw = resp.choices[0].message.content.strip()
    return [line.strip().lstrip("•- ") for line in raw.splitlines() if line.strip()][:6]


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


def _load_annex_question_texts(
    annex_csv_path: Path, con=None
) -> dict[str, str]:
    """Return {q_id_lower: cleaned_question_text} for all questions.

    Tries MotherDuck table main_safe.ref_safe__annex first (prod).
    Falls back to local annex.csv (dev / offline).
    """
    # --- MotherDuck path ---
    if con is not None:
        try:
            cols_res = con.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = 'main_safe' AND table_name = 'ref_safe__annex' "
                "ORDER BY ordinal_position"
            ).fetchall()
            if cols_res:
                all_cols = [r[0] for r in cols_res]
                # Wave columns: everything after 'notes' (col index 6 → sanitised 'notes')
                # The header row maps: col[1]=element, col[2]=question_item, col[6]=notes,
                # col[7+] = wave columns (SAFE_2024Q1 → safe_2024q1, etc.)
                try:
                    notes_idx = all_cols.index("notes")
                except ValueError:
                    notes_idx = 6
                wave_cols = all_cols[notes_idx + 1:]  # newest is first (leftmost in XLSX)
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
                    # Take the first (most recent) non-empty wave column value
                    text = ""
                    for val in row[1:]:
                        if val and val.strip():
                            text = val.strip()
                            break
                    if text and q_id not in texts:
                        texts[q_id] = _clean_question_text(text)

                if texts:
                    print(f"  Loaded {len(texts)} question texts from MotherDuck annex table")
                    return texts
        except Exception as exc:
            print(f"  Warning: MotherDuck annex table unavailable ({exc}) — falling back to CSV")

    # --- Local CSV fallback ---
    texts = {}
    try:
        with open(annex_csv_path, newline="", encoding="utf-8-sig") as f:
            reader = csv.reader(f)
            rows = list(reader)
        for row in rows[1:]:
            if len(row) > 7 and row[1] == "question":
                q_id = row[2].strip().lower()
                text = row[7].strip()
                if q_id and text and q_id not in texts:
                    texts[q_id] = _clean_question_text(text)
    except FileNotFoundError:
        pass
    return texts


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
# 6. Slovak translation pass (translate-after-render, no re-querying)
# ---------------------------------------------------------------------------

def translate_to_slovak(
    rendered: list[dict],
    exec_bullets: list[str],
    cost_tracker: dict,
) -> tuple[list[dict], list[str]]:
    payload = {
        "exec_bullets": exec_bullets,
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
    # Strip accidental markdown fences
    if raw.startswith("```"):
        raw = raw.split("\n", 1)[-1].rsplit("```", 1)[0].strip()
    try:
        translated = json.loads(raw)
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
    return sk_rendered, translated.get("exec_bullets", exec_bullets)


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
  body        {{ font-family: Arial, sans-serif; background: #f4f4f4; color: #231f20;
                 max-width: 1200px; margin: 40px auto; padding: 0 24px; }}
  h1          {{ font-size: 22px; font-weight: bold; margin-bottom: 4px; }}
  .meta       {{ color: #6a6a6a; font-size: 13px; margin-bottom: 20px; }}
  section     {{ background: #fff; border: 1px solid #e0e0e0; border-radius: 6px;
                 padding: 24px 28px; margin-bottom: 20px; }}
  h2          {{ font-size: 18px; font-weight: bold; margin: 36px 0 12px 0; color: #231f20;
                 border-bottom: 2px solid #0777b3; padding-bottom: 6px; }}
  h3          {{ font-size: 15px; font-weight: bold; margin: 0 0 4px 0; color: #231f20; }}
  .section-subtitle {{ font-size: 11px; color: #888; margin: 0 0 12px 0; }}
  ul          {{ padding-left: 20px; margin: 0 0 16px 0; }}
  li          {{ margin-bottom: 6px; font-size: 13.5px; line-height: 1.5; }}
  img         {{ width: 100%; margin-top: 8px; }}
  .footnote   {{ font-size: 11px; color: #888; margin-top: 10px; line-height: 1.4; }}
  .footer     {{ color: #adadad; font-size: 11px; margin-top: 32px; text-align: center; }}
  .lang-switch {{ float: right; font-size: 12px; color: #0777b3; text-decoration: none;
                  border: 1px solid #0777b3; border-radius: 4px; padding: 2px 8px;
                  margin-top: 4px; }}
  .lang-switch:hover {{ background: #eef4fb; }}

  /* Exec summary + painting flexbox */
  .exec-flex     {{ display: flex; gap: 24px; align-items: flex-start; margin-bottom: 20px; }}
  .exec-painting {{ flex: 1; min-width: 0; }}
  .exec-summary  {{ flex: 3; min-width: 0; background: #eef4fb;
                    border-left: 4px solid #0777b3; padding: 20px 24px; border-radius: 6px; }}
  .exec-summary h2 {{ font-size: 16px; color: #0777b3; border-bottom: none;
                      margin: 0 0 10px 0; padding-bottom: 0; }}
  .exec-summary li {{ font-size: 14px; line-height: 1.7; }}

  /* TOC */
  #toc        {{ background: #fff; border: 1px solid #e0e0e0; border-radius: 6px;
                 padding: 16px 24px; margin-bottom: 20px; font-size: 13px; }}
  .toc-title  {{ font-weight: bold; margin: 0 0 8px 0; color: #231f20; font-size: 13px; }}
  #toc ul     {{ margin: 4px 0; padding-left: 18px; }}
  #toc li     {{ margin-bottom: 3px; }}
  #toc a      {{ color: #0777b3; text-decoration: none; }}
  #toc a:hover {{ text-decoration: underline; }}

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
    exec_bullets: list[str],
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

    # Group sections and emit h2 group headings between them
    by_group: dict[str, list[dict]] = {}
    for s in rendered_sections:
        g = s.get("group", "Other")
        by_group.setdefault(g, []).append(s)

    sections_parts = []
    for group in GROUP_ORDER:
        secs = by_group.get(group, [])
        if not secs:
            continue
        sections_parts.append(f"<h2>{group_labels.get(group, group)}</h2>")
        for s in secs:
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
                    chart_b64=base64.b64encode(s["chart_png"]).decode(),
                )
            )

    # Build exec-flex: painting (1) + exec summary (3)
    exec_h2 = _ui.get("exec_h2", "Executive Summary")
    painting_slot = (
        f'<div class="exec-painting">{painting_inner_html}</div>'
        if painting_inner_html else ""
    )
    exec_bullets_html = "\n".join(
        f"    <li>{b.lstrip('• ').strip()}</li>" for b in exec_bullets
    )
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
        h1_str=_ui.get("h1", "ECB SAFE Survey — Wave {wave}").format(wave=wave_str),
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
    latest_wave = max(df["wave_number"].max() for df in data.values())
    print(f"  Latest wave: {latest_wave}")

    cost_tracker = {"input_tokens": 0, "output_tokens": 0, "usd": 0.0, "calls": 0, "by_model": {}}

    print("Running interest checks (parallel)...")
    interest = check_all_interest(SECTIONS, data, cost_tracker)
    for sid, r in interest.items():
        flag = "✓" if r["interesting"] else "✗"
        print(f"  {flag} {sid}: {r['reason']} [chart={r['chart_type']}, best_panel={r['best_panel']}]")

    schema = DEV_SCHEMA if args.dev else PROD_SCHEMA
    tool_con = _get_connection(args.dev)
    print("Building mart schema catalogue...")
    mart_catalogue = build_mart_catalogue(tool_con, schema)

    print("Loading annex question texts...")
    question_texts = _load_annex_question_texts(ANNEX_CSV, con=tool_con)
    print(f"  Loaded {len(question_texts)} question texts from annex")

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

        print(f"  Generating finding + bullets for {sid}...")
        content = get_section_content_agentic(sec, data[sid], tool_con, schema, mart_catalogue, cost_tracker, question_texts)
        print(f"    finding: {content['finding']}")
        for b in content["bullets"]:
            print(f"    {b}")

        rendered.append({
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
        })

    tool_con.close()

    print("Generating executive summary...")
    exec_bullets = get_exec_summary(rendered, cost_tracker) if rendered else []
    for b in exec_bullets:
        print(f"  {b}")

    print("Building TOC...")
    toc_html = build_toc(rendered)

    print("Building question annex...")
    annex_html = build_annex_html(ANNEX_CSV)

    print("Fetching painting thumbnail...")
    painting_inner_html = _fetch_painting_inner_html()

    print("Assembling HTML (EN)...")
    html = build_html(rendered, annex_html, exec_bullets, toc_html, painting_inner_html, latest_wave)

    out_path = OUTPUT_DIR / "report_latest.html"
    out_path.write_text(html, encoding="utf-8")
    print(f"Saved → {out_path}")

    print("Translating to Slovak...")
    sk_rendered, sk_exec_bullets = translate_to_slovak(rendered, exec_bullets, cost_tracker)
    sk_toc_html = build_toc(sk_rendered, ui=_SK_UI)
    sk_html = build_html(sk_rendered, annex_html, sk_exec_bullets, sk_toc_html,
                         painting_inner_html, latest_wave, ui=_SK_UI)
    sk_path = OUTPUT_DIR / "report_latest_sk.html"
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
    print("Done.")


if __name__ == "__main__":
    main()
