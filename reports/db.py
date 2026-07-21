"""Database connections, data fetching, and query-tool validation."""

import os
import re
import textwrap
import time
from pathlib import Path

import duckdb
import pandas as pd
import yaml

from cost import _track_cost

PROD_SCHEMA = "main_safe"
SQL_DIR = Path(__file__).parent / "sql"
MARTS_SCHEMA_YML = Path(__file__).parent.parent / "dbt_project" / "models" / "marts" / "schema.yml"

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


def _get_connection() -> duckdb.DuckDBPyConnection:
    """Connect to MotherDuck, retrying transient connection failures (3 attempts,
    1s/2s backoff) — the pipeline is MotherDuck-only by design, so a flaky
    connection shouldn't abort a whole run on the first hiccup."""
    motherduck_token = os.environ["MOTHERDUCK_TOKEN"]
    last_error: Exception | None = None
    for attempt in range(3):
        try:
            return duckdb.connect(f"md:my_db?motherduck_token={motherduck_token}")
        except Exception as e:
            last_error = e
            if attempt < 2:
                time.sleep(2 ** attempt)
    raise last_error


def fetch_all(sections=None) -> dict[str, pd.DataFrame]:
    """Returns {section_id: df}. A section with an `sme_sql_file` also gets an
    extra `{section_id}__sme` entry (a small SK-only all-vs-sme comparison df,
    used solely by _sme_divergence_note) — this key is never a real section_id,
    so it's invisible to every other consumer that looks up data[sid]."""
    from config import SECTIONS as _SECTIONS
    _sections = sections or _SECTIONS
    con = _get_connection()
    results = {}
    for sec in _sections:
        sql = (SQL_DIR / sec["sql_file"]).read_text()
        results[sec["id"]] = con.execute(sql).df()
        if sec.get("sme_sql_file"):
            sme_sql = (SQL_DIR / sec["sme_sql_file"]).read_text()
            results[f"{sec['id']}__sme"] = con.execute(sme_sql).df()
    con.close()
    return results


def wave_period_label(wave_number: int, con, schema: str = PROD_SCHEMA) -> str | None:
    """Return the human-readable survey period for a wave (e.g. "2026Q1" for wave 38),
    from mart_safe__slovakia_kpis.survey_period_label. None on any failure — callers
    should fall back to showing just the wave number."""
    try:
        row = con.execute(
            f"SELECT survey_period_label FROM {schema}.mart_safe__slovakia_kpis "
            f"WHERE wave_number = {int(wave_number)} LIMIT 1"
        ).fetchone()
        val = row[0] if row else None
        return str(val) if val else None
    except Exception:
        return None


def _run_query_tool(sql: str, con, schema: str) -> str:
    """Validate and execute a tool-use SQL query. Returns markdown table or error string."""
    if _WRITE_RE.search(sql):
        return "ERROR: only SELECT queries are permitted."
    referenced = set(re.findall(r"(mart_safe__\w+|int_safe__\w+)", sql))
    disallowed = referenced - ALLOWED_MART_TABLES
    if disallowed:
        return f"ERROR: table(s) not in whitelist: {disallowed}"
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


def run_agentic_query_turns(
    client,
    system_prompt_cached: list[dict],
    messages: list[dict],
    tool_con,
    schema: str,
    cost_tracker: dict,
    model: str = "claude-sonnet-4-6",
    max_turns: int = MAX_TOOL_TURNS,
    extra_tools: list[dict] | None = None,
    extra_tool_handler=None,
) -> tuple[list[dict], int]:
    """Run the query_mart agentic tool loop: repeatedly call `model` with
    QUERY_MART_TOOL (plus any extra_tools) available, execute any query_mart
    calls via _run_query_tool, dispatch any other tool call to
    extra_tool_handler(name, input) -> str, and feed results back as
    tool_results — until the model stops calling tools or max_turns is
    reached.

    This is the single implementation of that loop, extracted from
    llm.py::get_section_content_agentic (which still owns everything specific
    to report-section generation: the emit-tool structured output, grounding
    checks, and revision retries) so reports/qa_tool.py's Q&A agent can reuse
    the exact same query loop — including offering its own search_narrative
    tool alongside query_mart via extra_tools/extra_tool_handler — rather than
    a second copy of the loop mechanics.

    Returns (messages, tool_calls_made) — `messages` has the full turn
    history appended (ending on the model's final non-tool-use response, or
    the last tool_result if max_turns was reached without the model
    stopping); the caller decides what happens next (force structured output,
    extract a plain-text answer, etc.), since that part is caller-specific.
    """
    tools = [QUERY_MART_TOOL] + (extra_tools or [])
    tool_calls_made = 0
    for turn in range(max_turns):
        response = client.messages.create(
            model=model,
            max_tokens=600,
            system=system_prompt_cached,
            tools=tools,
            messages=messages,
            extra_headers={"anthropic-beta": "prompt-caching-2024-07-31"},
        )
        _track_cost(cost_tracker, model, response.usage)

        messages.append({"role": "assistant", "content": response.content})
        if response.stop_reason != "tool_use":
            break

        tool_results = []
        for block in response.content:
            if block.type != "tool_use":
                continue
            if block.name == "query_mart":
                sql = block.input.get("sql", "")
                print(f"    [tool_use] query_mart called (turn {turn + 1}): {sql[:120]!r}")
                result = _run_query_tool(sql, tool_con, schema)
            elif extra_tool_handler is not None:
                print(f"    [tool_use] {block.name} called (turn {turn + 1}): {block.input}")
                result = extra_tool_handler(block.name, block.input)
            else:
                result = f"ERROR: no handler registered for tool {block.name!r}"
            tool_calls_made += 1
            tool_results.append({
                "type": "tool_result",
                "tool_use_id": block.id,
                "content": result,
            })
        messages.append({"role": "user", "content": tool_results})

    return messages, tool_calls_made
