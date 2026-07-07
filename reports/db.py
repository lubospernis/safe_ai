"""Database connections, data fetching, and query-tool validation."""

import os
import re
import textwrap
from pathlib import Path

import duckdb
import pandas as pd
import yaml

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
    motherduck_token = os.environ["MOTHERDUCK_TOKEN"]
    return duckdb.connect(f"md:my_db?motherduck_token={motherduck_token}")


def fetch_all(sections=None) -> dict[str, pd.DataFrame]:
    from config import SECTIONS as _SECTIONS
    _sections = sections or _SECTIONS
    con = _get_connection()
    results = {}
    for sec in _sections:
        sql = (SQL_DIR / sec["sql_file"]).read_text()
        results[sec["id"]] = con.execute(sql).df()
    con.close()
    return results


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
