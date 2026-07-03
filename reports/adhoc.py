"""Adhoc module spotlight: detect theme, build charts, generate LLM content."""

import base64
import json
import re
import textwrap
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import anthropic
import pandas as pd
from json_repair import repair_json

from charts import _build_adhoc_chart
from cost import _Usage, _track_cost
from llm import NBS_STYLE_GUIDE
from questionnaire import (
    build_response_label_context,
    fetch_adhoc_response_labels,
    persist_adhoc_labels,
    questionnaire_url_for_wave,
)

_SONNET_MODEL = "claude-sonnet-4-6"

SQL_DIR = Path(__file__).parent / "sql"

_MODULE_THEME_FALLBACK: dict[str, str] = {
    "qa1": "Artificial intelligence technologies",
    "qa1a": "Artificial intelligence technologies",
    "qa1b": "Artificial intelligence technologies",
    "qa1c": "Artificial intelligence technologies",
    "qa2": "AI Adoption",
    "qa2dec": "Green transition",
    "qa2inc": "Green transition",
    "qa3": "AI Adoption",
    "qa4": "AI Adoption",
    "qa5": "Geopolitical Risk",
    "qa6": "Energy Costs",
    "qb1": "AI Adoption",
    "qb2": "AI Adoption",
}

# Questions whose text matches these patterns are forward-looking / peer-perception.
# Current-state questions (no match) are preferred as the primary chart subject.
_EXPECTATION_PATTERN = re.compile(
    r"\b(next|expect|plan|forecast|future|thinking about other|similar firms|do you think)\b",
    re.IGNORECASE,
)

_ECB_FOCUS_INDEX = "https://www.ecb.europa.eu/press/economic-bulletin/focus/"

ADHOC_CONTENT_SYSTEM = textwrap.dedent(f"""
    You are a concise economic analyst writing a "Special Focus" sidebar for a Slovakia
    SAFE survey report. Your task: write 2–5 short bullets (≤ 35 words each) comparing
    Slovak firms (SK) to the Euro Area (EA) on the topic indicated.

    {NBS_STYLE_GUIDE}

    RULES — READ CAREFULLY:
    1. Only cite numbers that appear verbatim in the data table below. Do NOT invent,
       estimate, or paraphrase percentages — if a number is not in the table, omit it.
    2. Write a bullet ONLY if it reflects a genuine difference (≥ 3pp) or a striking
       majority/minority pattern. Skip sub-items where SK and EA are near-identical.
    3. If SK or EA data is missing for a sub-item, say so explicitly.
    4. Bullets start with a bolded 3–5 word label: **Label:** sentence. One sentence each.
    5. If routing notes are provided for a sub-item, reflect the base population in the
       bullet (e.g. "among AI-using firms" or "among firms that applied for a loan").
    6. Survey question texts are provided for context — use them to name what was measured,
       but do NOT invent response categories not in the data.
    7. Also return chart_sub_items: the list of sub-item codes (e.g. ["a", "c"]) that
       should appear in the chart — only sub-items mentioned in your bullets.
    8. Return valid JSON only (no markdown fences):
       {{"finding": "One sentence headline ≤ 20 words",
        "bullets": ["bullet 1", ..., "bullet N"],
        "chart_sub_items": ["a", ...]}}
    9. The finding must name the theme and the most striking SK vs EA difference.
       If results are broadly similar across sub-items, say so and write 2 bullets.
""").strip()

_SELECTION_SYSTEM = textwrap.dedent("""
    You are a statistical analyst evaluating ECB SAFE survey data for a Slovakia report.
    You will receive a table of response distributions for Slovak firms (SK) and the
    Euro Area (EA) across multiple survey sub-items. For each sub-item, classify it:

    - "interesting" if: categorical sub-item has SK vs EA difference ≥ 3pp on any
      response category, OR a clear majority/minority pattern (>50% in one code);
      continuous sub-item has SK vs EA mean difference ≥ 3pp.
    - "skip" if: all gaps are < 3pp and there is no striking pattern.

    Also note any routing restrictions you can infer from the question texts provided
    (e.g. "asked only of firms using AI" → routing_note = "among AI-using firms").

    Return JSON only (no markdown):
    {
      "interesting": ["a", "c"],
      "skip": ["b"],
      "routing_notes": {"a": "all firms", "c": "among AI-using firms", "b": ""}
    }

    If you cannot determine interestingness (too few rows, ambiguous data), include
    the sub-item in "interesting" so it is not silently dropped.
""").strip()

_ADHOC_REVIEW_SYSTEM = textwrap.dedent("""
    You are a critical editor reviewing a "Special Focus" sidebar for a financial survey
    report on ECB SAFE data for Slovakia. You will receive the finding, bullets, and
    the data that was provided to the analyst who wrote them.

    Score on four dimensions, each 1–10:
    - grounding (1–10): Every number in the bullets appears verbatim in the data table.
      Score 1 if you see a percentage, net balance, or count not in the data.
      Score 10 if every cited number is directly traceable to a row in the table.
    - coverage (1–10): The bullets cover the most striking SK vs EA differences.
      Score low if a large gap (≥10pp) is ignored while smaller ones are highlighted.
      Score 10 if the headline finding and bullets address the most important patterns.
    - readability (1–10): Plain English accessible to a non-expert. Complete sentences.
      No jargon. Score 1 if the text is robotic, boilerplate, or hard to parse.
    - chart_alignment (1–10): The listed chart sub-items match the bullets.
      Score low if the chart shows sub-items not mentioned in bullets, or omits the
      sub-item driving the headline finding.

    You are a strict reviewer. Score honestly — a 7 is a real failure here.
    Return JSON only (no markdown):
    {"grounding": <1-10>, "coverage": <1-10>, "readability": <1-10>,
     "chart_alignment": <1-10>, "verdict": "pass" or "fail", "reason": "<one sentence>"}

    Set verdict to "fail" if ANY dimension is below 8.
""").strip()

_DESCRIBE_QUESTION_SYSTEM = textwrap.dedent("""
    You are a statistical analyst summarising one ECB SAFE survey question for a Slovakia report.
    You receive the question text, response code meanings, and a data table (SK vs EA).

    Return JSON only (no markdown):
    {
      "description": "<1-2 sentence plain-English summary of what the data shows for SK vs EA>",
      "interest_score": <integer 1-5>,
      "key_finding": "<the single most striking number or gap, e.g. 'SK 30% vs EA 18%'>",
      "routing_note": "<who was asked this question, e.g. 'all firms' or 'firms using AI', or '' if unknown>"
    }

    interest_score rubric:
    1 = SK and EA nearly identical (< 3pp gap on all codes)
    2 = minor difference (3–5pp on one code)
    3 = notable difference (5–10pp) or clear majority/minority pattern
    4 = striking difference (10–20pp) or surprising result
    5 = very large gap (> 20pp) or result that directly contradicts expectations
""").strip()

_ADHOC_SPOTLIGHT_SYSTEM = textwrap.dedent(f"""
    You are a concise economic analyst writing a "Special Focus" sidebar for a Slovakia
    SAFE survey report. You will receive per-question summaries and charts for all adhoc
    questions in this wave.

    {NBS_STYLE_GUIDE}

    YOUR TASK:
    1. Select the 1–3 most interesting questions based on the interest scores and descriptions.
    2. For each selected question, write 1–2 tight bullets (≤ 35 words each).
    3. Write one overall finding sentence (≤ 20 words) naming the theme and most striking gap.
    4. If a specific breakdown would meaningfully strengthen ONE finding (e.g. by firm size
       or by country), you may request it — see "dig_deeper" below.

    RULES:
    - Only cite numbers that appear verbatim in the data descriptions/key_findings provided.
      Do NOT invent, estimate, or paraphrase percentages.
    - Bullets start with a bolded 3–5 word label: **Label:** sentence.
    - Reflect routing notes in bullets (e.g. "among AI-using firms").

    Return valid JSON only (no markdown fences):
    {{
      "finding": "<one sentence headline>",
      "bullets": ["bullet 1", ..., "bullet N"],
      "selected_question_ids": ["qa1", "qa3"],
      "dig_deeper": {{
        "question_id": "qa1",
        "sql": "SELECT country_code, sub_item, response_raw, pct_wtd, n_firms FROM {{schema}}.mart_safe__adhoc_responses WHERE wave_number = {{wave_number}} AND module_id = 'qa1' AND country_code IN ('SK','EA') AND response_raw >= 0 ORDER BY sub_item, country_code, response_raw"
      }} or null
    }}

    Use dig_deeper only when a follow-up query would materially change the bullets.
    The SQL must use {{schema}} and {{wave_number}} as literal format placeholders.
    Set dig_deeper to null if no follow-up is needed.
""").strip()

_ADHOC_CHART_SQL_CONTINUOUS = """
SELECT
    country_code,
    sub_item,
    FLOOR(CAST(response_raw AS INTEGER) / 10) * 10 AS response_raw,
    ROUND(SUM(n_firms_wtd) / MAX(n_total_wtd) * 100, 1) AS pct_wtd,
    SUM(n_firms) AS n_firms,
    SUM(n_firms_wtd) AS n_firms_wtd,
    MAX(n_total_wtd) AS n_total_wtd
FROM {schema}.mart_safe__adhoc_responses
WHERE wave_number = {wave_number}
  AND module_id = '{module_id}'
  AND country_code IN ('SK', 'EA', 'DE')
  AND response_raw >= 0
  AND response_raw <= 100
GROUP BY country_code, sub_item, FLOOR(CAST(response_raw AS INTEGER) / 10) * 10
ORDER BY sub_item, country_code, response_raw
"""

_ADHOC_CHART_SQL_CATEGORICAL = """
SELECT
    country_code,
    sub_item,
    response_raw,
    pct_wtd,
    n_firms
FROM {schema}.mart_safe__adhoc_responses
WHERE wave_number = {wave_number}
  AND module_id = '{module_id}'
  AND country_code IN ('SK', 'EA', 'DE')
  AND response_raw >= 0
ORDER BY sub_item, country_code, response_raw
"""


def _is_continuous(df: pd.DataFrame) -> bool:
    """True if response_raw looks like an open numeric estimate (>10 distinct values per group)."""
    if df.empty:
        return False
    max_distinct = df.groupby(["country_code", "sub_item"])["response_raw"].nunique().max()
    return int(max_distinct) > 10


def _fetch_adhoc_chart_data(df: pd.DataFrame, theme: dict, wave_number: int, schema: str, con) -> pd.DataFrame:
    """Fetch chart-ready data using a fixed SQL template based on question type."""
    template = _ADHOC_CHART_SQL_CONTINUOUS if _is_continuous(df) else _ADHOC_CHART_SQL_CATEGORICAL
    sql = template.format(schema=schema, wave_number=wave_number, module_id=theme["module_id"])
    return con.execute(sql).df()


def _fetch_question_texts(con, schema: str, module_ids: list[str], period_suffix: str | None = None) -> str:
    """Return a formatted block of survey question texts for the given module IDs from ref_safe__annex.

    If period_suffix is given (e.g. '2025Q4'), only rows whose question_item ends with that suffix
    are returned — preventing historical module reuse from polluting the context (QA1 was about
    exports in 2022 but AI adoption in 2025Q4).
    """
    try:
        annex_cols = con.execute(f"""
            SELECT column_name FROM information_schema.columns
            WHERE table_schema = '{schema}'
              AND table_name = 'ref_safe__annex'
              AND column_name LIKE 'safe_%'
            ORDER BY column_name DESC
        """).fetchall()
        wave_cols = [r[0] for r in annex_cols]
        if not wave_cols:
            return ""
        # NULLIF strips empty strings so COALESCE falls through to the column with actual content
        coalesce_expr = ", ".join(f"NULLIF({c}, '')" for c in wave_cols)
        if period_suffix:
            pattern = " OR ".join(
                f"UPPER(question_item) = UPPER('{m}_{period_suffix}')" for m in module_ids
            )
        else:
            pattern = " OR ".join(f"UPPER(question_item) LIKE UPPER('{m}%')" for m in module_ids)
        rows = con.execute(f"""
            SELECT question_item, COALESCE({coalesce_expr}) AS question_text
            FROM {schema}.ref_safe__annex
            WHERE element = 'question' AND ({pattern})
            ORDER BY question_item
        """).fetchall()
        if not rows:
            return ""
        lines = ["Survey question texts (use for plain-language descriptions only):"]
        for qitem, qtext in rows:
            if qtext:
                lines.append(f"  {qitem.upper()}: {qtext}")
        return "\n".join(lines)
    except Exception:
        return ""


def detect_adhoc_theme(wave_number: int, con, schema: str, mistral_client=None, cost_tracker: dict | None = None) -> dict | None:
    """Return {module_id, theme_label, question_texts} for the wave's adhoc modules, or None."""
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

    all_module_ids_wave = [r[0] for r in rows]

    # Look up period suffix (e.g. '2025Q4') to scope annex queries to the right wave
    period_suffix: str | None = None
    try:
        row = con.execute(
            f"SELECT survey_period_label FROM {schema}.mart_safe__slovakia_kpis "
            f"WHERE wave_number = {wave_number} LIMIT 1"
        ).fetchone()
        if row and row[0]:
            period_suffix = str(row[0])
    except Exception:
        pass

    # Fetch one question text per module to use for primary-module selection heuristic
    # and for theme classification. Build wave_cols once and reuse.
    wave_cols: list[str] = []
    coalesce_expr = ""
    try:
        annex_col_rows = con.execute(f"""
            SELECT column_name FROM information_schema.columns
            WHERE table_schema = '{schema}'
              AND table_name = 'ref_safe__annex'
              AND column_name LIKE 'safe_%'
            ORDER BY column_name DESC
        """).fetchall()
        wave_cols = [r[0] for r in annex_col_rows]
        if wave_cols:
            coalesce_expr = ", ".join(f"NULLIF({c}, '')" for c in wave_cols)
    except Exception:
        pass

    module_question_texts: dict[str, str] = {}  # {module_id: question_text}
    if wave_cols and period_suffix:
        try:
            for mid in all_module_ids_wave:
                q_row = con.execute(f"""
                    SELECT COALESCE({coalesce_expr}) as qt
                    FROM {schema}.ref_safe__annex
                    WHERE element = 'question'
                      AND UPPER(question_item) = UPPER('{mid}_{period_suffix}')
                    LIMIT 1
                """).fetchone()
                if q_row and q_row[0]:
                    module_question_texts[mid] = q_row[0]
        except Exception:
            pass

    # Re-rank modules: current-state questions before forward-looking/peer-perception ones,
    # and within current-state prefer non-routed questions (no [IF ...] conditional).
    # This is wave-agnostic — relies on question text content, not module ID prefixes.
    _ROUTED_PATTERN = re.compile(r"\[IF\b", re.IGNORECASE)
    # "Please indicate" questions are multi-select sub-topic lists; prefer open-ended
    # assessment questions ("How would you assess...", "What percentage...") as primary.
    _MULTI_SELECT_PATTERN = re.compile(r"^please indicate", re.IGNORECASE)

    def _module_rank(mid: str) -> tuple[int, int, int]:
        qt = module_question_texts.get(mid, "").lstrip("-• ")
        is_expectation = 1 if _EXPECTATION_PATTERN.search(qt) else 0
        is_routed = 1 if _ROUTED_PATTERN.search(qt) else 0
        is_multi_select = 1 if _MULTI_SELECT_PATTERN.search(qt) else 0
        return (is_expectation, is_routed, is_multi_select)

    ranked = sorted(all_module_ids_wave, key=_module_rank)
    primary_module_id = ranked[0]
    sibling_modules = [m for m in all_module_ids_wave if m != primary_module_id]

    # pdf_section_title is populated later (after PDF fetch); declare here so the
    # theme-label block below can reference it even if PDF is not yet fetched.
    pdf_section_title: str | None = None

    # Build question_texts dict for the primary module (sub_item → question_text mapping)
    question_texts: dict[str, str] = {}
    primary_question_text: str | None = module_question_texts.get(primary_module_id)
    if wave_cols and primary_question_text is None:
        # Period suffix not available — fall back to prefix match
        try:
            if period_suffix:
                period_filter = f"UPPER(question_item) = UPPER('{primary_module_id}_{period_suffix}')"
            else:
                period_filter = f"UPPER(question_item) LIKE UPPER('{primary_module_id}%')"
            ann_rows = con.execute(f"""
                SELECT question_item, COALESCE({coalesce_expr}) as question_text
                FROM {schema}.ref_safe__annex
                WHERE element = 'question' AND ({period_filter})
            """).fetchall()
            for qitem, qtext in ann_rows:
                if qtext:
                    sub = qitem.lower().replace(primary_module_id.lower(), "").strip() or "a"
                    question_texts[sub] = qtext
            primary_question_text = next((qt for _, qt in ann_rows if qt), None)
        except Exception:
            pass

    # Fetch questionnaire PDF FIRST so pdf_section_title is available for theme label selection.
    # Extracts: response code → label mappings, sub-item labels, and the human-readable
    # adhoc section title (e.g. "Artificial intelligence technologies" from [AD HOC QUESTIONS]).
    questionnaire_url = questionnaire_url_for_wave(wave_number, con, schema)
    response_labels: dict[str, dict[int, str]] = {}
    sub_item_labels: dict[str, dict[str, str]] = {}
    if questionnaire_url:
        all_module_ids = [r[0] for r in rows]
        print(f"  Fetching questionnaire PDF for response labels: {questionnaire_url}")
        response_labels, sub_item_labels, pdf_section_title = fetch_adhoc_response_labels(questionnaire_url, all_module_ids)
        total_codes = sum(len(v) for v in response_labels.values())
        total_subs = sum(len(v) for v in sub_item_labels.values())
        if total_codes or total_subs:
            print(f"  Questionnaire parsed: {total_codes} response codes, {total_subs} sub-item labels across {len(response_labels | sub_item_labels)} modules")
            if pdf_section_title:
                print(f"  Adhoc section title from PDF: {pdf_section_title!r}")
        else:
            print("  Questionnaire parse returned no labels — proceeding without them")
    else:
        print(f"  No questionnaire URL mapped for wave {wave_number} — proceeding without response labels")

    # Persist parsed labels to MotherDuck so they're queryable across sessions
    if (response_labels or sub_item_labels or pdf_section_title) and period_suffix:
        try:
            n = persist_adhoc_labels(
                wave_number=wave_number,
                period_suffix=period_suffix,
                questionnaire_url=questionnaire_url or "",
                response_labels=response_labels,
                sub_item_labels=sub_item_labels,
                section_title=pdf_section_title,
                con=con,
                schema=schema,
            )
            print(f"  Adhoc labels persisted: {n} rows → {schema}.ref_safe__adhoc_labels")
        except Exception as e:
            print(f"  Adhoc label persistence failed (non-fatal): {e}")

    # Cache read-back: if PDF was unavailable, try loading labels persisted by a prior run
    if not response_labels and not sub_item_labels:
        try:
            cached = con.execute(f"""
                SELECT question_id, label_type, code_key, label_text
                FROM {schema}.ref_safe__adhoc_labels
                WHERE wave_number = {wave_number}
            """).fetchall()
            for qid, ltype, ckey, ltext in cached:
                if ltype == "response_code":
                    response_labels.setdefault(qid, {})[int(ckey)] = ltext
                elif ltype == "sub_item":
                    sub_item_labels.setdefault(qid, {})[ckey] = ltext
                elif ltype == "section_title" and not pdf_section_title:
                    pdf_section_title = ltext
            if response_labels or sub_item_labels:
                print(f"  Loaded adhoc labels from ref_safe__adhoc_labels (PDF not fetched)")
        except Exception:
            pass

    # Theme label priority:
    # 1. PDF section title (e.g. "Artificial intelligence technologies" from [AD HOC QUESTIONS])
    # 2. Hardcoded fallback dict (for waves where PDF parsing fails or URL is unknown)
    # 3. Mistral classifier (for completely unknown modules)
    theme_label = pdf_section_title or _MODULE_THEME_FALLBACK.get(primary_module_id, "")
    if not theme_label:
        theme_label = primary_module_id.upper()
        if mistral_client and module_question_texts:
            all_qt_block = "\n".join(
                f"- {qt}" for qt in module_question_texts.values() if qt
            )
            model = "mistral-small-latest"
            try:
                resp = mistral_client.chat.complete(
                    model=model,
                    max_tokens=20,
                    messages=[{
                        "role": "user",
                        "content": (
                            f"These are ECB SAFE survey questions from this wave:\n"
                            f"{all_qt_block}\n\n"
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
                pass

    return {
        "module_id": primary_module_id,
        "theme_label": theme_label,
        "question_texts": question_texts,
        "primary_question_text": primary_question_text,
        "questionnaire_url": questionnaire_url,
        "response_labels": response_labels,
        "sub_item_labels": sub_item_labels,
        "sibling_modules": sibling_modules,
        "period_suffix": period_suffix,
    }


def _build_full_data_table(df: pd.DataFrame) -> tuple[str, str]:
    """Build the full data table string for all sub-items. Returns (table, data_note)."""
    is_cont = _is_continuous(df)
    if is_cont:
        df = df[df["response_raw"] <= 100].copy()
        lines = ["country_code | sub_item | mean_pct | median_pct | n_firms"]
        for (cc, sub), grp in df.groupby(["country_code", "sub_item"]):
            grp = grp.sort_values("response_raw")
            total_w = grp["n_firms_wtd"].sum()
            if total_w == 0:
                continue
            wtd_mean = (grp["response_raw"] * grp["n_firms_wtd"]).sum() / total_w
            cumw = grp["n_firms_wtd"].cumsum()
            median_val = int(grp[cumw >= total_w / 2]["response_raw"].iloc[0])
            lines.append(
                f"{cc} | {sub} | {wtd_mean:.1f}% | {median_val}% | {int(grp['n_firms'].sum())}"
            )
        return "\n".join(lines), (
            "\n\nNOTE: Each firm gave a numeric % estimate. "
            "The table shows the weighted mean and approximate median of those estimates."
        )
    else:
        pivot_lines = ["country_code | sub_item | response_raw | pct_wtd | n_firms"]
        for _, row in df.iterrows():
            pivot_lines.append(
                f"{row['country_code']} | {row['sub_item']} | {row['response_raw']} "
                f"| {row['pct_wtd']:.1f}% | {int(row['n_firms'])}"
            )
        return "\n".join(pivot_lines), ""


def _select_interesting_sub_items(
    df: pd.DataFrame,
    full_table: str,
    question_texts: dict,
    theme_label: str,
    cost_tracker: dict,
    mistral_client=None,
    response_label_ctx: str = "",
) -> tuple[list[str], dict[str, str]]:
    """Phase 1: ask Mistral Small to classify sub-items as interesting or not.

    Returns (interesting_sub_items, routing_notes). Falls back to all sub-items on failure.
    """
    all_subs = sorted(df["sub_item"].unique().tolist())
    if len(all_subs) <= 1:
        return all_subs, {s: "" for s in all_subs}

    qt_block = ""
    if question_texts:
        qt_block = "\n\nQuestion texts from survey annex:\n" + "\n".join(
            f"  Sub-item {k}: {v}" for k, v in question_texts.items()
        )

    label_block = f"\n\n{response_label_ctx}" if response_label_ctx else ""

    user_msg = (
        f"Topic: {theme_label}\n"
        f"Sub-items present: {', '.join(all_subs)}{qt_block}{label_block}\n\n"
        f"Data (SK vs EA):\n{full_table}\n\n"
        "Classify each sub-item and return routing notes."
    )

    if mistral_client is None:
        return all_subs, {s: "" for s in all_subs}

    try:
        model = "mistral-small-latest"
        resp = mistral_client.chat.complete(
            model=model,
            max_tokens=300,
            messages=[
                {"role": "system", "content": _SELECTION_SYSTEM},
                {"role": "user", "content": user_msg},
            ],
        )
        raw = resp.choices[0].message.content.strip()
        if resp.usage:
            _track_cost(cost_tracker, model,
                        _Usage(resp.usage.prompt_tokens, resp.usage.completion_tokens))
        if raw.startswith("```"):
            raw = raw.split("\n", 1)[-1].rsplit("```", 1)[0].strip()
        sel = json.loads(repair_json(raw))
        interesting = sel.get("interesting", all_subs)
        routing_notes = sel.get("routing_notes", {s: "" for s in all_subs})
        if not interesting:
            interesting = all_subs
        print(f"  Phase 1 selection: interesting={interesting}, skip={sel.get('skip', [])}")
        return interesting, routing_notes
    except Exception as e:
        print(f"  Phase 1 selection failed ({e}) — using all sub-items")
        return all_subs, {s: "" for s in all_subs}


def _describe_question(q: dict, mistral_client, cost_tracker: dict) -> dict:
    """Phase 1: Mistral Small describes one question's SK vs EA findings.

    Returns the input dict augmented with 'description', 'interest_score',
    'key_finding', 'routing_note'. Falls back to empty strings on failure.
    """
    label_lines = []
    if q.get("response_labels"):
        label_lines.append("Response codes:")
        for code, label in sorted(q["response_labels"].items()):
            label_lines.append(f"  {code} = {label}")
    if q.get("sub_item_labels"):
        label_lines.append("Sub-items:")
        for letter, label in sorted(q["sub_item_labels"].items()):
            label_lines.append(f"  {letter} = {label}")
    label_block = ("\n" + "\n".join(label_lines)) if label_lines else ""

    user_msg = (
        f"Question: {q.get('question_text') or q['question_id'].upper()}{label_block}\n\n"
        f"Data (SK vs EA):\n{q['data_table']}"
    )

    defaults = {"description": "", "interest_score": 3, "key_finding": "", "routing_note": ""}
    if mistral_client is None:
        return {**q, **defaults}

    try:
        model = "mistral-small-latest"
        resp = mistral_client.chat.complete(
            model=model,
            max_tokens=200,
            messages=[
                {"role": "system", "content": _DESCRIBE_QUESTION_SYSTEM},
                {"role": "user", "content": user_msg},
            ],
        )
        raw = resp.choices[0].message.content.strip()
        if resp.usage:
            _track_cost(cost_tracker, model,
                        _Usage(resp.usage.prompt_tokens, resp.usage.completion_tokens))
        if raw.startswith("```"):
            raw = raw.split("\n", 1)[-1].rsplit("```", 1)[0].strip()
        parsed = json.loads(repair_json(raw))
        return {**q, **{k: parsed.get(k, defaults[k]) for k in defaults}}
    except Exception as e:
        print(f"  _describe_question({q['question_id']}) failed: {e}")
        return {**q, **defaults}


def build_adhoc_spotlight(
    theme: dict,
    wave_number: int,
    con,
    schema: str,
    mistral_client,
    cost_tracker: dict,
    anthropic_client=None,
) -> dict | None:
    """Generate the adhoc spotlight section: question-by-question agentic pipeline.

    Phase 0: Enumerate all questions, fetch data + build data tables.
    Phase 1: Mistral Small describes each question (parallel).
    Phase 2: Build charts for every question.
    Phase 3: Sonnet (multimodal) selects interesting questions and writes the spotlight;
             optionally requests one follow-up SQL query per selected question.
    Phase 4: Mistral Large critical review.
    """
    response_labels = theme.get("response_labels", {})
    sub_item_labels = theme.get("sub_item_labels", {})
    sibling_modules = theme.get("sibling_modules", [])
    all_question_ids = [theme["module_id"]] + sibling_modules
    sql_template = (SQL_DIR / "adhoc_spotlight.sql").read_text()

    # ── Phase 0: enumerate questions, fetch data ──────────────────────────────
    question_contexts: list[dict] = []
    for qid in all_question_ids:
        try:
            sql = sql_template.format(wave_number=wave_number, module_id=qid, schema=schema)
            df = con.execute(sql).df()
        except Exception as e:
            print(f"  Phase 0 SQL failed for {qid}: {e}")
            continue
        if df.empty:
            continue
        df = df[df["response_raw"] >= 0].copy()
        if df.empty:
            continue
        is_cont = _is_continuous(df)
        if is_cont:
            df = df[df["response_raw"] <= 100].copy()

        data_table, data_note = _build_full_data_table(df)

        # Question text: from annex (via module_question_texts populated in detect_adhoc_theme),
        # or fall back to the primary question text for the primary module.
        qt = theme.get("question_texts", {}).get(qid, "")
        if not qt and qid == theme["module_id"]:
            qt = theme.get("primary_question_text") or ""

        # Merge PDF sub-item labels into question_texts for chart panel titles
        effective_qt: dict[str, str] = {}
        for letter, label in (sub_item_labels.get(qid.lower(), {})).items():
            effective_qt[letter] = label
        # Override with annex texts if available
        for k, v in (theme.get("question_texts") or {}).items():
            effective_qt[k] = v

        question_contexts.append({
            "question_id": qid,
            "question_text": qt,
            "is_continuous": is_cont,
            "df": df,
            "data_table": data_table + data_note,
            "response_labels": response_labels.get(qid.lower(), {}),
            "sub_item_labels": sub_item_labels.get(qid.lower(), {}),
            "effective_question_texts": effective_qt,
        })

    if not question_contexts:
        print("  Phase 0: no question data found — aborting adhoc spotlight")
        return None

    print(f"  Phase 0: {len(question_contexts)} questions enumerated: {[q['question_id'] for q in question_contexts]}")

    # ── Phase 1: describe each question (Mistral Small, parallel) ────────────
    cost_lock = __import__("threading").Lock()

    def _describe_with_lock(q: dict) -> dict:
        local_tracker: dict = {"input_tokens": 0, "output_tokens": 0, "usd": 0.0, "calls": 0, "by_model": {}}
        result = _describe_question(q, mistral_client, local_tracker)
        with cost_lock:
            for k in ("input_tokens", "output_tokens", "usd", "calls"):
                cost_tracker[k] = cost_tracker.get(k, 0) + local_tracker[k]
            for model, m in local_tracker["by_model"].items():
                bm = cost_tracker["by_model"].setdefault(model, {"calls": 0, "input": 0, "output": 0, "usd": 0.0, "cache_write": 0, "cache_read": 0})
                for mk in bm:
                    bm[mk] += m.get(mk, 0)
        return result

    with ThreadPoolExecutor(max_workers=len(question_contexts)) as pool:
        futures = {pool.submit(_describe_with_lock, q): q["question_id"] for q in question_contexts}
        described: dict[str, dict] = {}
        for future in as_completed(futures):
            qid = futures[future]
            described[qid] = future.result()

    question_contexts = [described[q["question_id"]] for q in question_contexts]
    for q in question_contexts:
        print(f"  Phase 1 [{q['question_id']}] score={q['interest_score']}: {q.get('key_finding', '')}")

    # ── Phase 2: build charts for all questions ───────────────────────────────
    for q in question_contexts:
        try:
            chart_theme = {
                **theme,
                "module_id": q["question_id"],
                "question_text": q["question_text"],
                "question_texts": q["effective_question_texts"],
            }
            chart_df = _fetch_adhoc_chart_data(q["df"], chart_theme, wave_number, schema, con)
            q["chart_png"] = _build_adhoc_chart(
                chart_df, chart_theme,
                is_continuous=q["is_continuous"],
                response_labels=response_labels,
            )
            q["chart_df"] = chart_df
            if q["chart_png"]:
                print(f"  Phase 2 chart built: {q['question_id']}")
        except Exception as e:
            q["chart_png"] = None
            q["chart_df"] = None
            print(f"  Phase 2 chart skipped {q['question_id']}: {e}")

    # ── Phase 3: Sonnet selects + writes spotlight (multimodal) ──────────────
    # Build the user message: text summaries + embedded chart images
    descriptions_block = "\n\n".join(
        f"[{q['question_id'].upper()}] interest={q['interest_score']}/5\n"
        f"Question: {q.get('question_text') or q['question_id'].upper()}\n"
        f"Description: {q.get('description') or '(no description)'}\n"
        f"Key finding: {q.get('key_finding') or '(none)'}\n"
        f"Routing: {q.get('routing_note') or 'all firms'}"
        for q in question_contexts
    )

    user_content: list = [
        {
            "type": "text",
            "text": (
                f"Topic: {theme['theme_label']}\n"
                f"Wave: {wave_number}\n\n"
                f"=== Question summaries ===\n{descriptions_block}\n\n"
                f"=== Charts (one per question, in order: "
                f"{', '.join(q['question_id'] for q in question_contexts)}) ==="
            ),
        }
    ]

    # Embed charts as images (Sonnet multimodal)
    for q in question_contexts:
        if q.get("chart_png"):
            user_content.append({
                "type": "image",
                "source": {
                    "type": "base64",
                    "media_type": "image/png",
                    "data": base64.b64encode(q["chart_png"]).decode(),
                },
            })

    user_content.append({
        "type": "text",
        "text": "Write the spotlight as specified. Return valid JSON only.",
    })

    result: dict = {}
    try:
        if anthropic_client is not None:
            model = _SONNET_MODEL
            resp = anthropic_client.messages.create(
                model=model,
                max_tokens=1000,
                system=_ADHOC_SPOTLIGHT_SYSTEM,
                messages=[{"role": "user", "content": user_content}],
            )
            raw = resp.content[0].text.strip()
            _track_cost(cost_tracker, model, _Usage(
                resp.usage.input_tokens, resp.usage.output_tokens,
            ))
        else:
            # Fallback: text-only Mistral Small
            model = "mistral-small-latest"
            text_only_msg = (
                f"Topic: {theme['theme_label']}\n\n"
                f"Question summaries:\n{descriptions_block}\n\n"
                "Write the spotlight as specified. Return valid JSON only."
            )
            resp = mistral_client.chat.complete(
                model=model,
                max_tokens=1000,
                messages=[
                    {"role": "system", "content": _ADHOC_SPOTLIGHT_SYSTEM},
                    {"role": "user", "content": text_only_msg},
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
        print(f"  Phase 3 Sonnet failed: {e}")
        return None

    # Optional dig-deeper: one follow-up SQL query if Sonnet requested it
    dig_deeper = result.get("dig_deeper")
    if dig_deeper and isinstance(dig_deeper, dict) and dig_deeper.get("sql"):
        try:
            dd_sql = dig_deeper["sql"].replace("{schema}", schema).replace("{wave_number}", str(wave_number))
            dd_df = con.execute(dd_sql).df()
            if not dd_df.empty:
                dd_table = dd_df.to_string(index=False, max_rows=40)
                print(f"  Dig deeper for {dig_deeper.get('question_id', '?')}: {len(dd_df)} rows")
                # Second Sonnet pass with the extra data
                follow_up_content = [
                    {"type": "text", "text": (
                        f"Here is the additional breakdown you requested:\n\n{dd_table}\n\n"
                        "Revise your JSON response if this changes any bullet or key finding. "
                        "Return the same JSON schema. Only update if the new data materially changes the story."
                    )}
                ]
                try:
                    if anthropic_client is not None:
                        resp2 = anthropic_client.messages.create(
                            model=_SONNET_MODEL,
                            max_tokens=1000,
                            system=_ADHOC_SPOTLIGHT_SYSTEM,
                            messages=[
                                {"role": "user", "content": user_content},
                                {"role": "assistant", "content": raw},
                                {"role": "user", "content": follow_up_content},
                            ],
                        )
                        raw2 = resp2.content[0].text.strip()
                        _track_cost(cost_tracker, _SONNET_MODEL, _Usage(
                            resp2.usage.input_tokens, resp2.usage.output_tokens,
                        ))
                        if raw2.startswith("```"):
                            raw2 = raw2.split("\n", 1)[-1].rsplit("```", 1)[0].strip()
                        result = json.loads(repair_json(raw2))
                        print("  Dig deeper pass complete — result updated")
                except Exception as e2:
                    print(f"  Dig deeper second pass failed ({e2}) — keeping original result")
        except Exception as e:
            print(f"  Dig deeper SQL failed ({e}) — skipping")

    bullets = result.get("bullets", [])
    if isinstance(bullets, str):
        bullets = [bullets]
    selected_qids = result.get("selected_question_ids", [q["question_id"] for q in question_contexts])
    print(f"  Phase 3: {len(bullets)} bullets, selected={selected_qids}")

    # ── Assemble chart_pngs from selected + all questions ────────────────────
    # Selected questions first (in order), then remaining ones (up to 3 total)
    q_by_id = {q["question_id"]: q for q in question_contexts}
    chart_pngs: list[bytes] = []
    for qid in selected_qids:
        if len(chart_pngs) >= 3:
            break
        q = q_by_id.get(qid)
        if q and q.get("chart_png"):
            chart_pngs.append(q["chart_png"])
    for q in question_contexts:
        if len(chart_pngs) >= 3:
            break
        if q["question_id"] not in selected_qids and q.get("chart_png"):
            chart_pngs.append(q["chart_png"])

    # ── Phase 4: critical review (Mistral Large, threshold ≥ 8) ─────────────
    review_scores: dict = {}
    review_passed = True
    if mistral_client is not None:
        try:
            # Full data for review: all question summaries + selected question data tables
            review_data_block = "\n\n".join(
                f"[{q['question_id'].upper()}]:\n{q['data_table']}"
                for q in question_contexts
                if q["question_id"] in selected_qids
            )
            review_text = (
                f"Topic: {theme['theme_label']}\n"
                f"Finding: {result.get('finding', '')}\n"
                "Bullets:\n" + "\n".join(f"  - {b}" for b in bullets) +
                f"\n\nCharts shown for: {', '.join(q['question_id'] for q in question_contexts if q.get('chart_png'))}\n"
                f"Data provided to the model:\n{review_data_block}"
            )
            review_model = "mistral-large-2512"
            review_resp = mistral_client.chat.complete(
                model=review_model,
                max_tokens=200,
                messages=[
                    {"role": "system", "content": _ADHOC_REVIEW_SYSTEM},
                    {"role": "user", "content": review_text},
                ],
            )
            review_raw = review_resp.choices[0].message.content.strip()
            if review_raw.startswith("```"):
                review_raw = review_raw.split("\n", 1)[-1].rsplit("```", 1)[0].strip()
            review_result = json.loads(repair_json(review_raw))
            if review_resp.usage:
                _track_cost(cost_tracker, review_model,
                            _Usage(review_resp.usage.prompt_tokens, review_resp.usage.completion_tokens))
            review_scores = {
                k: review_result.get(k, 10)
                for k in ("grounding", "coverage", "readability", "chart_alignment")
            }
            min_score = min(review_scores.values())
            review_passed = min_score >= 8
            verdict = "PASS" if review_passed else "WARN"
            print(
                f"  Phase 4 review: "
                + "  ".join(f"{k}={v}/10" for k, v in review_scores.items())
                + f"  → {verdict}"
            )
            if not review_passed:
                print(f"  Review reason: {review_result.get('reason', '')}")
        except Exception as e:
            print(f"  Phase 4 review failed ({e}) — skipping")

    question_descriptions = [
        {
            "question_id": q["question_id"],
            "question_text": q.get("question_text", ""),
            "description": q.get("description", ""),
            "interest_score": q.get("interest_score", 3),
            "key_finding": q.get("key_finding", ""),
        }
        for q in question_contexts
    ]

    return {
        "section_id": "adhoc_spotlight",
        "title": f"Special Focus: {theme['theme_label']}",
        "finding": result.get("finding", ""),
        "bullets": bullets,
        "chart_png": chart_pngs[0] if chart_pngs else None,
        "chart_pngs": chart_pngs,
        "theme_label": theme["theme_label"],
        "review_scores": review_scores,
        "review_passed": review_passed,
        "question_descriptions": question_descriptions,
    }


