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

from charts import SK_LABELS, _build_adhoc_chart
from cost import _Usage, _track_cost
from db import ALLOWED_MART_TABLES, _WRITE_RE
from llm import NBS_STYLE_GUIDE, _check_numeric_grounding, get_shortened_questions
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

    GROUNDING RULES:
    - Every comparative claim (e.g. "SK higher", "broadly comparable", "SK leads") MUST cite
      the exact percentage from the data table. Do NOT generalise across codes — if SK code 2
      is higher but code 3 is lower, say so explicitly; do NOT say "SK leads overall."
    - Sub-item labels (e.g. "In your country", "In Germany, France and Italy") describe the
      geographic scope of the question — they do NOT indicate the question is about exports or
      trade. Always use the full question text to determine what is being measured.
    - "broadly comparable" is only acceptable when ALL gaps are < 3pp. If any code differs by
      ≥ 5pp, name the specific code and gap rather than using a summary phrase.

    Return JSON only (no markdown):
    {
      "description": "<1-2 sentence plain-English summary — cite specific codes and percentages>",
      "interest_score": <integer 1-5>,
      "key_finding": "<the single most striking number or gap, e.g. 'SK 30% vs EA 18% on code 3'>",
      "routing_note": "<who was asked this question, e.g. 'all firms' or 'firms using AI', or '' if unknown>"
    }

    interest_score rubric:
    1 = SK and EA nearly identical (< 3pp gap on ALL codes/values)
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
    1. For EVERY question provided, write 1–3 tight bullets (≤ 35 words each), grounded only
       in that question's own data. Do not skip any question — even a low-interest question
       gets at least one bullet stating the key finding (or that SK and EA are broadly similar).
    2. Write one overall finding sentence (≤ 20 words) naming the theme and most striking gap
       across all questions.
    3. If a specific breakdown would meaningfully strengthen ONE finding, you may request it.

    GROUNDING RULES — CRITICAL:
    - Every comparative claim ("SK leads EA", "broadly comparable", "higher/lower than EA")
      MUST cite the exact number from the key_finding or description for that question.
    - Do NOT generalise across response codes. If SK code 2 (pilot use) is higher but code 3
      (moderate use) is lower, state both explicitly. Do NOT say "SK leads in AI use overall."
    - "broadly comparable" is only acceptable when ALL gaps are < 3pp. If the key_finding
      shows a gap ≥ 5pp, name the specific code and percentage — never summarise it away.
    - Sub-item labels like "In your country" / "In Germany, France and Italy" describe
      geographic scope of the question, not the subject matter. Never infer the question is
      about exports or trade from these labels alone — use the question text.

    Return valid JSON only (no markdown fences):
    {{
      "finding": "<one sentence headline>",
      "bullets": {{
        "<question_id_1>": ["bullet 1", "bullet 2"],
        "<question_id_2>": ["bullet 3"],
        "<question_id_3>": ["bullet 4"]
      }},
      "dig_deeper": {{
        "question_id": "qa1",
        "sql": "SELECT country_code, sub_item, response_raw, pct_wtd, n_firms FROM {{schema}}.mart_safe__adhoc_responses WHERE wave_number = {{wave_number}} AND module_id = 'qa1' AND country_code IN ('SK','EA') AND response_raw >= 0 ORDER BY sub_item, country_code, response_raw"
      }} or null
    }}

    The "bullets" value is a dict keyed by question_id — you MUST include one key for every
    question_id given in the question summaries below, with no exceptions.
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
    # Sanitize module_id: allow only word characters to prevent SQL injection
    safe_module_id = re.sub(r"[^\w]", "", theme["module_id"])
    sql = template.format(schema=schema, wave_number=wave_number, module_id=safe_module_id)
    return con.execute(sql).df()


# Pick-order sub-item labels: some adhoc questions store the microdata's internal
# "first choice"/"second choice" answer slots as sub_item 'a'/'b' (e.g. QA2's "please
# indicate the TWO main reasons" questions), but the annex has no representation of
# these at all — they are a microdata encoding artifact, not a real survey sub-item,
# so mart_safe__annex_items will never have a row for them. Applied only when a
# module's chart sub-items are exactly {'a', 'b'} and the annex items mart has no
# row for that module — see _resolve_item_labels().
_PICK_ORDER_LABELS: dict[str, str] = {"a": "First reason", "b": "Second reason"}

# Firm-size-split sub-item labels: some continuous modules ask the same item (a/b)
# separately for SME-sized vs large-firm peers, encoded as a microdata-only suffix
# (e.g. QB2's a_g3/a_g4/b_g3/b_g4 — g3 = SME peers, g4 = large-firm peers). Like
# _PICK_ORDER_LABELS, the annex has no representation of the g3/g4 split at all —
# these labels are mirrored from dbt_project/models/marts/mart_safe__ai_adoption.sql,
# the one place this split is already documented. Applied only when a module's chart
# sub-items are exactly this set and the annex items mart has no row for the module.
_FIRM_SIZE_SPLIT_LABELS: dict[str, str] = {
    "a_g3": "SME peers — your country",
    "a_g4": "Large firm peers — your country",
    "b_g3": "SME peers — DE / FR / IT",
    "b_g4": "Large firm peers — DE / FR / IT",
}


def _fetch_annex_labels(
    con, schema: str, module_ids: list[str], period_suffix: str | None,
) -> dict[str, dict]:
    """Fetch question/answer/item text for module_ids from the annex reference marts
    (mart_safe__annex_questions/_answers/_items — see dbt_project/models/marts).

    Returns {module_id.lower(): {"question_text": str, "answers": {code: label},
    "items": {item_key: label}}}. Always scoped to period_suffix when given, since
    adhoc module_ids are reused across eras for entirely different questions
    (confirmed by inspecting the raw annex — e.g. qa1 meant a price-expectations
    question in 2021H2, AI adoption in 2025Q4).
    """
    result: dict[str, dict] = {m.lower(): {"question_text": "", "answers": {}, "items": {}} for m in module_ids}
    if not module_ids:
        return result
    mids = [m.lower() for m in module_ids]
    placeholders = ", ".join(f"'{m}'" for m in mids)
    period_filter = f"AND question_period = '{period_suffix}'" if period_suffix else "AND question_period IS NULL"

    try:
        rows = con.execute(f"""
            SELECT module_id, question_text FROM {schema}.mart_safe__annex_questions
            WHERE module_id IN ({placeholders}) {period_filter}
        """).fetchall()
        for mid, qtext in rows:
            if qtext:
                result.setdefault(mid, {"question_text": "", "answers": {}, "items": {}})["question_text"] = qtext
    except Exception:
        pass

    try:
        rows = con.execute(f"""
            SELECT module_id, answer_code, answer_label FROM {schema}.mart_safe__annex_answers
            WHERE module_id IN ({placeholders}) {period_filter}
        """).fetchall()
        for mid, code, label in rows:
            if label:
                result.setdefault(mid, {"question_text": "", "answers": {}, "items": {}})["answers"][code] = label
    except Exception:
        pass

    try:
        rows = con.execute(f"""
            SELECT module_id, item_key, item_label FROM {schema}.mart_safe__annex_items
            WHERE module_id IN ({placeholders}) {period_filter}
        """).fetchall()
        for mid, key, label in rows:
            if label:
                result.setdefault(mid, {"question_text": "", "answers": {}, "items": {}})["items"][key] = label
    except Exception:
        pass

    return result


_BRACKETED_INSTRUCTION_RE = re.compile(r"\[.*?\]", re.DOTALL)
_ITEM_LETTER_PREFIX_RE = re.compile(r"^[a-z]\)\s*", re.IGNORECASE)
_PANEL_LABEL_LEN_THRESHOLD = 40  # chars — above this, try an LLM shortener too


def _clean_panel_label(text: str) -> str:
    """Strip interviewer instructions and item-letter prefixes from a raw annex
    item label before it's used as a chart panel title.

    Annex item text is written for the interviewer script, not a chart reader —
    e.g. QB1's sub-item b is "b)   In Germany, France and Italy [READ IF
    NECESSARY: Please provide your best estimate...]". Blind character
    truncation (as charts.py's ax.set_title(panel_title[:55]) does) cuts mid
    bracket and produces a garbled title; this strips the bracketed instruction
    and the leading item-letter prefix first so truncation (if still needed)
    lands on a clean sentence.
    """
    cleaned = _BRACKETED_INSTRUCTION_RE.sub("", text)
    cleaned = _ITEM_LETTER_PREFIX_RE.sub("", cleaned)
    return re.sub(r"\s+", " ", cleaned).strip()


def _shorten_long_panel_labels(
    labels: dict[str, str], mistral_client, cost_tracker: dict | None = None,
) -> dict[str, str]:
    """Deterministically clean every label, then LLM-shorten any still long
    after cleanup. Best-effort — falls back to the cleaned (not LLM-shortened)
    text on any failure, never raises."""
    cleaned = {k: _clean_panel_label(v) for k, v in labels.items()}
    if mistral_client is None:
        return cleaned
    from llm import _shorten_panel_label_llm  # local import: avoid import cycle at module load
    result = dict(cleaned)
    for key, text in cleaned.items():
        if len(text) <= _PANEL_LABEL_LEN_THRESHOLD:
            continue
        try:
            r = _shorten_panel_label_llm(text, mistral_client)
            if r.get("short_label"):
                result[key] = r["short_label"]
            if cost_tracker is not None and "_usage" in r:
                u = r["_usage"]
                _track_cost(cost_tracker, "mistral-small-latest", _Usage(u["input"], u["output"]))
        except Exception:
            pass
    return result


def _resolve_item_labels(module_id: str, sub_items: list[str], annex_items: dict[str, str]) -> dict[str, str]:
    """Resolve sub-item -> label for one module. annex_items is already the
    module's own {item_key: label} dict from _fetch_annex_labels().

    Falls back to _PICK_ORDER_LABELS or _FIRM_SIZE_SPLIT_LABELS when annex_items
    doesn't actually cover this module's real chart sub_items (either annex_items is
    empty, OR it has rows but for different keys — e.g. QB2's annex only has bare
    "a"/"b" item rows, but its real chart sub_items are "a_g3"/"a_g4"/"b_g3"/"b_g4",
    a firm-size split with no annex representation at all) AND sub_items look exactly
    like one of those known microdata-only encoding patterns — this is the fix for
    QA2/QB2-style questions where the chart previously showed bare "a"/"b" or
    "a_g3"/"a_g4" as panel titles.
    """
    if annex_items and set(sub_items) <= set(annex_items):
        return dict(annex_items)
    if set(sub_items) and set(sub_items) <= set(_PICK_ORDER_LABELS):
        return dict(_PICK_ORDER_LABELS)
    if set(sub_items) and set(sub_items) <= set(_FIRM_SIZE_SPLIT_LABELS):
        return dict(_FIRM_SIZE_SPLIT_LABELS)
    if annex_items:
        return dict(annex_items)
    return {}


_MODULE_ID_RE = re.compile(r"^([a-z]+?)(\d*)([a-z]*)$")


def _module_sort_key(module_id: str) -> tuple[str, int, str]:
    """Natural sort key for module IDs: 'qa1' < 'qa2' < ... < 'qa10' < 'qb1' < 'qb2',
    rather than plain string sort (which would put 'qa10' before 'qa2')."""
    m = _MODULE_ID_RE.match(module_id.lower())
    if not m:
        return (module_id.lower(), 0, "")
    prefix, digits, suffix = m.groups()
    return (prefix, int(digits) if digits else 0, suffix)


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

    # Fetch question/answer/item text for every module in the wave from the annex
    # reference marts, scoped to this wave's period_suffix (see mart_safe__annex_questions/
    # _answers/_items in dbt_project/models/marts — replaces the live
    # information_schema.columns + COALESCE introspection this function used to run).
    annex_labels = _fetch_annex_labels(con, schema, all_module_ids_wave, period_suffix)
    module_question_texts: dict[str, str] = {
        mid: data["question_text"] for mid, data in annex_labels.items() if data["question_text"]
    }

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

    # Sub-item -> label mapping for the primary module, sourced from the annex items
    # mart (with the pick-order fallback for questions like QA2 that have no annex
    # item representation at all — see _resolve_item_labels()).
    primary_question_text: str | None = module_question_texts.get(primary_module_id)
    primary_annex_items = annex_labels.get(primary_module_id, {}).get("items", {})
    question_texts: dict[str, str] = _resolve_item_labels(
        primary_module_id, list(primary_annex_items.keys()), primary_annex_items,
    )

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
        "module_question_texts": module_question_texts,
        "questionnaire_url": questionnaire_url,
        "response_labels": response_labels,
        "sub_item_labels": sub_item_labels,
        "sibling_modules": sibling_modules,
        "period_suffix": period_suffix,
        # Per-module {question_text, answers, items} from the annex reference marts —
        # covers every module in the wave, not just the primary one (see _fetch_annex_labels).
        "annex_labels": annex_labels,
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
    annex_labels = theme.get("annex_labels", {})
    # Natural question order (QA1, QA2, ..., QB1, QB2, ...) for readability — theme["module_id"]
    # is chosen by a topic-classification heuristic (e.g. for theme labeling) and is not
    # necessarily QA1, so render order must not depend on which module Sonnet/heuristics
    # picked as "primary".
    all_question_ids = sorted([theme["module_id"]] + sibling_modules, key=_module_sort_key)
    sql_template = (SQL_DIR / "adhoc_spotlight.sql").read_text()

    # ── Phase 0: enumerate questions, fetch data ──────────────────────────────
    question_contexts: list[dict] = []
    for qid in all_question_ids:
        try:
            safe_qid = re.sub(r"[^\w]", "", qid)
            sql = sql_template.format(wave_number=wave_number, module_id=safe_qid, schema=schema)
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

        # Question text: prefer module_question_texts (all modules, from annex), then
        # primary_question_text (primary module only), then empty. Strip leading bullet chars.
        qt = (theme.get("module_question_texts") or {}).get(qid, "")
        if not qt and qid == theme["module_id"]:
            qt = theme.get("primary_question_text") or ""
        qt = re.sub(r"^[-–•]\s*", "", qt).strip()

        # Sub-item -> label mapping for THIS question's own module (each sibling module
        # gets its own annex item labels, not the primary module's — a prior bug here
        # applied the primary module's item labels uniformly to every sibling question).
        # Merges PDF-parsed sub_item_labels (fallback) with the annex items mart
        # (preferred), then falls back to a pick-order label ("First reason"/"Second
        # reason") only when the annex has no item rows at all for this module and its
        # sub-items are exactly the {'a','b'} pick-order pattern (e.g. QA2).
        module_sub_items = sorted(df["sub_item"].unique().tolist()) if "sub_item" in df.columns else []
        module_annex_items = annex_labels.get(qid.lower(), {}).get("items", {})
        effective_qt: dict[str, str] = dict(sub_item_labels.get(qid.lower(), {}))
        effective_qt.update(
            _resolve_item_labels(qid, module_sub_items, module_annex_items)
        )
        # Clean interviewer-instruction brackets / item-letter prefixes out of every
        # panel label before it reaches chart-building, and LLM-shorten anything still
        # long after cleanup (see _shorten_long_panel_labels docstring).
        effective_qt = _shorten_long_panel_labels(effective_qt, mistral_client, cost_tracker)

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

    # Shortened chart captions: paraphrase each question's official annex wording
    # into a short, natural caption (e.g. QA3's long [IF ...] conditional wording
    # becomes "Why aren't you using AI more?") via the same mechanism the main
    # report uses for its section charts (get_shortened_questions() in llm.py).
    # Cached in ref_safe__chart_question_captions, keyed by question_id.
    chart_question_captions: dict[str, str] = {}
    try:
        pseudo_sections = [
            {"id": q["question_id"], "question_ids": [q["question_id"]]}
            for q in question_contexts
        ]
        annex_question_texts = {
            q["question_id"].lower(): q["question_text"]
            for q in question_contexts if q.get("question_text")
        }
        chart_question_captions = get_shortened_questions(
            pseudo_sections, annex_question_texts, con, schema, mistral_client, cost_tracker,
        )
    except Exception as e:
        print(f"  Chart question captions failed (non-fatal): {e}")

    # ── Phase 2: build charts for all questions ───────────────────────────────
    for q in question_contexts:
        try:
            chart_theme = {
                **theme,
                "module_id": q["question_id"],
                "question_text": q["question_text"],
                "question_texts": q["effective_question_texts"],
                "chart_question": chart_question_captions.get(q["question_id"], ""),
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

    # Compute a sign convention note from the data types present
    has_continuous = any(q["is_continuous"] for q in question_contexts)
    has_categorical = any(not q["is_continuous"] for q in question_contexts)
    if has_continuous and has_categorical:
        sign_note = (
            "Sign convention: continuous questions report weighted mean % (0–100 scale). "
            "Categorical questions report pct_wtd = % of firms selecting each response code. "
            "Do NOT write 'net X%' for these questions — use '% of firms' instead."
        )
    elif has_continuous:
        sign_note = (
            "Sign convention: response_raw is a numeric % estimate (0–100 scale). "
            "The data table shows weighted mean and median. "
            "Do NOT write 'net X%' — use '% of firms estimated' or 'average estimate of X%'."
        )
    else:
        sign_note = (
            "Sign convention: pct_wtd is the weighted % of firms selecting each response code. "
            "Response codes are integers (e.g. 1=yes, 2=no, or a Likert scale). "
            "Do NOT write 'net X%' — cite the specific code and its share."
        )

    user_content.append({
        "type": "text",
        "text": f"{sign_note}\n\nWrite the spotlight as specified. Return valid JSON only.",
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
            # Fallback: Pixtral (vision-capable) when no Anthropic client is passed —
            # keeps chart images in the prompt instead of silently dropping to text-only.
            model = "pixtral-12b-2409"
            mistral_content: list = []
            for block in user_content:
                if block["type"] == "text":
                    mistral_content.append({"type": "text", "text": block["text"]})
                elif block["type"] == "image":
                    b64 = block["source"]["data"]
                    media_type = block["source"]["media_type"]
                    mistral_content.append({
                        "type": "image_url",
                        "image_url": {"url": f"data:{media_type};base64,{b64}"},
                    })
            resp = mistral_client.chat.complete(
                model=model,
                max_tokens=1000,
                messages=[
                    {"role": "system", "content": _ADHOC_SPOTLIGHT_SYSTEM},
                    {"role": "user", "content": mistral_content},
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
            # Security: apply same write-keyword and table-whitelist checks as _run_query_tool()
            if _WRITE_RE.search(dd_sql):
                print("  [SECURITY] dig_deeper SQL contains write keywords — skipped")
                dig_deeper = None
            else:
                tables_used = set(re.findall(r'FROM\s+(?:\w+\.)?(\w+)', dd_sql, re.IGNORECASE))
                disallowed = tables_used - ALLOWED_MART_TABLES - {""}
                if disallowed:
                    print(f"  [SECURITY] dig_deeper SQL references non-whitelisted tables {disallowed} — skipped")
                    dig_deeper = None
        except Exception:
            dig_deeper = None
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

    # All questions get a section now (no 1-3 selection) — order matches Phase 0
    # enumeration. "selected_question_ids" is kept in the return dict under the same
    # key for backward compatibility with html_builder.py, but it now always equals
    # every question_id in question_contexts.
    all_qids = [q["question_id"] for q in question_contexts]
    raw_bullets = result.get("bullets", {})
    if isinstance(raw_bullets, dict):
        bullets_by_question: dict[str, list[str]] = {
            qid: (v if isinstance(v, list) else [v])
            for qid, v in raw_bullets.items()
        }
    elif isinstance(raw_bullets, list):
        # Backward-compat: a flat list with no per-question keys — attribute it all
        # to the first question rather than dropping it.
        bullets_by_question = {all_qids[0]: [b for b in raw_bullets if isinstance(b, str)]} if all_qids else {}
    else:
        bullets_by_question = {}

    # Any question the model didn't return bullets for still gets a section — fall
    # back to its own key_finding from Phase 1 so no question is ever silently empty.
    for qid in all_qids:
        if not bullets_by_question.get(qid):
            q = next((qc for qc in question_contexts if qc["question_id"] == qid), None)
            fallback = q.get("key_finding") or q.get("description") if q else ""
            bullets_by_question[qid] = [fallback] if fallback else []

    selected_qids = all_qids
    bullets: list[str] = [b for qid in selected_qids for b in bullets_by_question.get(qid, [])]
    print(f"  Phase 3: {len(bullets)} bullets across {len(selected_qids)} questions: {selected_qids}")

    # ── Programmatic grounding check (monitoring-only, mirrors llm.py) ────────
    grounding_warnings: list[str] = []
    for q in question_contexts:
        q_bullets = bullets_by_question.get(q["question_id"], [])
        if not q_bullets:
            continue
        value_cols = ["pct_wtd"] if not q["is_continuous"] else ["response_raw"]
        warns = _check_numeric_grounding(q_bullets, q["df"], value_cols)
        for w in warns:
            grounding_warnings.append(f"[{q['question_id']}] {w}")
    if grounding_warnings:
        print(f"  [GROUNDING WARN] {len(grounding_warnings)} warning(s):")
        for w in grounding_warnings:
            print(f"    {w}")

    # ── Assemble chart_pngs for every question (no cap) ───────────────────────
    chart_pngs: list[bytes] = []
    chart_rebuild_specs: list[dict] = []  # same order as chart_pngs — for SK chart rebuild
    for q in question_contexts:
        if q.get("chart_png"):
            chart_pngs.append(q["chart_png"])
            chart_rebuild_specs.append(q)

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
        "bullets_by_question": bullets_by_question,
        "selected_question_ids": selected_qids,
        "chart_png": chart_pngs[0] if chart_pngs else None,
        "chart_pngs": chart_pngs,
        "theme_label": theme["theme_label"],
        "review_scores": review_scores,
        "review_passed": review_passed,
        "grounding_warnings": grounding_warnings,
        "question_descriptions": question_descriptions,
        # Ingredients for rebuild_adhoc_charts_sk() — not cache-serializable (holds
        # DataFrames), so callers must rebuild SK charts in the same process run
        # before this dict is persisted/cached, or skip if reloading from cache.
        "_chart_rebuild_specs": chart_rebuild_specs,
        "_response_labels": response_labels,
    }


def rebuild_adhoc_charts_en(
    chart_rebuild_specs: list[dict],
    response_labels: dict,
    mistral_client,
    cost_tracker: dict | None = None,
    theme_label: str = "",
) -> list[bytes]:
    """Re-render adhoc spotlight charts in English from cached chart_rebuild_specs,
    without re-running any LLM description/bullet/review phase.

    Used by run_adhoc_report.py's --rebuild-charts-only dev flag: lets a chart- or
    label-formatting change (e.g. panel-label cleanup) be verified by rebuilding just
    the chart PNGs from a previous run's cached DataFrames, instead of re-paying for
    every LLM phase. Falls back to the original chart_png for any question that fails
    to rebuild, so a bug in the new chart code never drops a chart entirely (surfaces
    as a printed warning instead)."""
    pngs: list[bytes] = []
    for q in chart_rebuild_specs:
        try:
            qid = q["question_id"]
            effective_qt = _shorten_long_panel_labels(
                dict(q.get("effective_question_texts") or {}), mistral_client, cost_tracker,
            )
            chart_theme = {
                "module_id": qid,
                "question_text": q.get("question_text", ""),
                "question_texts": effective_qt,
                "theme_label": theme_label or qid,
            }
            png = _build_adhoc_chart(
                q["chart_df"], chart_theme,
                is_continuous=q["is_continuous"],
                response_labels=response_labels,
            )
            pngs.append(png if png else q["chart_png"])
        except Exception as e:
            print(f"  Chart rebuild failed for {q.get('question_id', '?')} — keeping cached chart: {e}")
            pngs.append(q["chart_png"])
    return pngs


def rebuild_adhoc_charts_sk(
    chart_rebuild_specs: list[dict],
    sk_question_texts: dict,
    response_labels: dict,
    theme_label_sk: str = "",
) -> list[bytes]:
    """Re-render adhoc spotlight charts with Slovak labels/question text, in the same
    order as the original chart_pngs list. Falls back to the original English chart_png
    for any question that fails to rebuild, so a translation hiccup never drops a chart."""
    sk_pngs: list[bytes] = []
    for q in chart_rebuild_specs:
        try:
            qid = q["question_id"]
            # sk_question_texts is keyed by top-level question id (from translate_to_slovak's
            # annex_questions payload) — there's no per-sub-item SK translation available, so
            # every sub-item panel falls back to the same translated top-level question text.
            translated_qt = sk_question_texts.get(qid.lower(), "")
            sk_effective_qt = {k: translated_qt for k in (q.get("effective_question_texts") or {})} \
                if translated_qt else dict(q.get("effective_question_texts") or {})
            chart_theme = {
                "module_id": qid,
                "question_text": translated_qt or q.get("question_text", ""),
                "question_texts": sk_effective_qt,
                "theme_label": theme_label_sk or qid,
            }
            png = _build_adhoc_chart(
                q["chart_df"], chart_theme,
                is_continuous=q["is_continuous"],
                response_labels=response_labels,
                labels=SK_LABELS,
            )
            sk_pngs.append(png if png else q["chart_png"])
        except Exception as e:
            print(f"  SK chart rebuild failed for {q.get('question_id', '?')} — keeping English chart: {e}")
            sk_pngs.append(q["chart_png"])
    return sk_pngs


