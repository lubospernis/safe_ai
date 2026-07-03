"""Adhoc module spotlight: detect theme, build charts, generate LLM content."""

import json
import textwrap
from pathlib import Path

import anthropic
import pandas as pd
from json_repair import repair_json

from charts import _build_adhoc_chart
from cost import _Usage, _track_cost
from questionnaire import (
    build_response_label_context,
    fetch_adhoc_response_labels,
    questionnaire_url_for_wave,
)

_SONNET_MODEL = "claude-sonnet-4-6"

SQL_DIR = Path(__file__).parent / "sql"

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

ADHOC_CONTENT_SYSTEM = textwrap.dedent("""
    You are a concise economic analyst writing a "Special Focus" sidebar for a Slovakia
    SAFE survey report. Your task: write 2–4 short bullets (≤ 35 words each) comparing
    Slovak firms (SK) to the Euro Area (EA) on the topic indicated.

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
       {"finding": "One sentence headline ≤ 20 words",
        "bullets": ["bullet 1", ..., "bullet N"],
        "chart_sub_items": ["a", ...]}
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

_ADHOC_CHART_SQL_CONTINUOUS = """
SELECT
    country_code,
    sub_item,
    FLOOR(CAST(response_raw AS INTEGER) / 10) * 10 AS response_raw,
    ROUND(SUM(n_firms_wtd) / MAX(n_total_wtd) * 100, 1) AS pct_wtd,
    SUM(n_firms) AS n_firms
FROM {schema}.mart_safe__adhoc_responses
WHERE wave_number = {wave_number}
  AND module_id = '{module_id}'
  AND country_code IN ('SK', 'EA')
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
  AND country_code IN ('SK', 'EA')
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


def _fetch_question_texts(con, schema: str, module_ids: list[str]) -> str:
    """Return a formatted block of survey question texts for the given module IDs from ref_safe__annex."""
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
        coalesce_expr = ", ".join(wave_cols)
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

    primary_module_id = rows[0][0]

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
            pass

    # Fetch questionnaire PDF to get response code → label mappings
    questionnaire_url = questionnaire_url_for_wave(wave_number, con, schema)
    response_labels: dict[str, dict[int, str]] = {}
    if questionnaire_url:
        all_module_ids = [r[0] for r in rows]
        print(f"  Fetching questionnaire PDF for response labels: {questionnaire_url}")
        response_labels = fetch_adhoc_response_labels(questionnaire_url, all_module_ids)
        if response_labels:
            total_codes = sum(len(v) for v in response_labels.values())
            print(f"  Questionnaire parsed: {total_codes} response codes across {len(response_labels)} modules")
        else:
            print("  Questionnaire parse returned no labels — proceeding without them")
    else:
        print(f"  No questionnaire URL mapped for wave {wave_number} — proceeding without response labels")

    return {
        "module_id": primary_module_id,
        "theme_label": theme_label,
        "question_texts": question_texts,
        "questionnaire_url": questionnaire_url,
        "response_labels": response_labels,
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


def build_adhoc_spotlight(
    theme: dict,
    wave_number: int,
    con,
    schema: str,
    mistral_client,
    cost_tracker: dict,
    anthropic_client=None,
) -> dict | None:
    """Generate the adhoc spotlight section using a 3-phase agentic design.

    Phase 1 (Mistral Small): classify sub-items as interesting/skip.
    Phase 2 (Sonnet): write 2-4 bullets for interesting sub-items, return chart_sub_items.
    Phase 3 (Mistral Large): critical review — scores all dims; warns if any < 8.
    """
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

    df = df[df["response_raw"] >= 0].copy()
    if df.empty:
        return None

    is_cont = _is_continuous(df)
    if is_cont:
        df = df[df["response_raw"] <= 100].copy()

    full_table, data_note = _build_full_data_table(df)

    # Build response label context from questionnaire PDF (may be empty if not mapped)
    response_labels = theme.get("response_labels", {})
    all_module_ids = list({theme["module_id"]} | set(response_labels.keys()))
    response_label_ctx = build_response_label_context(response_labels, all_module_ids)

    # ── Phase 1: select interesting sub-items ────────────────────────────────
    interesting_subs, routing_notes = _select_interesting_sub_items(
        df, full_table, theme.get("question_texts", {}), theme["theme_label"],
        cost_tracker, mistral_client=mistral_client,
        response_label_ctx=response_label_ctx,
    )

    # Filter df to interesting sub-items for the writing prompt
    df_interesting = df[df["sub_item"].isin(interesting_subs)].copy()
    if df_interesting.empty:
        df_interesting = df  # fallback

    interesting_table, _ = _build_full_data_table(df_interesting)

    # Build question context
    question_ctx = ""
    if theme.get("question_texts"):
        question_ctx = "\n\nSurvey question texts (use for plain-language descriptions only):\n" + "\n".join(
            f"  Sub-item {k}: {v}" for k, v in theme["question_texts"].items()
            if k in interesting_subs
        )

    routing_ctx = ""
    if any(v for v in routing_notes.values()):
        routing_ctx = "\n\nRouting notes (reflect in bullets):\n" + "\n".join(
            f"  Sub-item {k}: {v}" for k, v in routing_notes.items()
            if k in interesting_subs and v
        )

    # Include response labels in the writing prompt if available
    label_ctx = ""
    if response_label_ctx:
        label_ctx = f"\n\n{response_label_ctx}"

    # ── Phase 2: write bullets ───────────────────────────────────────────────
    user_msg = (
        f"Topic: {theme['theme_label']}{question_ctx}{routing_ctx}{label_ctx}\n\n"
        f"Data (wave {wave_number}, SK vs EA, interesting sub-items only):\n"
        f"{interesting_table}{data_note}\n\n"
        "Write the finding, 2-4 bullets, and chart_sub_items as specified."
    )

    try:
        if anthropic_client is not None:
            model = _SONNET_MODEL
            resp = anthropic_client.messages.create(
                model=model,
                max_tokens=800,
                system=ADHOC_CONTENT_SYSTEM,
                messages=[{"role": "user", "content": user_msg}],
            )
            raw = resp.content[0].text.strip()
            _track_cost(cost_tracker, model, _Usage(
                resp.usage.input_tokens, resp.usage.output_tokens,
            ))
        else:
            model = "mistral-small-latest"
            resp = mistral_client.chat.complete(
                model=model,
                max_tokens=800,
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

    bullets = result.get("bullets", [])
    if isinstance(bullets, str):
        bullets = [bullets]
    chart_sub_items = result.get("chart_sub_items", interesting_subs)
    if not chart_sub_items:
        chart_sub_items = interesting_subs
    print(f"  Phase 2: {len(bullets)} bullets, chart_sub_items={chart_sub_items}")

    # ── Build chart for selected sub-items ───────────────────────────────────
    chart_png = None
    chart_df = None
    chart_df_filtered = None
    try:
        chart_df = _fetch_adhoc_chart_data(df, theme, wave_number, schema, con)
        chart_df_filtered = chart_df[chart_df["sub_item"].isin(chart_sub_items)]
        if chart_df_filtered.empty:
            chart_df_filtered = chart_df
        chart_png = _build_adhoc_chart(
            chart_df_filtered, theme, is_continuous=is_cont,
            response_labels=response_labels,
        )
        if chart_png:
            print(f"  Adhoc chart built ({len(chart_df_filtered)} rows, subs={list(chart_df_filtered['sub_item'].unique())})")
    except Exception as e:
        print(f"  Adhoc chart skipped: {e}")

    # ── Phase 3: critical review (Mistral Large, threshold ≥ 8) ─────────────
    review_scores: dict = {}
    review_passed = True
    if mistral_client is not None:
        try:
            review_text = (
                f"Topic: {theme['theme_label']}\n"
                f"Finding: {result.get('finding', '')}\n"
                f"Bullets:\n" + "\n".join(f"  - {b}" for b in bullets) +
                f"\n\nChart shows sub-items: {chart_sub_items}\n"
                f"Full data provided to the model:\n{interesting_table}{data_note}"
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
                f"  Phase 3 review: "
                + "  ".join(f"{k}={v}/10" for k, v in review_scores.items())
                + f"  → {verdict}"
            )
            if not review_passed:
                print(f"  Review reason: {review_result.get('reason', '')}")
                # If chart_alignment is the failing dimension, rebuild chart with all interesting subs
                if review_scores.get("chart_alignment", 10) < 8 and chart_df is not None:
                    try:
                        chart_df_all = chart_df[chart_df["sub_item"].isin(interesting_subs)]
                        if chart_df_all.empty:
                            chart_df_all = chart_df
                        chart_png = _build_adhoc_chart(
                            chart_df_all, theme, is_continuous=is_cont,
                            response_labels=response_labels,
                        )
                        print(f"  Chart rebuilt with all interesting subs {interesting_subs} to fix alignment")
                    except Exception as rebuild_err:
                        print(f"  Chart rebuild failed: {rebuild_err}")
        except Exception as e:
            print(f"  Phase 3 review failed ({e}) — skipping")

    section = {
        "section_id": "adhoc_spotlight",
        "title": f"Special Focus: {theme['theme_label']}",
        "finding": result.get("finding", ""),
        "bullets": bullets,
        "chart_png": chart_png,
        "theme_label": theme["theme_label"],
        "review_scores": review_scores,
        "review_passed": review_passed,
    }
    return section


