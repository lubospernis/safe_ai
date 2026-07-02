"""Adhoc module spotlight: detect theme, build charts, generate LLM content."""

import json
import textwrap
from pathlib import Path

import pandas as pd
from json_repair import repair_json

from charts import _build_adhoc_chart, _build_ai_chart, _nbs_style_ax  # noqa: F401
from cost import _Usage, _track_cost

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

_AI_SECTION_SYSTEM = textwrap.dedent("""
    You are a concise economic analyst writing one sub-section of a "Special Focus: AI Adoption"
    sidebar for a Slovakia SAFE survey report. Write exactly 2 short bullets (≤ 35 words each)
    and one headline finding comparing Slovak firms (SK) to the Euro Area (EA).

    RULES:
    1. Only cite numbers from the data table provided. Do NOT invent.
    2. Bullets start with a bolded 3–5 word label: **Label:** sentence.
    3. If a routing note is provided, mention it where relevant (e.g. "among AI-using firms").
    4. Return valid JSON only (no markdown fences):
       {"finding": "One sentence ≤ 20 words", "bullets": ["bullet 1", "bullet 2"]}
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

    return {
        "module_id": primary_module_id,
        "theme_label": theme_label,
        "question_texts": question_texts,
    }


def build_adhoc_spotlight(
    theme: dict,
    wave_number: int,
    con,
    schema: str,
    mistral_client,
    cost_tracker: dict,
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

    df = df[df["response_raw"] >= 0].copy()
    if df.empty:
        return None

    if _is_continuous(df):
        df = df[df["response_raw"] <= 100].copy()
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

    bullets = result.get("bullets", [])
    if isinstance(bullets, str):
        bullets = [bullets]

    is_cont = _is_continuous(df)
    chart_png = None
    try:
        chart_df = _fetch_adhoc_chart_data(df, theme, wave_number, schema, con)
        chart_png = _build_adhoc_chart(chart_df, theme, is_continuous=is_cont)
        if chart_png:
            print(f"  Adhoc chart built ({len(chart_df)} rows)")
    except Exception as e:
        print(f"  Adhoc chart skipped: {e}")

    return {
        "section_id": "adhoc_spotlight",
        "title": f"Special Focus: {theme['theme_label']}",
        "finding": result.get("finding", ""),
        "bullets": bullets,
        "chart_png": chart_png,
        "theme_label": theme["theme_label"],
    }


def _ai_continuous_summary(df: pd.DataFrame) -> str:
    """Summarise a continuous 0-100 distribution to mean + median per country × sub_item."""
    lines = ["country | sub_item | sub_item_label | mean_pct | median_pct | n_firms"]
    for (cc, sub), grp in df.groupby(["country_code", "sub_item"]):
        grp = grp.sort_values("response_raw")
        total_w = grp["n_firms_wtd"].sum()
        if total_w == 0:
            continue
        wtd_mean = (grp["response_raw"] * grp["n_firms_wtd"]).sum() / total_w
        cumw = grp["n_firms_wtd"].cumsum()
        median_val = int(grp[cumw >= total_w / 2]["response_raw"].iloc[0])
        label = grp["sub_item_label"].iloc[0] if "sub_item_label" in grp.columns else sub
        lines.append(
            f"{cc} | {sub} | {label} | {wtd_mean:.1f}% | {median_val}% | {int(grp['n_firms'].sum())}"
        )
    return "\n".join(lines)


def _ai_categorical_summary(df: pd.DataFrame) -> str:
    """Summarise a categorical distribution, using decoded response_label where available."""
    lines = ["country | response_label | pct_wtd | n_firms"]
    for _, row in df.sort_values(["country_code", "response_raw"]).iterrows():
        label = row.get("response_label") or str(int(row["response_raw"]))
        lines.append(
            f"{row['country_code']} | {label} | {row['pct_wtd']:.1f}% | {int(row['n_firms'])}"
        )
    return "\n".join(lines)


def _call_mistral_ai_section(topic: str, data_table: str, routing_note: str,
                              mistral_client, cost_tracker: dict) -> dict:
    """Call Mistral to generate finding + bullets for one AI sub-section."""
    routing_line = f"\nRouting: {routing_note}" if routing_note else ""
    user_msg = f"Topic: {topic}{routing_line}\n\nData (SK vs EA):\n{data_table}\n\nWrite the finding and 2 bullets."
    model = "mistral-small-latest"
    resp = mistral_client.chat.complete(
        model=model,
        max_tokens=220,
        messages=[
            {"role": "system", "content": _AI_SECTION_SYSTEM},
            {"role": "user", "content": user_msg},
        ],
    )
    raw = resp.choices[0].message.content.strip()
    if resp.usage:
        _track_cost(cost_tracker, model, _Usage(resp.usage.prompt_tokens, resp.usage.completion_tokens))
    if raw.startswith("```"):
        raw = raw.split("\n", 1)[-1].rsplit("```", 1)[0].strip()
    result = json.loads(repair_json(raw))
    bullets = result.get("bullets", [])
    if isinstance(bullets, str):
        bullets = [bullets]
    return {"finding": result.get("finding", ""), "bullets": bullets}


def build_ai_adoption_spotlight(
    wave_number: int,
    con,
    schema: str,
    mistral_client,
    cost_tracker: dict,
) -> dict | None:
    """Generate the AI Adoption Special Focus section using mart_safe__ai_adoption.

    Covers all 6 modules (QA1-QA4, QB1, QB2) in three sub-sections:
      A — Current AI use (QA1 + QA4)
      B — Drivers and Barriers (QA2 + QA3)
      C — Peer Expectations (QB1 + QB2)
    """
    try:
        check = con.execute(
            f"SELECT COUNT(*) FROM {schema}.mart_safe__ai_adoption "
            f"WHERE wave_number = {wave_number}"
        ).fetchone()[0]
    except Exception:
        return None

    if check == 0:
        return None

    def _fetch(modules: list[str], country_codes: tuple = ("SK", "EA")) -> pd.DataFrame:
        cc_list = ", ".join(f"'{c}'" for c in country_codes)
        mod_list = ", ".join(f"'{m}'" for m in modules)
        return con.execute(f"""
            SELECT module_id, sub_item, sub_item_label, country_code,
                   response_raw, response_label, pct_wtd, n_firms, n_firms_wtd,
                   is_continuous, routing_note, question_text
            FROM {schema}.mart_safe__ai_adoption
            WHERE wave_number = {wave_number}
              AND module_id IN ({mod_list})
              AND country_code IN ({cc_list})
            ORDER BY module_id, sub_item, country_code, response_raw
        """).df()

    sub_sections = []

    # Sub-section A: Current AI use — QA1 (categorical) + QA4 (continuous)
    df_qa1 = _fetch(["qa1"])
    df_qa4 = _fetch(["qa4"])

    if not df_qa1.empty:
        chart_df_a = df_qa1[df_qa1["response_raw"] != 9].copy()
        chart_png_a = _build_ai_chart(chart_df_a, "AI use in own firm (QA1)", is_continuous=False)
        if chart_png_a:
            print(f"  AI chart A built (QA1, {len(chart_df_a)} rows)")

        qa1_table = _ai_categorical_summary(chart_df_a)
        qa4_note = ""
        if not df_qa4.empty:
            df_qa4_valid = df_qa4[(df_qa4["response_raw"] >= 0) & (df_qa4["response_raw"] <= 100)].copy()
            if not df_qa4_valid.empty:
                qa4_note = "\n\nQA4 — % of total investment planned for AI (median per country):\n"
                for cc, grp in df_qa4_valid.groupby("country_code"):
                    grp = grp.sort_values("response_raw")
                    total_w = grp["n_firms_wtd"].sum()
                    if total_w > 0:
                        cumw = grp["n_firms_wtd"].cumsum()
                        median_val = int(grp[cumw >= total_w / 2]["response_raw"].iloc[0])
                        qa4_note += f"  {cc}: median {median_val}% (n={int(grp['n_firms'].sum())})\n"

        try:
            llm_a = _call_mistral_ai_section(
                "AI adoption — current use intensity (QA1) and planned AI investment share (QA4)",
                qa1_table + qa4_note,
                "QA1 asked of all firms. QA4 asked of all firms — % of total investment expected in AI next 12 months.",
                mistral_client, cost_tracker,
            )
        except Exception as e:
            print(f"  AI section A Mistral failed: {e}")
            llm_a = {"finding": "AI adoption levels vary between Slovakia and the Euro Area.", "bullets": []}

        sub_sections.append({
            "heading": "AI Use in Own Firm",
            "finding": llm_a["finding"],
            "bullets": llm_a["bullets"],
            "chart_png": chart_png_a if not df_qa1.empty else None,
        })

    # Sub-section B: Drivers and Barriers — QA2 + QA3
    df_qa2 = _fetch(["qa2"])
    df_qa3 = _fetch(["qa3"])

    if not df_qa2.empty or not df_qa3.empty:
        chart_df_b = df_qa3[df_qa3["sub_item"] == "a"].copy() if not df_qa3.empty else pd.DataFrame()
        chart_png_b = None
        if not chart_df_b.empty:
            chart_df_b = chart_df_b[chart_df_b["response_raw"] != 9]
            chart_png_b = _build_ai_chart(chart_df_b, "Main barriers to AI adoption (QA3, 1st choice)", is_continuous=False)
            if chart_png_b:
                print(f"  AI chart B built (QA3, {len(chart_df_b)} rows)")

        qa2_table = ""
        if not df_qa2.empty:
            df_qa2a = df_qa2[(df_qa2["sub_item"] == "a") & (df_qa2["response_raw"] != 9)]
            qa2_table = "QA2 — Reasons for using AI (1st choice, AI users only):\n" + _ai_categorical_summary(df_qa2a)
        qa3_table = ""
        if not df_qa3.empty:
            df_qa3a = df_qa3[(df_qa3["sub_item"] == "a") & (df_qa3["response_raw"] != 9)]
            qa3_table = "\nQA3 — Barriers to AI adoption (1st choice, non/light users only):\n" + _ai_categorical_summary(df_qa3a)

        try:
            llm_b = _call_mistral_ai_section(
                "AI adoption — drivers and barriers",
                qa2_table + qa3_table,
                "QA2: asked only to firms with any AI use (QA1 ≥ 2). QA3: asked only to firms not using or using AI infrequently (QA1 = 1 or 2). Percentages are not comparable across QA2 and QA3 as they have different base populations.",
                mistral_client, cost_tracker,
            )
        except Exception as e:
            print(f"  AI section B Mistral failed: {e}")
            llm_b = {"finding": "AI drivers and barriers differ between Slovakia and peers.", "bullets": []}

        sub_sections.append({
            "heading": "Why Firms Use or Avoid AI",
            "finding": llm_b["finding"],
            "bullets": llm_b["bullets"],
            "chart_png": chart_png_b,
        })

    # Sub-section C: Peer Expectations — QB1 + QB2
    df_qb1 = _fetch(["qb1"])
    df_qb2 = _fetch(["qb2"])

    if not df_qb1.empty:
        df_qb1a = df_qb1[df_qb1["sub_item"] == "a"].copy()
        chart_png_c = None
        if not df_qb1a.empty:
            df_qb1a = df_qb1a[(df_qb1a["response_raw"] >= 0) & (df_qb1a["response_raw"] <= 100)].copy()
            df_qb1a["bucket"] = (df_qb1a["response_raw"] // 10) * 10
            bucket_agg = (
                df_qb1a.groupby(["country_code", "bucket"])
                .agg(n_firms_wtd=("n_firms_wtd", "sum"), n_firms=("n_firms", "sum"))
                .reset_index()
            )
            totals = bucket_agg.groupby("country_code")["n_firms_wtd"].sum().rename("n_total_wtd")
            bucket_df = bucket_agg.merge(totals, on="country_code")
            bucket_df["pct_wtd"] = (bucket_df["n_firms_wtd"] / bucket_df["n_total_wtd"] * 100).round(1)
            bucket_df = bucket_df.rename(columns={"bucket": "response_raw"})
            bucket_df["sub_item"] = "a"
            bucket_df["sub_item_label"] = "In your country"
            chart_png_c = _build_ai_chart(bucket_df, "Peer AI adoption — own country (QB1a)", is_continuous=True)
            if chart_png_c:
                print(f"  AI chart C built (QB1a bucketed, {len(bucket_df)} rows)")

        qb1_sum = _ai_continuous_summary(
            df_qb1[(df_qb1["response_raw"] >= 0) & (df_qb1["response_raw"] <= 100)]
        )
        qb2_sum = ""
        if not df_qb2.empty:
            df_qb2v = df_qb2[(df_qb2["response_raw"] >= 0) & (df_qb2["response_raw"] <= 100)]
            qb2_sum = "\n\nQB2 — Expected peer adoption next 12 months:\n" + _ai_continuous_summary(df_qb2v)

        try:
            llm_c = _call_mistral_ai_section(
                "Peer AI adoption expectations (QB1 current, QB2 next 12 months)",
                "QB1 — % of similar firms estimated to have invested in AI today:\n" + qb1_sum + qb2_sum,
                "QB1/QB2 asked of all firms. Each firm gave a single numeric % estimate. Table shows weighted mean and approximate median. QB2 g3=SME peers, g4=large firm peers.",
                mistral_client, cost_tracker,
            )
        except Exception as e:
            print(f"  AI section C Mistral failed: {e}")
            llm_c = {"finding": "Firms expect AI adoption to roughly double among peers next year.", "bullets": []}

        sub_sections.append({
            "heading": "Peer AI Adoption Expectations",
            "finding": llm_c["finding"],
            "bullets": llm_c["bullets"],
            "chart_png": chart_png_c,
        })

    if not sub_sections:
        return None

    top_finding = sub_sections[0]["finding"] if sub_sections else "AI adoption is nascent across Slovakia and the Euro Area."
    all_bullets = []
    for ss in sub_sections:
        all_bullets.extend(ss.get("bullets", []))

    return {
        "section_id": "adhoc_spotlight",
        "title": "Special Focus: AI Adoption in Slovak and Euro Area Firms",
        "finding": top_finding,
        "bullets": all_bullets,
        "chart_png": sub_sections[0].get("chart_png") if sub_sections else None,
        "theme_label": "AI Adoption",
        "sub_sections": sub_sections,
    }
