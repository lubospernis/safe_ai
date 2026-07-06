"""LLM generation: section bullets, exec summary, so-what pass, wave memory, ECB sharpener."""

import hashlib
import json
import os
import re
import textwrap
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime

import anthropic
import pandas as pd
from json_repair import repair_json

from cost import _Usage, _mistral_client, _track_cost
from db import (
    MART_QUERY_TEMPLATES, MAX_TOOL_TURNS, PROD_SCHEMA, QUERY_MART_TOOL, _get_connection,
    _run_query_tool,
)

EMIT_SECTION_TOOL = {
    "name": "emit_section_json",
    "description": "Emit the final finding, bullets, and chart_subtitle as structured output.",
    "input_schema": {
        "type": "object",
        "properties": {
            "finding": {"type": "string", "description": "Headline ≤ 12 words, active voice"},
            "bullets": {
                "type": "array",
                "items": {"type": "string"},
                "minItems": 1,
                "maxItems": 3,
            },
            "chart_subtitle": {"type": "string", "description": "≤ 9 words caption"},
        },
        "required": ["finding", "bullets", "chart_subtitle"],
    },
}

# Patterns that indicate leaked LLM reasoning rather than published bullet content
_LEAKED_PATTERNS = re.compile(
    r"^("
    r"I |My |We |Let me |I've |I'm |I'll |I need |I will |I can |I should |"
    r"Now |Next |Let's |Checking |Looking |Calculating |Computing |"
    r"Note:|Note that |Note: |Actually,|Actually |Wait,|Wait |"
    r"So |Therefore |Thus |Hence |This means|Given that|Based on|Using "
    r")",
    re.IGNORECASE,
)


_NUMBER_RE = re.compile(r'(?<!\d)(\d+(?:\.\d+)?)(?!\d)')


def _check_numeric_grounding(bullets: list[str], df: pd.DataFrame, value_cols: list[str]) -> list[str]:
    """Return list of numbers in bullets that don't appear in any value column of df.

    Monitoring-only: callers log warnings but do not block on these errors.
    Numbers ≤ single digit or > 200 are skipped (wave numbers, counts, not cited values).
    """
    data_numbers: set[str] = set()
    for col in value_cols:
        if col in df.columns:
            for v in df[col].dropna():
                try:
                    fv = float(v)
                    data_numbers.add(f"{fv:.1f}")
                    data_numbers.add(str(int(round(fv))))
                except (ValueError, TypeError):
                    pass
    errors = []
    for bullet in bullets:
        for m in _NUMBER_RE.finditer(bullet):
            num_str = m.group(1)
            try:
                iv = int(num_str.split(".")[0])
            except ValueError:
                continue
            if iv <= 9 or iv > 200:
                continue
            try:
                rounded = f"{float(num_str):.1f}"
                int_form = str(int(round(float(num_str))))
            except ValueError:
                continue
            if num_str not in data_numbers and rounded not in data_numbers and int_form not in data_numbers:
                errors.append(f"'{num_str}' not found in source data")
    return errors


def _check_exec_provenance(exec_bullets: list[dict], rendered: list[dict]) -> list[str]:
    """Return list of numbers in exec bullets that don't appear verbatim in the section bullets they cite."""
    section_bullet_text: dict[str, str] = {
        s["section_id"]: " ".join(s.get("bullets", []))
        for s in rendered
    }
    errors = []
    for item in exec_bullets:
        sid = item.get("section_id", "")
        bullet = item.get("bullet", "")
        source_text = section_bullet_text.get(sid, "")
        if not source_text:
            continue
        for m in _NUMBER_RE.finditer(bullet):
            num = m.group(1)
            try:
                iv = int(num.split(".")[0])
            except ValueError:
                continue
            if iv <= 9 or iv > 200:
                continue
            if num not in source_text:
                errors.append(f"[{sid}] '{num}' in exec bullet not found in section bullets")
    return errors


def _is_reasoning_leak(bullet: str) -> bool:
    if _LEAKED_PATTERNS.match(bullet):
        return True
    # Reasoning narration often ends in ":" (introducing a list or next thought)
    if bullet.rstrip().endswith(":") and len(bullet) < 120:
        return True
    return False

COUNTRIES = {"SK": "Slovakia", "EA": "Euro Area", "DE": "Germany"}
COUNTRY_ORDER = ["SK", "EA", "DE"]

ECB_SAFE_INDEX = "https://www.ecb.europa.eu/stats/ecb_surveys/safe/html/index.en.html"
ECB_BASE = "https://www.ecb.europa.eu"
_ECB_SHARPEN_MAX_CHARS = 8_000

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

NBS_STYLE_GUIDE = textwrap.dedent("""
    Language and tone (modelled on NBS/ECB financial stability reports):
    - Use precise quantitative language: "rose from X% to Y%", "declined by Z pp", "increased marginally"
    - Qualitative intensity must match data magnitude:
      * "marginally" / "slightly" = change ≤ 2 pp (or ≤ 0.3 score units for pressingness)
      * "moderately" = 2–5 pp (0.3–0.8 units)
      * "notably" / "significantly" = 5–10 pp (0.8–1.5 units) — only if data supports it
      * "sharply" / "substantially" = > 10 pp (> 1.5 units) — only if data supports it
    - NEVER use: "surged", "collapsed", "plummeted", "acute stress", "dramatic", "striking"
    - Mechanism language ("signalling", "suggesting", "indicating") requires either:
      (a) direct logical entailment from the data, or
      (b) support from a published ECB/NBS/BIS source.
      Otherwise: describe only what the data shows.
    - Bullet ordering: most important/surprising finding first; routine confirmations last.
      Never open with a bullet that summarises as "no change".
    - IMPROVEMENT vs RECOVERY: A net balance that became less negative means conditions are
      IMPROVING — NOT that they have "recovered", "rebounded", or "turned around". Use
      "improved by X pp" or "eased" or "less negative". Reserve "recovery" / "rebound" /
      "turned around" exclusively for net balances that are POSITIVE (> 0).
""").strip()

SECTION_CONTENT_SYSTEM = textwrap.dedent("""
    You are an ECB analyst writing content for a SAFE survey report focused on Slovakia.
    Return a JSON object with exactly three fields: "finding", "bullets", "chart_subtitle".

    CRITICAL DATA RULE:
    Only cite numbers that appear verbatim in the section data provided to you, or in a
    query_mart tool result you actually ran in this session. Do NOT invent, estimate, or
    paraphrase percentages, net balances, or counts. If a number you want to cite is not
    in the provided data, either call query_mart (only in the 4 permitted cases below)
    or omit the comparison entirely.

    {nbs_style_guide}

    "finding": A single declarative headline (max 12 words) summarising the most notable
      finding for Slovakia. Use active voice, name the direction. Do NOT mention question
      codes (Q10, Q5, etc.). Example: "Net tightening in interest rates reported by Slovak firms"
      NEVER use in the finding: "surged", "collapsed", "plummeted", "dramatic", "striking", "acute"

    "chart_subtitle": One sentence (max 9 words) for a chart caption.
      State the SK finding for the primary (pinned) panel with an actual number.
      Example: "8% of Slovak firms reported interest rate increases."
      Do NOT repeat the section title. Do NOT use question codes.

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
      - Bullet ordering: put the most analytically significant finding first — largest
        magnitude change, widest SK–EA divergence, or strongest reversal from prior wave.
        Routine confirmations go last.

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

    When to call query_mart — only these 4 cases justify a tool call:
    1. You need data from before wave 30 (use int_safe__core_questions_long)
    2. You need a sub_item or column NOT present in the section data provided above
    3. You need to verify a historical extreme (e.g. "is this the highest since wave X?")
    4. You want to use a qualitative intensity word ("notably", "sharply", "the widest gap
       in N waves") and need to verify the current value is exceptional in recent context —
       query int_safe__core_questions_long for the last 4 waves (wave_number >= current_wave - 3).
       If the data does not support the intensity word, use a milder term or drop it.

    Do NOT call query_mart to:
    - Discover table or column names (the catalogue above is complete and current)
    - Recalculate or reformat data already in the provided section data
    - Fetch the same wave/country/sub_item combinations already shown to you
    - Explore what tables exist (they are all listed above)

    If none of the 4 cases apply, write your JSON response immediately.
    Cite only numbers from the provided data or a tool result you actually ran.
{historical_context}
    CRITICAL OUTPUT RULE: The emit_section_json tool's "bullets" array must contain ONLY
    published report bullets — zero internal reasoning, zero computation steps, zero self-checks.
    Never put "Now checking...", "Let me analyze...", "I have all the data..." or any process
    narration into a bullet field. Complete all reasoning BEFORE calling the tool.

    Return valid JSON only — no markdown fences, no commentary.
""").strip().format(nbs_style_guide=NBS_STYLE_GUIDE, schema_catalogue="{schema_catalogue}",
                    query_templates="{query_templates}", historical_context="{historical_context}")

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

EXEC_SUMMARY_SYSTEM = textwrap.dedent(f"""
    You are an economist writing an executive summary of the latest ECB SAFE survey results
    for Slovakia. You will receive:
    1. Section-by-section findings from the report
    2. Cross-cutting themes identified by a first-pass analyst

    {NBS_STYLE_GUIDE}

    Your task: write EXACTLY 3–4 bullets. No more. Cover BOTH financing conditions AND
    the economic situation of firms. Prioritise the most striking or cross-cutting findings
    and ruthlessly drop the rest.

    SELECTION PRIORITY (apply in this order):
    1. Cross-cutting tensions that span financing conditions AND economic situation simultaneously
       (e.g. costs rising while turnover falls while credit tightens — all at once)
    2. Findings where Slovakia diverges sharply from the EA (≥ 10 pp gap, or opposite direction)
    3. Findings that reversed direction from the prior wave (a turning point)
    4. Single-section findings only if the magnitude is exceptional (≥ 15 pp swing, or clearly
       the highest/lowest in the data provided)

    DROP a bullet if: the finding is directionally consistent with EA and within 5 pp, or if it
    merely confirms a stable multi-wave trend with no new development this wave.

    The [LEAD] prefix on bullets marks each section's most analytically significant bullet —
    weight these more heavily than trailing bullets when selecting what to elevate.

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
    - Historical context (prior waves) may be provided below the section findings. Use it ONLY
      when it makes a current finding meaningfully more striking — e.g. "the sharpest reading
      in three waves" or "the first improvement since wave X". Do NOT mention prior waves just
      to show awareness of them. If the current wave finding stands on its own, omit the history.
      Never invent a historical comparison that is not explicitly supported by the context given.
    - Scope discipline: if a bullet makes a broad claim (e.g. "overall liquidity tightening",
      "financing access deteriorated", "credit conditions worsened"), the supporting evidence
      must span ALL major instruments (bank loans, credit lines, trade credit). Do NOT make an
      overall claim while citing only one instrument. Either: (a) aggregate across instruments,
      or (b) narrow the claim to the specific instrument — e.g. "credit line availability
      tightened" not "liquidity conditions tightened".
    - Number provenance: every number you cite in a bullet must appear verbatim in the bullets
      of the section you attribute that bullet to (its section_id). Do NOT mix numbers from
      different sections into one bullet. If you want to mention a number from section X, that
      bullet's section_id must be X.

    Return a JSON array only — no markdown fences, no commentary:
    [
      {{"bullet": "**Label:** explanation", "section_id": "bank_loan_terms"}},
      ...
    ]

    Valid section_id values (use exactly as written):
    bank_loan_terms, financing_gap, loan_applications, availability_expectations,
    financing_purpose, financing_factors, business_situation, outlook,
    expectations_quantitative, expectations_risk, business_problems, adhoc_spotlight

    For cross-cutting bullets spanning multiple sections, use the most relevant section_id.
    No leading bullet character inside the bullet text.

    Special rule for adhoc_spotlight: if adhoc_spotlight appears in the section findings,
    you MUST include exactly one bullet for it, prefixed with the 🔍 emoji,
    e.g. "🔍 **AI Peer Estimates:** Slovak firms estimated..."
    This bullet counts toward your 3–4 total. Drop the weakest other bullet if needed.
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
    """Return a divergence note if SK SMEs differ from SK all-firms by >= threshold in latest wave."""
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
    """Parse Sonnet JSON response into {"finding", "bullets", "chart_subtitle"}."""
    # Strip markdown fences and leading thinking/prose before the JSON object
    stripped = raw.strip().removeprefix("```json").removeprefix("```").removesuffix("```").strip()
    # Find the last JSON object in the response (model may emit thinking text first)
    matches = list(re.finditer(r'\{[^{}]*"finding"[^{}]*\}|\{.*?"finding".*?\}', stripped, re.DOTALL))
    candidate = matches[-1].group() if matches else stripped
    try:
        parsed = json.loads(repair_json(candidate))
        finding = str(parsed.get("finding", sec["title"]))
        raw_bullets = parsed.get("bullets", [])
        if isinstance(raw_bullets, list):
            bullets = [str(b).strip().lstrip("•- ") for b in raw_bullets if str(b).strip()]
        else:
            bullets = [b.strip().lstrip("•- ") for b in str(raw_bullets).splitlines() if b.strip()]
        chart_subtitle = str(parsed.get("chart_subtitle", "")).strip()
        # Validate: if finding is empty or still the default placeholder, it parsed wrong
        if not finding or finding == "string":
            finding = sec["title"]
    except (json.JSONDecodeError, AttributeError, TypeError):
        finding = sec["title"]
        chart_subtitle = ""
        bullets = [
            line.strip().lstrip("•- ") for line in raw.splitlines()
            if line.strip() and not line.strip().startswith(("```", "{", "}", "*", "#"))
        ]
    # Strip leaked LLM reasoning lines and cap at 3
    bullets = [b for b in bullets if not _is_reasoning_leak(b)][:3]
    return {"finding": finding, "bullets": bullets, "chart_subtitle": chart_subtitle}


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
    """Return {"finding": str, "bullets": [str, ...]} via an agentic Sonnet loop."""
    system_prompt = SECTION_CONTENT_SYSTEM.format(
        schema_catalogue=mart_catalogue,
        query_templates=MART_QUERY_TEMPLATES,
        historical_context=f"\n{historical_context}" if historical_context else "",
    )
    cached_system = [{"type": "text", "text": system_prompt,
                      "cache_control": {"type": "ephemeral"}}]

    base_data = _fmt_data_for_prompt(sec, df)
    divergence = _sme_divergence_note(df, sec["value_col"], sec.get("panel_col"))

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
            # Model finished reasoning — now force structured output via dedicated emit tool
            messages.append({"role": "assistant", "content": response.content})
            break

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

    # Force structured output: model MUST call emit_section_json — cannot return plain text
    messages.append({
        "role": "user",
        "content": "Now emit your structured output using the emit_section_json tool.",
    })
    emit_resp = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=500,
        system=cached_system,
        tools=[EMIT_SECTION_TOOL],
        tool_choice={"type": "any"},
        messages=messages,
        extra_headers={"anthropic-beta": "prompt-caching-2024-07-31"},
    )
    _track_cost(cost_tracker, "claude-sonnet-4-6", emit_resp.usage)

    for block in emit_resp.content:
        if getattr(block, "type", None) == "tool_use" and block.name == "emit_section_json":
            inp = block.input
            finding = str(inp.get("finding", sec["title"])).strip() or sec["title"]
            raw_bullets = inp.get("bullets", [])
            if isinstance(raw_bullets, list):
                bullets = [str(b).strip().lstrip("•- ") for b in raw_bullets if str(b).strip()]
            else:
                bullets = [str(raw_bullets).strip().lstrip("•- ")]
            bullets = [b for b in bullets if not _is_reasoning_leak(b)][:3]
            chart_subtitle = str(inp.get("chart_subtitle", "")).strip()
            value_cols = [sec.get("value_col", "net_balance_wtd"),
                          "pct_cited_wtd", "avg_pressingness_wtd"]
            grounding_warns = _check_numeric_grounding(bullets, df, value_cols)
            if grounding_warns:
                sid_label = sec.get("id", "?")
                for w in grounding_warns:
                    print(f"  [GROUNDING WARN] [{sid_label}] {w}")
            return {"finding": finding, "bullets": bullets, "chart_subtitle": chart_subtitle,
                    "tool_calls": tool_calls_made,
                    "grounding_warnings": grounding_warns if grounding_warns else []}

    # Last-resort fallback: extract text from emit response and parse
    text_blocks = [b.text for b in emit_resp.content if hasattr(b, "text")]
    raw = " ".join(text_blocks) if text_blocks else ""
    result = _parse_section_response(raw, sec)
    result["tool_calls"] = tool_calls_made
    return result


def get_exec_summary(
    rendered_sections: list[dict],
    cost_tracker: dict,
    historical_context: str = "",
    adhoc_section: dict | None = None,
    anthropic_client=None,
    mistral_client=None,
) -> list[dict]:
    """Two-pass exec summary. Returns list of {bullet, section_id} dicts."""
    section_ids = {s["section_id"] for s in rendered_sections}

    lines = ["Section findings:\n"]
    for s in rendered_sections:
        lines.append(f"## {s['title']} [section_id: {s['section_id']}]")
        if s.get("finding"):
            lines.append(f"Finding: {s['finding']}")
        if s.get("sign_note"):
            lines.append(f"Sign convention: {s['sign_note']}")
        for i, b in enumerate(s["bullets"]):
            prefix = "[LEAD] " if i == 0 else "       "
            lines.append(f"{prefix}{b}")
        lines.append("")
    section_text = "\n".join(lines)

    _EXEC_MODEL = "claude-opus-4-8"

    def _claude_complete(system: str, user: str, max_tokens: int) -> str:
        resp = anthropic_client.messages.create(
            model=_EXEC_MODEL,
            max_tokens=max_tokens,
            system=system,
            messages=[{"role": "user", "content": user}],
        )
        _track_cost(cost_tracker, _EXEC_MODEL, resp.usage)
        return resp.content[0].text.strip()

    # Pass 1: cross-section analyst — current wave only, no historical context
    themes = _claude_complete(EXEC_CROSS_SECTION_SYSTEM, section_text, 200)

    # Pass 2: editor — receives historical context so it can add wave comparisons where warranted
    history_block = (
        f"\n\nHistorical context (prior waves — use ONLY when it makes a current finding "
        f"more striking; omit otherwise):\n{historical_context}"
        if historical_context else ""
    )
    user_msg = (
        f"{section_text}\n\n"
        f"Cross-cutting themes identified by first-pass analyst:\n{themes}"
        f"{history_block}"
    )
    raw = _claude_complete(EXEC_SUMMARY_SYSTEM, user_msg, 600)
    raw = re.sub(r"^```(?:json)?\s*", "", raw)
    raw = re.sub(r"\s*```$", "", raw).strip()

    try:
        items = json.loads(repair_json(raw))
        result = []
        for item in items:
            if not isinstance(item, dict):
                continue
            bullet = str(item.get("bullet", "")).strip().lstrip("•- ")
            sid = str(item.get("section_id", "")).strip()
            if sid != "adhoc_spotlight":
                bullet = bullet.lstrip("🔍 ")
            if bullet:
                result.append({"bullet": bullet, "section_id": sid if sid in section_ids else ""})
        result = result[:4]
    except Exception:
        plain = [l.strip().lstrip("•- ") for l in raw.splitlines() if l.strip()]
        result = [{"bullet": b, "section_id": ""} for b in plain[:4]]

    # Guarantee one 🔍 adhoc bullet only when adhoc_spotlight was actually rendered
    adhoc_was_rendered = adhoc_section is not None and any(
        s.get("section_id") == "adhoc_spotlight" for s in rendered_sections
    )
    if adhoc_was_rendered and not any(r.get("section_id") == "adhoc_spotlight" for r in result):
        theme = adhoc_section.get("theme_label", "Special Focus")
        finding = adhoc_section.get("finding", "")
        fallback_bullet = f"🔍 **{theme}:** {finding}" if finding else f"🔍 **{theme}:** See Special Focus section."
        if len(result) >= 4:
            result = result[:3]  # drop weakest (last) to stay at 4
        result.append({"bullet": fallback_bullet, "section_id": "adhoc_spotlight"})

    # Exec provenance check — log warnings if exec bullets cite numbers not in section bullets
    prov_errors = _check_exec_provenance(result, rendered_sections)
    if prov_errors:
        for err in prov_errors:
            print(f"  [EXEC PROVENANCE WARN] {err}")

    return result


def _add_so_what(content: dict, sec: dict, mistral_client, cost_tracker: dict) -> dict:
    """Add implication clauses to purely-descriptive section bullets via Mistral Small."""
    bullets_text = "\n".join(f"- {b}" for b in content["bullets"])
    user_msg = (
        f"Section: {sec['title']}\n"
        f"Sign convention: {sec.get('sign_note', '')}\n\n"
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
        pass
    return content


SHORTEN_QUESTION_SYSTEM = textwrap.dedent("""
    You paraphrase official ECB survey question wording into a short, natural
    question a curious reader would ask when looking at a chart.

    Rules:
    - Produce ONE short question, at most 12 words, phrased in second person
      ("you"/"your firm").
    - End with a question mark. Do not include question codes (e.g. "Q10",
      "Q0B") or mention "ECB"/"SAFE".
    - Simplify the wording — do not copy the official phrasing verbatim.
    - If multiple question texts are given, separated by " | ", they describe
      the same theme from different angles — synthesise ONE question that
      captures the shared theme, do not concatenate them.

    Respond with JSON only, no markdown fences: {"short_question": "..."}
""").strip()


def _shorten_question_llm(source_text: str, mistral_client) -> dict:
    """Paraphrase annex question text into a short caption via Mistral Small.

    Returns {"short_question": str, "_usage": {...}} — short_question is ""
    on any failure (malformed response, API error).
    """
    result = {"short_question": ""}
    try:
        resp = mistral_client.chat.complete(
            model="mistral-small-latest",
            max_tokens=60,
            messages=[
                {"role": "system", "content": SHORTEN_QUESTION_SYSTEM},
                {"role": "user", "content": source_text[:2000]},
            ],
        )
        raw = resp.choices[0].message.content.strip()
        raw = re.sub(r"^```(?:json)?\s*", "", raw)
        raw = re.sub(r"\s*```$", "", raw).strip()
        parsed = json.loads(repair_json(raw))
        result["short_question"] = str(parsed.get("short_question", "")).strip()
        if resp.usage:
            result["_usage"] = {"input": resp.usage.prompt_tokens, "output": resp.usage.completion_tokens}
    except Exception:
        pass
    return result


def get_shortened_questions(
    sections: list[dict],
    question_texts: dict[str, str],
    con,
    schema: str,
    mistral_client,
    cost_tracker: dict,
    force_refresh: bool = False,
) -> dict[str, str]:
    """Return {section_id: short_question_caption} for chart "Q: ..." captions.

    Cached in {schema}.ref_safe__chart_question_captions (PK section_id), keyed
    on a hash of the section's annex question text(s) so a wording change
    auto-invalidates the cache. Sections with no annex text available are
    omitted from the result (graceful — chart just gets no "Q: ..." caption).
    """
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {schema}.ref_safe__chart_question_captions (
            section_id    VARCHAR NOT NULL,
            question_ids  VARCHAR NOT NULL,
            source_hash   VARCHAR NOT NULL,
            short_caption VARCHAR NOT NULL,
            model_id      VARCHAR NOT NULL,
            generated_at  TIMESTAMP NOT NULL,
            PRIMARY KEY (section_id)
        )
    """)
    cached: dict[str, tuple[str, str]] = {}
    try:
        rows = con.execute(
            f"SELECT section_id, source_hash, short_caption "
            f"FROM {schema}.ref_safe__chart_question_captions"
        ).fetchall()
        cached = {r[0]: (r[1], r[2]) for r in rows}
    except Exception:
        pass

    results: dict[str, str] = {}
    to_generate: list[tuple[str, str, str, str]] = []
    for sec in sections:
        q_ids = sec.get("question_ids", [])
        source_text = " | ".join(
            question_texts[q.lower()] for q in q_ids if q.lower() in question_texts
        )
        if not source_text:
            continue
        h = hashlib.sha256(source_text.encode()).hexdigest()[:16]
        hit = cached.get(sec["id"])
        if hit and hit[0] == h and not force_refresh:
            results[sec["id"]] = hit[1]
        else:
            to_generate.append((sec["id"], ",".join(q_ids), source_text, h))

    if not to_generate:
        return results

    new_rows = []
    with ThreadPoolExecutor(max_workers=5) as pool:
        futures = {
            pool.submit(_shorten_question_llm, text, mistral_client): (sid, qids, h)
            for sid, qids, text, h in to_generate
        }
        for future in as_completed(futures):
            sid, qids, h = futures[future]
            r = future.result()
            if "_usage" in r:
                u = r["_usage"]
                _track_cost(cost_tracker, "mistral-small-latest", _Usage(u["input"], u["output"]))
            caption = r.get("short_question", "")
            if not caption:
                continue
            results[sid] = caption
            new_rows.append((sid, qids, h, caption, "mistral-small-latest", datetime.utcnow()))

    if new_rows:
        try:
            con.executemany(
                f"INSERT OR REPLACE INTO {schema}.ref_safe__chart_question_captions VALUES (?,?,?,?,?,?)",
                new_rows,
            )
        except Exception as e:
            print(f"  [WARN] Failed to persist chart question captions: {e}")
    return results


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
        summary = re.sub(r"\*+", "", summary).strip()
        if resp.usage:
            _track_cost(cost_tracker, model,
                        _Usage(resp.usage.prompt_tokens, resp.usage.completion_tokens))
        # Validate: numbers in the summary should appear in the rendered bullet text
        all_bullets_text = " ".join(
            b for r in rendered for b in r.get("bullets", [])
        )
        summary_numbers = [m.group(1) for m in _NUMBER_RE.finditer(summary)
                           if len(m.group(1)) > 1 and int(m.group(1).split(".")[0]) <= 200]
        bad_nums = [n for n in summary_numbers if n not in all_bullets_text]
        if bad_nums:
            print(f"  [MEMORY WARN] Wave memory cites numbers not in bullets: {bad_nums} — skipping write")
            return
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


def translate_to_slovak(
    rendered: list[dict],
    exec_bullets: list[dict],
    cost_tracker: dict,
) -> tuple[list[dict], list[dict]]:
    exec_bullet_texts = [item.get("bullet", "") for item in exec_bullets]

    adhoc_s = next((s for s in rendered if s.get("section_id") == "adhoc_spotlight"), None)
    regular = [s for s in rendered if s.get("section_id") != "adhoc_spotlight"]

    payload: dict = {
        "exec_bullets": exec_bullet_texts,
        "sections": [
            {
                "id":      s["section_id"],
                "title":   s.get("title", ""),
                "finding": s["finding"],
                "bullets": s["bullets"],
            }
            for s in regular
        ],
    }
    if adhoc_s:
        payload["adhoc"] = {
            "theme_label": adhoc_s.get("theme_label", ""),
            "title":       adhoc_s.get("title", ""),
            "finding":     adhoc_s.get("finding", ""),
            "bullets":     adhoc_s.get("bullets", []),
            "sub_sections": [
                {
                    "heading": ss.get("heading", ""),
                    "finding": ss.get("finding", ""),
                    "bullets": ss.get("bullets", []),
                }
                for ss in adhoc_s.get("sub_sections", [])
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
    _TRANSLATE_MODEL = "mistral-medium-2505"
    resp = client.chat.complete(
        model=_TRANSLATE_MODEL,
        max_tokens=5500,
        messages=[{"role": "user", "content": prompt}],
    )
    if resp.usage:
        _track_cost(cost_tracker, _TRANSLATE_MODEL,
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
    for s in regular:
        t = by_id.get(s["section_id"], {})
        sk_rendered.append({
            **s,
            "title":   t.get("title",   s.get("title", "")),
            "finding": t.get("finding", s["finding"]),
            "bullets": t.get("bullets", s["bullets"]),
        })

    # Reconstruct translated adhoc section
    if adhoc_s:
        sk_adhoc = translated.get("adhoc") or {}
        orig_sub = adhoc_s.get("sub_sections", [])
        t_sub    = sk_adhoc.get("sub_sections", [])
        sk_rendered.append({
            **adhoc_s,
            "theme_label": sk_adhoc.get("theme_label", adhoc_s.get("theme_label", "")),
            "title":       sk_adhoc.get("title",       adhoc_s.get("title", "")),
            "finding":     sk_adhoc.get("finding",     adhoc_s.get("finding", "")),
            "bullets":     sk_adhoc.get("bullets",     adhoc_s.get("bullets", [])),
            "sub_sections": [
                {
                    **orig_ss,
                    "heading": t_ss.get("heading", orig_ss.get("heading", "")),
                    "finding": t_ss.get("finding", orig_ss.get("finding", "")),
                    "bullets": t_ss.get("bullets", orig_ss.get("bullets", [])),
                }
                for orig_ss, t_ss in zip(orig_sub, t_sub)
            ] if t_sub else orig_sub,
        })

    sk_bullet_texts = translated.get("exec_bullets", exec_bullet_texts)
    sk_exec_bullets = [
        {"bullet": str(text), "section_id": orig.get("section_id", "")}
        for text, orig in zip(sk_bullet_texts, exec_bullets)
    ]
    return sk_rendered, sk_exec_bullets


def _fetch_ecb_context() -> tuple[str, str]:
    """Fetch the latest ECB SAFE publication and return (url, plain_text)."""
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


def _sharpen_with_ecb(
    rendered: list[dict],
    ecb_text: str,
    mistral_client,
    cost_tracker: dict,
) -> list[dict]:
    """Post-generation pass: sharpen bullets against ECB publication."""
    if not ecb_text or not rendered:
        return rendered
    sk_mentions = len(re.findall(r'\bSlovak(?:ia)?\b', ecb_text, re.IGNORECASE))
    if sk_mentions < 2:
        print(f"  ECB sharpener: only {sk_mentions} Slovakia mention(s) in ECB text — skipping to avoid EA/SK mismatch")
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


def _find_ecb_focus_article(theme_label: str, mistral_client, cost_tracker: dict) -> str | None:
    """Search ECB Economic Bulletin focus articles for a match. Returns URL or None."""
    _ECB_FOCUS_INDEX = "https://www.ecb.europa.eu/press/economic-bulletin/focus/"
    import urllib.request
    try:
        req = urllib.request.Request(_ECB_FOCUS_INDEX, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=10) as r:
            html = r.read().decode("utf-8", errors="replace")
    except Exception:
        return None

    pairs = re.findall(
        r'href="(https://www\.ecb\.europa\.eu/press/economic-bulletin/focus/\d{4}/html/[^"]+)"'
        r'[^>]*>([^<]+)<',
        html,
    )
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
