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

from cost import _Usage, _anthropic_client, _mistral_client, _track_cost
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

# "13.8 pp above the EA's 32.7%" / "1.5 pp below the EA average of 15.6%" /
# "gap of 6.8 pp" / "worsening 8.8 pp from a net 25.0%" / "up 21.78 pp from" /
# "by 12.6 pp" / "worsening by 11.60 pp" style phrasing — the pp figure is a
# CORRECTLY COMPUTED difference of two other numbers in the same bullet (e.g.
# 46.5 - 32.7 = 13.8), not a raw value that appears anywhere in the source
# DataFrame. Confirmed via two real runs: wave 37 adhoc ("46.5% ... 13.8 pp
# above the EA's 32.7%") and wave 38 main report, which surfaced several more
# phrasings the first version of this pattern missed ("worsening 8.8 pp from",
# "up 21.78 pp from", "outpacing ... by 12.6 pp").
_PP_DELTA_RE = re.compile(
    r'pp\s+(?:above|below|higher|lower)'          # "13.8 pp above/below/higher/lower"
    r'|gap\s+of\s*$'                              # "... a gap of 13.8"
    r'|^\s*(?:pp\s+)?(?:from|vs)\b'                # "13.8 pp from ..." / "13.8 vs ..."
    r'|(?:worsening|deteriorating|easing|up|down|rising|falling|rose|fell)'
    r'\s+(?:by\s+)?$'                              # "worsening/up/down/by ... 13.8"
    r'|by\s*$',                                    # "outpacing ... by 13.8 pp"
    re.IGNORECASE,
)


def _is_verified_pp_delta(bullet: str, num_str: str, start: int, end: int) -> bool:
    """True if num_str at [start:end] is adjacent to pp-delta phrasing ("13.8 pp
    above ...", "up 21.78 pp from ...", "worsening by 11.60 pp") and equals the
    absolute difference of two OTHER numbers already present in the bullet,
    within 0.15 rounding tolerance."""
    following = bullet[end:end + 15]
    preceding = bullet[max(0, start - 20):start]
    if not (_PP_DELTA_RE.match(following.lstrip()) or _PP_DELTA_RE.search(preceding)):
        return False
    try:
        target = float(num_str)
    except ValueError:
        return False
    other_numbers = [
        float(m.group(1)) for m in _NUMBER_RE.finditer(bullet)
        if not (m.start() == start and m.end() == end)
    ]
    return any(
        abs(abs(a - b) - target) < 0.15
        for i, a in enumerate(other_numbers)
        for b in other_numbers[i + 1:]
    )


def _check_numeric_grounding(bullets: list[str], df: pd.DataFrame, value_cols: list[str]) -> list[str]:
    """Return list of numbers in bullets that don't appear in any value column of df.

    Monitoring-only: callers log warnings but do not block on these errors.
    Numbers <= single digit or > 200 are skipped (wave numbers, counts, not cited values).
    Also skipped: sample-size citations ("n=62"), wave references ("wave 37"),
    time-period references ("next 12 months"), pressingness-scale denominators
    ("6.19/10"), pp-deltas that are a verified difference of two other numbers
    in the same bullet ("13.8 pp above ...", where 13.8 = the two other cited
    percentages' difference), and sign-stripped net-balance citations (a
    DataFrame value of -33.8 cited in prose as "a net 33.8%" — the negative
    sign is conveyed by a word like "deteriorated"/"declined" instead of a
    minus sign, which is legitimate English, not a fabrication) — these are
    legitimate non-data-value numbers that previously produced a high
    false-positive rate (see ROADMAP.md A8).
    """
    data_numbers: set[str] = set()
    for col in value_cols:
        if col in df.columns:
            for v in df[col].dropna():
                try:
                    fv = float(v)
                    data_numbers.add(f"{fv:.1f}")
                    data_numbers.add(str(int(round(fv))))
                    # Absolute value too — a negative net balance is often cited
                    # in prose with the sign implied by a direction word rather
                    # than a literal minus sign (e.g. "-33.8" -> "deteriorated ...
                    # a net 33.8%"). See docstring above.
                    data_numbers.add(f"{abs(fv):.1f}")
                    data_numbers.add(str(int(round(abs(fv)))))
                except (ValueError, TypeError):
                    pass
    if "n_respondents" in df.columns:
        for v in df["n_respondents"].dropna():
            try:
                data_numbers.add(str(int(v)))
            except (ValueError, TypeError):
                pass

    errors = []
    for bullet in bullets:
        for m in _NUMBER_RE.finditer(bullet):
            num_str = m.group(1)
            start, end = m.span()

            # Sample-size citation: "n=62" or "(n=62)"
            preceding = bullet[max(0, start - 3):start]
            if re.search(r"n\s*=\s*$", preceding):
                continue

            # Pressingness-scale denominator: "6.19/10" — skip the number right after "/"
            if start > 0 and bullet[start - 1] == "/":
                continue

            # Wave reference: "wave 37", "prior wave" style mentions right before the number
            preceding_word = bullet[max(0, start - 6):start]
            if re.search(r"wave\s*$", preceding_word, re.IGNORECASE):
                continue

            # Time-period reference: "next 12 months", "24-month horizon" —
            # the number of months is a horizon label, not a cited data value.
            following_word = bullet[end:end + 10]
            if re.match(r"[\s-]*months?\b", following_word, re.IGNORECASE):
                continue

            # Verified pp-delta: "13.8 pp above the EA's 32.7%" where 13.8 is the
            # correctly-computed difference of two other numbers in the bullet.
            if _is_verified_pp_delta(bullet, num_str, start, end):
                continue

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
    - DEFAULT TO NO INTENSITY ADVERB. When you state both figures or the pp gap (e.g.
      "17.1% in Slovakia vs 31.2% in the EA"), the reader already sees the magnitude —
      an adverb on top is usually redundant, not informative, and often reads as
      editorializing. Only add one when it changes the reader's interpretation (e.g.
      flagging that an unexpectedly small change is noteworthy). Never stack an
      intensity adverb onto a sentence whose subject is itself a magnitude label
      (e.g. a response option literally called "moderate use") — that reads as
      redundant regardless of the pp value.
    - If you do use an intensity adverb, it must match data magnitude (these are the
      exact thresholds a code-enforced check will apply after you write the bullet —
      a value outside the stated range for a word will FAIL the quality gate and
      block publication):
      * "marginally" / "slightly" = change ≤ 2 pp
      * "mildly" = change ≤ 3 pp
      * "moderately" = change ≥ 5 pp
      * "notably" / "significantly" = change ≥ 10 pp — do not use below 10 pp
      * "sharply" / "substantially" = change ≥ 15 pp — do not use below 15 pp
      * "dramatically" = change ≥ 20 pp — do not use below 20 pp
      * For a change that doesn't clearly clear one of these bars (e.g. 6–9 pp, or
        11–14 pp), state the number plainly without an intensity adverb rather than
        guessing at a word that might fail the gate.
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
    - "moderately" / "moderate" describe MAGNITUDE only (a 2–5 pp change), never DIRECTION.
      Never write "moderating" as a verb implying a trend is calming down or easing — that
      claims the change is becoming smaller/less severe, which is a directional claim
      unrelated to magnitude. If a metric moved from a smaller adverse value to a larger
      adverse one (e.g. net 2% expecting deterioration to net 5%), that is a WORSENING —
      write "worsened by X pp" or "deteriorated further", never "moderating X pp".
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

    SELECTION PRIORITY — evidence-gated. Each section carries a [SIGNALS] line with
    structured, pre-computed fields. Trust these fields — do NOT re-derive divergence,
    historical extremity, or reversal from the prose yourself; the numbers there are
    already verified.

    A finding QUALIFIES for the exec summary only if at least one of these channels fires:
    1. Cross-cutting tension spanning financing conditions AND economic situation
       simultaneously (e.g. costs rising while turnover falls while credit tightens —
       all at once) — always qualifies regardless of signals.
    2. "ECB emphasis=yes" — the ECB's own publication foregrounds this theme (a
       supporting quote is provided).
    3. "SK-EA gap" ≥ 10pp — Slovakia diverges sharply from the euro area.
    4. "historical=..." shows a record or near-record reading (highest/lowest on record,
       or a large z-score), or the widest cross-country spread on record.
    5. "reversal=yes" — a genuine wave-over-wave turning point.

    TIER GATE — every section carries a "tier" field:
    - tier=core: needs ONE channel above to qualify.
    - tier=supporting: needs ONE channel above AND "reliable_n=yes".
    - tier=policy_technical, or any topic named in a "deprioritized:" field: needs
      TWO channels, or ONE exceptional channel (a record/near-record historical
      reading combined with "reliable_n=yes"). Do NOT elevate a policy_technical
      or deprioritized topic on a single large swing alone — this is exactly the
      failure mode to avoid: e.g. never elevate "access to public financial
      support" or a business-obstacle ranking purely because it had the biggest
      wave-over-wave move. When "reliable_n=no" is shown, treat the finding as
      noisy and do not elevate it regardless of magnitude.

    Prefer core-tier, financial-stability-relevant findings — use of financing,
    discouragement from applying, loan rejection, financing gaps, availability of
    external financing — over policy/administrative artifacts whenever both are
    plausible candidates.

    DROP a bullet if: no channel fires, if "reliable_n=no" on a supporting/policy_technical
    topic, if the finding is directionally consistent with EA and within 5 pp with no
    other qualifying channel, or if it merely confirms a stable multi-wave trend with no
    new development this wave.

    The [LEAD] prefix on bullets marks each section's most analytically significant bullet
    as determined by that section's own writer — weight [LEAD] bullets more heavily than
    trailing bullets, but ONLY after the tier/channel gate above is satisfied. A [LEAD]
    bullet on a topic that fails the gate must still be dropped or replaced by a
    qualifying bullet from the same section's other bullets.

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
    try:
        resp = client.chat.complete(
            model="mistral-small-latest",
            max_tokens=120,
            messages=[
                {"role": "system", "content": INTEREST_SYSTEM},
                {"role": "user", "content": "\n".join(lines)},
            ],
        )
    except Exception as e:
        # Interest-check is a cheap triage step, not a hard gate — an API outage
        # must not crash the whole report run. Default to "interesting" so the
        # section still gets full treatment (same conservative fallback used
        # below for JSON parse failures).
        print(f"  WARNING: interest check failed for section '{sec['id']}' ({e}) — defaulting to interesting.")
        result = {"interesting": True, "chart_type": "line", "best_panel": None, "reason": "api error"}
        result["section_id"] = sec["id"]
        return result

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

    lead_override = ""
    if sec.get("must_lead_with"):
        lead_override = (
            f"MANDATORY: the \"finding\" and bullet 1 MUST be about sub-item "
            f"'{sec['must_lead_with']}', regardless of which sub-item has the largest "
            f"wave-over-wave swing. Do not lead with any other sub-item even if its "
            f"change is larger this wave.\n"
        )

    section_header = (
        f"Sign convention: {sec['sign_note']}\n"
        f"Focus: {sec['focus']}\n"
        f"{lead_override}\n"
    )
    initial_msg = q_text_block + section_header + base_data + (f"\n\nSME divergence check:\n{divergence}" if divergence else "")

    if client is None:
        client = _anthropic_client()
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


# ---------------------------------------------------------------------------
# Exec-summary reasoning-channel signals — code-computed, not LLM-inferred.
# Each function reads the section's own lead panel (must_lead_with, else the
# first pinned_panels entry, else the whole section) from the already-loaded
# multi-wave DataFrame and returns a structured signal, so get_exec_summary can
# gate on real numbers instead of re-deriving them from prose.
# ---------------------------------------------------------------------------

_N_CANDIDATE_COLS = ("n_respondents", "n_respondents_need", "n_firms")


def _lead_panel(sec: dict) -> str | None:
    if sec.get("must_lead_with"):
        return sec["must_lead_with"]
    if sec.get("pinned_panels"):
        return sec["pinned_panels"][0]
    return None


def _lead_slice(df: pd.DataFrame, sec: dict) -> pd.DataFrame:
    """Restrict df to the section's lead panel and its 'main' rows (financing_gap
    carries both 'main' and 'sk_all' chart_type rows; only 'main' has SK/EA/DE)."""
    out = df
    if "chart_type" in out.columns:
        out = out[out["chart_type"] == "main"]
    panel_col = sec.get("panel_col")
    panel = _lead_panel(sec)
    if panel_col and panel is not None and panel_col in out.columns:
        out = out[out[panel_col].astype(str) == str(panel)]
    return out


def sk_ea_gap(df: pd.DataFrame, sec: dict) -> float | None:
    """Latest-wave absolute SK-EA pp gap on the section's lead metric."""
    sub = _lead_slice(df, sec)
    if sub.empty:
        return None
    value_col = sec["value_col"]
    latest_wave = sub["wave_number"].max()
    latest = sub[sub["wave_number"] == latest_wave]
    sk = latest[latest["country_code"] == "SK"][value_col]
    ea = latest[latest["country_code"] == "EA"][value_col]
    if sk.empty or ea.empty or pd.isna(sk.iloc[0]) or pd.isna(ea.iloc[0]):
        return None
    return abs(float(sk.iloc[0]) - float(ea.iloc[0]))


def historical_extremity(df: pd.DataFrame, sec: dict) -> dict:
    """Is the latest SK reading a record vs its own history, or the widest
    cross-country spread on record? Returns a dict with a summary bool plus
    the supporting scalars, so the caller can render human-readable text."""
    sub = _lead_slice(df, sec)
    value_col = sec["value_col"]
    result = {"is_record": False, "zscore": None, "widest_spread_on_record": False,
              "waves_covered": 0, "historical_extreme": False}
    if sub.empty:
        return result

    sk = sub[sub["country_code"] == "SK"].sort_values("wave_number")
    sk_vals = sk[value_col].dropna()
    result["waves_covered"] = int(sk_vals.shape[0])
    if not sk_vals.empty:
        latest_val = sk_vals.iloc[-1]
        result["is_record"] = bool(latest_val == sk_vals.max() or latest_val == sk_vals.min())
        prior = sk_vals.iloc[:-1]
        if len(prior) >= 3 and prior.std() > 0:
            result["zscore"] = float((latest_val - prior.mean()) / prior.std())

    spreads = []
    for wave, grp in sub.groupby("wave_number"):
        vals = grp[value_col].dropna()
        if len(vals) >= 2:
            spreads.append((wave, float(vals.max() - vals.min())))
    if spreads:
        spreads.sort(key=lambda x: x[0])
        latest_spread = spreads[-1][1]
        result["widest_spread_on_record"] = bool(latest_spread == max(s for _, s in spreads))

    result["historical_extreme"] = bool(
        result["is_record"]
        or (result["zscore"] is not None and abs(result["zscore"]) >= 2)
        or result["widest_spread_on_record"]
    )
    return result


def direction_reversal(df: pd.DataFrame, sec: dict, noise_floor: float = 2.0) -> bool:
    """Did SK's latest wave-over-wave delta flip sign vs the prior delta (a
    genuine turning point), both moves clearing a noise floor?"""
    sub = _lead_slice(df, sec)
    if sub.empty:
        return False
    value_col = sec["value_col"]
    sk_vals = sub[sub["country_code"] == "SK"].sort_values("wave_number")[value_col].dropna()
    if len(sk_vals) < 3:
        return False
    latest, prev, prev2 = sk_vals.iloc[-1], sk_vals.iloc[-2], sk_vals.iloc[-3]
    delta_latest = latest - prev
    delta_prev = prev - prev2
    if abs(delta_latest) < noise_floor or abs(delta_prev) < noise_floor:
        return False
    return bool((delta_latest > 0) != (delta_prev > 0))


def reliable_n(df: pd.DataFrame, sec: dict, threshold: int = 30) -> bool:
    """Latest SK n_respondents on the lead metric is >= threshold. Defaults to
    True (reliable) when no sample-size column is present in this section's
    data, rather than penalizing sections that simply don't carry an n column."""
    sub = _lead_slice(df, sec)
    if sub.empty:
        return True
    n_col = next((c for c in _N_CANDIDATE_COLS if c in sub.columns), None)
    if n_col is None:
        return True
    latest_wave = sub["wave_number"].max()
    latest_sk = sub[(sub["wave_number"] == latest_wave) & (sub["country_code"] == "SK")]
    if latest_sk.empty or pd.isna(latest_sk[n_col].iloc[0]):
        return True
    return bool(float(latest_sk[n_col].iloc[0]) >= threshold)


ECB_EMPHASIS_SYSTEM = textwrap.dedent("""
    You will receive a list of report section themes and the text of the ECB's own
    published SAFE survey analysis for the same wave.

    For EACH theme, decide whether the ECB publication discusses it prominently
    (not just a passing mention buried in a data table — an actual point the ECB
    text makes). If it does, quote the single supporting sentence (<=25 words,
    verbatim or lightly trimmed from the ECB text — do NOT invent or paraphrase
    into a claim the text doesn't make).

    Return JSON only — no markdown fences:
    {"section_id": {"emphasized": true/false, "quote": "<=25-word quote or empty string"}, ...}

    Include an entry for every section_id given, even if emphasized is false
    (quote "" in that case). Do not invent a quote for a theme the ECB text
    doesn't actually discuss.
""").strip()


def classify_ecb_emphasis(
    ecb_context: str,
    sections: list[dict],
    mistral_client,
    cost_tracker: dict,
) -> dict[str, dict]:
    """Return {section_id: {"emphasized": bool, "quote": str}} for each section,
    via one Mistral Small call covering all sections. Mirrors _sharpen_with_ecb's
    scope guard: returns {} if ecb_context is empty or mentions Slovakia fewer
    than twice (an EA-level page, not really about Slovakia specifically)."""
    if not ecb_context or ecb_context.lower().count("slovak") < 2:
        return {}
    themes = "\n".join(f"- {s['id']}: {s['title']}" for s in sections)
    try:
        resp = mistral_client.chat.complete(
            model="mistral-small-latest",
            max_tokens=800,
            messages=[
                {"role": "system", "content": ECB_EMPHASIS_SYSTEM},
                {"role": "user", "content": f"Section themes:\n{themes}\n\nECB publication text:\n{ecb_context}"},
            ],
        )
        if resp.usage:
            _track_cost(cost_tracker, "mistral-small-latest",
                        _Usage(resp.usage.prompt_tokens, resp.usage.completion_tokens))
        raw = resp.choices[0].message.content.strip()
        raw = re.sub(r"^```(?:json)?\s*", "", raw)
        raw = re.sub(r"\s*```$", "", raw).strip()
        parsed = json.loads(repair_json(raw))
        return {
            sid: {"emphasized": bool(v.get("emphasized", False)), "quote": str(v.get("quote", "")).strip()}
            for sid, v in parsed.items() if isinstance(v, dict)
        }
    except Exception:
        return {}


def _tier_and_deprioritized(sec: dict, df: pd.DataFrame) -> tuple[str, list[str]]:
    """Resolve the section's effective exec_tier (at its lead panel) and build
    human-readable labels for any policy_technical sub-items to name in the prompt."""
    subitem_tiers = sec.get("subitem_tiers")
    if not subitem_tiers:
        return sec.get("exec_tier", "supporting"), []

    panel_col = sec.get("panel_col")
    panel_label_col = sec.get("panel_label_col", panel_col)
    q_code = (sec.get("question_ids") or [""])[0].upper()
    deprioritized = []
    for sub_item, tier in subitem_tiers.items():
        if tier != "policy_technical":
            continue
        label = sub_item
        if panel_col and panel_label_col and panel_col in df.columns:
            match = df[df[panel_col].astype(str) == str(sub_item)]
            if not match.empty and panel_label_col in match.columns:
                label = match[panel_label_col].iloc[0]
        deprioritized.append(f"{label} ({q_code}{sub_item})")

    lead_panel = _lead_panel(sec)
    tier = subitem_tiers.get(lead_panel) or sec.get("exec_tier", "supporting")
    return tier, deprioritized


def build_section_signals(
    rendered: list[dict],
    data: dict[str, pd.DataFrame],
    sections_by_id: dict[str, dict],
    ecb_emphasis: dict[str, dict] | None = None,
) -> dict[str, dict]:
    """Assemble the reasoning-channel signals for each rendered standard section.
    Sections not present in sections_by_id (e.g. adhoc_spotlight, which has its
    own guarantee mechanism in get_exec_summary) are skipped."""
    ecb_emphasis = ecb_emphasis or {}
    signals: dict[str, dict] = {}
    for r in rendered:
        sid = r.get("section_id")
        sec = sections_by_id.get(sid)
        if sec is None or sid not in data:
            continue
        df = data[sid]
        tier, deprioritized = _tier_and_deprioritized(sec, df)
        extremity = historical_extremity(df, sec)
        emphasis = ecb_emphasis.get(sid, {})
        signals[sid] = {
            "tier": tier,
            "deprioritized_topics": deprioritized,
            "sk_ea_gap_pp": sk_ea_gap(df, sec),
            "historical_extreme": extremity["historical_extreme"],
            "waves_covered": extremity["waves_covered"],
            "direction_reversal": direction_reversal(df, sec),
            "reliable_n": reliable_n(df, sec),
            "ecb_emphasis": bool(emphasis.get("emphasized", False)),
            "ecb_quote": emphasis.get("quote", ""),
        }
    return signals


def _format_signals_line(sig: dict) -> str:
    """Render a section's signals dict as the [SIGNALS] prompt line."""
    parts = [f"tier={sig['tier']}"]
    if sig["deprioritized_topics"]:
        parts.append(f"deprioritized: {', '.join(sig['deprioritized_topics'])}")
    gap = sig["sk_ea_gap_pp"]
    parts.append(f"SK-EA gap={gap:.1f}pp" if gap is not None else "SK-EA gap=n/a")
    if sig["historical_extreme"]:
        parts.append(f"historical=record or near-record (of {sig['waves_covered']} waves)")
    else:
        parts.append("historical=no")
    parts.append(f"reversal={'yes' if sig['direction_reversal'] else 'no'}")
    parts.append(f"reliable_n={'yes' if sig['reliable_n'] else 'no'}")
    if sig["ecb_emphasis"]:
        parts.append(f'ECB emphasis=yes ("{sig["ecb_quote"]}")')
    else:
        parts.append("ECB emphasis=no")
    return "[SIGNALS] " + " | ".join(parts)


def get_exec_summary(
    rendered_sections: list[dict],
    cost_tracker: dict,
    historical_context: str = "",
    section_signals: dict[str, dict] | None = None,
    adhoc_section: dict | None = None,
    anthropic_client=None,
    mistral_client=None,
) -> list[dict]:
    """Two-pass exec summary. Returns list of {bullet, section_id} dicts."""
    section_ids = {s["section_id"] for s in rendered_sections}
    section_signals = section_signals or {}

    lines = ["Section findings:\n"]
    for s in rendered_sections:
        lines.append(f"## {s['title']} [section_id: {s['section_id']}]")
        if s.get("finding"):
            lines.append(f"Finding: {s['finding']}")
        if s.get("sign_note"):
            lines.append(f"Sign convention: {s['sign_note']}")
        if s["section_id"] in section_signals:
            lines.append(_format_signals_line(section_signals[s["section_id"]]))
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


SHORTEN_PANEL_LABEL_SYSTEM = textwrap.dedent("""
    You shorten a raw ECB survey item label into a short chart panel title.

    Rules:
    - Produce ONE short label or phrase, at most 6 words.
    - Strip interviewer instructions (e.g. bracketed "[READ IF NECESSARY: ...]"
      text), item-letter prefixes (e.g. "a)", "b)"), and any leftover punctuation
      artifacts — keep only the substantive content a chart reader needs.
    - Do not invent content not present in the source text.

    Respond with JSON only, no markdown fences: {"short_label": "..."}
""").strip()


def _shorten_panel_label_llm(source_text: str, mistral_client) -> dict:
    """Shorten a messy/long annex item label into a short chart panel title.

    Returns {"short_label": str, "_usage": {...}} — short_label is "" on any
    failure (malformed response, API error); caller should fall back to the
    deterministically-cleaned (but possibly still long) original text.
    """
    result = {"short_label": ""}
    try:
        resp = mistral_client.chat.complete(
            model="mistral-small-latest",
            max_tokens=40,
            messages=[
                {"role": "system", "content": SHORTEN_PANEL_LABEL_SYSTEM},
                {"role": "user", "content": source_text[:500]},
            ],
        )
        raw = resp.choices[0].message.content.strip()
        raw = re.sub(r"^```(?:json)?\s*", "", raw)
        raw = re.sub(r"\s*```$", "", raw).strip()
        parsed = json.loads(repair_json(raw))
        result["short_label"] = str(parsed.get("short_label", "")).strip()
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
    question_texts: dict | None = None,
) -> tuple[list[dict], list[dict], dict]:
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
    if question_texts:
        payload["annex_questions"] = question_texts
    if adhoc_s:
        payload["adhoc"] = {
            "theme_label": adhoc_s.get("theme_label", ""),
            "title":       adhoc_s.get("title", ""),
            "finding":     adhoc_s.get("finding", ""),
            "bullets":     adhoc_s.get("bullets", []),
            "bullets_by_question": adhoc_s.get("bullets_by_question", {}),
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
        "If an \"annex_questions\" object is present, translate every question text value "
        "(keep the keys, i.e. question IDs, unchanged) — these are official survey question "
        "wordings shown in a glossary, translate them faithfully rather than paraphrasing. "
        "Return valid JSON only — no markdown fences — with exactly the same structure as the input.\n\n"
        + json.dumps(payload, ensure_ascii=False)
    )
    client = _mistral_client()
    _TRANSLATE_MODEL = "mistral-large-2512"
    resp = client.chat.complete(
        model=_TRANSLATE_MODEL,
        max_tokens=7000,
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
        return rendered, exec_bullets, (question_texts or {})

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
        orig_bbq = adhoc_s.get("bullets_by_question", {})
        t_bbq    = sk_adhoc.get("bullets_by_question") or {}
        sk_rendered.append({
            **adhoc_s,
            "theme_label": sk_adhoc.get("theme_label", adhoc_s.get("theme_label", "")),
            "title":       sk_adhoc.get("title",       adhoc_s.get("title", "")),
            "finding":     sk_adhoc.get("finding",     adhoc_s.get("finding", "")),
            "bullets":     sk_adhoc.get("bullets",     adhoc_s.get("bullets", [])),
            # Fall back per-question (not all-or-nothing) so a translation gap for one
            # question doesn't blank out every other question's Slovak bullets.
            "bullets_by_question": {
                qid: t_bbq.get(qid, orig_bbq.get(qid, [])) for qid in orig_bbq
            },
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

    sk_question_texts = translated.get("annex_questions") or (question_texts or {})
    if question_texts and not translated.get("annex_questions"):
        print("  [SK] Annex question translation missing from response — falling back to English annex text")

    return sk_rendered, sk_exec_bullets, sk_question_texts


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
