"""LLM draft → judge → rewrite loop using the Anthropic SDK."""

from __future__ import annotations

import json
import os
import re
import time
from pathlib import Path

import anthropic

_PROMPTS_DIR = Path(__file__).parent / "prompts"

_DRAFT_MODEL = "claude-haiku-4-5-20251001"
# Set SAFE_AGENT_TEST=1 to use Haiku for the judge too (cheaper, for local testing)
_JUDGE_MODEL = "claude-haiku-4-5-20251001" if os.environ.get("SAFE_AGENT_TEST") else "claude-opus-4-8"
_MAX_TOKENS_DRAFT = 4096
_MAX_TOKENS_JUDGE = 2048
_PASS_THRESHOLD = 0.85
_MAX_ITERATIONS = 3

# Keep only this many recent waves in the LLM payload
_MAX_WAVES_IN_PAYLOAD = 5


def _load_prompt(name: str) -> str:
    return (_PROMPTS_DIR / f"{name}.md").read_text()


def _strip_fences(text: str) -> str:
    """Remove markdown code fences (```html ... ``` or ``` ... ```) from LLM output."""
    text = text.strip()
    if text.startswith("```"):
        lines = text.split("\n")
        # Drop first line (```html or ```) and last line (```)
        if lines[-1].strip() == "```":
            lines = lines[1:-1]
        else:
            lines = lines[1:]
        text = "\n".join(lines)
    return text.strip()


def _call(model: str, system: str, user: str, max_tokens: int, retries: int = 3) -> str:
    client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
    for attempt in range(retries):
        try:
            msg = client.messages.create(
                model=model,
                max_tokens=max_tokens,
                system=system,
                messages=[{"role": "user", "content": user}],
            )
            return msg.content[0].text
        except anthropic.RateLimitError as e:
            if attempt == retries - 1:
                raise
            wait = 60 * (attempt + 1)
            print(f"[llm_loop] rate limited, waiting {wait}s before retry {attempt + 2}/{retries}...")
            time.sleep(wait)
        except anthropic.APIError as e:
            if attempt == retries - 1:
                raise
            time.sleep(10)
    raise RuntimeError("unreachable")


def _build_llm_payload(data_json: dict, max_waves: int = _MAX_WAVES_IN_PAYLOAD) -> dict:
    """Build the payload the LLM receives: KPI rows only.

    The LLM must NOT pick series from raw marts — all number selection is done
    at the mart_safe__slovakia_kpis level.  Raw mart tables are included in
    data_json only for chart rendering (render_report.py), not sent to the LLM.
    """
    kpi_rows = data_json.get("kpis", [])
    if kpi_rows:
        all_waves = sorted({r["wave_number"] for r in kpi_rows}, reverse=True)
        keep_waves = set(all_waves[:max_waves])
        kpi_rows = [r for r in kpi_rows if r["wave_number"] in keep_waves]

    return {
        "latest_wave": data_json["latest_wave"],
        "latest_period_label": data_json["latest_period_label"],
        "kpis": kpi_rows,
    }


def _parse_verdict(raw: str) -> dict:
    """Extract JSON from judge response, tolerating markdown fences and truncation."""
    raw = raw.strip()

    # Strip markdown fences if present
    candidates = []
    if "```" in raw:
        parts = raw.split("```")
        for part in parts:
            candidates.append(part.lstrip("json").strip())
    candidates.append(raw)

    for candidate in candidates:
        try:
            return json.loads(candidate)
        except json.JSONDecodeError:
            pass

    # Regex fallback: extract score field from a potentially truncated JSON response
    m = re.search(r'"score"\s*:\s*([0-9]*\.?[0-9]+)', raw)
    if m:
        score = float(m.group(1))
        # Try to extract other fields too
        corrections = re.findall(r'"([^"]{10,})"', raw.split('"factual_corrections"')[-1][:500]) if "factual_corrections" in raw else []
        missing = re.findall(r'"([^"]{5,})"', raw.split('"missing_topics"')[-1][:300]) if "missing_topics" in raw else []
        print(f"[llm_loop] judge JSON truncated — recovered score={score:.2f} via regex")
        return {"score": score, "factual_corrections": corrections[:5], "missing_topics": missing[:5], "tone_issues": []}

    print(f"[llm_loop] judge JSON could not be parsed, raw[:200]: {raw[:200]}")
    return {"score": 0.0, "factual_corrections": [], "missing_topics": [], "tone_issues": []}


def run_llm_loop(data_json: dict, ecb_report: dict, mart_schema: str) -> dict:
    """
    Run the full draft → judge → rewrite loop.

    Returns:
        {
            "report_html": str,
            "executive_summary": str,
            "gap_section": str,
            "iterations": int,
            "final_score": float,
        }
    """
    draft_system = _load_prompt("draft")
    judge_system = _load_prompt("judge")
    gap_system = _load_prompt("gap_finder")

    # Build LLM payload: KPI rows only (raw marts stay in data_json for chart rendering)
    trimmed = _build_llm_payload(data_json)
    data_str = json.dumps(trimmed, indent=2, default=str)

    ecb_sk = ecb_report.get("slovakia_text", "")
    ecb_ea = ecb_report.get("euroarea_text", "")
    has_ecb = bool(ecb_sk or ecb_ea)

    if has_ecb:
        ecb_block = (
            f"=== ECB REPORT — SLOVAKIA SECTION ===\n{ecb_sk[:3000]}\n\n"
            f"=== ECB REPORT — EURO AREA SECTION ===\n{ecb_ea[:2000]}"
        )
    else:
        ecb_block = (
            "NOTE: The official ECB SAFE PDF could not be retrieved for this run. "
            "Base your analysis entirely on the survey data JSON provided. "
            "Do not reference or compare to the ECB report."
        )

    draft_user = (
        f"SURVEY DATA (JSON):\n```json\n{data_str}\n```\n\n"
        f"ECB REFERENCE:\n{ecb_block}"
    )

    print(f"[llm_loop] draft payload: ~{len(draft_user)//4} tokens estimated")
    draft = _strip_fences(_call(_DRAFT_MODEL, draft_system, draft_user, _MAX_TOKENS_DRAFT))

    final_score = 0.0
    prev_score = -1.0
    iterations_done = 0
    for i in range(_MAX_ITERATIONS):
        iterations_done = i + 1
        # Strip HTML tags so judge sees plain prose (easier to fact-check)
        draft_plain = re.sub(r"<[^>]+>", " ", draft)
        draft_plain = re.sub(r"\s{2,}", " ", draft_plain).strip()

        no_ecb_note = (
            "NOTE: No ECB reference was available for this wave. "
            "Score based only on internal consistency: correct directional language, no invented numbers, professional tone. "
            "Do not penalise for missing ECB comparison.\n\n"
        ) if not has_ecb else ""
        judge_user = (
            f"{no_ecb_note}"
            f"DRAFT REPORT (plain text):\n{draft_plain[:8000]}\n\n"
            f"ECB REFERENCE:\n{ecb_block}"
        )
        print(f"[llm_loop] judge input: draft_plain[:100]={draft_plain[:100]!r}")
        verdict_raw = _call(_JUDGE_MODEL, judge_system, judge_user, _MAX_TOKENS_JUDGE)
        print(f"[llm_loop] judge raw (first 400 chars): {verdict_raw[:400]}")
        verdict = _parse_verdict(verdict_raw)
        final_score = float(verdict.get("score", 0.0))
        print(f"[llm_loop] iteration {i+1}: score={final_score:.2f}")

        if final_score >= _PASS_THRESHOLD:
            print(f"[llm_loop] passed threshold at iteration {i+1}")
            break

        # Stop early if score didn't improve — rewriting won't help
        if final_score <= prev_score:
            print(f"[llm_loop] score not improving ({prev_score:.2f} → {final_score:.2f}), stopping early")
            break
        prev_score = final_score

        corrections = verdict.get("factual_corrections", [])
        missing = verdict.get("missing_topics", [])
        tone = verdict.get("tone_issues", [])

        # Cap rewrite prompt: corrections JSON + capped draft + data — stays within rate limits
        rewrite_user = (
            f"CURRENT DRAFT (truncated to 6000 chars):\n{draft[:6000]}\n\n"
            f"JUDGE FEEDBACK:\n"
            f"- Score: {final_score}\n"
            f"- Factual corrections: {json.dumps(corrections)}\n"
            f"- Missing topics: {json.dumps(missing)}\n"
            f"- Tone issues: {json.dumps(tone)}\n\n"
            f"ECB REFERENCE:\n{ecb_block}\n\n"
            f"SURVEY DATA:\n```json\n{data_str}\n```\n\n"
            "Rewrite the full report incorporating all corrections above."
        )
        draft = _strip_fences(_call(_DRAFT_MODEL, draft_system, rewrite_user, _MAX_TOKENS_DRAFT))

    gap_user = (
        f"ECB REFERENCE (excerpt):\n{ecb_report.get('full_text', '')[:4000]}\n\n"
        f"MART SCHEMA (tables, columns, and distinct label values):\n{mart_schema}"
    )
    gap_section = _strip_fences(_call(_DRAFT_MODEL, gap_system, gap_user, 2048))

    return {
        "report_html": draft,
        "executive_summary": _extract_exec_summary(draft),
        "gap_section": gap_section,
        "iterations": iterations_done,
        "final_score": final_score,
    }


def _extract_exec_summary(html: str) -> str:
    text = re.sub(r"<[^>]+>", "", html)
    lines = [l.strip() for l in text.splitlines() if l.strip()]
    summary_lines = []
    for line in lines:
        if not line.startswith("#") and len(line) > 40:
            summary_lines.append(line)
        if len(summary_lines) >= 5:
            break
    return "\n\n".join(summary_lines)
