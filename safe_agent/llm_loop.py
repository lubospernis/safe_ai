"""LLM draft → judge → rewrite loop using the Anthropic SDK."""

from __future__ import annotations

import json
import os
import time
from pathlib import Path

import anthropic

_PROMPTS_DIR = Path(__file__).parent / "prompts"

_DRAFT_MODEL = "claude-haiku-4-5-20251001"
# Set SAFE_AGENT_TEST=1 to use Haiku for the judge too (cheaper, for local testing)
_JUDGE_MODEL = "claude-haiku-4-5-20251001" if os.environ.get("SAFE_AGENT_TEST") else "claude-opus-4-8"
_MAX_TOKENS_DRAFT = 4096
_MAX_TOKENS_JUDGE = 1024
_PASS_THRESHOLD = 0.85
_MAX_ITERATIONS = 3

# Keep only this many recent waves in the LLM payload to stay under token limits
_MAX_WAVES_IN_PAYLOAD = 3

# Columns to DROP from each mart before sending to the LLM (reduce token count)
_DROP_COLS = {
    "financing_conditions":  {"n_total", "n_nonresponse", "total_weight", "net_balance_unwtd", "pct_improved_wtd", "pct_unchanged_wtd", "pct_deteriorated_wtd"},
    "business_situation":    {"n_total", "n_nonresponse", "total_weight", "net_balance_unwtd", "pct_increased_wtd", "pct_unchanged_wtd", "pct_decreased_wtd"},
    "pressing_problems":     {"n_nonresponse", "total_weight", "avg_pressingness_unwtd", "reference_period"},
    "loan_applications":     {"n_total", "n_valid_q7a", "n_applied", "n_discouraged", "n_no_need", "n_other_no_apply", "n_success_full", "n_success_partial", "n_too_costly", "n_rejected", "n_pending", "total_weight"},
    "outlook":               {"n_total", "n_nonresponse", "total_weight", "net_balance_unwtd", "pct_increase_wtd", "pct_unchanged_wtd", "pct_decrease_wtd"},
    "expectations":          {"n_nonresponse", "total_weight", "mean_unwtd", "p25_unwtd", "median_unwtd", "p75_unwtd", "pct_positive", "pct_zero", "pct_negative", "pct_downside_wtd", "pct_balanced_wtd", "pct_upside_wtd"},
}


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


def _trim_payload(data_json: dict, max_waves: int = _MAX_WAVES_IN_PAYLOAD) -> dict:
    """Keep only the most recent N waves and drop columns the LLM doesn't need."""
    trimmed = {
        "latest_wave": data_json["latest_wave"],
        "latest_period_label": data_json["latest_period_label"],
    }
    for key in ("financing_conditions", "business_situation", "pressing_problems", "loan_applications", "outlook", "expectations"):
        rows = data_json.get(key, [])
        if not rows:
            trimmed[key] = []
            continue
        all_waves = sorted({r["wave_number"] for r in rows}, reverse=True)
        keep_waves = set(all_waves[:max_waves])
        drop = _DROP_COLS.get(key, set())
        trimmed[key] = [
            {k: v for k, v in r.items() if k not in drop}
            for r in rows if r["wave_number"] in keep_waves
        ]
    return trimmed


def _parse_verdict(raw: str) -> dict:
    """Extract JSON from judge response, tolerating markdown fences."""
    raw = raw.strip()
    # Strip markdown fences if present
    if "```" in raw:
        parts = raw.split("```")
        for part in parts:
            candidate = part.lstrip("json").strip()
            try:
                return json.loads(candidate)
            except json.JSONDecodeError:
                continue
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        # Graceful degradation — keep iterating
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

    # Trim payload to stay within rate limits
    trimmed = _trim_payload(data_json)
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
    iterations_done = 0
    for i in range(_MAX_ITERATIONS):
        iterations_done = i + 1
        judge_user = (
            f"DRAFT REPORT:\n{draft}\n\n"
            f"ECB REFERENCE:\n{ecb_block}\n\n"
            f"SURVEY DATA:\n```json\n{data_str}\n```\n\n"
            + ("NOTE: No ECB PDF was available. Score based on internal data consistency and quality only. "
               "Do not penalise for missing ECB comparison." if not has_ecb else "")
        )
        verdict_raw = _call(_JUDGE_MODEL, judge_system, judge_user, _MAX_TOKENS_JUDGE)
        verdict = _parse_verdict(verdict_raw)
        final_score = float(verdict.get("score", 0.0))
        print(f"[llm_loop] iteration {i+1}: score={final_score:.2f}")

        if final_score >= _PASS_THRESHOLD:
            print(f"[llm_loop] passed threshold at iteration {i+1}")
            break

        corrections = verdict.get("factual_corrections", [])
        missing = verdict.get("missing_topics", [])
        tone = verdict.get("tone_issues", [])

        rewrite_user = (
            f"CURRENT DRAFT:\n{draft}\n\n"
            f"JUDGE FEEDBACK:\n"
            f"- Score: {final_score}\n"
            f"- Factual corrections: {json.dumps(corrections)}\n"
            f"- Missing topics: {json.dumps(missing)}\n"
            f"- Tone issues: {json.dumps(tone)}\n\n"
            f"ECB REFERENCE:\n{ecb_block}\n\n"
            f"SURVEY DATA:\n```json\n{data_str}\n```\n\n"
            "Rewrite the report incorporating all corrections above."
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
    import re
    text = re.sub(r"<[^>]+>", "", html)
    lines = [l.strip() for l in text.splitlines() if l.strip()]
    summary_lines = []
    for line in lines:
        if not line.startswith("#") and len(line) > 40:
            summary_lines.append(line)
        if len(summary_lines) >= 5:
            break
    return "\n\n".join(summary_lines)
