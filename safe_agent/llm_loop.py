"""LLM draft → judge → rewrite loop using the Anthropic SDK."""

from __future__ import annotations

import json
import os
from pathlib import Path

import anthropic

_PROMPTS_DIR = Path(__file__).parent / "prompts"

_DRAFT_MODEL = "claude-haiku-4-5-20251001"
_JUDGE_MODEL = "claude-opus-4-8"
_MAX_TOKENS_DRAFT = 4096
_MAX_TOKENS_JUDGE = 1024
_PASS_THRESHOLD = 0.85
_MAX_ITERATIONS = 3


def _load_prompt(name: str) -> str:
    return (_PROMPTS_DIR / f"{name}.md").read_text()


def _call(model: str, system: str, user: str, max_tokens: int) -> str:
    client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
    msg = client.messages.create(
        model=model,
        max_tokens=max_tokens,
        system=system,
        messages=[{"role": "user", "content": user}],
    )
    return msg.content[0].text


def _parse_verdict(raw: str) -> dict:
    """Extract JSON from judge response (may be wrapped in markdown code fences)."""
    raw = raw.strip()
    if "```" in raw:
        raw = raw.split("```")[1]
        if raw.startswith("json"):
            raw = raw[4:]
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        # Graceful degradation: treat as low score so we keep iterating
        return {"score": 0.0, "factual_corrections": [], "missing_topics": [], "tone_issues": []}


def run_llm_loop(data_json: dict, ecb_report: dict, mart_columns: list[str]) -> dict:
    """
    Run the full draft → judge → rewrite loop.

    Returns:
        {
            "report_html": str,       # final HTML report body (sections only, no <html> wrapper)
            "executive_summary": str, # plain-text executive summary for email
            "gap_section": str,       # HTML for the data gaps section
            "iterations": int,
            "final_score": float,
        }
    """
    draft_system = _load_prompt("draft")
    judge_system = _load_prompt("judge")
    gap_system = _load_prompt("gap_finder")

    data_str = json.dumps(data_json, indent=2, default=str)
    ecb_sk = ecb_report.get("slovakia_text", "")
    ecb_ea = ecb_report.get("euroarea_text", "")
    ecb_combined = f"=== ECB REPORT — SLOVAKIA SECTION ===\n{ecb_sk}\n\n=== ECB REPORT — EURO AREA SECTION ===\n{ecb_ea}"

    draft_user = (
        f"SURVEY DATA (JSON):\n```json\n{data_str}\n```\n\n"
        f"ECB REFERENCE REPORT EXCERPTS:\n{ecb_combined}"
    )

    draft = _call(_DRAFT_MODEL, draft_system, draft_user, _MAX_TOKENS_DRAFT)

    final_score = 0.0
    for i in range(_MAX_ITERATIONS):
        judge_user = (
            f"DRAFT REPORT:\n{draft}\n\n"
            f"ECB REFERENCE EXCERPTS:\n{ecb_combined}\n\n"
            f"SURVEY DATA:\n```json\n{data_str}\n```"
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
            f"- Factual corrections needed: {json.dumps(corrections)}\n"
            f"- Missing topics: {json.dumps(missing)}\n"
            f"- Tone issues: {json.dumps(tone)}\n\n"
            f"ECB REFERENCE:\n{ecb_combined}\n\n"
            f"SURVEY DATA:\n```json\n{data_str}\n```\n\n"
            "Please rewrite the report incorporating all corrections above."
        )
        draft = _call(_DRAFT_MODEL, draft_system, rewrite_user, _MAX_TOKENS_DRAFT)

    # Gap analysis
    columns_str = "\n".join(mart_columns)
    gap_user = (
        f"ECB REFERENCE REPORT (full text excerpt):\n{ecb_report.get('full_text', '')[:8000]}\n\n"
        f"AVAILABLE MART COLUMNS:\n{columns_str}"
    )
    gap_section = _call(_DRAFT_MODEL, gap_system, gap_user, 1024)

    # Extract executive summary (first HTML paragraph / section)
    exec_summary = _extract_exec_summary(draft)

    return {
        "report_html": draft,
        "executive_summary": exec_summary,
        "gap_section": gap_section,
        "iterations": min(i + 1, _MAX_ITERATIONS),
        "final_score": final_score,
    }


def _extract_exec_summary(html: str) -> str:
    """Pull plain-text executive summary from the first paragraph of the HTML draft."""
    import re
    # Remove HTML tags for plain text email
    text = re.sub(r"<[^>]+>", "", html)
    lines = [l.strip() for l in text.splitlines() if l.strip()]
    # Return first ~5 non-heading lines as summary
    summary_lines = []
    for line in lines:
        if not line.startswith("#") and len(line) > 40:
            summary_lines.append(line)
        if len(summary_lines) >= 5:
            break
    return "\n\n".join(summary_lines)
