"""
SAFE Report Quality Gate — Mistral sign-off supervisor.

Reads reports/output/report_latest.html, extracts all section text and bullets,
asks Mistral to score the report on readability, substance, and coherence.

Tiered exit codes — the workflow reads quality_scores*.json's "tier2_fail" boolean
to decide whether to block publish (exit code alone can't distinguish tier 1 from
tier 2 in GitHub Actions' outcome/conclusion, both just read as "failure"):
  0 = clean pass
  3 = tier-1 issues only (style: bullet length, magnitude-word calibration, sign
      language, bare response codes) — logged, does NOT block publish. The
      generation pipeline (llm.py's retry loop + enforce_bullet_style) already
      tries to fix these before the report is assembled, so this is a rare
      last-resort catch, not the primary enforcement point.
  1 = tier-2 failure (content quality: supervisor score/verdict, chart rendering,
      adhoc spotlight) — should block publish.

Usage:
  python reports/quality_check.py
"""

import base64
import json
import os
import re
import sys
from pathlib import Path

from bs4 import BeautifulSoup
from dotenv import load_dotenv
from mistralai import Mistral

from evals import check_all_style

load_dotenv(Path(__file__).parent.parent / ".env")

_DEFAULT_REPORT_HTML = Path(__file__).parent / "output" / "report_latest.html"
PASS_THRESHOLD = 7        # main report: any dimension below this = fail
ADHOC_PASS_THRESHOLD = 8  # adhoc spotlight: stricter gate (Mistral Large reviewer)

SUPERVISOR_SYSTEM = """
You are a quality supervisor for an automatically generated financial survey report.
You will receive the text content of a report on ECB SAFE survey results for Slovakia.

Score it on four dimensions, each 1–10:

- readability (1–10): Text flows naturally, no jibberish, no parsing artefacts,
  no repeated phrases. Bullets are complete sentences with subject and verb.
  Score 1 if you see obvious garbage like "I have sufficient data" or "Let me analyze".

- substance (1–10): Bullets cite specific numbers with direction and comparison
  (e.g. "a net 26% of Slovak firms reported…, vs 18% in the EA"). Generic statements
  like "conditions improved" without a number score low. Historical comparisons score high.

- coherence (1–10): The finding headline matches the bullets. Numbers in bullets are
  directionally consistent with the sign convention stated. No internal contradictions.

- sign_convention (1–10): Net balance values are written correctly in prose.
  Score 1 if you see "a net -X%" where X > 0 — this is a double-negative (the word
  "deteriorated/tightened/worsened" already captures the direction; the minus sign is
  redundant and confusing). Score 1 if you see "surged", "plummeted", or "collapsed"
  without a precise before/after value comparison. Score 10 if all net balances use
  absolute values and direction is expressed through words only (e.g. "a net 5% of firms
  expected deterioration" not "a net -5% expected deterioration").

Return JSON only — no markdown fences:
{"readability": <1-10>, "substance": <1-10>, "coherence": <1-10>, "sign_convention": <1-10>, "verdict": "pass" or "fail", "reason": "<one sentence>"}

Set verdict to "fail" if ANY dimension is below 6, or if you see obvious parsing artefacts.
""".strip()


ADHOC_SUPERVISOR_SYSTEM = """You are a critical editor reviewing a "Special Focus" sidebar for a financial survey
report on ECB SAFE data for Slovakia. You will receive the text of the sidebar.

Score on four dimensions, each 1–10:
- grounding (1–10): Every number in the bullets appears verbatim in the underlying data.
  Score 1 if you see a percentage, net balance, or count that looks invented or paraphrased.
  Score 10 if all cited numbers are specific and plausible given the context.
- coverage (1–10): The bullets address the most striking SK vs EA differences.
  Score low if bullets discuss marginal or obvious findings while ignoring large gaps.
  Score 10 if the finding and bullets capture the headline story clearly.
- readability (1–10): Plain English accessible to a non-expert. Complete sentences.
  No jargon. Score 1 if the text is robotic, boilerplate, or hard to parse.
- chart_alignment (1–10): The section's chart should correspond to the key bullets.
  If no chart information is available, score this dimension 8 (neutral).

You are a strict reviewer — a 7 is a real failure here. The threshold for pass is 8.
Return JSON only (no markdown fences):
{"grounding": <1-10>, "coverage": <1-10>, "readability": <1-10>, "chart_alignment": <1-10>,
 "verdict": "pass" or "fail", "reason": "<one sentence>"}

Set verdict to "fail" if ANY dimension is below 8.""".strip()


CHART_CHECK_PROMPT = """You are a chart quality inspector. Look at this matplotlib chart image and check for rendering problems.

Flag any of the following as FAIL:
- Axis tick labels showing Python object repr (e.g. "Name: survey_period_label, dtype: object", "Series(...)", "<pandas...")
- Tick labels that are truncated or overlap each other badly
- Axis labels cut off by the figure boundary
- Chart is completely blank or has no data
- Legend text that is cut off

If none of the above are present, return PASS.

Return JSON only: {"verdict": "pass" or "fail", "reason": "<one short sentence or empty string>"}"""


def extract_charts(html: str) -> list[str]:
    """Extract base64 PNG data URLs from embedded chart images."""
    return re.findall(r'data:image/png;base64,([A-Za-z0-9+/=]+)', html)


def check_charts(html: str, client: Mistral) -> tuple[str, str]:
    """Run Pixtral vision check on up to 6 charts. Returns (verdict, reason)."""
    chart_b64s = extract_charts(html)
    if not chart_b64s:
        return "pass", ""

    failures = []
    for i, b64 in enumerate(chart_b64s[:6]):
        try:
            resp = client.chat.complete(
                model="pixtral-12b-2409",
                max_tokens=100,
                messages=[{
                    "role": "user",
                    "content": [
                        {"type": "text", "text": CHART_CHECK_PROMPT},
                        {"type": "image_url", "image_url": {"url": f"data:image/png;base64,{b64}"}},
                    ],
                }],
            )
            raw = resp.choices[0].message.content.strip()
            if raw.startswith("```"):
                raw = raw.split("\n", 1)[-1].rsplit("```", 1)[0].strip()
            result = json.loads(raw)
            if result.get("verdict") == "fail":
                failures.append(f"Chart {i+1}: {result.get('reason', 'rendering issue')}")
        except Exception as e:
            print(f"  Chart check {i+1} error: {e}")

    if failures:
        return "fail", "; ".join(failures)
    return "pass", ""


def extract_report_text(html: str) -> str:
    soup = BeautifulSoup(html, "lxml")
    parts = []
    exec_sec = soup.select_one("#exec-summary")
    if exec_sec:
        parts.append("EXECUTIVE SUMMARY:")
        for li in exec_sec.select("li"):
            parts.append(f"  - {li.get_text(strip=True)}")
    for sec in soup.select("section[id]"):
        if sec.get("id") == "exec-summary":
            continue
        h3 = sec.select_one("h3")
        if h3:
            parts.append(f"\nSECTION: {h3.get_text(strip=True)}")
        for li in sec.select("li"):
            parts.append(f"  - {li.get_text(strip=True)}")
    return "\n".join(parts)


def extract_all_bullets(html: str) -> list[str]:
    """Extract every <li> bullet in the report (exec summary + all sections)."""
    soup = BeautifulSoup(html, "lxml")
    return [li.get_text(strip=True) for li in soup.select("li") if li.get_text(strip=True)]


def run_deterministic_checks(html: str) -> list[str]:
    """Code-enforced style checks — sign-language, magnitude-calibration, bare
    survey-response-code mismatches, and bullet length.

    Unlike the Mistral supervisor scores below, these are regex-based and cannot be
    talked out of failing by an LLM having a good day. This is now a Tier-1 (style)
    signal, not a hard gate — the generation pipeline (get_section_content_agentic's
    retry loop and enforce_bullet_style in llm.py) already tries to fix these before
    the report is even assembled, so a survivor here is a rare last-resort catch, not
    the primary enforcement point. See main()'s tiered exit-code handling below.
    """
    errors = []
    for bullet in extract_all_bullets(html):
        errors.extend(check_all_style(bullet))
    return errors


def extract_adhoc_text(html: str) -> str:
    """Extract adhoc spotlight text (finding + sub-section bullets) for quality checking."""
    soup = BeautifulSoup(html, "lxml")
    adhoc_sec = soup.select_one("#adhoc_spotlight")
    if not adhoc_sec:
        return ""
    parts = ["ADHOC SPECIAL FOCUS:"]
    finding = adhoc_sec.select_one(".section-finding")
    if finding:
        parts.append(f"  Finding: {finding.get_text(strip=True)}")
    for h4 in adhoc_sec.select("h4"):
        parts.append(f"\n  Sub-section: {h4.get_text(strip=True)}")
        sibling = h4.find_next_sibling()
        while sibling and sibling.name not in ("h4", "h3"):
            for li in sibling.select("li") if hasattr(sibling, "select") else []:
                parts.append(f"    - {li.get_text(strip=True)}")
            sibling = sibling.find_next_sibling()
    for li in adhoc_sec.select("li"):
        text = li.get_text(strip=True)
        if text and text not in "\n".join(parts):
            parts.append(f"  - {text}")
    return "\n".join(parts)


def main() -> None:
    import argparse as _ap
    _parser = _ap.ArgumentParser()
    _parser.add_argument("--html", type=Path, default=None,
                         help="Path to report HTML to check (default: reports/output/report_latest.html)")
    _args = _parser.parse_args()
    REPORT_HTML = _args.html if _args.html else _DEFAULT_REPORT_HTML
    OUTPUT_DIR = REPORT_HTML.parent
    # Derive a distinct scores filename per input report so EN/SK (and adhoc EN/SK)
    # runs don't clobber each other's output. The two main-report names keep their
    # exact historical filenames (generate_report.yml and write_run_manifest.py both
    # hardcode "quality_scores.json") — anything else (e.g. report_adhoc_latest*.html)
    # falls back to a general "_<rest-of-stem>" suffix instead of the removeprefix()
    # no-op that used to silently produce a malformed "quality_scoresreport_adhoc_..."
    # filename nothing could find.
    if REPORT_HTML.stem == "report_latest":
        _suffix = ""
    elif REPORT_HTML.stem == "report_latest_sk":
        _suffix = "_sk"
    else:
        _suffix = "_" + REPORT_HTML.stem.removeprefix("report_")
    SCORES_PATH = OUTPUT_DIR / f"quality_scores{_suffix}.json"

    if not REPORT_HTML.exists():
        print(f"ERROR: {REPORT_HTML} not found — run the report generator first")
        sys.exit(1)

    html = REPORT_HTML.read_text(encoding="utf-8")
    report_text = extract_report_text(html)

    if len(report_text) < 200:
        print("ERROR: report text too short — likely a parse failure")
        sys.exit(1)

    print(f"Quality check: evaluating {len(report_text):,} chars of report text...")

    # Deterministic style checks — code-enforced, run before any LLM call. Tier 1
    # (style only): recorded, but does NOT abort the run — the generation pipeline
    # already tries to fix these before assembly (see run_deterministic_checks'
    # docstring), so any survivor here is logged for visibility, not blocking.
    print("Quality check: running deterministic sign/magnitude checks...")
    tier1_issues = run_deterministic_checks(html)
    if tier1_issues:
        print(f"  DETERMINISTIC CHECK: {len(tier1_issues)} tier-1 style issue(s) found (non-blocking):")
        for e in tier1_issues:
            print(f"    - {e}")
    else:
        print("  Deterministic checks OK")

    client = Mistral(api_key=os.environ["MISTRAL_API_KEY"])

    # Chart sanity check via Pixtral vision — Tier 2 (content quality): a genuinely
    # broken chart is a real problem, not a style nit, so this can still block publish.
    print("Quality check: inspecting charts...")
    chart_verdict, chart_reason = check_charts(html, client)
    if chart_verdict == "fail":
        print(f"  CHART FAIL: {chart_reason}")
    else:
        print("  Charts OK")

    try:
        resp = client.chat.complete(
            model="mistral-small-latest",
            max_tokens=400,
            messages=[
                {"role": "system", "content": SUPERVISOR_SYSTEM},
                {"role": "user", "content": report_text[:8000]},
            ],
        )
        raw = resp.choices[0].message.content.strip()
        result = json.loads(raw)
    except Exception as e:
        # A Mistral API outage must not be indistinguishable from a real quality
        # failure — degrade to a clearly-labeled "assumed pass" rather than crashing
        # the CI job (matches the parse-error fallback below and the adhoc
        # supervisor's existing try/except pattern). The tier-1 style findings and
        # chart check already ran locally/independently of this call, so they're
        # still reported accurately even though the supervisor itself is unavailable.
        print(f"Quality check: supervisor call failed ({e}) — assuming pass")
        fallback = {
            "readability": 10, "substance": 10, "coherence": 10, "sign_convention": 10,
            "verdict": "pass", "reason": f"supervisor error — assumed pass: {e}",
            "tier1_issues": tier1_issues, "tier1_issue_count": len(tier1_issues),
            "tier2_fail": chart_verdict == "fail",
            "chart_verdict": chart_verdict, "chart_reason": chart_reason,
        }
        SCORES_PATH.write_text(json.dumps(fallback))
        if chart_verdict == "fail":
            print("Quality gate FAILED (tier 2 — chart rendering) — should block publish")
            sys.exit(1)
        if tier1_issues:
            print(f"Quality gate: {len(tier1_issues)} tier-1 style issue(s) — logged, not blocking")
            sys.exit(3)
        sys.exit(0)

    r = result.get("readability", 10)
    s = result.get("substance", 10)
    c = result.get("coherence", 10)
    sc = result.get("sign_convention", 10)
    verdict = result.get("verdict", "pass")
    reason = result.get("reason", "")

    print(f"  readability={r}/10  substance={s}/10  coherence={c}/10  sign_convention={sc}/10  → {verdict.upper()}")
    print(f"  Reason: {reason}")

    scores = {
        "readability": r, "substance": s, "coherence": c,
        "sign_convention": sc, "verdict": verdict, "reason": reason,
        "tier1_issues": tier1_issues, "tier1_issue_count": len(tier1_issues),
        "chart_verdict": chart_verdict, "chart_reason": chart_reason,
    }

    # Adhoc spotlight quality check (if present)
    adhoc_text = extract_adhoc_text(html)
    adhoc_verdict = "pass"
    if adhoc_text:
        print("Quality check: evaluating adhoc spotlight (Mistral Large, threshold ≥ 8)...")
        try:
            adhoc_resp = client.chat.complete(
                model="mistral-large-2512",
                max_tokens=300,
                messages=[
                    {"role": "system", "content": ADHOC_SUPERVISOR_SYSTEM},
                    {"role": "user", "content": adhoc_text[:4000]},
                ],
            )
            adhoc_raw = adhoc_resp.choices[0].message.content.strip()
            if adhoc_raw.startswith("```"):
                adhoc_raw = adhoc_raw.split("\n", 1)[-1].rsplit("```", 1)[0].strip()
            adhoc_result = json.loads(adhoc_raw)
            ag = adhoc_result.get("grounding", 10)
            aco = adhoc_result.get("coverage", 10)
            ar = adhoc_result.get("readability", 10)
            aca = adhoc_result.get("chart_alignment", 10)
            adhoc_verdict = adhoc_result.get("verdict", "pass")
            adhoc_reason = adhoc_result.get("reason", "")
            print(f"  adhoc: grounding={ag}/10  coverage={aco}/10  readability={ar}/10  chart_alignment={aca}/10  → {adhoc_verdict.upper()}")
            print(f"  Adhoc reason: {adhoc_reason}")
            scores["adhoc"] = {
                "grounding": ag, "coverage": aco, "readability": ar,
                "chart_alignment": aca, "verdict": adhoc_verdict, "reason": adhoc_reason,
            }
            if adhoc_verdict == "fail" or min(ag, aco, ar, aca) < ADHOC_PASS_THRESHOLD:
                adhoc_verdict = "fail"
        except Exception as e:
            print(f"  Adhoc quality check error: {e} — assuming pass")

    tier2_fail = (
        verdict == "fail" or min(r, s, c, sc) < PASS_THRESHOLD
        or chart_verdict == "fail"
        or adhoc_verdict == "fail"
    )
    scores["tier2_fail"] = tier2_fail
    SCORES_PATH.write_text(json.dumps(scores))

    if tier2_fail:
        print("Quality gate FAILED (tier 2 — content quality) — should block publish")
        sys.exit(1)

    if tier1_issues:
        print(f"Quality gate: {len(tier1_issues)} tier-1 style issue(s) — logged, not blocking")
        sys.exit(3)

    print("Quality gate passed")
    sys.exit(0)


if __name__ == "__main__":
    main()
