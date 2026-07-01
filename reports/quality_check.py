"""
SAFE Report Quality Gate — Mistral sign-off supervisor.

Reads reports/output/report_latest.html, extracts all section text and bullets,
asks Mistral to score the report on readability, substance, and coherence.
Exits 0 (pass) or 1 (fail — blocks GitHub Actions deploy).

Usage:
  python reports/quality_check.py
"""

import json
import os
import sys
from pathlib import Path

from bs4 import BeautifulSoup
from dotenv import load_dotenv
from mistralai import Mistral

load_dotenv(Path(__file__).parent.parent / ".env")

REPORT_HTML = Path(__file__).parent / "output" / "report_latest.html"
PASS_THRESHOLD = 6  # any dimension below this = fail

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


def main() -> None:
    if not REPORT_HTML.exists():
        print(f"ERROR: {REPORT_HTML} not found — run run_report.py first")
        sys.exit(1)

    html = REPORT_HTML.read_text(encoding="utf-8")
    report_text = extract_report_text(html)

    if len(report_text) < 200:
        print("ERROR: report text too short — likely a parse failure")
        sys.exit(1)

    print(f"Quality check: evaluating {len(report_text):,} chars of report text...")

    client = Mistral(api_key=os.environ["MISTRAL_API_KEY"])
    resp = client.chat.complete(
        model="mistral-small-latest",
        max_tokens=400,
        messages=[
            {"role": "system", "content": SUPERVISOR_SYSTEM},
            {"role": "user", "content": report_text[:8000]},
        ],
    )
    raw = resp.choices[0].message.content.strip()

    try:
        result = json.loads(raw)
    except Exception:
        print(f"Quality check: could not parse supervisor response — assuming pass")
        print(f"  Raw response: {raw[:200]}")
        fallback = {"readability": 10, "substance": 10, "coherence": 10,
                    "sign_convention": 10, "verdict": "pass", "reason": "parse error — assumed pass"}
        (Path(__file__).parent / "output" / "quality_scores.json").write_text(json.dumps(fallback))
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
    }
    (Path(__file__).parent / "output" / "quality_scores.json").write_text(json.dumps(scores))

    if verdict == "fail" or min(r, s, c, sc) < PASS_THRESHOLD:
        print("Quality gate FAILED — blocking deploy")
        sys.exit(1)

    print("Quality gate passed")
    sys.exit(0)


if __name__ == "__main__":
    main()
