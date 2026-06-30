"""
ECB SAFE Gap Agent — compares the latest ECB SAFE publication against our automated
report and writes a structured gap report to reports/output/gap_report.md.

Usage:
  python reports/gap_agent.py                 # auto-discovers latest ECB URL
  python reports/gap_agent.py --ecb-url <URL> # explicit URL override

The latest ECB report URL is discovered automatically from the ECB SAFE index page.
Output is pushed to gh-pages alongside the main report.
"""

import argparse
import os
import re
import sys
from datetime import date
from pathlib import Path

import anthropic
import requests
from bs4 import BeautifulSoup

OUTPUT_DIR = Path(__file__).parent / "output"
REPORT_HTML = OUTPUT_DIR / "report_latest.html"
GAP_REPORT = OUTPUT_DIR / "gap_report.md"

ECB_INDEX = "https://www.ecb.europa.eu/stats/ecb_surveys/safe/html/index.en.html"
ECB_BASE  = "https://www.ecb.europa.eu"

MAX_CHARS = 24_000  # ~6k tokens each; stay well within Sonnet context


def discover_ecb_url() -> str:
    """Scrape the ECB SAFE index page and return the URL of the latest report."""
    resp = requests.get(ECB_INDEX, timeout=30, headers={"User-Agent": "Mozilla/5.0"})
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "lxml")
    seen = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        # Match clean report URLs: ecb.safeYYYYMM.en.html with no fragment or cache-buster
        if re.search(r"/ecb\.safe\d{6}\.en\.html$", href):
            if href not in seen:
                seen.add(href)
                url = href if href.startswith("http") else ECB_BASE + href
                return url
    raise RuntimeError(f"Could not find latest SAFE report URL on {ECB_INDEX}")


def fetch_ecb_text(url: str) -> str:
    """Fetch ECB report page and return clean plain text."""
    resp = requests.get(url, timeout=30, headers={"User-Agent": "Mozilla/5.0"})
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "lxml")
    # Remove nav, footer, scripts, styles
    for tag in soup(["nav", "footer", "script", "style", "aside"]):
        tag.decompose()
    text = soup.get_text(separator="\n")
    # Collapse whitespace
    text = re.sub(r"\n{3,}", "\n\n", text).strip()
    return text[:MAX_CHARS]


def extract_our_report_text(html_path: Path) -> str:
    """Extract section headings and bullet text from our generated report."""
    soup = BeautifulSoup(html_path.read_text(encoding="utf-8"), "lxml")
    parts = []
    for section in soup.find_all("section"):
        heading = section.find(["h2", "h3"])
        if heading:
            parts.append(f"\n## {heading.get_text(strip=True)}")
        for li in section.find_all("li"):
            parts.append(f"- {li.get_text(strip=True)}")
    return "\n".join(parts)[:MAX_CHARS]


GAP_SYSTEM = """
You are an analyst comparing two reports based on the same underlying survey:
the ECB's own published SAFE report and an automated report generated from the
same microdata.

Your task: identify topics, questions, or angles that the ECB report covers which
are absent, shallow, or missing from the automated report. Be specific about which
survey questions (Q0B, Q2, Q5, Q6A, Q7A/B, Q9, Q10, Q11, Q23, Q26, Q31, Q33, Q34, etc.)
are unrepresented, and briefly explain what insight each gap would add.

Output structured markdown with these sections:

## Summary
One paragraph: overall coverage assessment.

## Gaps Found
For each gap: the ECB section title, the question(s) involved, and what analytical
value is missing from our report. Be concrete — e.g. "Q23 (expected availability of
financing) is absent; this would show whether firms anticipate conditions to ease or
tighten over the next quarter."

## Questions Not Reflected
A simple bullet list of question IDs that appear in the ECB report but not in ours.

## Suggested Additions
Prioritised list of additions (highest analytical value first), with one sentence
explaining the rationale for each.

Be direct and analytical. Do not compliment either report.
""".strip()


def run_gap_analysis(ecb_text: str, our_text: str) -> str:
    client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
    user_msg = (
        f"## ECB SAFE PUBLICATION\n\n{ecb_text}\n\n"
        f"---\n\n## OUR AUTOMATED REPORT\n\n{our_text}"
    )
    msg = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=1500,
        system=GAP_SYSTEM,
        messages=[{"role": "user", "content": user_msg}],
    )
    return msg.content[0].text.strip()


def main() -> None:
    parser = argparse.ArgumentParser(description="ECB SAFE gap analysis agent")
    parser.add_argument("--ecb-url", help="ECB SAFE report URL (skips auto-discovery)")
    args = parser.parse_args()

    if args.ecb_url:
        ecb_url = args.ecb_url
    else:
        print(f"Discovering latest ECB SAFE report URL from {ECB_INDEX} ...")
        ecb_url = discover_ecb_url()
        print(f"  Found: {ecb_url}")

    if not REPORT_HTML.exists():
        print(f"ERROR: {REPORT_HTML} not found — run run_report.py first")
        sys.exit(1)

    print(f"Fetching ECB report from {ecb_url} ...")
    ecb_text = fetch_ecb_text(ecb_url)
    print(f"  {len(ecb_text):,} chars extracted")

    print("Extracting our report text...")
    our_text = extract_our_report_text(REPORT_HTML)
    print(f"  {len(our_text):,} chars extracted")

    print("Running gap analysis (Sonnet)...")
    analysis = run_gap_analysis(ecb_text, our_text)

    output = (
        f"# SAFE Report Gap Analysis\n\n"
        f"_Generated {date.today().strftime('%d %b %Y')} — "
        f"ECB source: [{ecb_url}]({ecb_url})_\n\n"
        f"{analysis}\n"
    )

    GAP_REPORT.write_text(output, encoding="utf-8")
    print(f"Saved → {GAP_REPORT}")


if __name__ == "__main__":
    main()
