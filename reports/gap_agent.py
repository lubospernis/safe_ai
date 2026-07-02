"""
ECB SAFE Gap Agent — compares the latest ECB SAFE publication against our automated
report and writes a structured gap report to reports/output/gap_report.md.

Outputs:
  gap_report.md         — full gap analysis (overwritten each run)
  interpretation_context.md — interpretation notes for the next report run
  MotherDuck main_safe.ref_safe__gap_log — structural gaps accumulate across waves

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

import requests
from mistralai import Mistral
from bs4 import BeautifulSoup

OUTPUT_DIR = Path(__file__).parent / "output"
REPORT_HTML = OUTPUT_DIR / "report_latest.html"
GAP_REPORT = OUTPUT_DIR / "gap_report.md"
INTERP_CONTEXT = OUTPUT_DIR / "interpretation_context.md"

ECB_INDEX = "https://www.ecb.europa.eu/stats/ecb_surveys/safe/html/index.en.html"
ECB_BASE  = "https://www.ecb.europa.eu"

MAX_CHARS = 24_000  # ~6k tokens each; stay well within context


def discover_ecb_url() -> str:
    """Scrape the ECB SAFE index page and return the URL of the latest report."""
    resp = requests.get(ECB_INDEX, timeout=30, headers={"User-Agent": "Mozilla/5.0"})
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "lxml")
    seen = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
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
    for tag in soup(["nav", "footer", "script", "style", "aside"]):
        tag.decompose()
    text = soup.get_text(separator="\n")
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
You are a gap analyst reviewing an automated Slovakia SAFE survey report against the
latest ECB SAFE publication for the same wave.

Your output must be valid Markdown with EXACTLY these two sections and no others:

## Structural Gaps
Gaps that would require adding a new mart table, new SQL query, or ECB data not yet
ingested in our pipeline. These cannot be fixed without a code change.
List each gap as: **Short title (max 6 words):** One sentence — what is missing,
which ECB question/section it relates to, and why it matters specifically for Slovakia.
Maximum 8 gaps. Focus on the highest analytical value.

## Interpretation Notes
Framing, comparison angles, or emphasis shifts that the NEXT REPORT RUN could incorporate
WITHOUT any structural data changes — e.g. "compare SK labour cost trend against EA",
"note ECB contextualises bank willingness against the bank lending survey".
Maximum 5 bullet points, each ≤ 25 words.

Do NOT include a summary, introduction, or any other sections.
Be direct and analytical. Do not compliment either report.
""".strip()


def run_gap_analysis(ecb_text: str, our_text: str) -> str:
    client = Mistral(api_key=os.environ["MISTRAL_API_KEY"])
    user_msg = (
        f"## ECB SAFE PUBLICATION\n\n{ecb_text}\n\n"
        f"---\n\n## OUR AUTOMATED REPORT\n\n{our_text}"
    )
    resp = client.chat.complete(
        model="mistral-medium-latest",
        max_tokens=1500,
        messages=[
            {"role": "system", "content": GAP_SYSTEM},
            {"role": "user", "content": user_msg},
        ],
    )
    return resp.choices[0].message.content.strip()


def _persist_structural_gaps(md_text: str, wave_number: int) -> None:
    """Parse ## Structural Gaps section and upsert items to ref_safe__gap_log."""
    token = os.environ.get("MOTHERDUCK_TOKEN")
    if not token:
        print("  No MOTHERDUCK_TOKEN — skipping structural gap persistence")
        return
    match = re.search(r"## Structural Gaps\n(.*?)(?=\n## |\Z)", md_text, re.DOTALL)
    if not match:
        print("  Warning: ## Structural Gaps section not found in gap analysis output")
        return
    section = match.group(1).strip()
    items = re.findall(r"\*\*(.+?)\*\*:?\s*(.+)", section)
    if not items:
        print("  Warning: no **Title:** items found in Structural Gaps section")
        return
    try:
        import duckdb
        con = duckdb.connect(f"md:my_db?motherduck_token={token}")
        con.execute("""
            CREATE TABLE IF NOT EXISTS main_safe.ref_safe__gap_log (
                gap_title       TEXT,
                wave_number     INTEGER,
                logged_date     DATE,
                gap_description TEXT,
                status          TEXT DEFAULT 'open',
                PRIMARY KEY (gap_title, wave_number)
            )
        """)
        inserted = 0
        for title, desc in items:
            con.execute("""
                INSERT OR IGNORE INTO main_safe.ref_safe__gap_log
                    (gap_title, wave_number, logged_date, gap_description, status)
                VALUES (?, ?, ?, ?, 'open')
            """, [title.strip(), wave_number, date.today(), desc.strip()])
            inserted += 1
        con.close()
        print(f"  Persisted {inserted} structural gaps for wave {wave_number}")
    except Exception as e:
        print(f"  Structural gap persistence failed: {e}")


def _write_interpretation_context(md_text: str) -> None:
    """Extract ## Interpretation Notes and write to interpretation_context.md."""
    match = re.search(r"## Interpretation Notes\n(.*?)(?=\n## |\Z)", md_text, re.DOTALL)
    if not match:
        print("  Warning: ## Interpretation Notes section not found")
        return
    content = match.group(1).strip()
    INTERP_CONTEXT.write_text(
        "# Interpretation context from gap analysis\n\n" + content,
        encoding="utf-8",
    )
    print(f"  Saved interpretation context → {INTERP_CONTEXT}")


def _extract_wave_number(our_report_html: Path) -> int | None:
    """Try to extract the latest wave number from the generated report."""
    try:
        soup = BeautifulSoup(our_report_html.read_text(encoding="utf-8"), "lxml")
        # Look for wave reference in meta or data attributes
        for el in soup.find_all(attrs={"data-wave": True}):
            return int(el["data-wave"])
        # Fallback: scan text for "wave N" patterns
        text = soup.get_text()
        m = re.search(r"wave\s+(\d+)", text, re.IGNORECASE)
        if m:
            return int(m.group(1))
    except Exception:
        pass
    return None


def main() -> None:
    parser = argparse.ArgumentParser(description="ECB SAFE gap analysis agent")
    parser.add_argument("--ecb-url", help="ECB SAFE report URL (skips auto-discovery)")
    parser.add_argument("--wave", type=int, help="Wave number (for MotherDuck logging)")
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

    print("Running gap analysis (Mistral Medium)...")
    analysis = run_gap_analysis(ecb_text, our_text)

    output = (
        f"# SAFE Report Gap Analysis\n\n"
        f"_Generated {date.today().strftime('%d %b %Y')} — "
        f"ECB source: [{ecb_url}]({ecb_url})_\n\n"
        f"{analysis}\n"
    )

    GAP_REPORT.write_text(output, encoding="utf-8")
    print(f"Saved → {GAP_REPORT}")

    # Extract wave number for MotherDuck logging — read last entry from run_log.json
    wave = args.wave
    if not wave:
        run_log_path = OUTPUT_DIR / "run_log.json"
        if run_log_path.exists():
            try:
                import json
                entries = json.loads(run_log_path.read_text())
                if entries:
                    wave = entries[-1].get("wave_number")
            except Exception:
                pass
    if not wave:
        wave = _extract_wave_number(REPORT_HTML)

    print("Persisting structural gaps to MotherDuck...")
    if wave:
        _persist_structural_gaps(analysis, wave)
    else:
        print("  Could not determine wave number — skipping MotherDuck persistence")

    print("Writing interpretation context for next run...")
    _write_interpretation_context(analysis)


if __name__ == "__main__":
    main()
