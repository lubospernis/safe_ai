"""
SAFE Survey Newsletter — digest email via Resend.

Parses the generated report HTML, extracts the executive summary bullets and
section findings, and sends a lightweight digest email to all subscribers in
newsletter/subscribers.json.

Usage:
  RESEND_API_KEY=re_... python reports/send_newsletter.py

Required environment variables:
  RESEND_API_KEY   — Resend API key (get from resend.com dashboard)

Optional environment variables:
  NEWSLETTER_FROM  — sender address (default: placeholder, must be a verified
                     Resend domain before sending to non-test addresses)
  PAGES_URL        — URL of the published report (default: lubospernis.github.io/safe_ai)
"""

import json
import os
import sys
from pathlib import Path

import resend
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

ROOT = Path(__file__).parent.parent
REPORT_HTML = Path(__file__).parent / "output" / "report_latest.html"
SUBSCRIBERS_JSON = ROOT / "newsletter" / "subscribers.json"

PAGES_URL = os.environ.get("PAGES_URL", "https://lubospernis.github.io/safe_ai/")
FROM_ADDRESS = os.environ.get("NEWSLETTER_FROM", "onboarding@resend.dev")


# ---------------------------------------------------------------------------
# Parse report
# ---------------------------------------------------------------------------

def parse_report(html: str) -> dict:
    soup = BeautifulSoup(html, "lxml")

    # Wave / date line from <p class="meta"> and report title from <h1>
    meta_el = soup.select_one("p.meta")
    meta_text = meta_el.get_text(" | ", strip=True) if meta_el else ""
    h1_el = soup.select_one("h1")
    h1_text = h1_el.get_text(strip=True) if h1_el else "ECB SAFE Survey"

    # Executive summary bullets
    exec_section = soup.select_one("#exec-summary")
    exec_bullets = (
        [li.get_text(strip=True) for li in exec_section.select("li")]
        if exec_section else []
    )

    # Section findings: h3 (finding headline) + p.section-subtitle (question context)
    findings = []
    for sec in soup.select("section[id]"):
        if sec.get("id") == "exec-summary":
            continue
        h3 = sec.select_one("h3")
        subtitle = sec.select_one("p.section-subtitle")
        if h3:
            findings.append({
                "finding": h3.get_text(strip=True),
                "subtitle": subtitle.get_text(strip=True) if subtitle else "",
            })

    return {
        "meta": meta_text,
        "h1": h1_text,
        "exec_bullets": exec_bullets,
        "findings": findings,
    }


# ---------------------------------------------------------------------------
# Build email HTML
# ---------------------------------------------------------------------------

# Inline CSS constants — email-safe, no <style> block
_BODY = "margin:0;padding:0;background:#f4f4f4;font-family:Arial,sans-serif;"
_WRAP = "max-width:600px;margin:40px auto;background:#ffffff;border-radius:6px;overflow:hidden;"
_HEADER = (
    "background:#0777b3;color:#ffffff;padding:28px 32px;"
    "font-size:20px;font-weight:bold;letter-spacing:0.3px;"
)
_SUBHEADER = "padding:10px 32px 0;color:#6a6a6a;font-size:12px;"
_SECTION = "padding:24px 32px 0;"
_H2 = "margin:0 0 12px;font-size:15px;font-weight:bold;color:#0777b3;border-bottom:2px solid #0777b3;padding-bottom:6px;"
_LI = "margin-bottom:8px;font-size:13.5px;line-height:1.6;color:#231f20;"
_FINDING = "margin:0 0 4px;font-size:13px;font-weight:bold;color:#231f20;"
_SUBTITLE = "margin:0 0 12px;font-size:11px;color:#888;"
_CTA_WRAP = "padding:28px 32px;"
_CTA = (
    "display:inline-block;background:#0777b3;color:#ffffff;padding:12px 24px;"
    "border-radius:4px;text-decoration:none;font-size:14px;font-weight:bold;"
)
_FOOTER = "padding:16px 32px 28px;font-size:11px;color:#adadad;border-top:1px solid #f0f0f0;"


def build_email_html(data: dict, pages_url: str) -> str:
    bullets_html = "\n".join(
        f'<li style="{_LI}">{b}</li>' for b in data["exec_bullets"]
    )

    findings_html = ""
    for f in data["findings"]:
        findings_html += (
            f'<p style="{_FINDING}">{f["finding"]}</p>'
            f'<p style="{_SUBTITLE}">{f["subtitle"]}</p>'
        )

    return f"""<!DOCTYPE html>
<html lang="en">
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head>
<body style="{_BODY}">
  <div style="{_WRAP}">

    <div style="{_HEADER}">ECB SAFE Survey — Automatic Report</div>
    <div style="{_SUBHEADER}">{data['meta']}</div>

    <div style="{_SECTION}">
      <h2 style="{_H2}">Executive Summary</h2>
      <ul style="padding-left:18px;margin:0 0 8px;">
{bullets_html}
      </ul>
    </div>

    <div style="{_SECTION}">
      <h2 style="{_H2}">Key Findings</h2>
{findings_html}
    </div>

    <div style="{_CTA_WRAP}">
      <a href="{pages_url}" style="{_CTA}">View full report with charts →</a>
    </div>

    <div style="{_FOOTER}">
      You're receiving this because you're subscribed to the ECB SAFE survey digest.
      Source: ECB SAFE microdata. Net balance = % reporting increase minus % reporting decrease.
    </div>

  </div>
</body>
</html>"""


# ---------------------------------------------------------------------------
# Send
# ---------------------------------------------------------------------------

def send_newsletter() -> None:
    api_key = os.environ.get("RESEND_API_KEY", "")
    if not api_key:
        print("RESEND_API_KEY not set — skipping newsletter send.")
        sys.exit(0)

    resend.api_key = api_key

    if not REPORT_HTML.exists():
        print(f"Report not found at {REPORT_HTML} — skipping.")
        sys.exit(1)

    html = REPORT_HTML.read_text(encoding="utf-8")
    data = parse_report(html)

    if not data["exec_bullets"]:
        print("No executive summary bullets found in report — skipping.")
        sys.exit(1)

    subscribers = json.loads(SUBSCRIBERS_JSON.read_text())["subscribers"]
    if not subscribers:
        print("No subscribers — nothing to send.")
        sys.exit(0)

    # Build subject from meta line, e.g. "Slovakia · Euro Area · Germany  |  Generated 30 Jun 2026"
    meta = data["meta"]
    subject = f"{data['h1']} — Slovakia | {meta.split('|')[-1].strip()}"

    email_html = build_email_html(data, PAGES_URL)

    sent, failed = 0, 0
    for sub in subscribers:
        try:
            resend.Emails.send({
                "from": FROM_ADDRESS,
                "to": [sub["email"]],
                "subject": subject,
                "html": email_html,
            })
            print(f"  ✓ Sent to {sub['email']}")
            sent += 1
        except Exception as e:
            print(f"  ✗ Failed for {sub['email']}: {e}")
            failed += 1

    print(f"Newsletter done — {sent} sent, {failed} failed.")
    if failed:
        sys.exit(1)


if __name__ == "__main__":
    send_newsletter()
