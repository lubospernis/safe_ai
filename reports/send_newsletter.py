"""
SAFE Survey Newsletter — standard report digest email via Gmail SMTP.

Parses the standard report HTML (report_latest.html / report_latest_sk.html),
extracts executive summary bullets and section findings, and sends a digest
email to each subscriber of the "safe-regular" newsletter in their preferred
language (looked up from Supabase allowed_emails.lang, defaults to "sk" if
absent — see subscriptions_db.py).

Adhoc special-focus emails are sent separately by send_adhoc_newsletter.py,
to subscribers of the separate "safe-adhoc" newsletter.

Usage:
  GMAIL_ADDRESS=you@gmail.com GMAIL_16CHAR=xxxxxxxxxxxxxxxx python reports/send_newsletter.py

Required environment variables:
  GMAIL_ADDRESS         — Gmail address to send from
  GMAIL_16CHAR          — Gmail App Password (16 chars, see email_smtp.py docstring)
  SUPABASE_URL          — Supabase project URL
  SUPABASE_SECRET_KEY   — Supabase secret key (see subscriptions_db.py)

Optional environment variables:
  PAGES_URL        — URL of the published EN report (default: lubospernis.github.io/safe_ai)
"""

import json
import os
import sys
from pathlib import Path

from bs4 import BeautifulSoup

from email_smtp import send_email
from subscriptions_db import NEWSLETTER_REGULAR, get_subscribers

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

REPORT_HTML = Path(__file__).parent / "output" / "report_latest.html"
REPORT_HTML_SK = Path(__file__).parent / "output" / "report_latest_sk.html"

_PAGES_BASE = "https://lubospernis.github.io/safe_ai"
_links_path = Path(__file__).parent / "output" / "latest_links.json"
if _links_path.exists():
    _links = json.loads(_links_path.read_text())
else:
    _links = {}
PAGES_URL = os.environ.get("PAGES_URL", _links.get("en", f"{_PAGES_BASE}/"))
PAGES_URL_SK = _links.get("sk", f"{_PAGES_BASE}/sk.html")

_LABELS = {
    "en": {"exec_summary": "Executive Summary", "findings": "Key Findings",
           "cta": "View full report with charts →",
           "footer": "You're receiving this because you're subscribed to the ECB SAFE survey digest. "
                     "Source: ECB SAFE microdata. Net balance = % reporting increase minus % reporting decrease."},
    "sk": {"exec_summary": "Zhrnutie", "findings": "Kľúčové zistenia",
           "cta": "Zobraziť celú správu s grafmi →",
           "footer": "Tento e-mail dostávate, pretože ste odberateľom prehľadu z prieskumu ECB SAFE. "
                     "Zdroj: mikroúdaje ECB SAFE. Čisté saldo = % nárastu mínus % poklesu."},
}


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
    import re as _re
    exec_bullets = (
        [li.get_text(strip=True) for li in exec_section.select("li")]
        if exec_section else []
    )
    exec_bullets = [_re.sub(r":(\S)", r": \1", b) for b in exec_bullets]

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


def build_email_html(data: dict, pages_url: str, lang: str = "en") -> str:
    labels = _LABELS.get(lang, _LABELS["en"])
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
<html lang="{lang}">
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head>
<body style="{_BODY}">
  <div style="{_WRAP}">

    <div style="{_HEADER}">{data["h1"]}</div>
    <div style="{_SUBHEADER}">{data['meta']}</div>

    <div style="{_SECTION}">
      <h2 style="{_H2}">{labels['exec_summary']}</h2>
      <ul style="padding-left:18px;margin:0 0 8px;">
{bullets_html}
      </ul>
    </div>

    <div style="{_CTA_WRAP}">
      <a href="{pages_url}" style="{_CTA}">{labels['cta']}</a>
    </div>

    <div style="{_SECTION}">
      <h2 style="{_H2}">{labels['findings']}</h2>
{findings_html}
    </div>

    <div style="{_CTA_WRAP}">
      <a href="{pages_url}" style="{_CTA}">{labels['cta']}</a>
    </div>

    <div style="{_FOOTER}">
      {labels['footer']}
    </div>

  </div>
</body>
</html>"""


# ---------------------------------------------------------------------------
# Send
# ---------------------------------------------------------------------------

def send_newsletter() -> None:
    if not os.environ.get("GMAIL_ADDRESS") or not os.environ.get("GMAIL_16CHAR"):
        print("GMAIL_ADDRESS/GMAIL_16CHAR not set — skipping newsletter send.")
        sys.exit(0)

    if not REPORT_HTML.exists():
        print(f"Report not found at {REPORT_HTML} — skipping.")
        sys.exit(1)

    subscribers = get_subscribers(NEWSLETTER_REGULAR)
    if not subscribers:
        print("No subscribers — nothing to send.")
        sys.exit(0)

    have_sk = REPORT_HTML_SK.exists()
    if not have_sk and any(sub.get("lang") == "sk" for sub in subscribers):
        print(f"WARNING: SK report not found at {REPORT_HTML_SK} — SK subscribers will get the EN version.")

    # Build per-language email content once, reused across all matching subscribers.
    email_html_by_lang: dict[str, tuple[str, str]] = {}  # lang -> (subject, html)
    for lang, report_path, pages_url in (
        ("en", REPORT_HTML, PAGES_URL),
        ("sk", REPORT_HTML_SK if have_sk else REPORT_HTML, PAGES_URL_SK if have_sk else PAGES_URL),
    ):
        html = report_path.read_text(encoding="utf-8")
        data = parse_report(html)
        if not data["exec_bullets"]:
            print(f"No executive summary bullets found in {lang.upper()} report — skipping {lang}.")
            continue
        meta = data["meta"]
        subject = f"{data['h1']} — Slovakia | {meta.split('|')[-1].strip()}"
        email_html_by_lang[lang] = (subject, build_email_html(data, pages_url, lang))

    if "en" not in email_html_by_lang:
        print("No usable EN report content — aborting send.")
        sys.exit(1)

    sent, failed = 0, 0
    for sub in subscribers:
        lang = sub.get("lang", "sk")
        if lang not in email_html_by_lang:
            lang = "en"
        subject, email_html = email_html_by_lang[lang]
        try:
            send_email(sub["email"], subject, email_html)
            print(f"  ✓ Sent to {sub['email']} ({lang})")
            sent += 1
        except Exception as e:
            print(f"  ✗ Failed for {sub['email']}: {e}")
            failed += 1

    print(f"Newsletter done — {sent} sent, {failed} failed.")
    if failed:
        sys.exit(1)


if __name__ == "__main__":
    send_newsletter()
