"""
SAFE Survey — Adhoc Special Focus Newsletter via Gmail SMTP.

Parses report_adhoc_latest.html, extracts the adhoc spotlight section, and sends
a focused "Special Focus" email to subscribers of the separate "safe-adhoc"
newsletter (distinct from the regular quarterly digest — see
reports/send_newsletter.py and subscriptions_db.py). Runs only on adhoc waves
(the generate_adhoc_report workflow produces the HTML only when adhoc data exists).

Usage:
  GMAIL_ADDRESS=you@gmail.com GMAIL_16CHAR=xxxxxxxxxxxxxxxx python reports/send_adhoc_newsletter.py

Required environment variables:
  GMAIL_ADDRESS         — Gmail address to send from
  GMAIL_16CHAR          — Gmail App Password (16 chars, see email_smtp.py docstring)
  SUPABASE_URL          — Supabase project URL
  SUPABASE_SECRET_KEY   — Supabase secret key (see subscriptions_db.py)

Optional environment variables:
  PAGES_URL        — URL of the published adhoc report
                     (default: lubospernis.github.io/safe_ai/adhoc.html)
"""

import json
import os
import sys
from pathlib import Path

from bs4 import BeautifulSoup

from email_smtp import send_email
from subscriptions_db import NEWSLETTER_ADHOC, get_subscribers

REPORT_HTML = Path(__file__).parent / "output" / "report_adhoc_latest.html"

_PAGES_BASE = "https://lubospernis.github.io/safe_ai"
_links_path = Path(__file__).parent / "output" / "latest_adhoc_links.json"
if _links_path.exists():
    _links = json.loads(_links_path.read_text())
    PAGES_URL = os.environ.get("PAGES_URL", _links.get("en", f"{_PAGES_BASE}/adhoc.html"))
else:
    PAGES_URL = os.environ.get("PAGES_URL", f"{_PAGES_BASE}/adhoc.html")

# Inline CSS — email-safe
_BODY = "margin:0;padding:0;background:#f4f4f4;font-family:Arial,sans-serif;"
_WRAP = "max-width:600px;margin:40px auto;background:#ffffff;border-radius:6px;overflow:hidden;"
_HEADER_ACCENT = "#bd4e35"
_HEADER = (
    f"background:{_HEADER_ACCENT};color:#ffffff;padding:28px 32px;"
    "font-size:20px;font-weight:bold;letter-spacing:0.3px;"
)
_SUBHEADER = "padding:10px 32px 0;color:#6a6a6a;font-size:12px;"
_EYEBROW = "padding:20px 32px 0;font-size:11px;font-weight:bold;color:#bd4e35;letter-spacing:0.8px;text-transform:uppercase;"
_SECTION = "padding:16px 32px 0;"
_H2 = f"margin:0 0 12px;font-size:15px;font-weight:bold;color:{_HEADER_ACCENT};border-bottom:2px solid {_HEADER_ACCENT};padding-bottom:6px;"
_FINDING = "margin:0 0 8px;font-size:14px;font-weight:bold;color:#231f20;line-height:1.5;"
_LI = "margin-bottom:8px;font-size:13.5px;line-height:1.6;color:#231f20;"
_CTA_WRAP = "padding:28px 32px;"
_CTA = (
    f"display:inline-block;background:{_HEADER_ACCENT};color:#ffffff;padding:12px 24px;"
    "border-radius:4px;text-decoration:none;font-size:14px;font-weight:bold;"
)
_ECB_LINK = f"display:inline-block;margin-top:12px;font-size:12px;color:{_HEADER_ACCENT};"
_FOOTER = "padding:16px 32px 28px;font-size:11px;color:#adadad;border-top:1px solid #f0f0f0;"


def parse_adhoc_report(html: str) -> dict | None:
    """Extract adhoc spotlight content from report HTML.

    Returns None if no adhoc spotlight block is found (non-adhoc report).
    """
    soup = BeautifulSoup(html, "lxml")

    meta_el = soup.select_one("p.meta")
    meta_text = meta_el.get_text(" | ", strip=True) if meta_el else ""

    details_el = soup.select_one("details#adhoc_spotlight")
    if not details_el:
        return None

    theme_label = details_el.get("data-theme", "Special Focus")

    # Finding headline — first h3 inside the spotlight
    h3 = details_el.select_one("h3")
    finding = h3.get_text(strip=True) if h3 else ""

    # Bullets — all li inside the spotlight
    import re as _re
    bullets = [
        _re.sub(r":(\S)", r": \1", li.get_text(strip=True))
        for li in details_el.select("li")
    ]

    # ECB article link (if present)
    ecb_a = details_el.select_one("a[href]")
    ecb_article_url = ecb_a["href"] if ecb_a else None

    return {
        "meta": meta_text,
        "theme_label": theme_label,
        "finding": finding,
        "bullets": bullets,
        "ecb_article_url": ecb_article_url,
    }


def build_adhoc_email_html(data: dict, pages_url: str) -> str:
    bullets_html = "\n".join(
        f'<li style="{_LI}">{b}</li>' for b in data["bullets"]
    )

    ecb_link_html = ""
    if data.get("ecb_article_url"):
        ecb_link_html = (
            f'<a href="{data["ecb_article_url"]}" style="{_ECB_LINK}" '
            f'target="_blank" rel="noopener">Read ECB analysis →</a>'
        )

    return f"""<!DOCTYPE html>
<html lang="en">
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head>
<body style="{_BODY}">
  <div style="{_WRAP}">

    <div style="{_HEADER}">Special Focus: {data["theme_label"]}</div>
    <div style="{_SUBHEADER}">{data["meta"]}</div>

    <div style="{_EYEBROW}">ECB SAFE Survey — Slovakia</div>

    <div style="{_SECTION}">
      <h2 style="{_H2}">Key Finding</h2>
      <p style="{_FINDING}">{data["finding"]}</p>
    </div>

    <div style="{_SECTION}">
      <h2 style="{_H2}">Details</h2>
      <ul style="padding-left:18px;margin:0 0 8px;">
{bullets_html}
      </ul>
      {ecb_link_html}
    </div>

    <div style="{_CTA_WRAP}">
      <a href="{pages_url}" style="{_CTA}">View full special focus report →</a>
    </div>

    <div style="{_FOOTER}">
      You're receiving this because you're subscribed to the ECB SAFE survey digest.
      Source: ECB SAFE microdata. Net balance = % reporting increase minus % reporting decrease.
    </div>

  </div>
</body>
</html>"""


def send_adhoc_newsletter() -> None:
    if not os.environ.get("GMAIL_ADDRESS") or not os.environ.get("GMAIL_16CHAR"):
        print("GMAIL_ADDRESS/GMAIL_16CHAR not set — skipping newsletter send.")
        sys.exit(0)

    if not REPORT_HTML.exists():
        print(f"Adhoc report not found at {REPORT_HTML} — skipping.")
        sys.exit(0)

    html = REPORT_HTML.read_text(encoding="utf-8")
    data = parse_adhoc_report(html)

    if not data:
        print("No adhoc spotlight section found in report — skipping.")
        sys.exit(0)

    if not data.get("finding"):
        print("Adhoc spotlight has no finding — skipping.")
        sys.exit(0)

    subscribers = get_subscribers(NEWSLETTER_ADHOC)
    if not subscribers:
        print("No subscribers — nothing to send.")
        sys.exit(0)

    meta = data["meta"]
    subject = f"Special Focus: {data['theme_label']} — Slovakia | {meta.split('|')[-1].strip()}"

    email_html = build_adhoc_email_html(data, PAGES_URL)

    sent, failed = 0, 0
    for sub in subscribers:
        try:
            send_email(sub["email"], subject, email_html)
            print(f"  ✓ Sent to {sub['email']}")
            sent += 1
        except Exception as e:
            print(f"  ✗ Failed for {sub['email']}: {e}")
            failed += 1

    print(f"Adhoc newsletter done — {sent} sent, {failed} failed.")
    if failed:
        sys.exit(1)


if __name__ == "__main__":
    send_adhoc_newsletter()
