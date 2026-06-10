"""Download and extract text from the latest ECB SAFE report HTML page."""

from __future__ import annotations

import hashlib
import os
import re
import tempfile
from pathlib import Path

import requests
from bs4 import BeautifulSoup

_CACHE_DIR = Path(tempfile.gettempdir()) / "safe_agent_ecb_cache"
_CACHE_DIR.mkdir(exist_ok=True)

_HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; safe-agent/1.0)"}


def _cache_path(url: str) -> Path:
    key = hashlib.md5(url.encode()).hexdigest()
    return _CACHE_DIR / f"{key}.html"


def _download_page(url: str) -> str:
    cached = _cache_path(url)
    if cached.exists():
        print(f"[fetch_ecb] using cached page: {cached}")
        return cached.read_text(encoding="utf-8")
    print(f"[fetch_ecb] downloading {url}")
    resp = requests.get(url, timeout=60, headers=_HEADERS)
    resp.raise_for_status()
    cached.write_text(resp.text, encoding="utf-8")
    return resp.text


def _extract_text_from_html(html: str) -> str:
    """Extract readable text from ECB SAFE HTML report."""
    soup = BeautifulSoup(html, "lxml")

    # Remove navigation, scripts, styles
    for tag in soup(["script", "style", "nav", "header", "footer"]):
        tag.decompose()

    # ECB report content is typically in <main> or <article> or <div class="content">
    main = soup.find("main") or soup.find("article") or soup.find("div", class_=re.compile("content|report|publication", re.I))
    target = main if main else soup.body

    return target.get_text(separator="\n", strip=True) if target else soup.get_text(separator="\n", strip=True)


def _filter_relevant_sections(text: str, country_keyword: str = "Slovakia") -> tuple[str, str]:
    """Return (country_section, euroarea_section) from full report text."""
    lines = text.split("\n")

    # Grab lines around every occurrence of the keyword (±50 lines)
    sk_lines: list[str] = []
    sk_indices = [i for i, l in enumerate(lines) if country_keyword.lower() in l.lower()]
    for idx in sk_indices:
        sk_lines.extend(lines[max(0, idx - 3): idx + 50])

    # Euro area: first few occurrences
    ea_lines: list[str] = []
    ea_indices = [i for i, l in enumerate(lines) if "euro area" in l.lower()]
    for idx in ea_indices[:4]:
        ea_lines.extend(lines[max(0, idx - 3): idx + 50])

    return "\n".join(sk_lines[:600]), "\n".join(ea_lines[:400])


def fetch_ecb_report() -> dict:
    """
    Fetch the ECB SAFE report from ECB_REPORT_URL env var.

    Set ECB_REPORT_URL to the latest ECB SAFE HTML report, e.g.:
      https://www.ecb.europa.eu/stats/ecb_surveys/safe/html/ecb.safe202604.en.html

    The URL changes each wave — check https://www.ecb.europa.eu/stats/ecb_surveys/safe/
    Returns:
        {
            "full_text": str,
            "slovakia_text": str,
            "euroarea_text": str,
            "source_url": str,
        }
    """
    url = os.environ.get("ECB_REPORT_URL", "").strip()
    if not url:
        raise RuntimeError(
            "ECB_REPORT_URL is not set. "
            "Find the latest report at https://www.ecb.europa.eu/stats/ecb_surveys/safe/ "
            "and set: export ECB_REPORT_URL=https://www.ecb.europa.eu/stats/ecb_surveys/safe/html/ecb.safe202604.en.html"
        )

    html = _download_page(url)
    full_text = _extract_text_from_html(html)
    slovakia_text, euroarea_text = _filter_relevant_sections(full_text)

    return {
        "full_text": full_text[:15000],
        "slovakia_text": slovakia_text,
        "euroarea_text": euroarea_text,
        "source_url": url,
    }
