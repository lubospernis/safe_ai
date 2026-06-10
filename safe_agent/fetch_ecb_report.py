"""Download and extract text from the latest ECB SAFE report PDF."""

from __future__ import annotations

import hashlib
import os
import re
import tempfile
from pathlib import Path

import requests

# ECB publishes the main SAFE results report at a stable URL pattern.
# The URL uses the current year+half e.g. ecb.safesme202501 for 2025H1.
_ECB_REPORT_BASE = (
    "https://www.ecb.europa.eu/stats/accesstofinancesofenterprises/pdf/"
)
# Fallback: the survey results overview (always updated to latest wave)
_ECB_FALLBACK_URL = (
    "https://www.ecb.europa.eu/stats/accesstofinancesofenterprises/pdf/"
    "ecb.safesme.en.pdf"
)

_CACHE_DIR = Path(tempfile.gettempdir()) / "safe_agent_ecb_cache"
_CACHE_DIR.mkdir(exist_ok=True)


def _cache_path(url: str) -> Path:
    key = hashlib.md5(url.encode()).hexdigest()
    return _CACHE_DIR / f"{key}.pdf"


def _download_pdf(url: str) -> bytes:
    cached = _cache_path(url)
    if cached.exists():
        return cached.read_bytes()
    resp = requests.get(url, timeout=60, headers={"User-Agent": "safe-agent/1.0"})
    resp.raise_for_status()
    cached.write_bytes(resp.content)
    return resp.content


def _extract_text(pdf_bytes: bytes) -> str:
    try:
        import fitz  # PyMuPDF

        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        pages = [page.get_text() for page in doc]
        doc.close()
        return "\n".join(pages)
    except ImportError:
        # Fallback to pdfminer if PyMuPDF not installed
        from io import BytesIO

        from pdfminer.high_level import extract_text as pm_extract

        return pm_extract(BytesIO(pdf_bytes))


def _filter_relevant_pages(text: str, country_keyword: str = "Slovakia") -> tuple[str, str]:
    """Return (country_section, euroarea_section) extracted from full PDF text."""
    lines = text.split("\n")

    sk_lines: list[str] = []
    ea_lines: list[str] = []
    in_sk = False
    in_ea = False

    for line in lines:
        low = line.lower()
        if country_keyword.lower() in low:
            in_sk = True
            in_ea = False
        elif "euro area" in low and not in_sk:
            in_ea = True
            in_sk = False
        elif re.match(r"^[A-Z][a-z]+$", line.strip()) and len(line.strip()) > 4:
            # New country heading — reset
            if in_sk or in_ea:
                in_sk = False
                in_ea = False

        if in_sk:
            sk_lines.append(line)
        if in_ea:
            ea_lines.append(line)

    # If no Slovakia section found, return chunks around the keyword
    if not sk_lines:
        idx = [i for i, l in enumerate(lines) if country_keyword.lower() in l.lower()]
        for i in idx:
            sk_lines.extend(lines[max(0, i - 2) : i + 60])

    return "\n".join(sk_lines[:500]), "\n".join(ea_lines[:500])


def fetch_ecb_report(wave_year: int | None = None, wave_half: str | None = None) -> dict:
    """
    Returns:
        {
            "full_text": str,
            "slovakia_text": str,
            "euroarea_text": str,
            "source_url": str,
        }
    """
    url = _ECB_FALLBACK_URL
    if wave_year and wave_half:
        half_num = "1" if wave_half.upper() in ("H1", "Q1", "Q2") else "2"
        candidate = (
            f"{_ECB_REPORT_BASE}"
            f"ecb.safesme{wave_year}{half_num:0>2}.en.pdf"
        )
        try:
            pdf_bytes = _download_pdf(candidate)
            url = candidate
        except requests.HTTPError:
            pdf_bytes = _download_pdf(_ECB_FALLBACK_URL)
    else:
        pdf_bytes = _download_pdf(url)

    full_text = _extract_text(pdf_bytes)
    slovakia_text, euroarea_text = _filter_relevant_pages(full_text)

    return {
        "full_text": full_text[:20000],  # cap to avoid context overflow
        "slovakia_text": slovakia_text,
        "euroarea_text": euroarea_text,
        "source_url": url,
    }
