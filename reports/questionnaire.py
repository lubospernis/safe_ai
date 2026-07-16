"""Parse ECB SAFE questionnaire PDFs to extract adhoc question answer codes.

The questionnaire URL uses a publication-counter suffix, not the survey wave period.
The caller passes the URL directly (stored in the wave's theme dict); this module
handles download, parse, and extraction.

Usage:
    labels = fetch_adhoc_response_labels(questionnaire_url, module_ids=["qa1", "qa2", "qa3"])
    # Returns: {"qa1": {1: "Not currently in use", 2: "Very infrequent use ...", ...}, ...}

    context = build_response_label_context(labels, module_ids=["qa1"])
    # Returns a formatted string ready for injection into LLM prompts.
"""

import re
import urllib.request
from pathlib import Path
from tempfile import NamedTemporaryFile

import pymupdf

# Base URL template — suffix is YYYYMM matching the ECB release month
_QUESTIONNAIRE_BASE = (
    "https://www.ecb.europa.eu/stats/accesstofinancesofenterprises"
    "/pdf/questionnaire/ecb.safeq{suffix}.en.pdf"
)

# Map survey_period_label → questionnaire URL suffix (YYYYMM = release month).
# The ECB releases results ~2 months after the reference quarter ends:
#   Q4 → Feb next year, Q1 → Apr, Q2 → Jul, Q3 → Oct (approximate).
# Confirmed entries take precedence; unconfirmed use the formula below.
_PERIOD_TO_QUESTIONNAIRE_SUFFIX: dict[str, str] = {
    "2025Q4": "202602",  # Wave 37 — confirmed
    "2026Q1": "202604",  # Wave 38 — confirmed (user-provided)
    "2026Q2": "202607",  # Wave 39 — confirmed (user-provided, releasing Jul 2026)
}

# Approximate release month by survey quarter (used as fallback formula).
# These are typical ECB release months; add confirmed entries above when known.
_QUARTER_TO_RELEASE_MONTH: dict[str, int] = {
    "Q1": 4,   # Q1 results → April release
    "Q2": 7,   # Q2 results → July release
    "Q3": 10,  # Q3 results → October release
    "Q4": 2,   # Q4 results → February next year
}


def _suffix_from_period(period_label: str) -> str | None:
    """Derive questionnaire URL suffix from survey_period_label (e.g. '2026Q2' → '202607').

    Uses the confirmed lookup first; falls back to the release-month formula.
    """
    if period_label in _PERIOD_TO_QUESTIONNAIRE_SUFFIX:
        return _PERIOD_TO_QUESTIONNAIRE_SUFFIX[period_label]
    # Parse e.g. "2026Q2"
    import re
    m = re.match(r"^(\d{4})(Q[1-4])$", period_label)
    if not m:
        return None
    year = int(m.group(1))
    quarter = m.group(2)
    month = _QUARTER_TO_RELEASE_MONTH.get(quarter)
    if month is None:
        return None
    # Q4 releases in February of the NEXT year
    release_year = year + 1 if quarter == "Q4" else year
    return f"{release_year}{month:02d}"


def _wave_period_label(wave_number: int, con, schema: str) -> str | None:
    """Look up survey_period_label for a given wave_number from the mart."""
    from db import wave_period_label
    val = wave_period_label(wave_number, con, schema)
    return val if val and not hasattr(val, "_mock_name") else None


def _dlt_load_month(con) -> str | None:
    """Return the YYYYMM string of the most recent raw microdata dlt load.

    The ECB questionnaire PDF URL suffix equals the publication month, which
    matches the month the raw microdata was loaded via dlt.
    """
    try:
        row = con.execute(
            "SELECT strftime(MAX(l.inserted_at), '%Y%m') "
            "FROM raw.safe_microdata m "
            "JOIN raw._dlt_loads l ON l.load_id = m._dlt_load_id "
            "LIMIT 1"
        ).fetchone()
        val = row[0] if row else None
        if val and isinstance(val, str) and re.match(r"^\d{6}$", val):
            return val
        return None
    except Exception:
        return None


def questionnaire_url_for_wave(wave_number: int, con, schema: str) -> str | None:
    """Return the questionnaire PDF URL for a given wave, or None on failure.

    Priority:
    1. Confirmed suffix from _PERIOD_TO_QUESTIONNAIRE_SUFFIX table.
    2. dlt load month from raw.safe_microdata (YYYYMM of the latest load).
    3. Release-month formula derived from survey_period_label.
    """
    period = _wave_period_label(wave_number, con, schema)

    # Try confirmed table first
    if period and period in _PERIOD_TO_QUESTIONNAIRE_SUFFIX:
        suffix = _PERIOD_TO_QUESTIONNAIRE_SUFFIX[period]
        return _QUESTIONNAIRE_BASE.format(suffix=suffix)

    # Derive from dlt load timestamp (most reliable automatic source)
    suffix = _dlt_load_month(con)
    if suffix:
        return _QUESTIONNAIRE_BASE.format(suffix=suffix)

    # Fallback: formula from survey period label
    if period:
        suffix = _suffix_from_period(period)
        if suffix:
            return _QUESTIONNAIRE_BASE.format(suffix=suffix)

    return None


def _download_pdf(url: str) -> Path | None:
    """Download PDF to a temp file and return the path. Returns None on failure."""
    try:
        with NamedTemporaryFile(suffix=".pdf", delete=False) as f:
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=15) as resp:
                f.write(resp.read())
            return Path(f.name)
    except Exception:
        return None


def _extract_text(pdf_path: Path) -> str:
    """Extract all text from a PDF via pymupdf."""
    try:
        doc = pymupdf.open(str(pdf_path))
        parts = []
        for page in doc:
            text = page.get_text()
            if text.strip():
                parts.append(text)
        return "\n".join(parts)
    except Exception:
        return ""


def _parse_adhoc_blocks(
    text: str, module_ids: list[str]
) -> tuple[dict[str, dict[int, str]], dict[str, dict[str, str]], str | None]:
    """Extract answer code → label mappings, sub-item labels, and the adhoc section title.

    The ECB questionnaire PDF renders each element on its own line:
        [AD HOC QUESTIONS]
        Artificial intelligence technologies
        QB1_2025Q4. Question text here...
        a) In your country
        b) In Germany, France and Italy
        •
        Not currently in use
        1
        ...

    Returns:
        response_labels: {module_id_lower: {code_int: label_str}}
        sub_item_labels: {module_id_lower: {"a": "In your country", "b": "In Germany..."}}
        section_title: The human-readable title of the adhoc block (e.g. "Artificial intelligence
                       technologies"), or None if not found.
    """
    response_labels: dict[str, dict[int, str]] = {}
    sub_item_labels: dict[str, dict[str, str]] = {}
    section_title: str | None = None
    module_ids_upper = {m.upper() for m in module_ids}

    lines = text.split("\n")

    # Question header: QA1_2025Q4. or QB1_2025Q4.
    question_re = re.compile(r"^(Q[AB]\d*[A-Z]?\d*)_\d{4}[A-Z0-9]*\.", re.IGNORECASE)
    # Sub-item label: "a) Some text" or "b) Some text"
    sub_item_re = re.compile(r"^([a-z])\)\s*(.+)", re.IGNORECASE)
    # A line that is just an integer
    code_only_re = re.compile(r"^\s*(-?\d+)\s*$")
    # Bullet character
    bullet_re = re.compile(r"^[•·]\s*$")

    current_module: str | None = None
    state = "seek"          # seek | label | code
    pending_label_parts: list[str] = []
    # Section-title detection state: after "[AD HOC QUESTIONS]" the next non-empty
    # non-question line is the human-readable section title.
    _saw_adhoc_header = False
    _adhoc_header_re = re.compile(r"\[AD\s+HOC\s+QUESTIONS?\]", re.IGNORECASE)

    def _flush_label_pending() -> None:
        nonlocal state, pending_label_parts
        pending_label_parts = []
        state = "seek"

    for line in lines:
        stripped = line.strip()
        if not stripped:
            continue

        # ── Detect [AD HOC QUESTIONS] header ─────────────────────────────────
        if _adhoc_header_re.search(stripped):
            _saw_adhoc_header = True
            continue

        # Line immediately after [AD HOC QUESTIONS] is the section title
        if _saw_adhoc_header and section_title is None:
            _saw_adhoc_header = False
            # Only capture if it looks like a title (not a question code, not a bracket line)
            if not question_re.match(stripped) and not stripped.startswith("["):
                section_title = stripped
            continue

        # ── Check for a question header ──────────────────────────────────────
        qm = question_re.match(stripped)
        if qm:
            qid = qm.group(1).upper()
            matched_module = None
            for mid in module_ids_upper:
                if qid.upper().startswith(mid) or mid.startswith(qid.upper()):
                    matched_module = mid.lower()
                    break
            if matched_module:
                current_module = matched_module
                if current_module not in response_labels:
                    response_labels[current_module] = {}
                if current_module not in sub_item_labels:
                    sub_item_labels[current_module] = {}
            else:
                current_module = None
            _flush_label_pending()
            state = "seek"
            continue

        if current_module is None:
            continue

        # ── Capture sub-item labels (a) ... b) ...) before any bullets appear ──
        if state == "seek":
            sim = sub_item_re.match(stripped)
            if sim:
                letter = sim.group(1).lower()
                label_text = sim.group(2).strip()
                # Strip [READ IF NECESSARY ...] and anything in brackets
                label_text = re.sub(r"\s*\[.*", "", label_text).strip()
                label_text = label_text.rstrip(".,;").strip()
                # Fix PDF extraction artifacts like "I n Germany" → "In Germany"
                label_text = re.sub(r"\bI n\b", "In", label_text)
                if label_text:
                    sub_item_labels[current_module][letter] = label_text
                continue
            if bullet_re.match(stripped):
                state = "label"
                pending_label_parts = []
            continue

        # ── State: label — accumulating label text ───────────────────────────
        if state == "label":
            cm = code_only_re.match(stripped)
            if cm:
                code = int(cm.group(1))
                label = " ".join(pending_label_parts).strip()
                label = re.sub(r"\[READ IF NECESSARY[^\]]*\]", "", label).strip()
                label = label.rstrip(".,;").strip()
                if label and 1 <= code <= 20 and code not in (7, 9):
                    response_labels[current_module][code] = label
                state = "seek"
                pending_label_parts = []
            elif bullet_re.match(stripped):
                pending_label_parts = []
            elif stripped.startswith("[") or stripped.startswith("<"):
                pass
            else:
                pending_label_parts.append(stripped)
            continue

    return response_labels, sub_item_labels, section_title


def fetch_adhoc_response_labels(
    url: str, module_ids: list[str]
) -> tuple[dict[str, dict[int, str]], dict[str, dict[str, str]], str | None]:
    """Download questionnaire PDF and extract answer labels, sub-item labels, and section title.

    Returns:
        (response_labels, sub_item_labels, section_title)
        response_labels: {module_id_lower: {code_int: label_str}}
        sub_item_labels: {module_id_lower: {"a": "In your country", "b": "In Germany..."}}
        section_title: human-readable adhoc block title (e.g. "Artificial intelligence
                       technologies"), or None if not found in the PDF.
    All dicts/strings are empty/None on any failure.
    """
    pdf_path = _download_pdf(url)
    if not pdf_path:
        return {}, {}, None
    try:
        text = _extract_text(pdf_path)
        if not text:
            return {}, {}, None
        return _parse_adhoc_blocks(text, module_ids)
    finally:
        try:
            pdf_path.unlink()
        except Exception:
            pass


def persist_adhoc_labels(
    wave_number: int,
    period_suffix: str,
    questionnaire_url: str,
    response_labels: dict[str, dict[int, str]],
    sub_item_labels: dict[str, dict[str, str]],
    section_title: str | None,
    con,
    schema: str,
) -> int:
    """Persist parsed questionnaire labels to ref_safe__adhoc_labels. Returns rows upserted."""
    from datetime import datetime
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {schema}.ref_safe__adhoc_labels (
            wave_number    INTEGER   NOT NULL,
            period_suffix  VARCHAR   NOT NULL,
            question_id    VARCHAR   NOT NULL,
            label_type     VARCHAR   NOT NULL,
            code_key       VARCHAR   NOT NULL,
            label_text     VARCHAR   NOT NULL,
            questionnaire_url VARCHAR,
            inserted_at    TIMESTAMP NOT NULL,
            PRIMARY KEY (wave_number, question_id, label_type, code_key)
        )
    """)
    rows = []
    now = datetime.utcnow()
    if section_title:
        rows.append((wave_number, period_suffix, "*", "section_title", "", section_title, questionnaire_url, now))
    for qid, code_map in response_labels.items():
        for code_int, label in code_map.items():
            rows.append((wave_number, period_suffix, qid, "response_code", str(code_int), label, questionnaire_url, now))
    for qid, sub_map in sub_item_labels.items():
        for letter, label in sub_map.items():
            rows.append((wave_number, period_suffix, qid, "sub_item", letter, label, questionnaire_url, now))
    if rows:
        con.executemany(
            f"INSERT OR REPLACE INTO {schema}.ref_safe__adhoc_labels VALUES (?,?,?,?,?,?,?,?)",
            rows,
        )
    return len(rows)


def build_response_label_context(
    labels: dict[str, dict[int, str]],
    module_ids: list[str],
    sub_item_labels: dict[str, dict[str, str]] | None = None,
) -> str:
    """Format response labels and sub-item labels into a prompt-ready context block."""
    if not labels and not sub_item_labels:
        return ""

    lines = ["Response code meanings (from ECB questionnaire):"]
    for mid in module_ids:
        code_map = labels.get(mid.lower(), {})
        sil = (sub_item_labels or {}).get(mid.lower(), {})
        if not code_map and not sil:
            continue
        lines.append(f"  {mid.upper()}:")
        if sil:
            for letter, label in sorted(sil.items()):
                lines.append(f"    sub-item {letter} = {label}")
        for code, label in sorted(code_map.items()):
            lines.append(f"    code {code} = {label}")

    if len(lines) == 1:
        return ""
    return "\n".join(lines)
