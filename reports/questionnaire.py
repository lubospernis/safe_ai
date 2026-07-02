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
    try:
        row = con.execute(
            f"SELECT survey_period_label FROM {schema}.mart_safe__slovakia_kpis "
            f"WHERE wave_number = {wave_number} LIMIT 1"
        ).fetchone()
        val = row[0] if row else None
        return str(val) if val and not hasattr(val, "_mock_name") else None
    except Exception:
        return None


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


def _parse_adhoc_blocks(text: str, module_ids: list[str]) -> dict[str, dict[int, str]]:
    """Extract answer code → label mappings for the requested adhoc module IDs.

    The ECB questionnaire PDF renders each element on its own line:
        QA1_2025Q4. Question text here...
        •
        Not currently in use
        1
        •
        Very infrequent use or use in pilot projects/experimentally
        2
        ...

    We use a 3-state machine: SEEK_BULLET → COLLECT_LABEL → EXPECT_CODE.
    Returns: {module_id_lower: {code_int: label_str, ...}}
    For numeric open questions (% estimates), no discrete codes → returns {}.
    """
    result: dict[str, dict[int, str]] = {}
    module_ids_upper = {m.upper() for m in module_ids}

    lines = text.split("\n")

    # Question header: QA1_2025Q4. or QB1_2025Q4. (may have trailing text on same line)
    question_re = re.compile(r"^(Q[AB]\d*[A-Z]?\d*)_\d{4}[A-Z0-9]*\.", re.IGNORECASE)
    # A line that is just an integer (the answer code)
    code_only_re = re.compile(r"^\s*(-?\d+)\s*$")
    # A line that IS the bullet character
    bullet_re = re.compile(r"^[•·]\s*$")

    current_module: str | None = None
    # 3-state machine per bullet entry
    state = "seek"          # seek | label | code
    pending_label_parts: list[str] = []

    def _flush_label_pending() -> None:
        """If we were collecting a label but hit a non-code line, discard it."""
        nonlocal state, pending_label_parts
        pending_label_parts = []
        state = "seek"

    for line in lines:
        stripped = line.strip()
        if not stripped:
            continue

        # ── Check for a question header ──────────────────────────────────────
        qm = question_re.match(stripped)
        if qm:
            qid = qm.group(1).upper()
            matched_module = None
            for mid in module_ids_upper:
                # Match QA1 against "QA1", "QA" etc. — use prefix matching
                if qid.upper().startswith(mid) or mid.startswith(qid.upper()):
                    matched_module = mid.lower()
                    break
            if matched_module:
                current_module = matched_module
                if current_module not in result:
                    result[current_module] = {}
            else:
                current_module = None
            _flush_label_pending()
            state = "seek"
            continue

        # Only process answer content when inside a relevant module block
        if current_module is None:
            continue

        # ── State: seek — waiting for a bullet ──────────────────────────────
        if state == "seek":
            if bullet_re.match(stripped):
                state = "label"
                pending_label_parts = []
            continue

        # ── State: label — accumulating label text ───────────────────────────
        if state == "label":
            cm = code_only_re.match(stripped)
            if cm:
                # This line IS the code — finalise
                code = int(cm.group(1))
                label = " ".join(pending_label_parts).strip()
                # Strip [READ IF NECESSARY: ...] clauses and trailing noise
                label = re.sub(r"\[READ IF NECESSARY[^\]]*\]", "", label).strip()
                label = label.rstrip(".,;").strip()
                # Valid response codes are 1–20; skip DK/NA and out-of-range
                if label and 1 <= code <= 20 and code not in (7, 9):
                    result[current_module][code] = label
                state = "seek"
                pending_label_parts = []
            elif bullet_re.match(stripped):
                # New bullet before code — previous one had no code (multiline label ran into next)
                pending_label_parts = []
            elif stripped.startswith("[") or stripped.startswith("<"):
                # Filter/scripting instruction — skip, don't append to label
                pass
            else:
                pending_label_parts.append(stripped)
            continue

    return result


def fetch_adhoc_response_labels(
    url: str, module_ids: list[str]
) -> dict[str, dict[int, str]]:
    """Download questionnaire PDF and extract answer labels for the given module IDs.

    Returns a dict mapping lowercased module_id → {response_code: label}.
    Returns empty dict on any failure (network, parse, no adhoc section).
    """
    pdf_path = _download_pdf(url)
    if not pdf_path:
        return {}
    try:
        text = _extract_text(pdf_path)
        if not text:
            return {}
        return _parse_adhoc_blocks(text, module_ids)
    finally:
        try:
            pdf_path.unlink()
        except Exception:
            pass


def build_response_label_context(
    labels: dict[str, dict[int, str]], module_ids: list[str]
) -> str:
    """Format response labels into a prompt-ready context block.

    Returns empty string if labels dict is empty.
    """
    if not labels:
        return ""

    lines = ["Response code meanings (from ECB questionnaire):"]
    for mid in module_ids:
        code_map = labels.get(mid.lower(), {})
        if code_map:
            lines.append(f"  {mid.upper()}:")
            for code, label in sorted(code_map.items()):
                lines.append(f"    {code} = {label}")

    if len(lines) == 1:
        return ""  # nothing was added
    return "\n".join(lines)
