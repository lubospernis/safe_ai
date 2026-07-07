"""Deterministic style checks for generated report bullets.

Code-enforced (not LLM-only) checks, run against every bullet before publish —
see quality_check.py, which calls these against report HTML.
"""

import re

_RECOVERY_RE = re.compile(
    r"\b(recover(?:ed|y|ing)|rebound(?:ed)?|turned\s+around)\b", re.IGNORECASE
)
# Matches negative net balance context: "net -22", "−37pp", "-22pp", "net 22%" (signed context)
_NEGATIVE_VALUE_RE = re.compile(
    r"(?:net\s+)?[−\-]\s*\d+(?:\.\d+)?(?:\s*pp|%)|net\s+\d+\s*%\s*(?:of\s+firms)?\s*reported\s+fall",
    re.IGNORECASE,
)

_MAG_WORDS: dict[str, tuple[str, float]] = {
    # word: (direction, threshold_pp)
    # direction "max" = word implies small change, flag if pp > threshold
    # direction "min" = word implies large change, flag if pp < threshold
    "marginally": ("max", 2.0),
    "slightly": ("max", 2.0),
    "mildly": ("max", 3.0),
    "moderately": ("min", 5.0),
    "notably": ("min", 10.0),
    "significantly": ("min", 10.0),
    "sharply": ("min", 15.0),
    "substantially": ("min", 15.0),
    "dramatically": ("min", 20.0),
}
_PP_RE = re.compile(r"(\d+(?:\.\d+)?)\s*pp", re.IGNORECASE)

# Matches "code 2", "code 3", "on code 3", "response code 4" — a raw survey response
# code number cited with no label, meaningless to a reader who hasn't seen the annex.
_BARE_CODE_RE = re.compile(r"\b(?:response\s+)?code\s+\d+\b", re.IGNORECASE)


def check_sign_language(bullet: str) -> list[str]:
    """Flag recovery/rebound language when paired with a negative value in the same bullet."""
    errors = []
    if _RECOVERY_RE.search(bullet) and _NEGATIVE_VALUE_RE.search(bullet):
        errors.append(f"Recovery language with negative value: {bullet[:120]}")
    return errors


def check_magnitude_calibration(bullet: str) -> list[str]:
    """Flag mismatch between intensity adverb and pp change magnitude."""
    errors = []
    bullet_lower = bullet.lower()
    pp_matches = [float(m) for m in _PP_RE.findall(bullet)]
    if not pp_matches:
        return errors
    max_pp = max(pp_matches)
    for word, (direction, threshold) in _MAG_WORDS.items():
        if word not in bullet_lower:
            continue
        if direction == "max" and max_pp > threshold:
            errors.append(
                f"'{word}' used for {max_pp}pp change (implies ≤{threshold}pp): {bullet[:80]}"
            )
        elif direction == "min" and max_pp < threshold:
            errors.append(
                f"'{word}' used for {max_pp}pp change (implies ≥{threshold}pp): {bullet[:80]}"
            )
    return errors


def check_bare_response_codes(bullet: str) -> list[str]:
    """Flag a raw survey response code number (e.g. "code 2") cited with no label —
    meaningless to a reader who hasn't seen the ECB SAFE annex."""
    errors = []
    m = _BARE_CODE_RE.search(bullet)
    if m:
        errors.append(f"Bare response code cited without a label ('{m.group(0)}'): {bullet[:120]}")
    return errors
