"""Deterministic style checks for generated report bullets, plus the golden-set
assertion checker (Layer 2 of the eval harness — see reports/eval_harness.py and
reports/eval_judge.py for Layers that compose these against real pipeline output).

Code-enforced (not LLM-only) checks, run against every bullet before publish —
see quality_check.py, which calls these against report HTML.
"""

import re
from pathlib import Path

import yaml

GOLDEN_DIR = Path(__file__).parent.parent / "tests" / "golden"

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


# How far (chars) from a magnitude word to look for "its" pp figure. A dense
# bullet often reports several unrelated pp changes for different sub-claims
# ("X rose 9.2pp ... Y deteriorated marginally to -2.5pp ... Z gap of
# +12.6pp") — comparing every word against the bullet's single largest pp
# figure (the original implementation) produces both false positives (a word
# validly describing a small local figure gets flagged against an unrelated
# larger figure elsewhere) and false negatives (a real mismatch gets masked
# by an even-larger unrelated figure). Found via a real wave-38 run.
#
# Known remaining limitation: proximity alone can't distinguish a LEVEL
# ("deteriorated to a net -2.5pp") from a DELTA ("deteriorated by 2.5pp") —
# a word can validly describe either quantity, and only the delta is a "change
# magnitude" in the sense these thresholds are calibrated for. A bullet citing
# both a level and a small true delta near the same word may still occasionally
# mis-flag; this proximity fix only closes the "wrong number entirely" class of
# false positive, not this narrower level-vs-delta ambiguity.
_PROXIMITY_WINDOW = 60


def check_magnitude_calibration(bullet: str) -> list[str]:
    """Flag mismatch between an intensity adverb and the pp change it actually
    describes (the nearest pp figure within _PROXIMITY_WINDOW chars), not the
    bullet's global max pp figure."""
    errors = []
    bullet_lower = bullet.lower()
    pp_spans = [(float(m.group(1)), m.start(), m.end()) for m in _PP_RE.finditer(bullet)]
    if not pp_spans:
        return errors
    for word, (direction, threshold) in _MAG_WORDS.items():
        for wm in re.finditer(re.escape(word), bullet_lower):
            w_start, w_end = wm.span()
            # pp figures within the proximity window of this occurrence of the word,
            # each with its distance so we can pick the truly nearest one.
            nearby = [
                (pp, min(abs(p_start - w_end), abs(w_start - p_end)))
                for pp, p_start, p_end in pp_spans
                if p_start - _PROXIMITY_WINDOW <= w_end and w_start <= p_end + _PROXIMITY_WINDOW
            ]
            if not nearby:
                continue
            local_pp = min(nearby, key=lambda t: t[1])[0]
            if direction == "max" and local_pp > threshold:
                errors.append(
                    f"'{word}' used for {local_pp}pp change (implies ≤{threshold}pp): {bullet[:80]}"
                )
            elif direction == "min" and local_pp < threshold:
                errors.append(
                    f"'{word}' used for {local_pp}pp change (implies ≥{threshold}pp): {bullet[:80]}"
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


# Prompts already say "one sentence per bullet, max ~25 words" (SECTION_CONTENT_SYSTEM)
# but that guidance was never code-enforced — real bullets were found running 50+ words
# (double the target) in a live wave-38 report. 35 is deliberately looser than the
# prompt's own 25-word target: this is a hard ceiling catching genuine runaway
# compound-clause bullets, not a re-statement of the style goal, and needs slack for
# Slovak (often needs more words than English for the same content) since this check
# runs against both EN and SK reports via the same quality_check.py pass.
_MAX_BULLET_WORDS = 35


def check_bullet_length(bullet: str) -> list[str]:
    """Flag a bullet that blows well past the "~25 words, one sentence" style guidance —
    a strong signal of the "while X, Y also happened, and Z" compound-clause pattern
    that makes bullets hard to skim."""
    word_count = len(bullet.split())
    if word_count > _MAX_BULLET_WORDS:
        return [f"Bullet is {word_count} words (target ~25, hard ceiling {_MAX_BULLET_WORDS}): {bullet[:100]}"]
    return []


def check_all_style(bullet: str) -> list[str]:
    """Run every deterministic style check against one bullet. Single call site for
    both the generation-time retry loop (llm.py) and the post-hoc CI gate
    (quality_check.py) so they can never drift out of sync."""
    errors = []
    errors.extend(check_sign_language(bullet))
    errors.extend(check_magnitude_calibration(bullet))
    errors.extend(check_bare_response_codes(bullet))
    errors.extend(check_bullet_length(bullet))
    return errors


# ── Layer 2: golden assertion checks ─────────────────────────────────────────
#
# Golden files (tests/golden/wave_N.yaml) encode confirmed production failure
# modes for a specific historical wave — see wave_37.yaml for the four bugs
# this currently catches. reports/eval_harness.py runs these against real
# pipeline output (not just hand-typed test-file examples) as part of the
# scheduled regression harness.


def _load_golden(wave: int) -> dict:
    path = GOLDEN_DIR / f"wave_{wave}.yaml"
    if not path.exists():
        return {}
    return yaml.safe_load(path.read_text())


def run_golden_assertions(section_id: str, bullets: list[str], wave: int) -> list[str]:
    """Check bullets against golden YAML assertions for a given section and wave.

    Returns a list of error strings (empty = pass).
    """
    golden = _load_golden(wave)
    if not golden:
        return []
    errors = []
    for sec in golden.get("sections", []):
        if sec["section_id"] != section_id:
            continue
        for assertion in sec.get("assertions", []):
            atype = assertion["type"]

            if atype == "contains_number":
                val = str(assertion["value"])
                if not any(val in b for b in bullets):
                    errors.append(
                        f"[{section_id}] Expected number {val} not found in any bullet"
                    )

            elif atype == "not_contains_phrase":
                phrase = assertion["phrase"].lower()
                for b in bullets:
                    if phrase in b.lower():
                        errors.append(
                            f"[{section_id}] Forbidden phrase '{phrase}' in: {b[:100]}"
                        )

            elif atype == "direction_word":
                expected = assertion["expected"].lower()
                if not any(expected in b.lower() for b in bullets):
                    errors.append(
                        f"[{section_id}] Expected direction word '{expected}' not found in any bullet"
                    )

    return errors
