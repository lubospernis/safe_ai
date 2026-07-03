"""Eval harness for SAFE report quality.

Layer 1 — deterministic style checks (no golden set, run in CI on every commit).
Layer 2 — golden assertion checks (wave-specific YAML, run against actual run output).

Layer 3 (LLM-as-judge) lives in reports/eval_judge.py and is run manually.
"""

import re
from pathlib import Path

import yaml

GOLDEN_DIR = Path(__file__).parent / "golden"

# ── Layer 1: deterministic style checks ──────────────────────────────────────

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


# ── Layer 2: golden assertion checks ─────────────────────────────────────────


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


# ── pytest tests — Layer 1 (no data needed, run in CI) ───────────────────────


def test_sign_language_flags_recovery_with_negative():
    bad = "Slovak firms' profits partly recovered — net -22pp vs prior quarter net -38pp."
    assert check_sign_language(bad), "Should flag 'recovered' with negative context"


def test_sign_language_passes_recovery_with_purely_positive():
    good = "Slovak firm confidence recovered to net +5pp, the first positive reading since wave 33."
    # No negative value in the bullet — recovery is legitimate
    assert not check_sign_language(good)


def test_sign_language_passes_improvement_language():
    good = "Slovak firms' profit pressures eased — net -22pp vs prior quarter net -38pp."
    assert not check_sign_language(good)


def test_sign_language_passes_neutral_bullet():
    good = "Loan application rates held steady at 32% across the euro area."
    assert not check_sign_language(good)


def test_magnitude_flags_marginally_for_large_pp():
    bad = "Slovak firms marginally increased loan applications, rising by 8pp."
    assert check_magnitude_calibration(bad)


def test_magnitude_flags_notably_for_small_pp():
    bad = "Slovak firms notably adjusted their outlook, shifting by 1pp."
    assert check_magnitude_calibration(bad)


def test_magnitude_passes_marginally_for_small_pp():
    good = "Loan rejection rates marginally improved, easing by 1.5pp."
    assert not check_magnitude_calibration(good)


def test_magnitude_passes_notably_for_large_pp():
    good = "Slovak firms notably tightened their financing gap, rising by 12pp."
    assert not check_magnitude_calibration(good)


def test_magnitude_passes_no_pp_in_bullet():
    good = "Access to finance remains the third most pressing concern for Slovak SMEs."
    assert not check_magnitude_calibration(good)


# ── pytest tests — Layer 2 (golden YAML) ────────────────────────────────────


def test_golden_wave37_loads():
    golden = _load_golden(37)
    assert golden, "wave_37.yaml should exist and parse"
    assert "sections" in golden


def test_golden_assertions_catch_recovery_language():
    bullets = [
        "Slovak firms' profits partly recovered — net -22pp vs prior wave net -38pp.",
        "Turnover also remained deeply negative at net -15pp.",
    ]
    errors = run_golden_assertions("business_situation", bullets, wave=37)
    assert any("recovered" in e for e in errors), f"Expected recovery error, got: {errors}"


def test_golden_assertions_catch_broadly_comparable():
    bullets = [
        "AI use in Slovakia is broadly comparable to the euro area average.",
        "Slovak firms show moderate AI adoption at 46.5%.",
    ]
    errors = run_golden_assertions("adhoc_spotlight", bullets, wave=37)
    assert any("broadly comparable" in e for e in errors), f"Expected phrase error, got: {errors}"


def test_golden_assertions_catch_export_framing():
    bullets = [
        "Slovak firms estimate higher export shares to Germany, France and Italy.",
        "QB1 data shows divergence from euro area peers.",
    ]
    errors = run_golden_assertions("adhoc_spotlight", bullets, wave=37)
    assert any("export" in e for e in errors), f"Expected export error, got: {errors}"


def test_golden_assertions_pass_clean_bullets():
    bullets = [
        "Slovak profit pressures eased — net -22pp (n=237), improving by 15pp from wave 36.",
        "Conditions remain adverse: more than one in five Slovak firms reports falling profits.",
    ]
    errors = run_golden_assertions("business_situation", bullets, wave=37)
    assert not errors, f"Expected no errors on clean bullets, got: {errors}"
