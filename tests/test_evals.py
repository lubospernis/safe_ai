"""Eval harness for SAFE report quality.

Layer 1 — deterministic style checks (no golden set, run in CI on every commit).
Layer 2 — golden assertion checks (wave-specific YAML, run against actual run output).

Layer 3 (LLM-as-judge) lives in reports/eval_judge.py and is run manually.
"""

from pathlib import Path

import yaml

from evals import (
    check_bare_response_codes, check_bullet_length, check_magnitude_calibration,
    check_sign_language,
)

GOLDEN_DIR = Path(__file__).parent / "golden"

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


def test_magnitude_uses_local_not_global_max_pp():
    # A dense bullet reports several unrelated pp figures for different
    # sub-claims (real style found in production, wave 38 financing_gap
    # bullets). "notably" correctly describes the nearby 12.3pp figure, but
    # the original implementation compared EVERY word in the bullet against
    # the bullet's single global max (58.2pp) instead of the figure it's
    # actually next to — wrongly flagging a correct usage.
    good = (
        "The credit line gap widened to +58.2pp in wave 38, driven by need rising "
        "notably to a net +12.3pp while availability held broadly flat."
    )
    assert not check_magnitude_calibration(good)


def test_magnitude_still_flags_mismatch_in_multi_clause_bullet():
    # A dense bullet with multiple pp figures must still catch a genuine
    # mismatch on ONE of them — the local-proximity fix must not blanket-
    # suppress real errors elsewhere in the same bullet.
    bad = (
        "The credit line gap widened to +14.8pp in wave 38 from +4.8pp in wave 37, "
        "while availability marginally deteriorated to a striking -19.5pp shift."
    )
    errors = check_magnitude_calibration(bad)
    assert errors
    assert any("19.5" in e for e in errors)


def test_bare_response_code_flags_code_number():
    bad = (
        "Slovakia has a higher share of firms using AI very infrequently or experimentally "
        "(SK 46.5% vs EA 32.7% on code 2) and a lower share with moderate use "
        "(SK 17.1% vs EA 31.2% on code 3)."
    )
    errors = check_bare_response_codes(bad)
    assert errors, "Should flag bare 'code 2'/'code 3' citations"


def test_bare_response_code_passes_labelled_bullet():
    good = (
        "Slovakia has a higher share of firms using AI very infrequently or experimentally "
        "(SK 46.5% vs EA 32.7%) and a lower share with moderate use (SK 17.1% vs EA 31.2%)."
    )
    assert not check_bare_response_codes(good)


def test_bullet_length_flags_real_production_runaway_bullet():
    # Real bullet from a live wave-38 report (financing_gap section) — 51 words,
    # a "while X, Y also happened, and Z" compound-clause pattern the ~25-word
    # prompt guidance was never code-enforced against.
    bad = (
        "The credit-line financing gap widened by 10.0 pp to +14.8 pp — driven almost "
        "entirely by a 9.2 pp rise in liquidity need — moving Slovak firms from less "
        "stressed than the EA to more stressed, while fixed-investment financing "
        "purposes fell 6.1 pp to 34.2% and hiring financing rose to 31.1%."
    )
    errors = check_bullet_length(bad)
    assert errors
    assert "51" in errors[0]


def test_bullet_length_passes_bullet_within_target():
    good = "Bank loan interest rates tightened for a net 12% of Slovak firms (n=80), easing 1.8pp from wave 37."
    assert not check_bullet_length(good)


def test_bullet_length_ceiling_is_looser_than_prompt_target():
    # The 35-word hard ceiling is deliberately looser than the prompt's own
    # ~25-word target (catches genuine runaways, not a restatement of the
    # style goal) — a 30-word bullet should NOT be flagged.
    thirty_words = " ".join(["word"] * 30)
    assert not check_bullet_length(thirty_words)


def test_bare_response_code_passes_unrelated_bullet():
    good = "Loan application rates held steady at 32% across the euro area."
    assert not check_bare_response_codes(good)


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
