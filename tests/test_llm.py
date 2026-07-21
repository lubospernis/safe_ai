import json
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from llm import (
    _check_exec_provenance, _check_numeric_grounding, _clean_num_token,
    _fix_exec_provenance_mislabels,
    _fmt_data_for_prompt, _NUMBER_RE, _parse_section_response,
    _shorten_question_llm, _sme_divergence_note, build_section_signals,
    classify_ecb_emphasis, direction_reversal, get_exec_summary,
    get_shortened_questions, historical_extremity, reliable_n, sk_ea_gap, translate_to_slovak,
)


# ── _parse_section_response ──────────────────────────────────────────────────

def test_parse_valid_json(section_stub):
    raw = '{"finding": "Rates rose", "bullets": ["• A", "• B", "• C"]}'
    result = _parse_section_response(raw, section_stub)
    assert result["finding"] == "Rates rose"
    assert len(result["bullets"]) == 3
    assert result["bullets"][0] == "A"   # bullet prefix stripped


def test_parse_bullets_string_coerced(section_stub):
    raw = '{"finding": "X", "bullets": "single bullet"}'
    result = _parse_section_response(raw, section_stub)
    assert isinstance(result["bullets"], list)


def test_parse_malformed_json_fallback(section_stub):
    raw = "finding: Rates rose\n- bullet A\n- bullet B"
    result = _parse_section_response(raw, section_stub)
    assert isinstance(result["finding"], str)
    assert isinstance(result["bullets"], list)


def test_parse_capped_at_3_bullets(section_stub):
    raw = '{"finding": "X", "bullets": ["a", "b", "c", "d", "e"]}'
    result = _parse_section_response(raw, section_stub)
    assert len(result["bullets"]) <= 3


def test_parse_fenced_json(section_stub):
    raw = '```json\n{"finding": "Y", "bullets": ["b1", "b2", "b3"]}\n```'
    result = _parse_section_response(raw, section_stub)
    assert result["finding"] == "Y"


# ── _sme_divergence_note ─────────────────────────────────────────────────────

def _make_df_with_firm_size(sk_all, sk_sme):
    rows = []
    for firm_size, val in [("all", sk_all), ("sme", sk_sme)]:
        rows.append({
            "wave_number": 38, "country_code": "SK",
            "net_balance_wtd": val, "firm_size": firm_size,
        })
    # Add EA row so df isn't SK-only
    rows.append({"wave_number": 38, "country_code": "EA",
                 "net_balance_wtd": 2.0, "firm_size": "all"})
    return pd.DataFrame(rows)


def test_sme_divergence_triggers_above_threshold():
    df = _make_df_with_firm_size(sk_all=5.0, sk_sme=40.0)
    note = _sme_divergence_note(df, "net_balance_wtd", None, threshold=30.0)
    assert note != ""
    assert "SME" in note


def test_sme_divergence_silent_below_threshold():
    df = _make_df_with_firm_size(sk_all=5.0, sk_sme=10.0)
    note = _sme_divergence_note(df, "net_balance_wtd", None, threshold=30.0)
    assert note == ""


def test_sme_divergence_no_firm_size_column():
    df = pd.DataFrame([{"wave_number": 38, "country_code": "SK", "net_balance_wtd": 5.0}])
    note = _sme_divergence_note(df, "net_balance_wtd", None)
    assert note == ""


# ── _fmt_data_for_prompt ─────────────────────────────────────────────────────

def test_fmt_data_for_prompt_contains_wave(section_stub, net_balance_df):
    text = _fmt_data_for_prompt(section_stub, net_balance_df)
    assert "38" in text   # latest wave
    assert "37" in text   # previous wave


def test_fmt_data_for_prompt_no_nan(section_stub, net_balance_df):
    text = _fmt_data_for_prompt(section_stub, net_balance_df)
    assert "NaN" not in text
    assert "nan" not in text


def test_fmt_data_for_prompt_contains_countries(section_stub, net_balance_df):
    text = _fmt_data_for_prompt(section_stub, net_balance_df)
    assert "SK" in text
    assert "EA" in text


def test_fmt_data_for_prompt_delta_present(section_stub, net_balance_df):
    # Should include Δ= lines showing wave-over-wave change
    text = _fmt_data_for_prompt(section_stub, net_balance_df)
    assert "Δ=" in text


# ── _fmt_data_for_prompt: financing_gap composite ────────────────────────────

def _financing_gap_section_stub():
    return {"id": "financing_gap", "sql_file": "financing_gap.sql"}


def _financing_gap_df(gaps_by_country_instrument: dict[str, dict[str, float]]) -> pd.DataFrame:
    """gaps_by_country_instrument: {country: {sub_item: gap_value}}."""
    labels = {"a": "Bank loans", "b": "Trade credit", "f": "Credit lines"}
    rows = []
    for wave in [37, 38]:
        for country, by_inst in gaps_by_country_instrument.items():
            for sub_item, gap in by_inst.items():
                rows.append({
                    "wave_number": wave, "country_code": country, "chart_type": "main",
                    "sub_item": sub_item, "sub_item_label": labels[sub_item],
                    "need_nb": 5.0, "availability_nb": 2.0,
                    "financing_gap_wtd": gap, "n_respondents_need": 100,
                })
    return pd.DataFrame(rows)


def test_financing_gap_composite_appears_with_three_instruments():
    df = _financing_gap_df({
        "SK": {"a": -4.0, "b": 6.3, "f": 14.8},
        "EA": {"a": 1.7, "b": -2.0, "f": 3.7},
    })
    text = _fmt_data_for_prompt(_financing_gap_section_stub(), df)
    assert "COMPOSITE" in text
    # (-4.0 + 6.3 + 14.8) / 3 = 5.7
    assert "SK=+5.7pp" in text


def test_financing_gap_composite_absent_with_one_instrument():
    # A single instrument isn't a "composite" — must not appear.
    df = _financing_gap_df({"SK": {"a": -4.0}, "EA": {"a": 1.7}})
    text = _fmt_data_for_prompt(_financing_gap_section_stub(), df)
    assert "COMPOSITE" not in text


def test_financing_gap_composite_labelled_as_own_average_not_ecb():
    df = _financing_gap_df({
        "SK": {"a": -4.0, "b": 6.3},
        "EA": {"a": 1.7, "b": -2.0},
    })
    text = _fmt_data_for_prompt(_financing_gap_section_stub(), df)
    assert "average across" in text
    assert "NOT ECB's own published composite" in text


# ── _check_numeric_grounding ─────────────────────────────────────────────────

def test_grounding_check_passes_real_number():
    df = pd.DataFrame([{"net_balance_wtd": 12.3, "wave_number": 38}])
    warnings = _check_numeric_grounding(["Net balance improved to 12.3pp"], df, ["net_balance_wtd"])
    assert warnings == []


def test_grounding_check_flags_invented_number():
    df = pd.DataFrame([{"net_balance_wtd": 12.3, "wave_number": 38}])
    warnings = _check_numeric_grounding(["Net balance improved to 99.7pp"], df, ["net_balance_wtd"])
    assert len(warnings) == 1
    assert "99.7" in warnings[0]


def test_grounding_check_skips_small_numbers():
    df = pd.DataFrame([{"net_balance_wtd": 5.0, "wave_number": 38}])
    # Single-digit numbers (≤9) should be skipped — wave references, counts, etc.
    warnings = _check_numeric_grounding(["In wave 5 of the survey"], df, ["net_balance_wtd"])
    assert warnings == []


def test_grounding_check_skips_large_numbers():
    df = pd.DataFrame([{"net_balance_wtd": 5.0, "wave_number": 38}])
    # Numbers > 200 should be skipped — year references etc.
    warnings = _check_numeric_grounding(["In 2024 the net balance was 5.0pp"], df, ["net_balance_wtd"])
    assert warnings == []


def test_grounding_check_accepts_rounded_integer():
    df = pd.DataFrame([{"net_balance_wtd": 15.0, "wave_number": 38}])
    # "15" should match 15.0 in the data
    warnings = _check_numeric_grounding(["Net balance was 15pp"], df, ["net_balance_wtd"])
    assert warnings == []


def test_grounding_check_skips_sample_size_citation():
    df = pd.DataFrame([{"net_balance_wtd": 5.0, "wave_number": 38}])
    # "(n=62)" is a sample-size citation, not a data value — must not be flagged
    # even though 62 isn't in net_balance_wtd.
    warnings = _check_numeric_grounding(
        ["A net 5% of Slovak firms (n=62) reported tighter conditions"], df, ["net_balance_wtd"]
    )
    assert warnings == []


def test_grounding_check_skips_wave_number_reference():
    df = pd.DataFrame([{"net_balance_wtd": 5.0, "wave_number": 38}])
    # "wave 37" is a wave reference, not a cited data value.
    warnings = _check_numeric_grounding(
        ["This reversed a trend seen since wave 37 of the survey"], df, ["net_balance_wtd"]
    )
    assert warnings == []


def test_grounding_check_skips_slovak_wave_reference_number_first():
    df = pd.DataFrame([{"net_balance_wtd": 12.83, "wave_number": 39}])
    # Real false positive from a production run (2026-07-20, wave 39 SK report):
    # "v 39. vlne" ("in wave 39") puts the number before the Slovak wave word,
    # unlike English's "wave 39" — must not be flagged as an invented number.
    warnings = _check_numeric_grounding(
        ["Čistých 12,83 % slovenských firiem (n=242) hlásilo pokles obratu v 39. vlne"],
        df, ["net_balance_wtd"],
    )
    assert warnings == []


def test_grounding_check_skips_slovak_wave_reference_word_first():
    df = pd.DataFrame([{"net_balance_wtd": 14.67, "wave_number": 39}])
    # "vlny 38" / "vo vlne 37" / "vlna 36" — Slovak declensions of "vlna" (wave)
    # preceding the number, mirroring the existing English "wave 37" exclusion.
    warnings = _check_numeric_grounding(
        ["Čistá investičná dôvera vzrástla o 3,88 pp z vlny 38"],
        df, ["net_balance_wtd"],
    )
    assert warnings == []


def test_grounding_check_skips_slovak_months_reference():
    df = pd.DataFrame([{"pct_wtd": 4.63, "wave_number": 39}])
    # "v prištích 12 mesiacoch" ("in the next 12 months") — Slovak equivalent of
    # the existing English "next 12 months" horizon-label exclusion.
    warnings = _check_numeric_grounding(
        ["Slovenské firmy očakávajú, že priemerné predajné ceny stúpnu o 4,63% v prištích 12 mesiacoch"],
        df, ["pct_wtd"],
    )
    assert warnings == []


def test_grounding_check_skips_pressingness_scale_denominator():
    df = pd.DataFrame([{"avg_pressingness_wtd": 6.19, "wave_number": 38}])
    # "6.19/10" — the "10" is the fixed scale denominator, not a cited data value.
    warnings = _check_numeric_grounding(
        ["Scored 6.19/10, a moderate concern for firms"], df, ["avg_pressingness_wtd"]
    )
    assert warnings == []


def test_grounding_check_uses_n_respondents_column_for_sample_sizes():
    df = pd.DataFrame([{"net_balance_wtd": 5.0, "n_respondents": 62, "wave_number": 38}])
    # Even without the n= prefix pattern, a real n_respondents value should be accepted.
    warnings = _check_numeric_grounding(["Sample size reached 62 firms"], df, ["net_balance_wtd"])
    assert warnings == []


def test_grounding_check_still_flags_invented_number_near_n_equals():
    df = pd.DataFrame([{"net_balance_wtd": 5.0, "n_respondents": 62, "wave_number": 38}])
    # A genuinely invented number elsewhere in the bullet (not right after "n=") must
    # still be flagged — the n= exclusion should not blanket-suppress the whole bullet.
    warnings = _check_numeric_grounding(
        ["A net 5% of firms (n=62) reported a swing of 99.7pp"], df, ["net_balance_wtd"]
    )
    assert len(warnings) == 1
    assert "99.7" in warnings[0]


def test_grounding_check_accepts_verified_pp_delta():
    # Real false positive found in production (wave 37 adhoc spotlight):
    # "46.5% ... 13.8 pp above the EA's 32.7%" — 46.5 - 32.7 = 13.8 exactly,
    # a correctly computed difference, but neither pp_delta_col contains "13.8"
    # directly since it's arithmetic on two already-grounded percentages.
    df = pd.DataFrame([{"pct_wtd": 46.5, "wave_number": 37}, {"pct_wtd": 32.7, "wave_number": 37}])
    warnings = _check_numeric_grounding(
        ["Slovak firms report 46.5% very infrequent AI use, 13.8 pp above the EA's 32.7%"],
        df, ["pct_wtd"],
    )
    assert warnings == []


def test_grounding_check_accepts_pp_below_phrasing():
    df = pd.DataFrame([{"pct_wtd": 14.1, "wave_number": 37}, {"pct_wtd": 15.6, "wave_number": 37}])
    warnings = _check_numeric_grounding(
        ["Slovak firms estimate 14.1% had invested, 1.5 pp below the EA average of 15.6%"],
        df, ["pct_wtd"],
    )
    assert warnings == []


def test_grounding_check_still_flags_incorrect_pp_delta():
    # The pp-delta phrasing is present, but the number does NOT match the actual
    # difference of the two cited percentages (46.5 - 32.7 = 13.8, not 25.0) —
    # this is a genuine fabrication and must still be flagged.
    df = pd.DataFrame([{"pct_wtd": 46.5, "wave_number": 37}, {"pct_wtd": 32.7, "wave_number": 37}])
    warnings = _check_numeric_grounding(
        ["Slovak firms report 46.5% very infrequent AI use, 25.0 pp above the EA's 32.7%"],
        df, ["pct_wtd"],
    )
    assert len(warnings) == 1
    assert "25.0" in warnings[0]


def test_grounding_check_accepts_worsening_by_pp_phrasing():
    # Real false positive found in production (wave 38 main report,
    # financing_factors): "worsening 8.8 pp from a net 25.0%" and
    # "worsening by 11.60 pp from a net 22.43%" — pp-delta phrasing the
    # original _PP_DELTA_RE pattern (only "pp above/below") didn't match.
    df = pd.DataFrame([{"net_balance_wtd": -33.8, "wave_number": 38}, {"net_balance_wtd": -25.0, "wave_number": 37}])
    warnings = _check_numeric_grounding(
        ["Access to public support deteriorated for a net 33.8% of firms, worsening 8.8 pp from a net 25.0% in the prior wave"],
        df, ["net_balance_wtd"],
    )
    assert warnings == []


def test_grounding_check_accepts_up_from_phrasing():
    df = pd.DataFrame([{"net_balance_wtd": -83.62, "wave_number": 38}, {"net_balance_wtd": -61.84, "wave_number": 37}])
    warnings = _check_numeric_grounding(
        ["Labour costs rose for a net 83.62% of firms, up 21.78 pp from a net 61.84% in wave 37"],
        df, ["net_balance_wtd"],
    )
    assert warnings == []


def test_grounding_check_accepts_outpacing_by_phrasing():
    df = pd.DataFrame([{"pct_wtd": 32.8, "wave_number": 38}, {"pct_wtd": 20.2, "wave_number": 38}])
    warnings = _check_numeric_grounding(
        ["Slovak firms rose to 32.8%, outpacing the euro area's 20.2% by 12.6 pp"],
        df, ["pct_wtd"],
    )
    assert warnings == []


def test_grounding_check_accepts_sign_stripped_negative_net_balance():
    # Real false positive found in production (wave 38, financing_factors):
    # DataFrame has net_balance_wtd = -33.8, but the bullet cites it as
    # "a net 33.8%" (sign conveyed by "deteriorated" instead of a minus sign)
    # — legitimate English, not a fabrication.
    df = pd.DataFrame([{"net_balance_wtd": -33.8, "wave_number": 38}])
    warnings = _check_numeric_grounding(
        ["Access to public support deteriorated for a net 33.8% of Slovak firms"],
        df, ["net_balance_wtd"],
    )
    assert warnings == []


def test_grounding_check_skips_month_horizon_reference():
    # Real false positive found in production (wave 38, expectations_quantitative):
    # "over the next 12 months" — a time horizon, not a cited data value.
    df = pd.DataFrame([{"net_balance_wtd": 5.0, "wave_number": 38}])
    warnings = _check_numeric_grounding(
        ["Slovak firms expected input prices to rise over the next 12 months"],
        df, ["net_balance_wtd"],
    )
    assert warnings == []


def test_grounding_check_still_flags_invented_number_despite_new_skips():
    # Ensure the new sign-stripped / month-horizon / broadened pp-delta skips
    # don't blanket-suppress genuine fabrications elsewhere in the same bullet.
    df = pd.DataFrame([{"net_balance_wtd": -33.8, "wave_number": 38}])
    warnings = _check_numeric_grounding(
        ["Access to public support deteriorated for a net 33.8% of firms over the next 12 months, an invented 77.2% swing"],
        df, ["net_balance_wtd"],
    )
    assert len(warnings) == 1
    assert "77.2" in warnings[0]


# ── _NUMBER_RE / _clean_num_token: EN thousands-comma vs SK decimal-comma ───
# Real production bug: Slovak formats decimals with a comma ("43,8" = 43.8,
# not English "43.8"), which the translation pass renders despite its prompt
# saying to keep numbers unchanged. The old cleaner only handled English
# thousands-separator commas ("5,087" -> "5087"), so "43,8" tokenized as two
# separate numbers ("43" and "8"), and the grounding safety-net in
# enforce_bullet_style then flagged the real, correctly cited "43" as
# fabricated — aborting every SK run with dozens of false positives.


def _tokens(text: str) -> list[str]:
    return [_clean_num_token(m.group(1)) for m in _NUMBER_RE.finditer(text)]


def test_number_re_slovak_decimal_comma_is_one_token():
    assert _tokens("Čistých 43,8 % slovenských firiem") == ["43.8"]


def test_number_re_slovak_decimal_comma_two_digits():
    assert _tokens("Regulácia dosiahla 6,10/10 (n=236)") == ["6.10", "10", "236"]


def test_number_re_english_thousands_comma_still_works():
    assert _tokens("a net 54% (n=5,087)") == ["54", "5087"]


def test_number_re_english_decimal_period_unaffected():
    assert _tokens("A net 43.8% of Slovak firms (n=76)") == ["43.8", "76"]


def test_grounding_check_accepts_slovak_decimal_comma_citation():
    df = pd.DataFrame([{"net_balance_wtd": 43.8, "wave_number": 39}])
    warnings = _check_numeric_grounding(
        ["Čistých 43,8 % slovenských firiem (n=76) uviedlo sprísnenie podmienok."],
        df, ["net_balance_wtd"],
    )
    assert warnings == []


# ── _check_exec_provenance / _fix_exec_provenance_mislabels ─────────────────

def _rendered(section_id, *bullets):
    return {"section_id": section_id, "bullets": list(bullets)}


def test_exec_provenance_passes_when_numbers_match_claimed_section():
    rendered = [_rendered("bank_loan_terms", "Rates rose 12.3pp for Slovak firms.")]
    exec_bullets = [{"bullet": "Rates rose 12.3pp.", "section_id": "bank_loan_terms"}]
    assert _check_exec_provenance(exec_bullets, rendered) == []


def test_exec_provenance_flags_mismatch():
    rendered = [_rendered("bank_loan_terms", "Rates rose 12.3pp for Slovak firms.")]
    exec_bullets = [{"bullet": "A striking 99.7pp shift occurred.", "section_id": "bank_loan_terms"}]
    errors = _check_exec_provenance(exec_bullets, rendered)
    assert len(errors) == 1
    assert "99.7" in errors[0]


def test_fix_mislabel_retags_bullet_to_the_correct_section():
    # Real bug found in production (wave 38): an exec bullet entirely about
    # business_situation (labour costs, turnover, profits) was tagged with
    # section_id "bank_loan_terms" instead. The numbers are real and grounded
    # — just attributed to the wrong section — so this should be silently
    # re-tagged to the section they actually belong to.
    rendered = [
        _rendered("bank_loan_terms", "Bank loan interest rates rose 9.2pp."),
        _rendered(
            "business_situation",
            "Labour costs rose 21.8 pp and 21 pp above the EA; turnover fell 26 pp; profits fell 23 pp below the EA's 17 pp.",
        ),
    ]
    exec_bullets = [{
        "bullet": "**Margin squeeze intensifies:** labour costs up 21.8 pp and 21 pp above the EA, "
                  "turnover fell for a net 26%, profits declined — 23 pp below the EA's net 17%.",
        "section_id": "bank_loan_terms",
    }]
    fixed = _fix_exec_provenance_mislabels(exec_bullets, rendered)
    assert fixed[0]["section_id"] == "business_situation"
    # And the provenance check is clean after the fix.
    assert _check_exec_provenance(fixed, rendered) == []


def test_fix_mislabel_leaves_genuine_cross_cutting_bullet_untouched():
    # A bullet that genuinely cites numbers from TWO different sections is
    # ambiguous — there's no single correct section to re-tag it to, so it
    # must be left as-is (still caught by the provenance warning, not silently
    # "fixed" to an arbitrary guess).
    rendered = [
        _rendered("bank_loan_terms", "Rates rose 12.3pp for Slovak firms."),
        _rendered("business_situation", "Labour costs rose 21.8pp."),
    ]
    exec_bullets = [{
        "bullet": "Rates rose 12.3pp while labour costs climbed 21.8pp.",
        "section_id": "bank_loan_terms",
    }]
    fixed = _fix_exec_provenance_mislabels(exec_bullets, rendered)
    assert fixed[0]["section_id"] == "bank_loan_terms"  # unchanged


def test_fix_mislabel_leaves_bullet_untouched_when_no_section_matches():
    rendered = [_rendered("bank_loan_terms", "Rates rose 9.2pp.")]
    exec_bullets = [{"bullet": "A striking 99.7pp shift occurred.", "section_id": "bank_loan_terms"}]
    fixed = _fix_exec_provenance_mislabels(exec_bullets, rendered)
    assert fixed[0]["section_id"] == "bank_loan_terms"  # unchanged, still flagged by the check
    assert len(_check_exec_provenance(fixed, rendered)) == 1


# ── _shorten_question_llm ────────────────────────────────────────────────────

def _mock_mistral_response(content: str, prompt_tokens=20, completion_tokens=8):
    client = MagicMock()
    resp = MagicMock()
    resp.choices[0].message.content = content
    resp.usage.prompt_tokens = prompt_tokens
    resp.usage.completion_tokens = completion_tokens
    client.chat.complete.return_value = resp
    return client


def test_shorten_question_llm_valid_json():
    client = _mock_mistral_response('{"short_question": "Which problems are pressing for you?"}')
    result = _shorten_question_llm("Please indicate the pressingness of the following problems.", client)
    assert result["short_question"] == "Which problems are pressing for you?"
    assert result["_usage"] == {"input": 20, "output": 8}


def test_shorten_question_llm_fenced_json():
    client = _mock_mistral_response('```json\n{"short_question": "How is your turnover changing?"}\n```')
    result = _shorten_question_llm("source text", client)
    assert result["short_question"] == "How is your turnover changing?"


def test_shorten_question_llm_malformed_json_repaired():
    # Missing closing brace — json_repair should still recover the field
    client = _mock_mistral_response('{"short_question": "Are your financing needs changing?"')
    result = _shorten_question_llm("source text", client)
    assert result["short_question"] == "Are your financing needs changing?"


def test_shorten_question_llm_api_exception_returns_empty():
    client = MagicMock()
    client.chat.complete.side_effect = RuntimeError("API down")
    result = _shorten_question_llm("source text", client)
    assert result == {"short_question": ""}


# ── get_shortened_questions ──────────────────────────────────────────────────

def _sections_stub():
    return [
        {"id": "business_problems", "question_ids": ["q0b"]},
        {"id": "bank_loan_terms", "question_ids": ["q10"]},
    ]


def _mock_con(cached_rows=None):
    con = MagicMock()
    cached_rows = cached_rows or []

    def execute_side_effect(sql, *args, **kwargs):
        result = MagicMock()
        if "SELECT section_id, source_hash, short_caption" in sql:
            result.fetchall.return_value = cached_rows
        return result

    con.execute.side_effect = execute_side_effect
    return con


def test_get_shortened_questions_cache_hit_skips_llm():
    question_texts = {"q0b": "What problems are pressing for your enterprise?"}
    import hashlib
    h = hashlib.sha256(question_texts["q0b"].encode()).hexdigest()[:16]
    con = _mock_con(cached_rows=[("business_problems", h, "Which problems are pressing for you?")])
    mistral_client = MagicMock()
    tracker = {"input_tokens": 0, "output_tokens": 0, "usd": 0.0, "calls": 0, "by_model": {}}

    result = get_shortened_questions(
        _sections_stub(), question_texts, con, "main_safe", mistral_client, tracker,
    )

    assert result == {"business_problems": "Which problems are pressing for you?"}
    mistral_client.chat.complete.assert_not_called()


def test_get_shortened_questions_hash_mismatch_regenerates():
    question_texts = {"q0b": "New wording for the pressingness question."}
    con = _mock_con(cached_rows=[("business_problems", "stale_hash", "Old caption?")])
    mistral_client = _mock_mistral_response('{"short_question": "What problems trouble your firm?"}')
    tracker = {"input_tokens": 0, "output_tokens": 0, "usd": 0.0, "calls": 0, "by_model": {}}

    result = get_shortened_questions(
        _sections_stub(), question_texts, con, "main_safe", mistral_client, tracker,
    )

    assert result["business_problems"] == "What problems trouble your firm?"
    mistral_client.chat.complete.assert_called()
    assert tracker["calls"] == 1


def test_get_shortened_questions_force_refresh_regenerates_on_hit():
    question_texts = {"q0b": "What problems are pressing for your enterprise?"}
    import hashlib
    h = hashlib.sha256(question_texts["q0b"].encode()).hexdigest()[:16]
    con = _mock_con(cached_rows=[("business_problems", h, "Old caption?")])
    mistral_client = _mock_mistral_response('{"short_question": "Fresh caption?"}')
    tracker = {"input_tokens": 0, "output_tokens": 0, "usd": 0.0, "calls": 0, "by_model": {}}

    result = get_shortened_questions(
        _sections_stub(), question_texts, con, "main_safe", mistral_client, tracker,
        force_refresh=True,
    )

    assert result["business_problems"] == "Fresh caption?"
    mistral_client.chat.complete.assert_called()


def test_get_shortened_questions_missing_annex_text_omitted():
    # Neither section has an entry in question_texts
    con = _mock_con(cached_rows=[])
    mistral_client = MagicMock()
    tracker = {"input_tokens": 0, "output_tokens": 0, "usd": 0.0, "calls": 0, "by_model": {}}

    result = get_shortened_questions(
        _sections_stub(), {}, con, "main_safe", mistral_client, tracker,
    )

    assert result == {}
    mistral_client.chat.complete.assert_not_called()
    # No cache write should have been attempted since nothing was generated
    con.executemany.assert_not_called()


# ── exec-summary reasoning-channel signals ──────────────────────────────────

def _sec_stub(panel="f", value_col="net_balance_wtd", question_ids=None):
    return {
        "value_col": value_col,
        "panel_col": "sub_item",
        "pinned_panels": [panel],
        "question_ids": question_ids or ["q11"],
    }


def _make_multiwave_df(sub_item, waves, sk_vals, ea_vals, value_col="net_balance_wtd", n_respondents=None):
    rows = []
    for i, w in enumerate(waves):
        n = n_respondents[i] if n_respondents else 50
        rows.append({"wave_number": w, "country_code": "SK", "sub_item": sub_item,
                     value_col: sk_vals[i], "n_respondents": n})
        rows.append({"wave_number": w, "country_code": "EA", "sub_item": sub_item,
                     value_col: ea_vals[i], "n_respondents": 500})
    return pd.DataFrame(rows)


def test_sk_ea_gap_basic():
    df = _make_multiwave_df("f", [37, 38], sk_vals=[5.0, 12.0], ea_vals=[5.0, 4.0])
    assert sk_ea_gap(df, _sec_stub()) == pytest.approx(8.0)


def test_sk_ea_gap_missing_data_returns_none():
    df = _make_multiwave_df("f", [38], sk_vals=[5.0], ea_vals=[float("nan")])
    assert sk_ea_gap(df, _sec_stub()) is None


def test_historical_extremity_record():
    df = _make_multiwave_df("f", [35, 36, 37, 38], sk_vals=[5.0, 6.0, 7.0, 20.0], ea_vals=[5, 5, 5, 5])
    result = historical_extremity(df, _sec_stub())
    assert result["is_record"] is True
    assert result["historical_extreme"] is True
    assert result["waves_covered"] == 4


def test_historical_extremity_not_extreme():
    df = _make_multiwave_df("f", [35, 36, 37, 38], sk_vals=[5.0, 5.2, 4.8, 5.1], ea_vals=[5, 5, 5, 5])
    result = historical_extremity(df, _sec_stub())
    assert result["is_record"] is False
    assert result["historical_extreme"] is False


def test_historical_extremity_widest_spread():
    rows = []
    for w, sk, ea in [(35, 5.0, 5.0), (36, 5.0, 5.0), (37, 5.0, 5.0), (38, 5.0, 25.0)]:
        rows.append({"wave_number": w, "country_code": "SK", "sub_item": "f", "net_balance_wtd": sk, "n_respondents": 50})
        rows.append({"wave_number": w, "country_code": "EA", "sub_item": "f", "net_balance_wtd": ea, "n_respondents": 500})
    df = pd.DataFrame(rows)
    result = historical_extremity(df, _sec_stub())
    assert result["widest_spread_on_record"] is True
    assert result["historical_extreme"] is True


def test_direction_reversal_detected():
    df = _make_multiwave_df("f", [36, 37, 38], sk_vals=[10.0, 4.0, 9.0], ea_vals=[5, 5, 5])
    assert direction_reversal(df, _sec_stub()) is True


def test_direction_reversal_continuation_not_flagged():
    df = _make_multiwave_df("f", [36, 37, 38], sk_vals=[10.0, 12.0, 15.0], ea_vals=[5, 5, 5])
    assert direction_reversal(df, _sec_stub()) is False


def test_direction_reversal_below_noise_floor():
    df = _make_multiwave_df("f", [36, 37, 38], sk_vals=[10.0, 10.5, 10.0], ea_vals=[5, 5, 5])
    assert direction_reversal(df, _sec_stub()) is False


def test_reliable_n_above_threshold():
    df = _make_multiwave_df("f", [38], sk_vals=[5.0], ea_vals=[5.0], n_respondents=[45])
    assert reliable_n(df, _sec_stub()) is True


def test_reliable_n_below_threshold():
    df = _make_multiwave_df("f", [38], sk_vals=[5.0], ea_vals=[5.0], n_respondents=[12])
    assert reliable_n(df, _sec_stub()) is False


def test_reliable_n_defaults_true_when_column_absent():
    df = pd.DataFrame([{"wave_number": 38, "country_code": "SK", "sub_item": "f", "net_balance_wtd": 5.0}])
    assert reliable_n(df, _sec_stub()) is True


# ── classify_ecb_emphasis ────────────────────────────────────────────────────

def test_classify_ecb_emphasis_scope_guard_no_slovakia_mentions():
    tracker = {"input_tokens": 0, "output_tokens": 0, "usd": 0.0, "calls": 0, "by_model": {}}
    result = classify_ecb_emphasis(
        "Some EA-wide text with no country mentions.",
        [{"id": "financing_gap", "title": "Financing Gap"}], MagicMock(), tracker,
    )
    assert result == {}


def test_classify_ecb_emphasis_scope_guard_single_mention():
    tracker = {"input_tokens": 0, "output_tokens": 0, "usd": 0.0, "calls": 0, "by_model": {}}
    result = classify_ecb_emphasis(
        "Slovakia is mentioned once here.",
        [{"id": "financing_gap", "title": "Financing Gap"}], MagicMock(), tracker,
    )
    assert result == {}


def test_classify_ecb_emphasis_parses_response():
    client = _mock_mistral_response(
        '{"financing_gap": {"emphasized": true, "quote": "euro area SMEs reported tightening"}}'
    )
    tracker = {"input_tokens": 0, "output_tokens": 0, "usd": 0.0, "calls": 0, "by_model": {}}
    result = classify_ecb_emphasis(
        "Slovakia's financing gap widened. Slovak firms reported worsening access.",
        [{"id": "financing_gap", "title": "Financing Gap"}], client, tracker,
    )
    assert result["financing_gap"]["emphasized"] is True
    assert "tightening" in result["financing_gap"]["quote"]


def test_classify_ecb_emphasis_api_exception_returns_empty():
    client = MagicMock()
    client.chat.complete.side_effect = RuntimeError("API down")
    tracker = {"input_tokens": 0, "output_tokens": 0, "usd": 0.0, "calls": 0, "by_model": {}}
    result = classify_ecb_emphasis(
        "Slovakia mentioned twice: Slovak firms reported X.",
        [{"id": "financing_gap", "title": "Financing Gap"}], client, tracker,
    )
    assert result == {}


# ── translate_to_slovak ──────────────────────────────────────────────────────

def test_translate_to_slovak_translates_annex_questions_when_provided():
    client = _mock_mistral_response(
        '{"exec_bullets": ["Preložený bod"], "sections": [], '
        '"annex_questions": {"q5": "Preložená otázka"}}'
    )
    tracker = {"input_tokens": 0, "output_tokens": 0, "usd": 0.0, "calls": 0, "by_model": {}}
    with patch("llm._mistral_client", return_value=client):
        sk_rendered, sk_exec_bullets, sk_question_texts = translate_to_slovak(
            [], [{"bullet": "Original bullet", "section_id": "x"}], tracker,
            question_texts={"q5": "Original question"},
        )
    assert sk_question_texts == {"q5": "Preložená otázka"}
    assert sk_exec_bullets[0]["bullet"] == "Preložený bod"


def test_translate_to_slovak_no_question_texts_returns_empty_dict():
    client = _mock_mistral_response('{"exec_bullets": ["Bod"], "sections": []}')
    tracker = {"input_tokens": 0, "output_tokens": 0, "usd": 0.0, "calls": 0, "by_model": {}}
    with patch("llm._mistral_client", return_value=client):
        _, _, sk_question_texts = translate_to_slovak(
            [], [{"bullet": "Original bullet", "section_id": "x"}], tracker,
        )
    assert sk_question_texts == {}


def test_translate_to_slovak_parse_failure_falls_back_to_english_questions():
    client = _mock_mistral_response("not valid json")
    tracker = {"input_tokens": 0, "output_tokens": 0, "usd": 0.0, "calls": 0, "by_model": {}}
    with patch("llm._mistral_client", return_value=client):
        sk_rendered, sk_exec_bullets, sk_question_texts = translate_to_slovak(
            [], [{"bullet": "Original bullet", "section_id": "x"}], tracker,
            question_texts={"q5": "Original question"},
        )
    assert sk_question_texts == {"q5": "Original question"}


def test_translate_to_slovak_malformed_section_entry_does_not_crash():
    """Reproduces a real production failure: Mistral's JSON response included
    a section entry missing 'id' (a repair_json-salvaged, truncated object)
    and `by_id = {s["id"]: s for s in ...}` raised KeyError, aborting the
    whole run. A malformed entry must be skipped (falling back to English
    content for that section only), not crash the translation entirely."""
    client = _mock_mistral_response(
        '{"exec_bullets": ["Bod"], "sections": ['
        '{"id": "financing_gap", "title": "Medzera", "finding": "Zistenie", "bullets": ["Bod 1"]}, '
        '{"title": "Chybný", "finding": "Bez id", "bullets": ["Bod 2"]}'
        ']}'
    )
    tracker = {"input_tokens": 0, "output_tokens": 0, "usd": 0.0, "calls": 0, "by_model": {}}
    rendered = [
        {"section_id": "financing_gap", "title": "Financing Gap",
         "finding": "Original finding", "bullets": ["Original bullet"]},
        {"section_id": "outlook", "title": "Outlook",
         "finding": "Other original finding", "bullets": ["Other original bullet"]},
    ]
    with patch("llm._mistral_client", return_value=client):
        sk_rendered, sk_exec_bullets, _ = translate_to_slovak(rendered, [], tracker)

    by_id = {s["section_id"]: s for s in sk_rendered}
    assert by_id["financing_gap"]["finding"] == "Zistenie"
    # The malformed entry never made it into by_id, so "outlook" falls back
    # to its original English content rather than crashing the whole call.
    assert by_id["outlook"]["finding"] == "Other original finding"


def test_translate_to_slovak_translates_bullets_by_question_and_synthesis():
    """Regression guard: per-question adhoc bullets and the cross-cutting synthesis
    bullets must be translated too, not just the flat 'bullets' list — a prior gap
    left every per-question adhoc subsection showing English text in the SK report."""
    client = _mock_mistral_response(json.dumps({
        "exec_bullets": ["Preložený exec bod"],
        "sections": [],
        "adhoc": {
            "theme_label": "Umelá inteligencia",
            "title": "Špeciálne zameranie",
            "finding": "Preložené zistenie",
            "bullets": ["Preložený bod"],
            "bullets_by_question": {"qa1": ["Preložený bod QA1"], "qa2": ["Preložený bod QA2"]},
        },
    }))
    tracker = {"input_tokens": 0, "output_tokens": 0, "usd": 0.0, "calls": 0, "by_model": {}}
    adhoc_section = {
        "section_id": "adhoc_spotlight",
        "title": "Special Focus",
        "finding": "English finding",
        "bullets": ["English bullet"],
        "bullets_by_question": {"qa1": ["English bullet QA1"], "qa2": ["English bullet QA2"]},
        "theme_label": "Artificial Intelligence",
    }
    with patch("llm._mistral_client", return_value=client):
        sk_rendered, _, _ = translate_to_slovak([adhoc_section], [], tracker)

    sk_adhoc = sk_rendered[0]
    assert sk_adhoc["bullets_by_question"]["qa1"] == ["Preložený bod QA1"]
    assert sk_adhoc["bullets_by_question"]["qa2"] == ["Preložený bod QA2"]


def test_translate_to_slovak_bullets_by_question_falls_back_per_question():
    """If the SK translation only returns some questions' bullets (or the model omits
    the key entirely), each question falls back to its own English bullets rather than
    losing every question because one was missing."""
    client = _mock_mistral_response(json.dumps({
        "exec_bullets": [],
        "sections": [],
        "adhoc": {
            "bullets_by_question": {"qa1": ["Preložený bod QA1"]},  # qa2 missing
        },
    }))
    tracker = {"input_tokens": 0, "output_tokens": 0, "usd": 0.0, "calls": 0, "by_model": {}}
    adhoc_section = {
        "section_id": "adhoc_spotlight",
        "title": "Special Focus",
        "finding": "English finding",
        "bullets": ["English bullet"],
        "bullets_by_question": {"qa1": ["English bullet QA1"], "qa2": ["English bullet QA2"]},
        "theme_label": "Artificial Intelligence",
    }
    with patch("llm._mistral_client", return_value=client):
        sk_rendered, _, _ = translate_to_slovak([adhoc_section], [], tracker)

    sk_adhoc = sk_rendered[0]
    assert sk_adhoc["bullets_by_question"]["qa1"] == ["Preložený bod QA1"]
    assert sk_adhoc["bullets_by_question"]["qa2"] == ["English bullet QA2"]


# ── build_section_signals ────────────────────────────────────────────────────

def test_build_section_signals_resolves_tier_and_deprioritized_subitem():
    sections_by_id = {
        "financing_factors": {
            "id": "financing_factors", "value_col": "net_balance_wtd", "panel_col": "sub_item",
            "panel_label_col": "sub_item_label", "pinned_panels": ["f"], "must_lead_with": "f",
            "exec_tier": "supporting", "question_ids": ["q11"],
            "subitem_tiers": {"f": "core", "a": "supporting", "b": "policy_technical"},
        },
        "loan_applications": {
            "id": "loan_applications", "value_col": "financing_gap_wtd", "panel_col": None,
            "panel_label_col": None, "pinned_panels": [], "exec_tier": "core",
            "question_ids": ["q7a", "q7b"],
        },
    }

    ff_rows = []
    for w in [37, 38]:
        ff_rows.append({"wave_number": w, "country_code": "SK", "sub_item": "b",
                        "sub_item_label": "Access to public financial support",
                        "net_balance_wtd": 5.0, "n_respondents": 20})
        ff_rows.append({"wave_number": w, "country_code": "SK", "sub_item": "f",
                        "sub_item_label": "Willingness of banks",
                        "net_balance_wtd": 2.0, "n_respondents": 80})
        ff_rows.append({"wave_number": w, "country_code": "EA", "sub_item": "f",
                        "sub_item_label": "Willingness of banks",
                        "net_balance_wtd": 2.0, "n_respondents": 800})
    ff_df = pd.DataFrame(ff_rows)

    la_rows = []
    for w in [37, 38]:
        la_rows.append({"wave_number": w, "country_code": "SK", "financing_gap_wtd": 10.0 + w, "n_respondents": 90})
        la_rows.append({"wave_number": w, "country_code": "EA", "financing_gap_wtd": 5.0, "n_respondents": 900})
    la_df = pd.DataFrame(la_rows)

    rendered = [
        {"section_id": "financing_factors", "title": "Factors", "finding": "F", "bullets": ["b1"]},
        {"section_id": "loan_applications", "title": "Loan Apps", "finding": "F2", "bullets": ["b1"]},
    ]
    data = {"financing_factors": ff_df, "loan_applications": la_df}

    signals = build_section_signals(rendered, data, sections_by_id, ecb_emphasis={})

    ff_sig = signals["financing_factors"]
    assert ff_sig["tier"] == "core"  # lead panel resolves to 'f' via must_lead_with
    assert ff_sig["deprioritized_topics"] == ["Access to public financial support (Q11b)"]

    la_sig = signals["loan_applications"]
    assert la_sig["tier"] == "core"
    assert la_sig["deprioritized_topics"] == []


def test_build_section_signals_skips_sections_not_in_registry():
    rendered = [{"section_id": "adhoc_spotlight", "title": "Spotlight", "finding": "F", "bullets": ["b1"]}]
    data = {"adhoc_spotlight": pd.DataFrame([{"wave_number": 38, "country_code": "SK"}])}
    signals = build_section_signals(rendered, data, sections_by_id={}, ecb_emphasis={})
    assert signals == {}


# ── get_exec_summary + [SIGNALS] prompt wiring ──────────────────────────────

def _exec_usage_mock():
    return MagicMock(input_tokens=10, output_tokens=5,
                      cache_creation_input_tokens=0, cache_read_input_tokens=0)


def _text_block(text):
    return MagicMock(type="text", text=text)


def _emit_exec_bullets_response(bullets):
    resp = MagicMock()
    resp.usage = _exec_usage_mock()
    resp.stop_reason = "tool_use"
    tool_block = MagicMock()
    tool_block.type = "tool_use"
    tool_block.name = "emit_exec_bullets"
    tool_block.input = {"bullets": bullets}
    resp.content = [tool_block]
    return resp


def test_get_exec_summary_includes_signals_line_in_prompt():
    anthropic_client = MagicMock()
    call_count = {"n": 0}

    def create_side_effect(*args, **kwargs):
        call_count["n"] += 1
        if call_count["n"] == 1:
            # Pass 1: cross-section themes (plain text, no tools)
            resp = MagicMock()
            resp.usage = _exec_usage_mock()
            resp.content = [_text_block("No major cross-cutting themes.")]
            return resp
        if call_count["n"] == 2:
            # Pass 2, first tool-use turn: model finishes without calling compute_delta
            resp = MagicMock()
            resp.usage = _exec_usage_mock()
            resp.stop_reason = "end_turn"
            resp.content = [_text_block("(reasoning, no tool call)")]
            return resp
        # Forced emit call
        return _emit_exec_bullets_response(
            [{"bullet": "**Test:** something.", "section_id": "financing_factors"}]
        )

    anthropic_client.messages.create.side_effect = create_side_effect

    rendered = [{
        "section_id": "financing_factors", "title": "Factors", "finding": "F",
        "bullets": ["b1"], "sign_note": "",
    }]
    signals = {
        "financing_factors": {
            "tier": "policy_technical",
            "deprioritized_topics": ["Access to public financial support (Q11b)"],
            "sk_ea_gap_pp": 3.1, "historical_extreme": False, "waves_covered": 4,
            "direction_reversal": True, "reliable_n": False,
            "ecb_emphasis": False, "ecb_quote": "",
        }
    }
    tracker = {"input_tokens": 0, "output_tokens": 0, "usd": 0.0, "calls": 0, "by_model": {}}

    result = get_exec_summary(rendered, tracker, section_signals=signals, anthropic_client=anthropic_client)

    assert result == [{"bullet": "**Test:** something.", "section_id": "financing_factors"}]

    pass2_call = anthropic_client.messages.create.call_args_list[1]
    pass2_user_msg = pass2_call.kwargs["messages"][0]["content"]
    assert "[SIGNALS]" in pass2_user_msg
    assert "tier=policy_technical" in pass2_user_msg
    assert "deprioritized: Access to public financial support (Q11b)" in pass2_user_msg


def test_get_exec_summary_uses_compute_delta_tool_result_verbatim():
    anthropic_client = MagicMock()
    call_count = {"n": 0}
    seen_messages_by_call: list[list] = []

    def create_side_effect(*args, **kwargs):
        call_count["n"] += 1
        # Snapshot a deep-enough copy now — the real `messages` list is mutated
        # in-place after this call, so call_args_list would otherwise only ever
        # show its FINAL state for every recorded call (a Mock/mutable-list
        # gotcha, not something the real code needs to guard against).
        seen_messages_by_call.append(list(kwargs.get("messages", [])))
        if call_count["n"] == 1:
            resp = MagicMock()
            resp.usage = _exec_usage_mock()
            resp.content = [_text_block("theme")]
            return resp
        if call_count["n"] == 2:
            # Model calls compute_delta(83.62, 61.84) instead of subtracting itself
            resp = MagicMock()
            resp.usage = _exec_usage_mock()
            resp.stop_reason = "tool_use"
            tool_block = MagicMock()
            tool_block.type = "tool_use"
            tool_block.id = "call_1"
            tool_block.input = {"a": 83.62, "b": 61.84, "label": "labour costs delta"}
            resp.content = [tool_block]
            return resp
        if call_count["n"] == 3:
            # Model finishes reasoning after seeing the tool result
            resp = MagicMock()
            resp.usage = _exec_usage_mock()
            resp.stop_reason = "end_turn"
            resp.content = [_text_block("ok")]
            return resp
        return _emit_exec_bullets_response(
            [{"bullet": "**Costs:** up 21.78pp.", "section_id": "business_situation"}]
        )

    anthropic_client.messages.create.side_effect = create_side_effect

    rendered = [{
        "section_id": "business_situation", "title": "Situation", "finding": "F",
        "bullets": ["Labour costs rose to 83.62% from 61.84%."], "sign_note": "",
    }]
    tracker = {"input_tokens": 0, "output_tokens": 0, "usd": 0.0, "calls": 0, "by_model": {}}

    result = get_exec_summary(rendered, tracker, anthropic_client=anthropic_client)

    assert result[0]["bullet"] == "**Costs:** up 21.78pp."
    # Call #3 (index 2) is the first one made AFTER the tool result was appended —
    # its messages must contain the exact computed value, not a re-derived one.
    call3_messages = seen_messages_by_call[2]
    tool_result_msg = call3_messages[-1]["content"]
    assert tool_result_msg[0]["content"] == "21.78"


# ── get_section_content_agentic mandatory-lead instruction ─────────────────

def test_get_section_content_agentic_includes_mandatory_lead_instruction():
    from llm import get_section_content_agentic

    client = MagicMock()
    call_count = {"n": 0}

    def create_side_effect(*args, **kwargs):
        call_count["n"] += 1
        resp = MagicMock()
        resp.usage = MagicMock(input_tokens=10, output_tokens=5,
                               cache_creation_input_tokens=0, cache_read_input_tokens=0)
        if call_count["n"] == 1:
            resp.stop_reason = "end_turn"
            resp.content = [MagicMock(type="text", text="reasoning done")]
        else:
            block = MagicMock()
            block.type = "tool_use"
            block.name = "emit_section_json"
            block.input = {"finding": "Test finding", "bullets": ["b1"], "chart_subtitle": "cap"}
            resp.content = [block]
        return resp

    client.messages.create.side_effect = create_side_effect

    sec = {
        "id": "financing_factors", "title": "Factors", "value_col": "net_balance_wtd",
        "panel_col": "sub_item", "sign_note": "note", "focus": "Lead with f",
        "must_lead_with": "f", "pinned_panels": ["f"],
    }
    df = pd.DataFrame([
        {"wave_number": 37, "country_code": "SK", "sub_item": "f", "net_balance_wtd": 1.0},
        {"wave_number": 38, "country_code": "SK", "sub_item": "f", "net_balance_wtd": 2.0},
    ])
    tracker = {"input_tokens": 0, "output_tokens": 0, "usd": 0.0, "calls": 0, "by_model": {}}

    get_section_content_agentic(sec, df, MagicMock(), "main_safe", "catalogue", tracker, client=client)

    first_call_messages = client.messages.create.call_args_list[0].kwargs["messages"]
    user_msg = first_call_messages[0]["content"]
    assert "MANDATORY" in user_msg
    assert "sub-item 'f'" in user_msg


def test_get_section_content_agentic_no_mandatory_instruction_when_unset():
    from llm import get_section_content_agentic

    client = MagicMock()
    call_count = {"n": 0}

    def create_side_effect(*args, **kwargs):
        call_count["n"] += 1
        resp = MagicMock()
        resp.usage = MagicMock(input_tokens=10, output_tokens=5,
                               cache_creation_input_tokens=0, cache_read_input_tokens=0)
        if call_count["n"] == 1:
            resp.stop_reason = "end_turn"
            resp.content = [MagicMock(type="text", text="reasoning done")]
        else:
            block = MagicMock()
            block.type = "tool_use"
            block.name = "emit_section_json"
            block.input = {"finding": "Test finding", "bullets": ["b1"], "chart_subtitle": "cap"}
            resp.content = [block]
        return resp

    client.messages.create.side_effect = create_side_effect

    sec = {
        "id": "bank_loan_terms", "title": "Terms", "value_col": "net_balance_wtd",
        "panel_col": "sub_item", "sign_note": "note", "focus": "Lead with SK",
        "pinned_panels": ["a"],
    }
    df = pd.DataFrame([
        {"wave_number": 37, "country_code": "SK", "sub_item": "a", "net_balance_wtd": 1.0},
        {"wave_number": 38, "country_code": "SK", "sub_item": "a", "net_balance_wtd": 2.0},
    ])
    tracker = {"input_tokens": 0, "output_tokens": 0, "usd": 0.0, "calls": 0, "by_model": {}}

    get_section_content_agentic(sec, df, MagicMock(), "main_safe", "catalogue", tracker, client=client)

    first_call_messages = client.messages.create.call_args_list[0].kwargs["messages"]
    user_msg = first_call_messages[0]["content"]
    assert "MANDATORY" not in user_msg
