from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from llm import (
    _check_numeric_grounding, _fmt_data_for_prompt, _parse_section_response,
    _shorten_question_llm, _sme_divergence_note, build_section_signals,
    classify_ecb_emphasis, direction_reversal, get_exec_summary, get_shortened_questions,
    historical_extremity, reliable_n, sk_ea_gap, translate_to_slovak,
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

def test_get_exec_summary_includes_signals_line_in_prompt():
    anthropic_client = MagicMock()
    call_count = {"n": 0}

    def create_side_effect(*args, **kwargs):
        call_count["n"] += 1
        resp = MagicMock()
        resp.usage = MagicMock(input_tokens=10, output_tokens=5,
                               cache_creation_input_tokens=0, cache_read_input_tokens=0)
        if call_count["n"] == 1:
            resp.content = [MagicMock(text="No major cross-cutting themes.")]
        else:
            resp.content = [MagicMock(
                text='[{"bullet": "**Test:** something.", "section_id": "financing_factors"}]'
            )]
        return resp

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

    get_exec_summary(rendered, tracker, section_signals=signals, anthropic_client=anthropic_client)

    pass2_messages = anthropic_client.messages.create.call_args_list[-1].kwargs["messages"]
    pass2_user_msg = pass2_messages[0]["content"]
    assert "[SIGNALS]" in pass2_user_msg
    assert "tier=policy_technical" in pass2_user_msg
    assert "deprioritized: Access to public financial support (Q11b)" in pass2_user_msg


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
