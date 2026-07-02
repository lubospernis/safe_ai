import pandas as pd
import pytest

from llm import _parse_section_response, _sme_divergence_note, _fmt_data_for_prompt


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
