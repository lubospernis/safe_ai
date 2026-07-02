"""Tests for the generic adhoc spotlight pipeline with an unknown topic (electrification mock).

Validates that detect_adhoc_theme() + build_adhoc_spotlight() produce valid output
structure for any new adhoc module without code changes — proving readiness for
future waves with novel topics like electrification efforts.

The 3-phase design:
  Phase 1 (Mistral Small): _select_interesting_sub_items() → ["a", "c"]
  Phase 2 (Sonnet): build bullets + chart_sub_items
  Phase 3 (Mistral Large): critical review scores
"""

import json
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from adhoc import (
    _fetch_question_texts,
    _is_continuous,
    _select_interesting_sub_items,
    build_adhoc_spotlight,
    detect_adhoc_theme,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def electrification_rows():
    """Fake mart_safe__adhoc_responses rows for a new 'qe1' electrification module."""
    return [("qe1", 120)]


@pytest.fixture
def electrification_df():
    """Categorical response data for qe1 — firm electrification readiness."""
    rows = []
    for country, responses in [
        ("SK", [(1, 35.0, 42), (2, 28.0, 34), (3, 22.0, 26), (4, 15.0, 18)]),
        ("EA", [(1, 28.0, 210), (2, 31.0, 233), (3, 25.0, 188), (4, 16.0, 120)]),
    ]:
        for resp_raw, pct_wtd, n_firms in responses:
            rows.append({
                "country_code": country,
                "sub_item": "a",
                "response_raw": resp_raw,
                "pct_wtd": pct_wtd,
                "n_firms": n_firms,
                "n_firms_wtd": float(n_firms),
                "n_total_wtd": 120.0,
            })
    return pd.DataFrame(rows)


@pytest.fixture
def mock_con(electrification_rows, electrification_df):
    """Mock DuckDB connection returning electrification fixture data."""
    con = MagicMock()

    def execute_side_effect(sql, *args, **kwargs):
        result = MagicMock()
        sql_lower = sql.lower()
        if "sum(n_firms)" in sql_lower and "group by module_id" in sql_lower:
            result.fetchall.return_value = electrification_rows
        elif "information_schema.columns" in sql_lower:
            result.fetchall.return_value = [("safe_2025q4",)]
        elif "ref_safe__annex" in sql_lower:
            result.fetchall.return_value = [
                ("qe1", "To what extent has your firm invested in electrification of equipment or vehicles?"),
            ]
        elif "mart_safe__slovakia_kpis" in sql_lower:
            # Return None so questionnaire URL lookup gracefully skips
            result.fetchone.return_value = None
        else:
            result.df.return_value = electrification_df
            result.fetchall.return_value = []
        return result

    con.execute.side_effect = execute_side_effect
    return con


@pytest.fixture
def mock_mistral_theme():
    """Mistral client that returns a theme label for electrification (Phase 1 + detection)."""
    client = MagicMock()

    def complete_side_effect(*args, **kwargs):
        resp = MagicMock()
        # Phase 1 selection call returns JSON; theme detection call returns plain text
        messages = kwargs.get("messages", args[0] if args else [])
        last_user = next(
            (m["content"] for m in reversed(messages) if m["role"] == "user"), ""
        )
        if "Classify each sub-item" in last_user or "interesting" in last_user.lower():
            resp.choices[0].message.content = json.dumps({
                "interesting": ["a"],
                "skip": [],
                "routing_notes": {"a": "all firms"},
            })
        else:
            resp.choices[0].message.content = "Electrification Efforts"
        resp.usage.prompt_tokens = 100
        resp.usage.completion_tokens = 15
        return resp

    client.chat.complete.side_effect = complete_side_effect
    return client


@pytest.fixture
def mock_anthropic_spotlight():
    """Anthropic client returning a valid JSON spotlight for electrification (Phase 2)."""
    client = MagicMock()
    resp = MagicMock()
    resp.content[0].text = json.dumps({
        "finding": "Slovak firms lag Euro Area peers in electrification readiness.",
        "bullets": [
            "**SK readiness gap:** 35% of Slovak firms report low electrification readiness (code 1), vs 28% in the EA.",
            "**EA more advanced:** 31% of EA firms report moderate readiness (code 2), slightly above Slovakia's 28%.",
        ],
        "chart_sub_items": ["a"],
    })
    resp.usage.input_tokens = 800
    resp.usage.output_tokens = 120
    client.messages.create.return_value = resp
    return client


@pytest.fixture
def mock_mistral_review():
    """Mistral Large client returning a passing review (Phase 3)."""
    client = MagicMock()
    resp = MagicMock()
    resp.choices[0].message.content = json.dumps({
        "grounding": 9, "coverage": 8, "readability": 9, "chart_alignment": 8,
        "verdict": "pass", "reason": "Well grounded and clear.",
    })
    resp.usage.prompt_tokens = 200
    resp.usage.completion_tokens = 50
    client.chat.complete.return_value = resp
    return client


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_detect_adhoc_theme_unknown_module(mock_con, mock_mistral_theme):
    """detect_adhoc_theme() should detect a new module and return a labelled theme."""
    cost = {"input_tokens": 0, "output_tokens": 0, "usd": 0.0, "calls": 0, "by_model": {}}
    result = detect_adhoc_theme(39, mock_con, "main_safe", mock_mistral_theme, cost)

    assert result is not None
    assert result["module_id"] == "qe1"
    assert isinstance(result["theme_label"], str)
    assert len(result["theme_label"]) > 0


def test_detect_adhoc_theme_returns_question_texts(mock_con, mock_mistral_theme):
    """detect_adhoc_theme() should include question_texts dict from the annex."""
    cost = {"input_tokens": 0, "output_tokens": 0, "usd": 0.0, "calls": 0, "by_model": {}}
    result = detect_adhoc_theme(39, mock_con, "main_safe", mock_mistral_theme, cost)

    assert result is not None
    assert isinstance(result["question_texts"], dict)


def test_select_interesting_sub_items(electrification_df, mock_mistral_theme):
    """_select_interesting_sub_items() should return the interesting list from Mistral."""
    cost = {"input_tokens": 0, "output_tokens": 0, "usd": 0.0, "calls": 0, "by_model": {}}
    full_table = "country_code | sub_item | response_raw | pct_wtd | n_firms\nSK | a | 1 | 35.0% | 42"
    interesting, routing = _select_interesting_sub_items(
        electrification_df, full_table, {}, "Electrification",
        cost, mistral_client=mock_mistral_theme,
    )
    assert isinstance(interesting, list)
    assert len(interesting) > 0
    assert isinstance(routing, dict)


def test_build_adhoc_spotlight_output_structure(
    mock_con, mock_mistral_theme, mock_anthropic_spotlight
):
    """build_adhoc_spotlight() should return a valid section dict for an unknown topic."""
    cost = {"input_tokens": 0, "output_tokens": 0, "usd": 0.0, "calls": 0, "by_model": {}}
    theme = {
        "module_id": "qe1",
        "theme_label": "Electrification Efforts",
        "question_texts": {"a": "To what extent has your firm invested in electrification?"},
    }

    with patch("adhoc.SQL_DIR") as mock_sql_dir:
        mock_sql_dir.__truediv__ = lambda self, other: mock_sql_dir
        mock_sql_dir.read_text.return_value = (
            "SELECT country_code, sub_item, response_raw, pct_wtd, n_firms, n_firms_wtd, n_total_wtd "
            "FROM {schema}.mart_safe__adhoc_responses "
            "WHERE wave_number = {wave_number} AND module_id = '{module_id}'"
        )
        result = build_adhoc_spotlight(
            theme, 39, mock_con, "main_safe",
            mistral_client=mock_mistral_theme,
            cost_tracker=cost,
            anthropic_client=mock_anthropic_spotlight,
        )

    assert result is not None
    assert result["section_id"] == "adhoc_spotlight"
    assert isinstance(result["finding"], str) and len(result["finding"]) > 0
    assert isinstance(result["bullets"], list)
    assert 2 <= len(result["bullets"]) <= 4
    assert result["theme_label"] == "Electrification Efforts"
    assert result["title"].startswith("Special Focus:")


def test_build_adhoc_spotlight_html_compatible_keys(
    mock_con, mock_mistral_theme, mock_anthropic_spotlight
):
    """Output dict must have all keys that html_builder.py expects for a spotlight section."""
    required_keys = {"section_id", "title", "finding", "bullets", "theme_label",
                     "review_scores", "review_passed"}
    cost = {"input_tokens": 0, "output_tokens": 0, "usd": 0.0, "calls": 0, "by_model": {}}
    theme = {
        "module_id": "qe1",
        "theme_label": "Electrification Efforts",
        "question_texts": {},
    }

    with patch("adhoc.SQL_DIR") as mock_sql_dir:
        mock_sql_dir.__truediv__ = lambda self, other: mock_sql_dir
        mock_sql_dir.read_text.return_value = (
            "SELECT country_code, sub_item, response_raw, pct_wtd, n_firms, n_firms_wtd, n_total_wtd "
            "FROM {schema}.mart_safe__adhoc_responses "
            "WHERE wave_number = {wave_number} AND module_id = '{module_id}'"
        )
        result = build_adhoc_spotlight(
            theme, 39, mock_con, "main_safe",
            mistral_client=mock_mistral_theme,
            cost_tracker=cost,
            anthropic_client=mock_anthropic_spotlight,
        )

    assert result is not None
    assert required_keys.issubset(result.keys())


def test_is_continuous_categorical(electrification_df):
    """Electrification data with 4 distinct coded responses should NOT be treated as continuous."""
    assert _is_continuous(electrification_df) is False
