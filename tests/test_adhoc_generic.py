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
    _is_continuous,
    _module_sort_key,
    _resolve_item_labels,
    _select_interesting_sub_items,
    build_adhoc_spotlight,
    detect_adhoc_theme,
    rebuild_adhoc_charts_sk,
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
        elif "mart_safe__annex_questions" in sql_lower:
            result.fetchall.return_value = [
                ("qe1", "To what extent has your firm invested in electrification of equipment or vehicles?"),
            ]
        elif "mart_safe__annex_answers" in sql_lower:
            result.fetchall.return_value = []
        elif "mart_safe__annex_items" in sql_lower:
            result.fetchall.return_value = []
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
    """Mistral client that returns a theme label for electrification (Phase 1 + detection),
    and a per-question description for _describe_question() so Phase 1 produces a real
    key_finding (needed for get_adhoc_synthesis() to have non-empty input)."""
    client = MagicMock()

    def complete_side_effect(*args, **kwargs):
        resp = MagicMock()
        messages = kwargs.get("messages", args[0] if args else [])
        system = next((m["content"] for m in messages if m["role"] == "system"), "")
        last_user = next(
            (m["content"] for m in reversed(messages) if m["role"] == "user"), ""
        )
        if "statistical analyst summarising one ECB SAFE survey question" in system:
            resp.choices[0].message.content = json.dumps({
                "description": "Slovak firms report lower electrification readiness than the EA.",
                "interest_score": 4,
                "key_finding": "SK 35% vs EA 28% on code 1 (low readiness)",
                "routing_note": "all firms",
            })
        elif "You paraphrase official ECB survey question wording" in system:
            resp.choices[0].message.content = json.dumps({
                "short_question": "How ready is your firm for electrification?",
            })
        elif "Classify each sub-item" in last_user or "interesting" in last_user.lower():
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
    """Anthropic client that serves both the Phase 3 spotlight call (bullets dict keyed
    by question_id) and get_adhoc_synthesis's cross-cutting call (bullet array),
    distinguished by the system prompt each function passes."""
    client = MagicMock()

    def create_side_effect(*args, **kwargs):
        resp = MagicMock()
        system = kwargs.get("system", "")
        if "cross-cutting synthesis" in system:
            resp.content[0].text = json.dumps([
                {"bullet": "**Cross-cutting:** Electrification readiness gaps mirror broader digital adoption trends."},
            ])
        else:
            resp.content[0].text = json.dumps({
                "finding": "Slovak firms lag Euro Area peers in electrification readiness.",
                "bullets": {
                    "qe1": [
                        "**SK readiness gap:** 35% of Slovak firms report low electrification readiness (code 1), vs 28% in the EA.",
                        "**EA more advanced:** 31% of EA firms report moderate readiness (code 2), slightly above Slovakia's 28%.",
                    ],
                },
            })
        resp.usage.input_tokens = 800
        resp.usage.output_tokens = 120
        return resp

    client.messages.create.side_effect = create_side_effect
    return client


@pytest.fixture
def mock_mistral_theme_with_pixtral_fallback():
    """Mistral client that also handles the Phase 3 Pixtral vision-spotlight fallback
    (used when no Anthropic client is passed to build_adhoc_spotlight)."""
    client = MagicMock()

    def complete_side_effect(*args, **kwargs):
        resp = MagicMock()
        messages = kwargs.get("messages", args[0] if args else [])
        last_user = next(
            (m["content"] for m in reversed(messages) if m["role"] == "user"), ""
        )
        if isinstance(last_user, list):
            # Phase 3 Pixtral fallback: user content is a list of text/image_url blocks
            resp.choices[0].message.content = json.dumps({
                "finding": "Slovak firms lag Euro Area peers in electrification readiness.",
                "bullets": [
                    "**SK readiness gap:** 35% of Slovak firms report low electrification readiness (code 1), vs 28% in the EA.",
                    "**EA more advanced:** 31% of EA firms report moderate readiness (code 2), slightly above Slovakia's 28%.",
                ],
                "chart_sub_items": ["a"],
            })
        elif "Classify each sub-item" in last_user or "interesting" in last_user.lower():
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


def test_resolve_item_labels_uses_annex_when_present():
    """When mart_safe__annex_items has rows for a module (e.g. QB1-style), those
    labels are used as-is, in preference to any fallback."""
    labels = _resolve_item_labels("qb1", ["a", "b"], {"a": "In your country", "b": "In Germany, France and Italy"})
    assert labels == {"a": "In your country", "b": "In Germany, France and Italy"}


def test_resolve_item_labels_pick_order_fallback_for_qa2_style_questions():
    """QA2's 'a'/'b' sub-items are a microdata pick-order artifact (first/second reason
    chosen in a pick-two question) with NO annex representation at all — the annex
    items mart will never have a row for these. Regression guard for the bug where
    chart panel titles fell back to the bare sub-item code ('a'/'b') instead of a
    readable label."""
    labels = _resolve_item_labels("qa2", ["a", "b"], {})
    assert labels == {"a": "First reason", "b": "Second reason"}


def test_resolve_item_labels_no_fallback_for_non_pick_order_sub_items():
    """A module with no annex items AND sub-items that aren't the {'a','b'} pick-order
    pattern should get no fabricated labels — falls through to the bare-code display
    in charts.py, which is the correct (if unhelpful) behaviour for genuinely unknown
    sub-items rather than incorrectly guessing pick-order semantics."""
    labels = _resolve_item_labels("qe1", ["a", "b", "c"], {})
    assert labels == {}


def test_module_sort_key_orders_questions_naturally():
    """Regression guard: adhoc questions must render in natural reading order
    (QA1, QA2, ..., QB1, QB2, ...) rather than whatever order mart_safe__adhoc_responses
    happened to return them in (sorted by firm count, not module id) — a prior bug
    rendered wave 37 as qa1, qb1, qb2, qa3, qa2, qa4 instead of qa1..qa4, qb1, qb2."""
    ids = ["qa1", "qb1", "qb2", "qa3", "qa2", "qa4"]
    assert sorted(ids, key=_module_sort_key) == ["qa1", "qa2", "qa3", "qa4", "qb1", "qb2"]


def test_module_sort_key_handles_double_digit_suffixes():
    """Plain string sort would put 'qa10' before 'qa2' — natural sort must not."""
    ids = ["qa10", "qa2", "qa1"]
    assert sorted(ids, key=_module_sort_key) == ["qa1", "qa2", "qa10"]


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

    # Every question in question_contexts must get its own bullets and a chart —
    # not just a top-1-3 selection (this fixture has a single question, qe1, but the
    # assertion generalises: selected_question_ids must equal every question present).
    assert result["selected_question_ids"] == ["qe1"]
    assert "qe1" in result["bullets_by_question"]
    assert len(result["bullets_by_question"]["qe1"]) > 0
    assert len(result["chart_pngs"]) == 1

    # Cross-cutting synthesis is populated separately from per-question bullets.
    assert isinstance(result["synthesis_bullets"], list)
    assert len(result["synthesis_bullets"]) > 0
    assert result["synthesis_bullets"] != result["bullets_by_question"]["qe1"]


def test_build_adhoc_spotlight_threads_shortened_chart_caption(
    mock_con, mock_mistral_theme, mock_anthropic_spotlight
):
    """Phase 2 must fetch a shortened chart caption via get_shortened_questions()
    (mirroring the main report's chart_question mechanism) and pass it through the
    chart theme as 'chart_question', so charts.py's _adhoc_chart_caption() renders
    the short paraphrase instead of the raw (possibly long/conditional) annex text."""
    from adhoc import _build_adhoc_chart as real_build_adhoc_chart

    cost = {"input_tokens": 0, "output_tokens": 0, "usd": 0.0, "calls": 0, "by_model": {}}
    theme = {
        "module_id": "qe1",
        "theme_label": "Electrification Efforts",
        "question_texts": {"a": "To what extent has your firm invested in electrification?"},
        # module_question_texts must be populated for question_text to resolve non-empty
        # in Phase 0 — needed so annex_question_texts has something to shorten.
        "module_question_texts": {
            "qe1": "To what extent has your firm invested in electrification of equipment or vehicles?",
        },
        "primary_question_text": "To what extent has your firm invested in electrification of equipment or vehicles?",
    }
    seen_themes = []

    def spy_build_adhoc_chart(df, chart_theme, **kwargs):
        seen_themes.append(chart_theme)
        return real_build_adhoc_chart(df, chart_theme, **kwargs)

    with patch("adhoc.SQL_DIR") as mock_sql_dir, \
         patch("adhoc._build_adhoc_chart", side_effect=spy_build_adhoc_chart):
        mock_sql_dir.__truediv__ = lambda self, other: mock_sql_dir
        mock_sql_dir.read_text.return_value = (
            "SELECT country_code, sub_item, response_raw, pct_wtd, n_firms, n_firms_wtd, n_total_wtd "
            "FROM {schema}.mart_safe__adhoc_responses "
            "WHERE wave_number = {wave_number} AND module_id = '{module_id}'"
        )
        build_adhoc_spotlight(
            theme, 39, mock_con, "main_safe",
            mistral_client=mock_mistral_theme,
            cost_tracker=cost,
            anthropic_client=mock_anthropic_spotlight,
        )

    assert len(seen_themes) == 1
    assert seen_themes[0]["chart_question"] == "How ready is your firm for electrification?"


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


def test_build_adhoc_spotlight_exposes_chart_rebuild_ingredients(
    mock_con, mock_mistral_theme, mock_anthropic_spotlight
):
    """build_adhoc_spotlight() must return _chart_rebuild_specs/_response_labels so the SK
    report can re-render charts with Slovak labels after translation."""
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
    specs = result["_chart_rebuild_specs"]
    assert len(specs) == len(result["chart_pngs"])
    for spec in specs:
        assert "chart_df" in spec and "is_continuous" in spec and "question_id" in spec

    sk_pngs = rebuild_adhoc_charts_sk(
        specs, sk_question_texts={"qe1": "Do akej miery vaša firma investovala do elektrifikácie?"},
        response_labels=result["_response_labels"], theme_label_sk="Elektrifikačné úsilie",
    )
    assert len(sk_pngs) == len(result["chart_pngs"])
    assert all(isinstance(p, bytes) for p in sk_pngs)


def test_build_adhoc_spotlight_result_is_cache_serializable(
    mock_con, mock_mistral_theme, mock_anthropic_spotlight
):
    """Regression test: run_adhoc_report.py's section-cache write does
    json.dumps(adhoc_section minus chart_png/chart_pngs/_chart_rebuild_specs/
    _response_labels). _chart_rebuild_specs carries raw chart_df DataFrames
    (see rebuild_adhoc_charts_sk) which are not JSON-serializable — a prior
    version of the cache-write filter forgot to exclude them, crashing CI
    with 'TypeError: Object of type DataFrame is not JSON serializable'."""
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

    # Mirror run_adhoc_report.py's cache-entry filter exactly.
    cache_entry = {
        k: v for k, v in result.items()
        if k not in ("chart_png", "chart_pngs", "_chart_rebuild_specs", "_response_labels")
    }
    cache_entry["wave_number"] = 39

    # Must not raise TypeError: Object of type DataFrame is not JSON serializable
    json.dumps(cache_entry, indent=2, ensure_ascii=False)


def test_rebuild_adhoc_charts_sk_falls_back_to_english_on_error():
    """If rebuilding a chart raises, keep the original English PNG rather than dropping it."""
    bad_spec = {
        "question_id": "qe1",
        "chart_df": None,  # will cause _build_adhoc_chart to raise internally when accessed
        "is_continuous": False,
        "effective_question_texts": {"a": "orig"},
        "question_text": "orig",
        "chart_png": b"ENGLISH_FALLBACK_PNG",
    }
    with patch("adhoc._build_adhoc_chart", side_effect=RuntimeError("boom")):
        result = rebuild_adhoc_charts_sk([bad_spec], sk_question_texts={}, response_labels={})
    assert result == [b"ENGLISH_FALLBACK_PNG"]


def test_build_adhoc_spotlight_pixtral_fallback(
    mock_con, mock_mistral_theme_with_pixtral_fallback
):
    """When no anthropic_client is passed, Phase 3 must fall back to Pixtral (vision-capable),
    not Mistral Small text-only — the fallback must still receive chart images and produce
    valid output, not silently degrade to a text-only prompt."""
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
            mistral_client=mock_mistral_theme_with_pixtral_fallback,
            cost_tracker=cost,
            anthropic_client=None,
        )

    assert result is not None
    assert result["section_id"] == "adhoc_spotlight"
    assert isinstance(result["finding"], str) and len(result["finding"]) > 0
    assert isinstance(result["bullets"], list)
    assert 2 <= len(result["bullets"]) <= 4

    # Confirm the fallback call actually used Pixtral, with image_url blocks present
    # (i.e. chart images were NOT dropped in favor of a text-only prompt).
    calls = mock_mistral_theme_with_pixtral_fallback.chat.complete.call_args_list
    spotlight_calls = [c for c in calls if c.kwargs.get("model") == "pixtral-12b-2409"]
    assert len(spotlight_calls) == 1
    user_msg = next(
        m["content"] for m in spotlight_calls[0].kwargs["messages"] if m["role"] == "user"
    )
    assert isinstance(user_msg, list)
    assert any(block.get("type") == "image_url" for block in user_msg)
