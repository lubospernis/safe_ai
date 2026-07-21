import re
from unittest.mock import MagicMock, patch

from html_builder import (
    FEEDBACK_EMAIL, _fetch_painting_inner_html, _format_period_suffix, _load_sql_snippet,
    _md_to_html, _clean_question_text, _sql_tooltip_attr, build_annex_html, build_html,
    build_toc, render_section,
)


def test_md_to_html_bold():
    assert _md_to_html("**Foo:** bar") == "<strong>Foo:</strong> bar"


def test_md_to_html_multiple():
    result = _md_to_html("**A** and **B**")
    assert "<strong>A</strong>" in result
    assert "<strong>B</strong>" in result


def test_md_to_html_plain_passthrough():
    text = "plain text with no markdown"
    assert _md_to_html(text) == text


def test_md_to_html_empty():
    assert _md_to_html("") == ""


def test_md_to_html_no_partial_match():
    # Single asterisks should not be converted
    result = _md_to_html("*not bold*")
    assert "<strong>" not in result


def test_clean_question_text_strips_prefix():
    assert _clean_question_text("Q10. What were the terms?") == "What were the terms?"


def test_clean_question_text_slash_prefix():
    assert _clean_question_text("Q7A/Q7A_g1. Did you apply?") == "Did you apply?"


def test_clean_question_text_no_prefix():
    text = "No prefix here"
    assert _clean_question_text(text) == text


def test_clean_question_text_strips_whitespace():
    # Leading/trailing whitespace on the whole string is stripped
    assert _clean_question_text("Q2.   Some text  ") == "Some text"


def test_build_toc_empty():
    assert build_toc([]) == ""


def test_build_toc_contains_section_id():
    sections = [{
        "section_id": "bank_loan_terms",
        "finding": "Rates tightened",
        "group": "Financing Conditions",
    }]
    toc = build_toc(sections)
    assert "bank_loan_terms" in toc
    assert "Rates tightened" in toc


def test_build_toc_adhoc_spotlight_appended_alongside_regular_sections():
    sections = [
        {"section_id": "bank_loan_terms", "finding": "Rates tightened", "group": "Financing Conditions"},
        {"section_id": "adhoc_spotlight", "finding": "AI adoption", "theme_label": "AI Adoption", "group": "Other"},
    ]
    toc = build_toc(sections)
    assert "adhoc_spotlight" in toc
    assert "AI Adoption" in toc


def test_build_toc_standalone_adhoc_report_still_renders_toc():
    """A standalone adhoc-only report (no regular sections) still gets a TOC now
    that every adhoc question is its own <section> — falls back to a single
    "Special Focus" link when question_descriptions isn't provided."""
    sections = [
        {"section_id": "adhoc_spotlight", "finding": "AI adoption", "theme_label": "AI Adoption", "group": "Other"},
    ]
    toc = build_toc(sections)
    assert "adhoc_spotlight" in toc
    assert "AI Adoption" in toc


def test_build_toc_lists_every_adhoc_question_as_its_own_entry():
    """Since every adhoc question renders as its own <section id="{qid}">, the TOC
    must link to each one individually (nested under the theme), not just to the
    spotlight as a whole."""
    sections = [{
        "section_id": "adhoc_spotlight",
        "finding": "AI adoption",
        "theme_label": "AI Adoption",
        "group": "Other",
        "question_descriptions": [
            {"question_id": "qa1", "question_text": "How would you assess the use of AI technologies?"},
            {"question_id": "qb1", "question_text": "- What percentage of similar firms invested in AI?"},
        ],
    }]
    toc = build_toc(sections)
    assert '<a href="#qa1">QA1' in toc
    assert '<a href="#qb1">QB1' in toc
    assert "How would you assess the use of AI technologies?" in toc
    # Leading bullet/dash artefacts must be stripped from the TOC label.
    assert "- What percentage" not in toc
    assert "What percentage of similar firms invested in AI?" in toc


# ── _fetch_painting_inner_html retry behaviour ──────────────────────────────

def _fake_response():
    resp = MagicMock()
    resp.raise_for_status.return_value = None
    resp.content = b"fake-image-bytes"
    resp.headers = {"Content-Type": "image/jpeg"}
    return resp


def test_fetch_painting_retries_and_recovers_from_transient_failure():
    with patch("requests.get", side_effect=[ConnectionError("transient"), _fake_response()]) as mock_get, \
         patch("time.sleep"):
        html = _fetch_painting_inner_html()
    assert mock_get.call_count == 2
    assert "<img" in html
    assert "data:image/jpeg;base64," in html


def test_fetch_painting_gives_up_after_max_attempts():
    with patch("requests.get", side_effect=ConnectionError("still down")) as mock_get, \
         patch("time.sleep"):
        html = _fetch_painting_inner_html(max_attempts=3)
    assert mock_get.call_count == 3
    assert html == ""


def test_fetch_painting_succeeds_first_try_no_retry():
    with patch("requests.get", side_effect=[_fake_response()]) as mock_get:
        html = _fetch_painting_inner_html()
    assert mock_get.call_count == 1
    assert "<img" in html


def _no_image_placeholder_response():
    resp = MagicMock()
    resp.raise_for_status.return_value = None
    resp.content = b"fake-no-image-placeholder-bytes"
    resp.headers = {
        "Content-Type": "image/jpeg",
        "Content-Disposition": 'inline; filename="no-image-im.jpg"',
    }
    return resp


def test_fetch_painting_skips_no_image_placeholder_without_retrying():
    """webumenia.sk sometimes returns HTTP 200 with its own 'no image' placeholder —
    this must be detected and skipped immediately, not retried (it's not transient)."""
    with patch("requests.get", side_effect=[_no_image_placeholder_response()]) as mock_get, \
         patch("time.sleep") as mock_sleep:
        html = _fetch_painting_inner_html()
    assert mock_get.call_count == 1
    assert mock_sleep.call_count == 0
    assert html == ""


def _mock_annex_con():
    """Mock DuckDB connection returning one annex row for Q5 with English question text."""
    con = MagicMock()
    cols = ["group", "element", "question_item", "notes_extra", "sample", "notes", "safe_2024q1"]

    def execute_side_effect(sql, *args, **kwargs):
        result = MagicMock()
        if "information_schema.columns" in sql:
            result.fetchall.return_value = [(c,) for c in cols]
        else:
            result.fetchall.return_value = [
                ("Q5", "ECB module", "What is the change in your financing need?"),
            ]
        return result

    con.execute.side_effect = execute_side_effect
    return con


def test_build_annex_html_uses_english_text_by_default():
    con = _mock_annex_con()
    html = build_annex_html(con=con)
    assert "What is the change in your financing need?" in html


def test_build_annex_html_question_texts_override_replaces_text():
    con = _mock_annex_con()
    html = build_annex_html(con=con, question_texts_override={"q5": "Aká je zmena vo vašej potrebe financovania?"})
    assert "Aká je zmena vo vašej potrebe financovania?" in html
    assert "What is the change in your financing need?" not in html


def test_build_annex_html_override_is_case_insensitive_and_partial():
    """Only overridden question IDs should be replaced; others keep the English fallback text."""
    con = _mock_annex_con()
    html = build_annex_html(con=con, question_texts_override={"q5": "Preložený text"})
    assert "Preložený text" in html


def _adhoc_section_stub():
    return {
        "section_id": "adhoc_spotlight",
        "finding": "Slovak firms trail the EA in AI adoption depth.",
        "title": "Special Focus",
        "theme_label": "Artificial intelligence technologies",
        "group": "Other",
        "selected_question_ids": ["qa1", "qa2"],
        "bullets_by_question": {
            "qa1": ["46.5% of Slovak firms report only pilot AI use."],
            "qa2": ["Improving non-core processes is the top reason cited."],
        },
        "question_descriptions": [
            {"question_id": "qa1", "question_text": "How would you assess the use of AI technologies?",
             "interest_score": 4, "description": "desc", "key_finding": "kf"},
            {"question_id": "qa2", "question_text": "Please indicate the two main reasons.",
             "interest_score": 3, "description": "desc", "key_finding": "kf"},
        ],
        "chart_pngs": [b"fake-png-bytes-1", b"fake-png-bytes-2"],
    }


# ── _format_period_suffix / report title period label ───────────────────────

def test_format_period_suffix_formats_quarter_and_year():
    assert _format_period_suffix("2026Q1") == " (Q1 2026)"


def test_format_period_suffix_empty_on_none():
    assert _format_period_suffix(None) == ""


def test_format_period_suffix_empty_on_malformed_label():
    assert _format_period_suffix("not-a-period") == ""


def test_build_html_title_includes_period_label_when_given():
    html = build_html(
        rendered_sections=[], annex_html="", exec_bullets=[], toc_html="",
        latest_wave=38, period_label="2026Q1",
    )
    assert "Wave 38 (Q1 2026)" in html


def test_build_html_title_falls_back_to_wave_only_without_period_label():
    html = build_html(
        rendered_sections=[], annex_html="", exec_bullets=[], toc_html="",
        latest_wave=38,
    )
    assert "Wave 38 ·" in html
    assert "(Q1" not in html


# ── lang-switch link (SK is now the Pages root, en.html the secondary) ──────

def test_build_html_en_report_links_to_sk_at_index():
    html = build_html(rendered_sections=[], annex_html="", exec_bullets=[], toc_html="")
    assert '<a class="lang-switch" href="index.html">🇸🇰 SK</a>' in html


def test_build_html_sk_report_links_to_en_at_en_html():
    from html_builder import _SK_UI
    html = build_html(rendered_sections=[], annex_html="", exec_bullets=[], toc_html="", ui=_SK_UI)
    assert '<a class="lang-switch" href="en.html">🇬🇧 EN</a>' in html


def test_build_html_adhoc_per_question_chart_has_no_inline_width_style():
    """Regression guard: per-question adhoc chart-img tags must rely on the shared
    .chart-img CSS class for sizing, not an inline style= override (the root cause of
    adhoc charts rendering oversized relative to the main report's charts)."""
    html = build_html(
        rendered_sections=[_adhoc_section_stub()],
        annex_html="",
        exec_bullets=[],
        toc_html="",
    )
    chart_imgs = re.findall(r'<img class="chart-img[^"]*"[^>]*>', html)
    assert len(chart_imgs) == 2
    for tag in chart_imgs:
        assert "style=" not in tag
        assert "chart-img--adhoc" in tag


# ── SQL-provenance-on-hover ──────────────────────────────────────────────────

def test_load_sql_snippet_reads_real_sql_file():
    text = _load_sql_snippet("bank_loan_terms.sql")
    assert text  # a real section .sql file must yield non-empty content
    assert "select" in text.lower()


def test_load_sql_snippet_missing_file_returns_empty_string():
    assert _load_sql_snippet("does_not_exist.sql") == ""


def test_sql_tooltip_attr_contains_filename_column_and_sql_text():
    attr = _sql_tooltip_attr("bank_loan_terms.sql", "net_balance_wtd")
    assert attr.startswith(' title="')
    assert "bank_loan_terms.sql" in attr
    assert "net_balance_wtd" in attr


def test_sql_tooltip_attr_empty_without_sql_file():
    assert _sql_tooltip_attr(None, "net_balance_wtd") == ""
    assert _sql_tooltip_attr("", "net_balance_wtd") == ""


def test_sql_tooltip_attr_empty_for_missing_file():
    assert _sql_tooltip_attr("does_not_exist.sql", "net_balance_wtd") == ""


def test_sql_tooltip_attr_escapes_html_special_characters():
    # SQL routinely contains quotes and comparison operators (e.g. WHERE x = 'a'
    # AND y < 10) — a raw quote must not break out of the title="..." attribute.
    with patch("html_builder._load_sql_snippet", return_value="WHERE x = 'a' AND y < 10"):
        attr = _sql_tooltip_attr("fake.sql", "net_balance_wtd")
    assert attr.startswith(' title="') and attr.endswith('"')
    assert attr.count('"') == 2  # only the two attribute-delimiting quotes remain unescaped
    assert "&lt;" in attr  # the literal "<" was escaped, not passed through raw


def test_render_section_includes_sql_icon_when_tooltip_given():
    html = render_section(
        section_id="sec1", headline="Finding", subtitle="Subtitle",
        bullets=["A bullet"], sql_tooltip_attr=' title="SQL source: x.sql"',
    )
    assert 'class="sql-info-icon"' in html
    assert 'title="SQL source: x.sql"' in html


def test_render_section_omits_sql_icon_without_tooltip():
    html = render_section(
        section_id="sec1", headline="Finding", subtitle="Subtitle", bullets=["A bullet"],
    )
    assert "sql-info-icon" not in html


def test_build_html_regular_section_carries_sql_tooltip():
    section = {
        "section_id": "bank_loan_terms",
        "title": "Changes in Terms and Conditions of Bank Financing (Q10)",
        "group": "Financing Conditions",
        "finding": "Rates tightened",
        "bullets": ["A net 43.8% of firms reported tighter conditions"],
        "sql_file": "bank_loan_terms.sql",
        "value_col": "net_balance_wtd",
    }
    html = build_html(
        rendered_sections=[section], annex_html="", exec_bullets=[], toc_html="",
    )
    assert 'class="sql-info-icon"' in html
    assert "bank_loan_terms.sql" in html


def test_build_html_regular_section_without_sql_file_has_no_icon():
    section = {
        "section_id": "sec1", "title": "T", "group": "Financing Conditions",
        "finding": "F", "bullets": ["B"],
    }
    html = build_html(
        rendered_sections=[section], annex_html="", exec_bullets=[], toc_html="",
    )
    assert '<span class="sql-info-icon"' not in html


# ── Report-level feedback link ───────────────────────────────────────────────

def test_build_html_en_feedback_link_uses_english_label_and_mailto():
    html = build_html(rendered_sections=[], annex_html="", exec_bullets=[], toc_html="", latest_wave=38)
    assert f'href="mailto:{FEEDBACK_EMAIL}?subject=' in html
    assert "Was this useful? Let us know" in html


def test_build_html_sk_feedback_link_uses_slovak_label():
    from html_builder import _SK_UI
    html = build_html(rendered_sections=[], annex_html="", exec_bullets=[], toc_html="", ui=_SK_UI, latest_wave=38)
    assert f'href="mailto:{FEEDBACK_EMAIL}?subject=' in html
    assert "Bola táto správa užitočná?" in html


def test_build_html_feedback_link_subject_mentions_wave():
    html = build_html(rendered_sections=[], annex_html="", exec_bullets=[], toc_html="", latest_wave=39)
    # Subject is URL-encoded (spaces -> %20) — check the encoded wave marker survives.
    assert "Wave%2039" in html or "Wave+39" in html
