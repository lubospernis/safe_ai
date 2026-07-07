import re
from unittest.mock import MagicMock, patch

from html_builder import (
    _fetch_painting_inner_html, _md_to_html, _clean_question_text, build_annex_html, build_html, build_toc,
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


def test_build_toc_adhoc_spotlight_appended():
    sections = [
        {"section_id": "adhoc_spotlight", "finding": "AI adoption", "theme_label": "AI Adoption", "group": "Other"},
    ]
    toc = build_toc(sections)
    assert "adhoc_spotlight" in toc
    assert "AI Adoption" in toc


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
