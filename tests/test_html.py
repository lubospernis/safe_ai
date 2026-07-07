from unittest.mock import MagicMock, patch

from html_builder import (
    _fetch_painting_inner_html, _md_to_html, _clean_question_text, build_annex_html, build_toc,
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
