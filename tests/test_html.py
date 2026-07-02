from html_builder import _md_to_html, _clean_question_text, build_toc


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
