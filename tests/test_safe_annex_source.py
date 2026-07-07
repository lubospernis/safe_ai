from safe_microdata import _safe_annex, _safe_annex_long

_COL_NAMES = ["element", "question_item", "answer", "sample", "notes", "safe_2025q4", "safe_2025q3"]


def _row(element, question_item, answer="", sample="ad-hoc ECB", notes="", q4="", q3=""):
    return [element, question_item, answer, sample, notes, q4, q3]


def test_safe_annex_long_unpivots_only_non_blank_wave_cells():
    rows = [
        _row("question", "qa1_2025q4", q4="How would you assess the use of AI technologies?"),
        _row("answer", "qa1_2025q4", answer="1", q4="- Not currently in use"),
        _row("item", "qb1_2025q4", q4="a) In your country"),
    ]
    long_rows = list(_safe_annex_long(_COL_NAMES, rows))

    assert len(long_rows) == 3
    assert all(r["wave_label"] == "safe_2025q4" for r in long_rows)
    q_row = next(r for r in long_rows if r["element"] == "question")
    assert q_row["text"] == "How would you assess the use of AI technologies?"
    a_row = next(r for r in long_rows if r["element"] == "answer")
    assert a_row["answer"] == "1"
    assert a_row["text"] == "- Not currently in use"


def test_safe_annex_long_emits_one_row_per_non_blank_wave_column():
    """A question worded differently across two waves should yield two long rows,
    one per wave_label, preserving both historical texts."""
    rows = [_row("question", "qa1_2025q4", q4="Current wording", q3="Older wording")]
    long_rows = list(_safe_annex_long(_COL_NAMES, rows))

    assert len(long_rows) == 2
    by_wave = {r["wave_label"]: r["text"] for r in long_rows}
    assert by_wave == {"safe_2025q4": "Current wording", "safe_2025q3": "Older wording"}


def test_safe_annex_long_skips_blank_cells():
    rows = [_row("question", "qa1_2025q4", q4="", q3="")]
    assert list(_safe_annex_long(_COL_NAMES, rows)) == []


def test_safe_annex_wide_preserves_original_row_shape():
    rows = [_row("question", "qa1_2025q4", q4="Current wording")]
    wide_rows = list(_safe_annex(_COL_NAMES, rows))

    assert wide_rows == [{
        "element": "question",
        "question_item": "qa1_2025q4",
        "answer": "",
        "sample": "ad-hoc ECB",
        "notes": "",
        "safe_2025q4": "Current wording",
        "safe_2025q3": "",
    }]
