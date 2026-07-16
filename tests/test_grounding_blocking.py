import pytest

from run_report import UngroundedNumberError as MainUngroundedError, _check_grounding_blocking
from run_adhoc_report import (
    UngroundedNumberError as AdhocUngroundedError,
    _check_grounding_blocking as _check_adhoc_grounding_blocking,
)


def test_main_report_passes_with_no_warnings():
    rendered = [
        {"section_id": "bank_loan_terms", "grounding_warnings": []},
        {"section_id": "business_situation", "grounding_warnings": []},
    ]
    _check_grounding_blocking(rendered)  # must not raise


def test_main_report_raises_on_any_warning():
    rendered = [
        {"section_id": "bank_loan_terms", "grounding_warnings": []},
        {"section_id": "business_situation", "grounding_warnings": ["'99.7' not found in source data"]},
    ]
    with pytest.raises(MainUngroundedError, match="business_situation"):
        _check_grounding_blocking(rendered)


def test_main_report_error_message_includes_section_and_count():
    rendered = [{"section_id": "outlook", "grounding_warnings": ["'1' not found", "'2' not found"]}]
    with pytest.raises(MainUngroundedError, match=r"2 ungrounded number"):
        _check_grounding_blocking(rendered)


def test_adhoc_passes_with_no_warnings():
    _check_adhoc_grounding_blocking({"grounding_warnings": []})  # must not raise


def test_adhoc_passes_with_missing_key():
    _check_adhoc_grounding_blocking({})  # must not raise — defaults to []


def test_adhoc_raises_on_warning():
    with pytest.raises(AdhocUngroundedError, match="13.8"):
        _check_adhoc_grounding_blocking({"grounding_warnings": ["[qa1] '13.8' not found in source data"]})
