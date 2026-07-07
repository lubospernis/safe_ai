from unittest.mock import MagicMock, patch

from fetch_release_calendar import fetch_safe_release

# Trimmed real fragment of the ECB stats calendar page (dt/dd pairs), including
# a neighbouring non-SAFE dataset to make sure the SAFE-row filter actually filters.
_CALENDAR_HTML = """
<html><body><dl>
<dt>
20/07/2026 10:00 CET
</dt>
<dd>
Euro area investment fund statistics  (Dataset: IVF)<br>
Reference period: May-2026<br>
</dd>
<dt>
20/07/2026 10:00 CET
</dt>
<dd>
Survey on the access to finance of enterprises (Dataset: SAFE)<br>
Reference period: Q2 2026<br>
Includes press release.<br>
</dd>
<dt>
21/07/2026 10:00 CET
</dt>
<dd>
Euro area Bank Lending Survey (Dataset: BLS)<br>
Reference period: Q2 2026 / Q3 2026<br>
Includes press release.<br>
</dd>
</dl></body></html>
"""


def _mock_response(html: str):
    resp = MagicMock()
    resp.raise_for_status.return_value = None
    resp.text = html
    return resp


def test_fetch_safe_release_parses_date_and_reference_period():
    with patch("requests.get", return_value=_mock_response(_CALENDAR_HTML)):
        result = fetch_safe_release()
    assert result is not None
    assert result["next_release_date"].isoformat() == "2026-07-20"
    assert result["next_release_time_cet"] == "10:00 CET"
    assert result["reference_period"] == "Q2 2026"


def test_fetch_safe_release_returns_none_when_no_safe_row():
    html_without_safe = _CALENDAR_HTML.replace("Dataset: SAFE)", "Dataset: NOT_SAFE)")
    with patch("requests.get", return_value=_mock_response(html_without_safe)):
        result = fetch_safe_release()
    assert result is None


def test_fetch_safe_release_returns_none_on_request_failure():
    with patch("requests.get", side_effect=ConnectionError("network down")):
        result = fetch_safe_release()
    assert result is None
