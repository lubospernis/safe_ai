from unittest.mock import MagicMock, patch

from gap_agent import _get_with_retry


def _mock_response(text: str = "ok"):
    resp = MagicMock()
    resp.raise_for_status.return_value = None
    resp.text = text
    return resp


def test_get_with_retry_succeeds_first_try():
    with patch("requests.get", return_value=_mock_response("hello")) as mock_get, \
         patch("time.sleep") as mock_sleep:
        resp = _get_with_retry("https://example.com")
    assert resp.text == "hello"
    assert mock_get.call_count == 1
    mock_sleep.assert_not_called()


def test_get_with_retry_retries_then_succeeds():
    import requests
    with patch(
        "requests.get",
        side_effect=[requests.RequestException("transient"), _mock_response("hello")],
    ) as mock_get, patch("time.sleep") as mock_sleep:
        resp = _get_with_retry("https://example.com")
    assert resp.text == "hello"
    assert mock_get.call_count == 2
    assert mock_sleep.call_count == 1


def test_get_with_retry_raises_after_exhausting_attempts():
    import requests
    import pytest
    with patch("requests.get", side_effect=requests.RequestException("still down")), \
         patch("time.sleep"):
        with pytest.raises(requests.RequestException):
            _get_with_retry("https://example.com", attempts=3)
