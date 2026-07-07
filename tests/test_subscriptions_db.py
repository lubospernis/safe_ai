import os
from unittest.mock import MagicMock, patch

import pytest

from subscriptions_db import NEWSLETTER_ADHOC, NEWSLETTER_REGULAR, get_subscribers


def _mock_table_chain(return_data):
    """Build a MagicMock that supports .select(...).eq(...).execute().data
    and .select(...).in_(...).execute().data, both returning return_data."""
    table = MagicMock()
    query = MagicMock()
    table.select.return_value = query
    query.eq.return_value = query
    query.in_.return_value = query
    query.execute.return_value = MagicMock(data=return_data)
    return table


@pytest.fixture(autouse=True)
def _supabase_env(monkeypatch):
    monkeypatch.setenv("SUPABASE_URL", "https://example.supabase.co")
    monkeypatch.setenv("SUPABASE_SECRET_KEY", "test-secret-key")


def test_get_subscribers_filters_by_newsletter_id():
    client = MagicMock()

    def table_side_effect(name):
        if name == "subscriptions":
            return _mock_table_chain([{"email": "a@example.com"}, {"email": "b@example.com"}])
        if name == "allowed_emails":
            return _mock_table_chain([
                {"email": "a@example.com", "lang": "sk"},
                {"email": "b@example.com", "lang": "en"},
            ])
        raise AssertionError(f"unexpected table: {name}")

    client.table.side_effect = table_side_effect

    with patch("subscriptions_db.create_client", return_value=client) as mock_create:
        result = get_subscribers(NEWSLETTER_REGULAR)

    mock_create.assert_called_once_with("https://example.supabase.co", "test-secret-key")
    assert result == [
        {"email": "a@example.com", "lang": "sk"},
        {"email": "b@example.com", "lang": "en"},
    ]


def test_get_subscribers_returns_empty_list_when_no_subscribers():
    client = MagicMock()
    client.table.return_value = _mock_table_chain([])

    with patch("subscriptions_db.create_client", return_value=client):
        result = get_subscribers(NEWSLETTER_ADHOC)

    assert result == []


def test_get_subscribers_defaults_lang_to_en_when_missing_from_allowed_emails():
    """A subscriber not present in allowed_emails must not crash the lookup —
    defaults to 'en' rather than being dropped or raising."""
    client = MagicMock()

    def table_side_effect(name):
        if name == "subscriptions":
            return _mock_table_chain([{"email": "orphan@example.com"}])
        if name == "allowed_emails":
            return _mock_table_chain([])  # not in allowed_emails at all
        raise AssertionError(f"unexpected table: {name}")

    client.table.side_effect = table_side_effect

    with patch("subscriptions_db.create_client", return_value=client):
        result = get_subscribers(NEWSLETTER_REGULAR)

    assert result == [{"email": "orphan@example.com", "lang": "en"}]


def test_get_subscribers_queries_correct_newsletter_id():
    client = MagicMock()
    subs_table = _mock_table_chain([])
    client.table.return_value = subs_table

    with patch("subscriptions_db.create_client", return_value=client):
        get_subscribers("safe-adhoc")

    subs_table.select.assert_called_once_with("email")
    subs_table.select.return_value.eq.assert_called_once_with("newsletter_id", "safe-adhoc")
