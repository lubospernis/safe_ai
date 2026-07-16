"""Supabase-backed subscriber lookups for the newsletter send scripts.

Replaces the old GitHub-committed newsletter/subscribers.json file (single
flat list, no per-newsletter distinction). Subscriptions now live in the
Supabase `public.subscriptions` table (one row per (email, newsletter_id) —
see web/supabase-setup.sql), keyed on the same newsletter_id values the web
app uses ("safe-regular", "safe-adhoc").

lang is NOT stored on subscriptions — it lives solely on public.allowed_emails
(the same table that gates login) and is looked up here by email, since every
subscriber must already be a logged-in allowed_emails user.

Required environment variables:
  SUPABASE_URL          — Supabase project URL
  SUPABASE_SECRET_KEY   — a server-side key that bypasses RLS (Settings → API
                           Keys → Secret keys, sb_secret_...). Do not use the
                           anon/publishable key here, it would see no rows
                           under RLS.
"""

import os

from supabase import create_client, Client

NEWSLETTER_REGULAR = "safe-regular"
NEWSLETTER_ADHOC = "safe-adhoc"


def _get_client() -> Client:
    url = os.environ["SUPABASE_URL"]
    key = os.environ["SUPABASE_SECRET_KEY"]
    return create_client(url, key)


def get_subscribers(newsletter_id: str) -> list[dict]:
    """Return [{"email": ..., "lang": ...}] for everyone subscribed to newsletter_id.

    lang defaults to "sk" for any subscriber missing from allowed_emails
    (should not happen in practice, but the subscriber list and the
    allowed-login list are not formally guaranteed to be in lockstep).
    """
    client = _get_client()

    subs = (
        client.table("subscriptions")
        .select("email")
        .eq("newsletter_id", newsletter_id)
        .execute()
    ).data
    emails = [s["email"] for s in subs]
    if not emails:
        return []

    langs = (
        client.table("allowed_emails")
        .select("email, lang")
        .in_("email", emails)
        .execute()
    ).data
    lang_by_email = {r["email"]: r.get("lang", "sk") for r in langs}

    return [{"email": e, "lang": lang_by_email.get(e, "sk")} for e in emails]
