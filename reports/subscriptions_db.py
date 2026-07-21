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
from urllib.parse import urlsplit, urlunsplit

from supabase import create_client, Client

NEWSLETTER_REGULAR = "safe-regular"
NEWSLETTER_ADHOC = "safe-adhoc"


def _get_client() -> Client:
    url = os.environ["SUPABASE_URL"]
    key = os.environ["SUPABASE_SECRET_KEY"]
    return create_client(_normalize_supabase_url(url), key)


def _normalize_supabase_url(url: str) -> str:
    """Strip any path/query/fragment, keeping only scheme+host.

    supabase-py's Client.__init__ builds every sub-URL (rest, auth, storage,
    realtime) via `URL(supabase_url).joinpath("rest", "v1")` etc. — it assumes
    SUPABASE_URL is the bare project origin. If the env var instead already
    contains a path (e.g. someone pasted the REST endpoint,
    "https://xxx.supabase.co/rest/v1", instead of the dashboard's project URL),
    joinpath appends onto it instead of replacing it, producing a duplicated
    path like "/rest/v1/rest/v1/<table>". PostgREST then can't resolve that to
    any known resource and returns PGRST125 "Invalid path specified in request
    URL" — a real production failure (2026-07-21): the newsletter send workflow
    aborted with this exact error on its first real run, since the Supabase
    subscriber lookup added to send_newsletter.py had never previously fired
    (every run before it had nothing new to send, so the bug went unnoticed).
    """
    parts = urlsplit(url)
    return urlunsplit((parts.scheme, parts.netloc, "", "", ""))


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
