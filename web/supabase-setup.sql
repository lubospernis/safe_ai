-- ============================================================
-- STEP 1: Run this SQL in the Supabase SQL editor:
-- https://supabase.com/dashboard/project/mbdbfuigrrjgxxbzcwjs/sql
-- ============================================================
--
-- STEP 2: In the Supabase Dashboard → Authentication → URL Configuration:
--   Site URL:         https://web-lubospernis-projects.vercel.app
--   Redirect URLs:    https://web-lubospernis-projects.vercel.app/**
--                     http://localhost:3000/**
--
-- STEP 3: In the Supabase Dashboard → Authentication → Settings:
--   JWT Expiry:       7776000  (= 90 days in seconds)
-- ============================================================

-- 1. Create the allowed_emails table
-- Slovak is the default language for new signups; English remains available
-- as an explicit opt-in (set lang = 'en' for a specific user to switch).
CREATE TABLE IF NOT EXISTS public.allowed_emails (
  email TEXT PRIMARY KEY,
  lang TEXT NOT NULL DEFAULT 'sk' CHECK (lang IN ('en', 'sk')),
  added_at TIMESTAMPTZ DEFAULT now()
);

-- 1b. If the table already exists from before the lang column was added, run this:
-- ALTER TABLE public.allowed_emails ADD COLUMN IF NOT EXISTS lang TEXT NOT NULL DEFAULT 'sk' CHECK (lang IN ('en', 'sk'));

-- 1c. If the table already exists WITH the lang column defaulting to 'en', run this to
-- flip the default for future signups only (does NOT change any existing subscriber's
-- current lang — deliberately, so no one's existing preference is silently switched):
-- ALTER TABLE public.allowed_emails ALTER COLUMN lang SET DEFAULT 'sk';

-- 2. Enable Row Level Security
ALTER TABLE public.allowed_emails ENABLE ROW LEVEL SECURITY;

-- 3. Policies: authenticated user can SELECT and UPDATE their own row only
--    (UPDATE is needed so a user can change their own `lang` preference)
DROP POLICY IF EXISTS "self_read" ON public.allowed_emails;
CREATE POLICY "self_read" ON public.allowed_emails
  FOR SELECT
  USING (email = auth.jwt() ->> 'email');

DROP POLICY IF EXISTS "self_update_lang" ON public.allowed_emails;
CREATE POLICY "self_update_lang" ON public.allowed_emails
  FOR UPDATE
  USING (email = auth.jwt() ->> 'email')
  WITH CHECK (email = auth.jwt() ->> 'email');

-- 4. Insert the initial allowed email list (defaults to English; update lang manually as needed)
INSERT INTO public.allowed_emails (email) VALUES
  ('marek.licak@nbs.sk'),
  ('pavol.jurca@nbs.sk'),
  ('karol.zelenak@nbs.sk'),
  ('anna.kandricakova@nbs.sk'),
  ('jiri.prochazka@nbs.sk'),
  ('stefan.rychtarik@nbs.sk'),
  ('daniel.hajdiak@nbs.sk'),
  ('viktor.lintner@nbs.sk')
ON CONFLICT (email) DO NOTHING;

-- ============================================================
-- Subscriptions — one row per (email, newsletter_id). Replaces the old
-- GitHub-committed newsletter/subscribers.json file. lang is NOT stored here —
-- it lives solely on allowed_emails.lang and is looked up by email at send
-- time (reports/subscriptions_db.py), since every subscriber must already be
-- a logged-in (hence allowed_emails) user.
-- ============================================================

-- 5. Create the subscriptions table
CREATE TABLE IF NOT EXISTS public.subscriptions (
  id             BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  email          TEXT NOT NULL,
  newsletter_id  TEXT NOT NULL CHECK (newsletter_id IN ('safe-regular', 'safe-adhoc')),
  subscribed_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (email, newsletter_id)
);

CREATE INDEX IF NOT EXISTS subscriptions_newsletter_id_idx
  ON public.subscriptions (newsletter_id);

-- 6. Enable Row Level Security
ALTER TABLE public.subscriptions ENABLE ROW LEVEL SECURITY;

-- 7. Policies: authenticated user can read/insert/delete only their own rows.
--    No UPDATE policy — subscribe/unsubscribe is insert/delete, not update.
--    No policy is needed for the service role: service_role bypasses RLS
--    entirely by default, so reports/subscriptions_db.py (using the
--    service-role key) can read all rows unrestricted with zero extra policy.
DROP POLICY IF EXISTS "self_read" ON public.subscriptions;
CREATE POLICY "self_read" ON public.subscriptions
  FOR SELECT
  USING (email = auth.jwt() ->> 'email');

DROP POLICY IF EXISTS "self_insert" ON public.subscriptions;
CREATE POLICY "self_insert" ON public.subscriptions
  FOR INSERT
  WITH CHECK (email = auth.jwt() ->> 'email');

DROP POLICY IF EXISTS "self_delete" ON public.subscriptions;
CREATE POLICY "self_delete" ON public.subscriptions
  FOR DELETE
  USING (email = auth.jwt() ->> 'email');

-- 7b. Drop the old static newsletter_id allowlist now that newsletters are a
-- real table (see below) — validation moves to the app layer (subscribe/
-- unsubscribe routes check against public.newsletters directly). A FK isn't
-- used here so a newsletter row can be deleted/renamed without being blocked
-- by old subscription rows, matching how this schema already prefers RLS +
-- app-level checks over FKs elsewhere.
-- "subscriptions_newsletter_id_check" is Postgres's default {table}_{column}_check
-- naming for the inline CHECK above — if this DROP is a no-op (constraint name
-- differs), find the real name first:
--   SELECT conname FROM pg_constraint WHERE conrelid = 'public.subscriptions'::regclass AND contype = 'c';
ALTER TABLE public.subscriptions DROP CONSTRAINT IF EXISTS subscriptions_newsletter_id_check;

-- ============================================================
-- Newsletters — one row per tile shown on the home page. Replaces the
-- hardcoded array in web/lib/newsletters.ts so new tiles (including
-- link-only "custom project" cards) can be added without a code change.
-- ============================================================

-- 8. Create the newsletters table
CREATE TABLE IF NOT EXISTS public.newsletters (
  id              TEXT PRIMARY KEY,
  icon            TEXT NOT NULL,
  name_en         TEXT NOT NULL,
  name_sk         TEXT NOT NULL,
  description_en  TEXT NOT NULL,
  description_sk  TEXT NOT NULL,
  periodicity_en  TEXT NOT NULL,
  periodicity_sk  TEXT NOT NULL,
  -- Exactly one of these is expected to be set per row (app-level convention,
  -- not DB-enforced): link_url for a plain static-link "custom project"
  -- card; links_json_url for a SAFE-style card whose title link and
  -- last_updated/next_release badges come from a fetched {en,sk,
  -- last_updated,next_release} JSON file (see web/lib/latestLinks.ts).
  link_url        TEXT,
  links_json_url  TEXT,
  is_experimental BOOLEAN NOT NULL DEFAULT false,
  is_subscribable BOOLEAN NOT NULL DEFAULT true,
  sort_order      INTEGER NOT NULL DEFAULT 0,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 9. Enable Row Level Security
ALTER TABLE public.newsletters ENABLE ROW LEVEL SECURITY;

-- 10. Policy: any authenticated user can read (public reference data for
--     rendering tiles). No INSERT/UPDATE/DELETE policy — admin changes are
--     made directly via the Supabase SQL editor, same as allowed_emails.
DROP POLICY IF EXISTS "authenticated_read" ON public.newsletters;
CREATE POLICY "authenticated_read" ON public.newsletters
  FOR SELECT
  USING (auth.role() = 'authenticated');

-- 11. Seed the two existing SAFE newsletters (migrated from the old
-- hardcoded web/lib/newsletters.ts + web/lib/strings.ts STRINGS.newsletters).
INSERT INTO public.newsletters
  (id, icon, name_en, name_sk, description_en, description_sk, periodicity_en, periodicity_sk,
   links_json_url, is_experimental, is_subscribable, sort_order)
VALUES
  (
    'safe-regular', '🏦',
    'SAFE Slovakia', 'SAFE Slovensko',
    'Quarterly ECB Survey on the Access to Finance of Enterprises — Slovakia focus. Covers financing conditions, loan applications, business situation, and forward-looking expectations.',
    'Štvrťročný prieskum ECB o prístupe firiem k financovaniu — zameranie na Slovensko. Zahŕňa podmienky financovania, žiadosti o úvery, obchodnú situáciu a výhľad do budúcnosti.',
    'Quarterly', 'Štvrťročne',
    'https://raw.githubusercontent.com/lubospernis/safe_ai/main/reports/output/latest_links.json',
    false, true, 0
  ),
  (
    'safe-adhoc', '🔦',
    'SAFE Slovakia — Special Focus', 'SAFE Slovensko — Špeciálna téma',
    'Ad-hoc deep dive on a special survey topic (e.g. AI adoption, green transition), sent whenever the ECB adds a one-off module to the SAFE survey.',
    'Mimoriadny prehľad na špeciálnu tému prieskumu (napr. adopcia AI, zelená transformácia), zasielaný vždy, keď ECB doplní do prieskumu SAFE jednorazový modul.',
    'Ad hoc', 'Príležitostne',
    'https://raw.githubusercontent.com/lubospernis/safe_ai/main/reports/output/latest_adhoc_links.json',
    true, true, 1
  )
ON CONFLICT (id) DO NOTHING;
