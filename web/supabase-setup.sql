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
CREATE TABLE IF NOT EXISTS public.allowed_emails (
  email TEXT PRIMARY KEY,
  lang TEXT NOT NULL DEFAULT 'en' CHECK (lang IN ('en', 'sk')),
  added_at TIMESTAMPTZ DEFAULT now()
);

-- 1b. If the table already exists from before the lang column was added, run this:
-- ALTER TABLE public.allowed_emails ADD COLUMN IF NOT EXISTS lang TEXT NOT NULL DEFAULT 'en' CHECK (lang IN ('en', 'sk'));

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

-- ============================================================
-- Chat query log — one row per chatbot question (success or failure). Used
-- for both audit (question, generated SQL, answer, cost) and the app-side
-- rate limiter (web/lib/chat/log.ts::checkRateLimit).
-- ============================================================

-- 8. Create the query_log table
CREATE TABLE IF NOT EXISTS public.query_log (
  id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  email         TEXT NOT NULL,
  question      TEXT NOT NULL,
  sql_generated TEXT,
  answer_text   TEXT,
  cost_usd      NUMERIC(10, 6) NOT NULL DEFAULT 0,
  error         TEXT,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS query_log_email_idx ON public.query_log (email);
CREATE INDEX IF NOT EXISTS query_log_created_at_idx ON public.query_log (created_at);

-- 9. Enable Row Level Security
ALTER TABLE public.query_log ENABLE ROW LEVEL SECURITY;

-- 10. Policies: authenticated user can read/insert only their own rows.
--     No UPDATE/DELETE policy — logs are append-only, matching subscriptions.
DROP POLICY IF EXISTS "self_insert" ON public.query_log;
CREATE POLICY "self_insert" ON public.query_log
  FOR INSERT
  WITH CHECK (email = auth.jwt() ->> 'email');

DROP POLICY IF EXISTS "self_read" ON public.query_log;
CREATE POLICY "self_read" ON public.query_log
  FOR SELECT
  USING (email = auth.jwt() ->> 'email');
