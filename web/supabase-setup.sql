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
