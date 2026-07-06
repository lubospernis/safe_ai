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
  added_at TIMESTAMPTZ DEFAULT now()
);

-- 2. Enable Row Level Security
ALTER TABLE public.allowed_emails ENABLE ROW LEVEL SECURITY;

-- 3. Policy: authenticated user can SELECT their own row only
DROP POLICY IF EXISTS "self_read" ON public.allowed_emails;
CREATE POLICY "self_read" ON public.allowed_emails
  FOR SELECT
  USING (email = auth.jwt() ->> 'email');

-- 4. Insert the initial allowed email list
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
