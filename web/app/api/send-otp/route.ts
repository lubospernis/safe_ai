import { createServerSideClient } from "@/lib/supabase-server";
import { NextResponse } from "next/server";

const ALLOWED_DOMAIN = "nbs.sk";

export async function POST(request: Request) {
  const { email } = await request.json();

  if (typeof email !== "string" || !email.toLowerCase().endsWith(`@${ALLOWED_DOMAIN}`)) {
    return NextResponse.json(
      { error: `Only @${ALLOWED_DOMAIN} email addresses are allowed.` },
      { status: 403 },
    );
  }

  const supabase = await createServerSideClient();
  const { error } = await supabase.auth.signInWithOtp({
    email: email.trim().toLowerCase(),
    options: {
      shouldCreateUser: true,
      emailRedirectTo: `${process.env.NEXT_PUBLIC_SITE_URL}/auth/callback`,
    },
  });

  if (error) {
    return NextResponse.json({ error: error.message }, { status: 400 });
  }

  return NextResponse.json({ ok: true });
}
