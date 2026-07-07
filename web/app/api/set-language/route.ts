import { createServerSideClient } from "@/lib/supabase-server";
import { updateSubscriberLang } from "@/lib/github";
import { NextResponse } from "next/server";

export async function POST(request: Request) {
  const supabase = await createServerSideClient();
  const {
    data: { user },
    error,
  } = await supabase.auth.getUser();

  if (error || !user?.email) {
    return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
  }

  const { lang } = await request.json();
  if (lang !== "en" && lang !== "sk") {
    return NextResponse.json({ error: "Invalid language" }, { status: 400 });
  }

  const { error: updateError } = await supabase
    .from("allowed_emails")
    .update({ lang })
    .eq("email", user.email);

  if (updateError) {
    console.error("set-language error:", updateError);
    return NextResponse.json({ error: "Failed to update language" }, { status: 500 });
  }

  try {
    // Keep the GitHub subscribers file in sync if this user is already subscribed
    await updateSubscriberLang(user.email, lang);
  } catch (err) {
    console.error("set-language: subscriber sync failed:", err);
    // Non-fatal — Supabase is the source of truth for future subscribes
  }

  return NextResponse.json({ ok: true });
}
