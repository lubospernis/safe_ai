import { createServerSideClient } from "@/lib/supabase-server";
import { subscribe } from "@/lib/subscriptions";
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

  const { newsletterId } = await request.json();
  const { data: newsletterRow } = await supabase
    .from("newsletters")
    .select("id")
    .eq("id", newsletterId)
    .eq("is_subscribable", true)
    .maybeSingle();
  if (!newsletterRow) {
    return NextResponse.json({ error: "Invalid newsletter" }, { status: 400 });
  }

  try {
    await subscribe(supabase, user.email, newsletterId);
    return NextResponse.json({ ok: true });
  } catch (err) {
    console.error("subscribe error:", err);
    return NextResponse.json({ error: "Failed to subscribe" }, { status: 500 });
  }
}
