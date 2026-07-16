import { createServerSideClient } from "@/lib/supabase-server";
import { unsubscribe } from "@/lib/subscriptions";
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
  // No is_subscribable filter here — a user must always be able to
  // unsubscribe from something they're subscribed to, even if the tile is
  // later marked non-subscribable.
  const { data: newsletterRow } = await supabase
    .from("newsletters")
    .select("id")
    .eq("id", newsletterId)
    .maybeSingle();
  if (!newsletterRow) {
    return NextResponse.json({ error: "Invalid newsletter" }, { status: 400 });
  }

  try {
    await unsubscribe(supabase, user.email, newsletterId);
    return NextResponse.json({ ok: true });
  } catch (err) {
    console.error("unsubscribe error:", err);
    return NextResponse.json(
      { error: "Failed to unsubscribe" },
      { status: 500 },
    );
  }
}
