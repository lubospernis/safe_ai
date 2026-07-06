import { createServerSideClient } from "@/lib/supabase-server";
import { addSubscriber } from "@/lib/github";
import { NextResponse } from "next/server";

export async function POST() {
  const supabase = await createServerSideClient();
  const {
    data: { user },
    error,
  } = await supabase.auth.getUser();

  if (error || !user?.email) {
    return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
  }

  try {
    await addSubscriber(user.email);
    return NextResponse.json({ ok: true });
  } catch (err) {
    console.error("subscribe error:", err);
    return NextResponse.json({ error: "Failed to subscribe" }, { status: 500 });
  }
}
