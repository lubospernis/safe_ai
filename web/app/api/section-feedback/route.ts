import { createServerSideClient } from "@/lib/supabase-server";
import { NextResponse } from "next/server";

type Verdict = "up" | "down" | null;

interface FeedbackInput {
  waveNumber: number;
  sectionId: string;
  language: "en" | "sk";
  verdict?: Verdict;
  comment?: string;
}

function normalizeVerdict(value: unknown): Verdict {
  if (value === "up" || value === "down") return value;
  return null;
}

function normalizeComment(value: unknown): string {
  if (typeof value !== "string") return "";
  return value.trim().slice(0, 2000);
}

export async function POST(request: Request) {
  const supabase = await createServerSideClient();
  const {
    data: { user },
    error,
  } = await supabase.auth.getUser();

  if (error || !user?.email) {
    return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
  }

  const body = (await request.json()) as Partial<FeedbackInput>;
  const waveNumber = Number(body.waveNumber);
  const sectionId = String(body.sectionId ?? "").trim();
  const language: "en" | "sk" = body.language === "en" ? "en" : "sk";
  const verdict = normalizeVerdict(body.verdict);
  const comment = normalizeComment(body.comment);

  if (!Number.isInteger(waveNumber) || waveNumber <= 0 || !sectionId) {
    return NextResponse.json({ error: "Invalid payload" }, { status: 400 });
  }
  if (!verdict && !comment) {
    return NextResponse.json({ error: "Provide verdict or comment" }, { status: 400 });
  }

  const { error: upsertError } = await supabase
    .from("report_section_feedback")
    .upsert(
      {
        email: user.email,
        wave_number: waveNumber,
        section_id: sectionId,
        language,
        verdict,
        comment,
        updated_at: new Date().toISOString(),
      },
      { onConflict: "email,wave_number,section_id" },
    );

  if (upsertError) {
    console.error("section-feedback upsert error:", upsertError);
    return NextResponse.json({ error: "Failed to save feedback" }, { status: 500 });
  }

  return NextResponse.json({ ok: true });
}

export async function GET(request: Request) {
  const supabase = await createServerSideClient();
  const {
    data: { user },
    error,
  } = await supabase.auth.getUser();

  if (error || !user?.email) {
    return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
  }

  const url = new URL(request.url);
  const waveNumber = Number(url.searchParams.get("waveNumber") ?? "0");
  if (!Number.isInteger(waveNumber) || waveNumber <= 0) {
    return NextResponse.json({ error: "Invalid waveNumber" }, { status: 400 });
  }

  const { data, error: readError } = await supabase
    .from("report_section_feedback")
    .select("section_id, verdict, comment")
    .eq("email", user.email)
    .eq("wave_number", waveNumber);

  if (readError) {
    console.error("section-feedback read error:", readError);
    return NextResponse.json({ error: "Failed to load feedback" }, { status: 500 });
  }

  return NextResponse.json({ items: data ?? [] });
}
