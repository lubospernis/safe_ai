import { createServerSideClient } from "@/lib/supabase-server";
import { STRINGS } from "@/lib/strings";
import { redirect } from "next/navigation";
import ChatClient from "./ChatClient";
import { getConnection } from "@/lib/chat/duckdb";
import { fetchTrendingHighlights, type Highlight } from "@/lib/chat/highlights";

export const dynamic = "force-dynamic";

async function loadHighlights(): Promise<Highlight[]> {
  let con;
  try {
    con = await getConnection();
    return await fetchTrendingHighlights(con);
  } catch {
    // Intro screen just omits "trending now" if MotherDuck is unreachable —
    // not worth failing the whole page load over.
    return [];
  } finally {
    con?.closeSync();
  }
}

export default async function ChatPage() {
  const supabase = await createServerSideClient();
  const {
    data: { user },
  } = await supabase.auth.getUser();

  if (!user?.email) redirect("/auth");

  const { data: allowedRow } = await supabase
    .from("allowed_emails")
    .select("lang")
    .eq("email", user.email)
    .maybeSingle();
  const lang: "en" | "sk" = allowedRow?.lang === "sk" ? "sk" : "en";
  const t = STRINGS[lang].chat;

  const highlights = await loadHighlights();

  return <ChatClient email={user.email} strings={t} highlights={highlights} lang={lang} />;
}
