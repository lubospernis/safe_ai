import { createServerSideClient } from "@/lib/supabase-server";
import { STRINGS } from "@/lib/strings";
import { redirect } from "next/navigation";
import ChatClient from "./ChatClient";

export const dynamic = "force-dynamic";

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

  return <ChatClient email={user.email} strings={t} />;
}
