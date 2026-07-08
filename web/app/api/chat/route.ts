import { Mistral } from "@mistralai/mistralai";
import { NextResponse } from "next/server";
import { createServerSideClient } from "@/lib/supabase-server";
import { getConnection } from "@/lib/chat/duckdb";
import { buildMartCatalogue } from "@/lib/chat/catalogue";
import { runChatAgent, type ChatTurn } from "@/lib/chat/agent";
import { checkRateLimit, logQuery } from "@/lib/chat/log";

export const runtime = "nodejs";
export const maxDuration = 60;

const MAX_QUESTION_LENGTH = 2000;
const MAX_HISTORY_TURNS = 10;

interface ChatRequestBody {
  question?: unknown;
  history?: unknown;
}

function parseHistory(raw: unknown): ChatTurn[] {
  if (!Array.isArray(raw)) return [];
  const turns: ChatTurn[] = [];
  for (const item of raw) {
    if (
      item &&
      typeof item === "object" &&
      typeof (item as ChatTurn).question === "string" &&
      typeof (item as ChatTurn).answerText === "string"
    ) {
      turns.push({ question: (item as ChatTurn).question, answerText: (item as ChatTurn).answerText });
    }
  }
  return turns.slice(-MAX_HISTORY_TURNS);
}

export async function POST(request: Request) {
  const supabase = await createServerSideClient();
  const {
    data: { user },
    error: authError,
  } = await supabase.auth.getUser();

  if (authError || !user?.email) {
    return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
  }
  const email = user.email;

  let body: ChatRequestBody;
  try {
    body = await request.json();
  } catch {
    return NextResponse.json({ error: "Invalid request body" }, { status: 400 });
  }

  const question = typeof body.question === "string" ? body.question.trim() : "";
  if (!question) {
    return NextResponse.json({ error: "Question is required" }, { status: 400 });
  }
  if (question.length > MAX_QUESTION_LENGTH) {
    return NextResponse.json(
      { error: `Question exceeds max length of ${MAX_QUESTION_LENGTH} characters` },
      { status: 400 },
    );
  }
  const history = parseHistory(body.history);

  const rateLimit = await checkRateLimit(supabase, email);
  if (!rateLimit.ok) {
    return NextResponse.json({ error: rateLimit.reason }, { status: 429 });
  }

  let con;
  try {
    con = await getConnection();
    const catalogue = await buildMartCatalogue(con, "main_safe");
    const mistralClient = new Mistral({ apiKey: process.env.MISTRAL_API_KEY! });

    const result = await runChatAgent(question, history, con, catalogue, mistralClient);

    await logQuery(supabase, {
      email,
      question,
      sql: result.sql,
      answerText: result.answerText,
      costUsd: result.cost.usd,
    });

    return NextResponse.json({
      answerText: result.answerText,
      sql: result.sql,
      table: result.table,
      costUsd: Math.round(result.cost.usd * 1_000_000) / 1_000_000,
    });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    console.error("chat route error:", err);
    await logQuery(supabase, {
      email,
      question,
      sql: null,
      answerText: null,
      costUsd: 0,
      error: message,
    });
    return NextResponse.json({ error: "Failed to answer question" }, { status: 500 });
  } finally {
    con?.closeSync();
  }
}
