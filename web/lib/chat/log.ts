import type { SupabaseClient } from "@supabase/supabase-js";

const MAX_REQUESTS_PER_HOUR = 10;
const MAX_USD_PER_DAY = 2.0;

export interface QueryLogEntry {
  email: string;
  question: string;
  sql: string | null;
  answerText: string | null;
  costUsd: number;
  error?: string;
}

/**
 * Insert one query_log row, success or failure. Uses the request-scoped
 * ANON-key client (respects RLS "self_insert"), same idiom as
 * web/lib/subscriptions.ts::subscribe(). Never throws — a logging failure
 * must not break the user-facing answer; callers should still get their
 * response even if this fails, just with a console warning.
 */
export async function logQuery(supabase: SupabaseClient, entry: QueryLogEntry): Promise<void> {
  const { error } = await supabase.from("query_log").insert({
    email: entry.email,
    question: entry.question,
    sql_generated: entry.sql,
    answer_text: entry.answerText,
    cost_usd: entry.costUsd,
    error: entry.error ?? null,
  });
  if (error) {
    console.error("logQuery error:", error);
  }
}

export type RateLimitResult = { ok: true } | { ok: false; reason: string };

/**
 * Reject if the caller has made too many requests in the last hour or spent
 * too much in the last day. Enforced by querying query_log itself — no new
 * infra (Redis etc.) needed, since this is a small internal-analyst tool.
 */
export async function checkRateLimit(supabase: SupabaseClient, email: string): Promise<RateLimitResult> {
  const oneHourAgo = new Date(Date.now() - 60 * 60 * 1000).toISOString();
  const oneDayAgo = new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString();

  const { count, error: countError } = await supabase
    .from("query_log")
    .select("id", { count: "exact", head: true })
    .eq("email", email)
    .gte("created_at", oneHourAgo);

  if (countError) {
    console.error("checkRateLimit count error:", countError);
  } else if ((count ?? 0) >= MAX_REQUESTS_PER_HOUR) {
    return { ok: false, reason: `Rate limit exceeded: max ${MAX_REQUESTS_PER_HOUR} questions per hour.` };
  }

  const { data: costRows, error: costError } = await supabase
    .from("query_log")
    .select("cost_usd")
    .eq("email", email)
    .gte("created_at", oneDayAgo);

  if (costError) {
    console.error("checkRateLimit cost error:", costError);
  } else {
    const totalUsd = (costRows ?? []).reduce((sum, row) => sum + (Number(row.cost_usd) || 0), 0);
    if (totalUsd >= MAX_USD_PER_DAY) {
      return { ok: false, reason: `Daily cost limit exceeded: max $${MAX_USD_PER_DAY.toFixed(2)} per day.` };
    }
  }

  return { ok: true };
}
