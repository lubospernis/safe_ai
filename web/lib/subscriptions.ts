import type { SupabaseClient } from "@supabase/supabase-js";

/**
 * Return the set of newsletter_id values the given email is subscribed to.
 * Relies on RLS ("self_read" policy on public.subscriptions) — the passed
 * client must be the request-scoped server client so auth.jwt() resolves to
 * the logged-in user.
 */
export async function getSubscriptions(
  supabase: SupabaseClient,
  email: string,
): Promise<Set<string>> {
  const { data, error } = await supabase
    .from("subscriptions")
    .select("newsletter_id")
    .eq("email", email);

  if (error) {
    console.error("getSubscriptions error:", error);
    return new Set();
  }
  return new Set((data ?? []).map((row) => row.newsletter_id as string));
}

/**
 * Subscribe email to newsletterId. Idempotent — a duplicate subscribe is a
 * no-op thanks to the UNIQUE(email, newsletter_id) constraint.
 */
export async function subscribe(
  supabase: SupabaseClient,
  email: string,
  newsletterId: string,
): Promise<void> {
  const { error } = await supabase
    .from("subscriptions")
    .upsert({ email, newsletter_id: newsletterId }, { onConflict: "email,newsletter_id" });

  if (error) throw new Error(`Failed to subscribe: ${error.message}`);
}

export async function unsubscribe(
  supabase: SupabaseClient,
  email: string,
  newsletterId: string,
): Promise<void> {
  const { error } = await supabase
    .from("subscriptions")
    .delete()
    .eq("email", email)
    .eq("newsletter_id", newsletterId);

  if (error) throw new Error(`Failed to unsubscribe: ${error.message}`);
}
