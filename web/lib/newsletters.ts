import type { SupabaseClient } from "@supabase/supabase-js";

export interface Newsletter {
  id: string;
  icon: string;
  isExperimental: boolean;
  isSubscribable: boolean;
  linkUrl: string | null;
  linksJsonUrl: string | null;
  name: { en: string; sk: string };
  description: { en: string; sk: string };
  periodicity: { en: string; sk: string };
}

interface NewsletterRow {
  id: string;
  icon: string;
  name_en: string;
  name_sk: string;
  description_en: string;
  description_sk: string;
  periodicity_en: string;
  periodicity_sk: string;
  link_url: string | null;
  links_json_url: string | null;
  is_experimental: boolean;
  is_subscribable: boolean;
}

function toNewsletter(row: NewsletterRow): Newsletter {
  return {
    id: row.id,
    icon: row.icon,
    isExperimental: row.is_experimental,
    isSubscribable: row.is_subscribable,
    linkUrl: row.link_url,
    linksJsonUrl: row.links_json_url,
    name: { en: row.name_en, sk: row.name_sk },
    description: { en: row.description_en, sk: row.description_sk },
    periodicity: { en: row.periodicity_en, sk: row.periodicity_sk },
  };
}

/**
 * Return every tile to render on the home page, ordered for display.
 * Relies on RLS ("authenticated_read" policy on public.newsletters) — the
 * passed client must be the request-scoped server client so auth.role()
 * resolves for the logged-in user.
 */
export async function getNewsletters(supabase: SupabaseClient): Promise<Newsletter[]> {
  const { data, error } = await supabase
    .from("newsletters")
    .select(
      "id, icon, name_en, name_sk, description_en, description_sk, periodicity_en, periodicity_sk, link_url, links_json_url, is_experimental, is_subscribable",
    )
    .order("sort_order", { ascending: true });

  if (error) {
    console.error("getNewsletters error:", error);
    return [];
  }
  return (data ?? []).map((row) => toNewsletter(row as NewsletterRow));
}
