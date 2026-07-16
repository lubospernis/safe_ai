export interface LatestLinks {
  wave: number;
  en: string;
  sk: string;
  last_updated?: string;
  next_release?: string;
  next_release_note?: string;
}

/** Fetches a SAFE-style {en, sk, last_updated, next_release} JSON file (see
 * a newsletter row's links_json_url in public.newsletters). Returns null on
 * any failure — the caller renders the tile without a link/date badges. */
export async function fetchLinks(url: string): Promise<LatestLinks | null> {
  try {
    const res = await fetch(url, { cache: "no-store" });
    if (!res.ok) return null;
    return (await res.json()) as LatestLinks;
  } catch {
    return null;
  }
}
