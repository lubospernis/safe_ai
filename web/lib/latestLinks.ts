const LATEST_LINKS_URL =
  "https://raw.githubusercontent.com/lubospernis/safe_ai/main/reports/output/latest_links.json";

export interface LatestLinks {
  wave: number;
  en: string;
  sk: string;
  last_updated?: string;
  next_release?: string;
  next_release_note?: string;
}

export async function getLatestLinks(): Promise<LatestLinks | null> {
  try {
    const res = await fetch(LATEST_LINKS_URL, { cache: "no-store" });
    if (!res.ok) return null;
    return (await res.json()) as LatestLinks;
  } catch {
    return null;
  }
}
