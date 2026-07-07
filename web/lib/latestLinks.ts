const LATEST_LINKS_URL =
  "https://raw.githubusercontent.com/lubospernis/safe_ai/main/reports/output/latest_links.json";
const LATEST_ADHOC_LINKS_URL =
  "https://raw.githubusercontent.com/lubospernis/safe_ai/main/reports/output/latest_adhoc_links.json";

export interface LatestLinks {
  wave: number;
  en: string;
  sk: string;
  last_updated?: string;
  next_release?: string;
  next_release_note?: string;
}

async function fetchLinks(url: string): Promise<LatestLinks | null> {
  try {
    const res = await fetch(url, { cache: "no-store" });
    if (!res.ok) return null;
    return (await res.json()) as LatestLinks;
  } catch {
    return null;
  }
}

/** Latest links for the regular quarterly report ("safe-regular"). */
export async function getLatestLinks(): Promise<LatestLinks | null> {
  return fetchLinks(LATEST_LINKS_URL);
}

/** Latest links for the ad-hoc Special Focus report ("safe-adhoc"). Distinct
 * from getLatestLinks() — the two reports are published independently and
 * the adhoc one may not exist yet for a given wave. */
export async function getLatestAdhocLinks(): Promise<LatestLinks | null> {
  return fetchLinks(LATEST_ADHOC_LINKS_URL);
}
