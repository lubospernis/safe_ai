const GITHUB_TOKEN = process.env.GITHUB_TOKEN!;
const GITHUB_REPO = process.env.GITHUB_REPO ?? "lubospernis/safe_ai";
const GITHUB_PATH =
  process.env.GITHUB_SUBSCRIBERS_PATH ?? "newsletter/subscribers.json";
const API_BASE = "https://api.github.com";

export type Lang = "en" | "sk";

interface Subscriber {
  email: string;
  lang: Lang;
  subscribed_at: string;
}

interface SubscribersFile {
  subscribers: Subscriber[];
}

interface GitHubFileResponse {
  content: string;
  sha: string;
}

async function getFile(): Promise<{ data: SubscribersFile; sha: string }> {
  const res = await fetch(
    `${API_BASE}/repos/${GITHUB_REPO}/contents/${GITHUB_PATH}`,
    {
      headers: {
        Authorization: `Bearer ${GITHUB_TOKEN}`,
        Accept: "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
      },
      cache: "no-store",
    },
  );
  if (!res.ok) throw new Error(`GitHub GET failed: ${res.status}`);
  const file: GitHubFileResponse = await res.json();
  const data: SubscribersFile = JSON.parse(
    Buffer.from(file.content, "base64").toString("utf8"),
  );
  return { data, sha: file.sha };
}

async function putFile(
  data: SubscribersFile,
  sha: string,
  message: string,
): Promise<void> {
  const content = Buffer.from(
    JSON.stringify(data, null, 2) + "\n",
    "utf8",
  ).toString("base64");
  const res = await fetch(
    `${API_BASE}/repos/${GITHUB_REPO}/contents/${GITHUB_PATH}`,
    {
      method: "PUT",
      headers: {
        Authorization: `Bearer ${GITHUB_TOKEN}`,
        Accept: "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ message, content, sha }),
    },
  );
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`GitHub PUT failed: ${res.status} ${text}`);
  }
}

export async function getSubscribers(): Promise<string[]> {
  const { data } = await getFile();
  return data.subscribers.map((s) => s.email);
}

export async function addSubscriber(email: string, lang: Lang): Promise<void> {
  const { data, sha } = await getFile();
  if (data.subscribers.some((s) => s.email === email)) return;
  data.subscribers.push({ email, lang, subscribed_at: new Date().toISOString() });
  await putFile(data, sha, `newsletter: subscribe ${email} (${lang}) [skip ci]`);
}

export async function removeSubscriber(email: string): Promise<void> {
  const { data, sha } = await getFile();
  const before = data.subscribers.length;
  data.subscribers = data.subscribers.filter((s) => s.email !== email);
  if (data.subscribers.length === before) return;
  await putFile(data, sha, `newsletter: unsubscribe ${email} [skip ci]`);
}

export async function updateSubscriberLang(email: string, lang: Lang): Promise<void> {
  const { data, sha } = await getFile();
  const sub = data.subscribers.find((s) => s.email === email);
  if (!sub || sub.lang === lang) return;
  sub.lang = lang;
  await putFile(data, sha, `newsletter: set ${email} lang=${lang} [skip ci]`);
}
