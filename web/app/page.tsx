import { createServerSideClient } from "@/lib/supabase-server";
import { getSubscriptions } from "@/lib/subscriptions";
import { fetchLinks, type LatestLinks } from "@/lib/latestLinks";
import { getNewsletters } from "@/lib/newsletters";
import { STRINGS } from "@/lib/strings";
import { redirect } from "next/navigation";
import SubscribeButton from "./SubscribeButton";
import LanguageToggle from "./LanguageToggle";
import styles from "./page.module.css";

export const dynamic = "force-dynamic";

function formatDate(iso: string, lang: "en" | "sk"): string {
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return iso;
  return d.toLocaleDateString(lang === "sk" ? "sk-SK" : "en-GB", {
    year: "numeric",
    month: "short",
    day: "numeric",
  });
}

export default async function Home() {
  const supabase = await createServerSideClient();
  const {
    data: { user },
  } = await supabase.auth.getUser();

  if (!user?.email) redirect("/auth");

  const email = user.email;

  let subscribedIds: Set<string> = new Set();
  try {
    subscribedIds = await getSubscriptions(supabase, email);
  } catch {
    // Non-fatal — show buttons in unknown state
  }

  const { data: allowedRow } = await supabase
    .from("allowed_emails")
    .select("lang")
    .eq("email", email)
    .maybeSingle();
  const lang: "en" | "sk" = allowedRow?.lang === "en" ? "en" : "sk";
  const t = STRINGS[lang];

  const newsletters = await getNewsletters(supabase);
  const fetchedLinks = await Promise.all(
    newsletters.map((nl) => (nl.linksJsonUrl ? fetchLinks(nl.linksJsonUrl) : Promise.resolve(null))),
  );
  const linksByNewsletter: Record<string, LatestLinks | null> = {};
  newsletters.forEach((nl, i) => {
    linksByNewsletter[nl.id] = fetchedLinks[i];
  });

  return (
    <main className={styles.main}>
      <header className={styles.header}>
        <span className={styles.headerLogo}>🏦</span>
        <div className={styles.headerText}>
          <h1 className={styles.headerTitle}>NBS Monitors</h1>
          <p className={styles.headerSub}>{t.subscriptionService} · {email}</p>
        </div>
        <LanguageToggle initialLang={lang} />
        <SignOutForm label={t.signOut} />
      </header>

      <section className={styles.list}>
        {newsletters.map((nl) => {
          const isSubscribed = subscribedIds.has(nl.id);
          const nlText = { name: nl.name[lang], description: nl.description[lang], periodicity: nl.periodicity[lang] };
          const latestLinks = linksByNewsletter[nl.id] ?? null;
          const reportUrl = nl.linkUrl ?? (latestLinks ? (latestLinks[lang] || latestLinks.en) : null);
          return (
            <div key={nl.id} className={styles.card}>
              <div className={styles.cardIcon}>{nl.icon}</div>
              <div className={styles.cardBody}>
                <h2 className={styles.cardTitle}>
                  {reportUrl ? (
                    <a
                      href={reportUrl}
                      target="_blank"
                      rel="noopener"
                      className={styles.cardTitleLink}
                    >
                      {nlText.name}
                    </a>
                  ) : (
                    nlText.name
                  )}
                </h2>
                <p className={styles.cardDesc}>{nlText.description}</p>
                <p className={styles.cardFormat}>{t.formatDescription}</p>
                <div className={styles.badgeRow}>
                  <span className={styles.badge}>{nlText.periodicity}</span>
                  {nl.isExperimental && (
                    <span className={styles.badgeWarn}>{t.experimentalBadge}</span>
                  )}
                  {latestLinks?.last_updated && (
                    <span className={styles.badgeMuted}>
                      {t.lastUpdated.replace("{date}", formatDate(latestLinks.last_updated, lang))}
                    </span>
                  )}
                  {latestLinks?.next_release && (
                    <span className={styles.badgeMuted}>
                      {t.nextRelease.replace("{date}", formatDate(latestLinks.next_release, lang))}
                    </span>
                  )}
                </div>
                {latestLinks?.next_release && (
                  <p className={styles.footnote}>{t.nextReleaseFootnote}</p>
                )}
              </div>
              {nl.isSubscribable && (
                <SubscribeButton
                  newsletterId={nl.id}
                  isSubscribed={isSubscribed}
                  subscribeLabel={t.subscribe}
                  unsubscribeLabel={t.unsubscribe}
                  errorLabel={t.somethingWentWrong}
                />
              )}
            </div>
          );
        })}
      </section>
    </main>
  );
}

function SignOutForm({ label }: { label: string }) {
  async function signOut() {
    "use server";
    const supabase = await createServerSideClient();
    await supabase.auth.signOut();
    redirect("/auth");
  }
  return (
    <form action={signOut}>
      <button type="submit" className={styles.signout}>
        {label}
      </button>
    </form>
  );
}
