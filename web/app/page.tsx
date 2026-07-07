import { createServerSideClient } from "@/lib/supabase-server";
import { getSubscribers } from "@/lib/github";
import { NEWSLETTERS } from "@/lib/newsletters";
import { STRINGS, type NewsletterId } from "@/lib/strings";
import { redirect } from "next/navigation";
import SubscribeButton from "./SubscribeButton";
import LanguageToggle from "./LanguageToggle";
import styles from "./page.module.css";

export const dynamic = "force-dynamic";

export default async function Home() {
  const supabase = await createServerSideClient();
  const {
    data: { user },
  } = await supabase.auth.getUser();

  if (!user?.email) redirect("/auth");

  let subscribers: string[] = [];
  try {
    subscribers = await getSubscribers();
  } catch {
    // Non-fatal — show buttons in unknown state
  }

  const email = user.email;

  const { data: allowedRow } = await supabase
    .from("allowed_emails")
    .select("lang")
    .eq("email", email)
    .maybeSingle();
  const lang: "en" | "sk" = allowedRow?.lang === "sk" ? "sk" : "en";
  const t = STRINGS[lang];

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
        {NEWSLETTERS.map((nl) => {
          const isSubscribed = subscribers.includes(email);
          const nlText = t.newsletters[nl.id as NewsletterId] ?? nl;
          return (
            <div key={nl.id} className={styles.card}>
              <div className={styles.cardIcon}>{nl.icon}</div>
              <div className={styles.cardBody}>
                <h2 className={styles.cardTitle}>{nlText.name}</h2>
                <p className={styles.cardDesc}>{nlText.description}</p>
                <span className={styles.badge}>{nlText.periodicity}</span>
              </div>
              <SubscribeButton
                newsletterId={nl.id}
                isSubscribed={isSubscribed}
                subscribeLabel={t.subscribe}
                unsubscribeLabel={t.unsubscribe}
                errorLabel={t.somethingWentWrong}
              />
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
