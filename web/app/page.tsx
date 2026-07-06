import { createServerSideClient } from "@/lib/supabase-server";
import { getSubscribers } from "@/lib/github";
import { NEWSLETTERS } from "@/lib/newsletters";
import { redirect } from "next/navigation";
import SubscribeButton from "./SubscribeButton";
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

  return (
    <main className={styles.main}>
      <header className={styles.header}>
        <span className={styles.headerLogo}>🏦</span>
        <div className={styles.headerText}>
          <h1 className={styles.headerTitle}>NBS Monitors</h1>
          <p className={styles.headerSub}>Newsletter subscription service · {email}</p>
        </div>
        <SignOutForm />
      </header>

      <section className={styles.list}>
        {NEWSLETTERS.map((nl) => {
          const isSubscribed = subscribers.includes(email);
          return (
            <div key={nl.id} className={styles.card}>
              <div className={styles.cardIcon}>{nl.icon}</div>
              <div className={styles.cardBody}>
                <h2 className={styles.cardTitle}>{nl.name}</h2>
                <p className={styles.cardDesc}>{nl.description}</p>
                <span className={styles.badge}>{nl.periodicity}</span>
              </div>
              <SubscribeButton newsletterId={nl.id} isSubscribed={isSubscribed} />
            </div>
          );
        })}
      </section>
    </main>
  );
}

function SignOutForm() {
  async function signOut() {
    "use server";
    const supabase = await createServerSideClient();
    await supabase.auth.signOut();
    redirect("/auth");
  }
  return (
    <form action={signOut}>
      <button type="submit" className={styles.signout}>
        Sign out
      </button>
    </form>
  );
}
