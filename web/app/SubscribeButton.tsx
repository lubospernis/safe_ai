"use client";

import { useState } from "react";
import styles from "./page.module.css";

interface Props {
  newsletterId: string;
  isSubscribed: boolean;
}

export default function SubscribeButton({ isSubscribed: initial }: Props) {
  const [subscribed, setSubscribed] = useState(initial);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

  async function toggle() {
    setLoading(true);
    setError("");
    const endpoint = subscribed ? "/api/unsubscribe" : "/api/subscribe";
    try {
      const res = await fetch(endpoint, { method: "POST" });
      if (!res.ok) throw new Error(await res.text());
      setSubscribed((s) => !s);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Something went wrong");
    } finally {
      setLoading(false);
    }
  }

  return (
    <div className={styles.btnWrap}>
      {error && <p className={styles.btnError}>{error}</p>}
      <button
        className={subscribed ? styles.btnUnsubscribe : styles.btnSubscribe}
        onClick={toggle}
        disabled={loading}
      >
        {loading ? "…" : subscribed ? "Unsubscribe" : "Subscribe"}
      </button>
    </div>
  );
}
