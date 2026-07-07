"use client";

import { useState } from "react";
import styles from "./page.module.css";

interface Props {
  newsletterId: string;
  isSubscribed: boolean;
  subscribeLabel: string;
  unsubscribeLabel: string;
  errorLabel: string;
}

export default function SubscribeButton({
  isSubscribed: initial,
  subscribeLabel,
  unsubscribeLabel,
  errorLabel,
}: Props) {
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
      setError(err instanceof Error ? err.message : errorLabel);
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
        {loading ? "…" : subscribed ? unsubscribeLabel : subscribeLabel}
      </button>
    </div>
  );
}
