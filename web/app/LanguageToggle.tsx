"use client";

import { useState } from "react";
import styles from "./page.module.css";

interface Props {
  initialLang: "en" | "sk";
}

export default function LanguageToggle({ initialLang }: Props) {
  const [lang, setLang] = useState<"en" | "sk">(initialLang);
  const [loading, setLoading] = useState(false);

  async function choose(next: "en" | "sk") {
    if (next === lang || loading) return;
    setLoading(true);
    const prev = lang;
    setLang(next);
    try {
      const res = await fetch("/api/set-language", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ lang: next }),
      });
      if (!res.ok) throw new Error(await res.text());
    } catch {
      setLang(prev);
    } finally {
      setLoading(false);
    }
  }

  return (
    <div className={styles.langToggle} role="group" aria-label="Newsletter language">
      <button
        type="button"
        className={lang === "en" ? styles.langActive : styles.langInactive}
        onClick={() => choose("en")}
        disabled={loading}
      >
        EN
      </button>
      <button
        type="button"
        className={lang === "sk" ? styles.langActive : styles.langInactive}
        onClick={() => choose("sk")}
        disabled={loading}
      >
        SK
      </button>
    </div>
  );
}
