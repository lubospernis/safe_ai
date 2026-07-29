"use client";

import { useMemo, useState } from "react";
import styles from "@/app/report/report.module.css";

type Verdict = "up" | "down" | null;

interface FeedbackInit {
  verdict: Verdict;
  comment: string;
}

interface SectionFeedbackProps {
  waveNumber: number;
  sectionId: string;
  language: "en" | "sk";
  labels: {
    title: string;
    like: string;
    dislike: string;
    comment: string;
    submit: string;
    saving: string;
    saved: string;
    error: string;
  };
  initial?: FeedbackInit;
}

export default function SectionFeedback({
  waveNumber,
  sectionId,
  language,
  labels,
  initial,
}: SectionFeedbackProps) {
  const [verdict, setVerdict] = useState<Verdict>(initial?.verdict ?? null);
  const [comment, setComment] = useState(initial?.comment ?? "");
  const [saving, setSaving] = useState(false);
  const [message, setMessage] = useState("");

  const canSubmit = useMemo(() => {
    // Comment-only feedback is valid; at least one field should be non-empty.
    return verdict !== null || comment.trim().length > 0;
  }, [verdict, comment]);

  async function saveFeedback() {
    if (!canSubmit || saving) return;
    setSaving(true);
    setMessage("");
    try {
      const res = await fetch("/api/section-feedback", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          waveNumber,
          sectionId,
          language,
          verdict,
          comment,
        }),
      });
      if (!res.ok) throw new Error(await res.text());
      setMessage(labels.saved);
    } catch {
      setMessage(labels.error);
    } finally {
      setSaving(false);
    }
  }

  return (
    <div className={styles.feedbackCard}>
      <p className={styles.feedbackTitle}>{labels.title}</p>
      <div className={styles.feedbackRow}>
        <button
          type="button"
          className={verdict === "up" ? styles.feedbackBtnActive : styles.feedbackBtn}
          onClick={() => setVerdict((v) => (v === "up" ? null : "up"))}
        >
          {labels.like}
        </button>
        <button
          type="button"
          className={verdict === "down" ? styles.feedbackBtnActive : styles.feedbackBtn}
          onClick={() => setVerdict((v) => (v === "down" ? null : "down"))}
        >
          {labels.dislike}
        </button>
      </div>
      <label className={styles.feedbackLabel} htmlFor={`comment-${sectionId}`}>
        {labels.comment}
      </label>
      <textarea
        id={`comment-${sectionId}`}
        className={styles.feedbackInput}
        value={comment}
        onChange={(e) => setComment(e.target.value)}
        rows={3}
      />
      <div className={styles.feedbackFooter}>
        <button
          type="button"
          className={styles.feedbackSubmit}
          disabled={saving || !canSubmit}
          onClick={saveFeedback}
        >
          {saving ? labels.saving : labels.submit}
        </button>
        {message && <span className={styles.feedbackMsg}>{message}</span>}
      </div>
    </div>
  );
}
