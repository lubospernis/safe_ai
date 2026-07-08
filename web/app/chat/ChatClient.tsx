"use client";

import { useState } from "react";
import styles from "./chat.module.css";

interface ChatStrings {
  title: string;
  inputPlaceholder: string;
  send: string;
  loading: string;
  costLabel: string;
  errorGeneric: string;
  errorRateLimit: string;
  noTableData: string;
  navLink: string;
}

interface Table {
  columns: string[];
  rows: (string | number | null)[][];
}

interface Turn {
  question: string;
  answerText: string;
  table: Table | null;
  costUsd: number;
  error?: string;
}

const MAX_HISTORY_TURNS = 4;

export default function ChatClient({ email, strings }: { email: string; strings: ChatStrings }) {
  const [turns, setTurns] = useState<Turn[]>([]);
  const [input, setInput] = useState("");
  const [loading, setLoading] = useState(false);

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    const question = input.trim();
    if (!question || loading) return;

    setInput("");
    setLoading(true);

    const history = turns.slice(-MAX_HISTORY_TURNS).map((t) => ({
      question: t.question,
      answerText: t.answerText,
    }));

    try {
      const res = await fetch("/api/chat", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ question, history }),
      });
      const data = await res.json();
      if (!res.ok) {
        const message = res.status === 429 ? strings.errorRateLimit : data.error || strings.errorGeneric;
        setTurns((prev) => [...prev, { question, answerText: "", table: null, costUsd: 0, error: message }]);
      } else {
        setTurns((prev) => [
          ...prev,
          { question, answerText: data.answerText, table: data.table, costUsd: data.costUsd },
        ]);
      }
    } catch {
      setTurns((prev) => [
        ...prev,
        { question, answerText: "", table: null, costUsd: 0, error: strings.errorGeneric },
      ]);
    } finally {
      setLoading(false);
    }
  }

  return (
    <main className={styles.main}>
      <header className={styles.header}>
        <span className={styles.headerLogo}>💬</span>
        <div className={styles.headerText}>
          <h1 className={styles.headerTitle}>{strings.title}</h1>
          <p className={styles.headerSub}>{email}</p>
        </div>
      </header>

      <div className={styles.thread}>
        {turns.map((turn, i) => (
          <div key={i} className={styles.turn}>
            <p className={styles.question}>{turn.question}</p>
            {turn.error ? (
              <p className={styles.error}>{turn.error}</p>
            ) : (
              <>
                <p className={styles.answer}>{turn.answerText}</p>
                {turn.table && turn.table.rows.length > 0 ? (
                  <div className={styles.tableWrap}>
                    <table className={styles.table}>
                      <thead>
                        <tr>
                          {turn.table.columns.map((col) => (
                            <th key={col}>{col}</th>
                          ))}
                        </tr>
                      </thead>
                      <tbody>
                        {turn.table.rows.map((row, ri) => (
                          <tr key={ri}>
                            {row.map((cell, ci) => (
                              <td key={ci}>{cell === null ? "" : String(cell)}</td>
                            ))}
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                ) : null}
                <span className={styles.cost}>
                  {strings.costLabel.replace("{cost}", turn.costUsd.toFixed(4))}
                </span>
              </>
            )}
          </div>
        ))}
        {loading && <p className={styles.loading}>{strings.loading}</p>}
      </div>

      <form className={styles.inputBar} onSubmit={handleSubmit}>
        <input
          className={styles.input}
          value={input}
          onChange={(e) => setInput(e.target.value)}
          placeholder={strings.inputPlaceholder}
          disabled={loading}
        />
        <button className={styles.sendBtn} type="submit" disabled={loading || !input.trim()}>
          {strings.send}
        </button>
      </form>
    </main>
  );
}
