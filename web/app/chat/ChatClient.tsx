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
  introDescription: string;
  introDimensionsTitle: string;
  introDimensionCountry: string;
  introDimensionFirmSize: string;
  introDimensionTime: string;
  introTrendingTitle: string;
  introTrendingWorsened: string;
  introTrendingImproved: string;
  introExamplesTitle: string;
  introExampleCountry: string;
  introExampleFirmSize: string;
  introExampleTrend: string;
  showSql: string;
}

interface Highlight {
  label: string;
  deltaPp: number;
  latest: number;
}

interface Table {
  columns: string[];
  rows: (string | number | null)[][];
}

interface Turn {
  question: string;
  answerText: string;
  table: Table | null;
  sql: string | null;
  costUsd: number;
  error?: string;
}

const MAX_HISTORY_TURNS = 10;

export default function ChatClient({
  email,
  strings,
  highlights,
}: {
  email: string;
  strings: ChatStrings;
  highlights: Highlight[];
  lang: "en" | "sk";
}) {
  const [turns, setTurns] = useState<Turn[]>([]);
  const [input, setInput] = useState("");
  const [loading, setLoading] = useState(false);

  async function ask(question: string) {
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
        setTurns((prev) => [
          ...prev,
          { question, answerText: "", table: null, sql: null, costUsd: 0, error: message },
        ]);
      } else {
        setTurns((prev) => [
          ...prev,
          { question, answerText: data.answerText, table: data.table, sql: data.sql, costUsd: data.costUsd },
        ]);
      }
    } catch {
      setTurns((prev) => [
        ...prev,
        { question, answerText: "", table: null, sql: null, costUsd: 0, error: strings.errorGeneric },
      ]);
    } finally {
      setLoading(false);
    }
  }

  function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    ask(input.trim());
  }

  const exampleQuestions = [strings.introExampleCountry, strings.introExampleFirmSize, strings.introExampleTrend];

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
        {turns.length === 0 && (
          <div className={styles.intro}>
            <p className={styles.introText}>{strings.introDescription}</p>

            <p className={styles.introSectionTitle}>{strings.introDimensionsTitle}</p>
            <ul className={styles.introList}>
              <li>{strings.introDimensionCountry}</li>
              <li>{strings.introDimensionFirmSize}</li>
              <li>{strings.introDimensionTime}</li>
            </ul>

            {highlights.length > 0 && (
              <>
                <p className={styles.introSectionTitle}>{strings.introTrendingTitle}</p>
                <ul className={styles.introList}>
                  {highlights.map((h) => {
                    const template = h.deltaPp >= 0 ? strings.introTrendingImproved : strings.introTrendingWorsened;
                    const deltaText = template.replace("{delta}", Math.abs(h.deltaPp).toFixed(1));
                    return (
                      <li key={h.label}>
                        {h.label}: {deltaText}
                      </li>
                    );
                  })}
                </ul>
              </>
            )}

            <p className={styles.introSectionTitle}>{strings.introExamplesTitle}</p>
            <div className={styles.exampleList}>
              {exampleQuestions.map((q) => (
                <button
                  key={q}
                  type="button"
                  className={styles.exampleBtn}
                  onClick={() => ask(q)}
                  disabled={loading}
                >
                  {q}
                </button>
              ))}
            </div>
          </div>
        )}

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
                {turn.sql ? (
                  <details className={styles.sqlDetails}>
                    <summary className={styles.sqlSummary}>{strings.showSql}</summary>
                    <pre className={styles.sqlPre}>{turn.sql}</pre>
                  </details>
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
