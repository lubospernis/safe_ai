"use client";

export const dynamic = "force-dynamic";

import { useState } from "react";
import { createClient } from "@/lib/supabase";
import styles from "./auth.module.css";

type Step = "email" | "code" | "done" | "error";

export default function AuthPage() {
  const [step, setStep] = useState<Step>("email");
  const [email, setEmail] = useState("");
  const [code, setCode] = useState("");
  const [errorMsg, setErrorMsg] = useState("");
  const [loading, setLoading] = useState(false);

  const supabase = createClient();

  async function requestOtp(e: React.FormEvent) {
    e.preventDefault();
    if (!email.trim()) return;
    if (!email.trim().toLowerCase().endsWith("@nbs.sk")) {
      setErrorMsg("Only @nbs.sk email addresses are allowed.");
      return;
    }
    setLoading(true);
    setErrorMsg("");
    const res = await fetch("/api/send-otp", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ email: email.trim().toLowerCase() }),
    });
    const data = await res.json();
    setLoading(false);
    if (!res.ok) {
      setErrorMsg(data.error ?? "Failed to send code.");
    } else {
      setStep("code");
    }
  }

  async function verifyCode(e: React.FormEvent) {
    e.preventDefault();
    if (!code.trim()) return;
    setLoading(true);
    setErrorMsg("");
    const { data, error } = await supabase.auth.verifyOtp({
      email: email.trim().toLowerCase(),
      token: code.trim(),
      type: "email",
    });
    if (error) {
      setLoading(false);
      setErrorMsg("Invalid or expired code. Please try again.");
      return;
    }
    // Check allowed_emails gate
    const userEmail = data.user?.email ?? "";
    const { data: allowed, error: gateError } = await supabase
      .from("allowed_emails")
      .select("email")
      .eq("email", userEmail)
      .maybeSingle();

    if (gateError || !allowed) {
      await supabase.auth.signOut();
      setLoading(false);
      setStep("error");
      setErrorMsg(
        "This email address is not authorised. Contact the administrator to request access.",
      );
      return;
    }

    setLoading(false);
    window.location.href = "/";
  }

  return (
    <main className={styles.main}>
      <div className={styles.card}>
        <div className={styles.logo}>🏦</div>
        <h1 className={styles.title}>NBS Monitors</h1>
        <p className={styles.subtitle}>Newsletter subscription service</p>

        {step === "email" && (
          <form onSubmit={requestOtp} className={styles.form}>
            <label className={styles.label} htmlFor="email">
              Work email
            </label>
            <input
              id="email"
              type="email"
              className={styles.input}
              value={email}
              onChange={(e) => setEmail(e.target.value)}
              placeholder="you@nbs.sk"
              required
              autoFocus
            />
            {errorMsg && <p className={styles.error}>{errorMsg}</p>}
            <button type="submit" className={styles.btn} disabled={loading}>
              {loading ? "Sending…" : "Send login code"}
            </button>
          </form>
        )}

        {step === "code" && (
          <form onSubmit={verifyCode} className={styles.form}>
            <p className={styles.hint}>
              We sent a 6-digit code to <strong>{email}</strong>. Check your
              inbox.
            </p>
            <label className={styles.label} htmlFor="code">
              Login code
            </label>
            <input
              id="code"
              type="text"
              inputMode="numeric"
              pattern="[0-9]{6}"
              className={styles.input}
              value={code}
              onChange={(e) => setCode(e.target.value)}
              placeholder="123456"
              maxLength={6}
              required
              autoFocus
            />
            {errorMsg && <p className={styles.error}>{errorMsg}</p>}
            <button type="submit" className={styles.btn} disabled={loading}>
              {loading ? "Verifying…" : "Confirm"}
            </button>
            <button
              type="button"
              className={styles.link}
              onClick={() => {
                setStep("email");
                setCode("");
                setErrorMsg("");
              }}
            >
              ← Use a different email
            </button>
          </form>
        )}

        {step === "error" && (
          <div className={styles.form}>
            <p className={styles.error}>{errorMsg}</p>
            <button
              type="button"
              className={styles.btn}
              onClick={() => {
                setStep("email");
                setEmail("");
                setCode("");
                setErrorMsg("");
              }}
            >
              Try again
            </button>
          </div>
        )}
      </div>
    </main>
  );
}
