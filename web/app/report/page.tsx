import { createServerSideClient } from "@/lib/supabase-server";
import { fetchLinks } from "@/lib/latestLinks";
import { STRINGS } from "@/lib/strings";
import { redirect } from "next/navigation";
import SectionFeedback from "@/components/SectionFeedback";
import SimpleLineChart from "@/components/SimpleLineChart";
import styles from "./report.module.css";

interface ReportSection {
  section_id: string;
  title: string;
  group: string;
  finding: string;
  bullets: string[];
  chart: {
    chart_type: string;
    best_panel?: string | number | null;
    panel_col?: string | null;
    value_col: string;
    series_col: string;
    records: Array<Record<string, string | number | null>>;
    chart_subtitle?: string;
    question_caption?: string;
  };
}

interface ReportPayload {
  wave: number;
  period_label?: string | null;
  language: "en" | "sk";
  exec_summary: Array<{ section_id: string; bullet: string }>;
  sections: ReportSection[];
}

interface ExistingFeedback {
  section_id: string;
  verdict: "up" | "down" | null;
  comment: string | null;
}

async function loadReportPayload(reportJsonUrl: string): Promise<ReportPayload | null> {
  try {
    const res = await fetch(reportJsonUrl, { cache: "no-store" });
    if (!res.ok) return null;
    return (await res.json()) as ReportPayload;
  } catch {
    return null;
  }
}

export default async function ReportPage() {
  const supabase = await createServerSideClient();
  const {
    data: { user },
  } = await supabase.auth.getUser();

  if (!user?.email) redirect("/auth");

  const { data: allowedRow } = await supabase
    .from("allowed_emails")
    .select("lang")
    .eq("email", user.email)
    .maybeSingle();

  const lang: "en" | "sk" = allowedRow?.lang === "en" ? "en" : "sk";
  const t = STRINGS[lang];

  const { data: newsletterRow } = await supabase
    .from("newsletters")
    .select("links_json_url")
    .eq("id", "safe-regular")
    .maybeSingle();

  const links = newsletterRow?.links_json_url ? await fetchLinks(newsletterRow.links_json_url) : null;
  const reportJsonUrl = links ? (lang === "sk" ? (links.report_json_sk ?? links.report_json_en) : (links.report_json_en ?? links.report_json_sk)) : null;

  if (!reportJsonUrl) {
    return (
      <main className={styles.main}>
        <h1 className={styles.title}>{lang === "sk" ? "Interaktívna správa" : "Interactive report"}</h1>
        <p>{t.reportUnavailable}</p>
      </main>
    );
  }

  const payload = await loadReportPayload(reportJsonUrl);
  if (!payload) {
    return (
      <main className={styles.main}>
        <h1 className={styles.title}>{lang === "sk" ? "Interaktívna správa" : "Interactive report"}</h1>
        <p>{t.reportLoadError}</p>
      </main>
    );
  }

  const { data: feedbackRows } = await supabase
    .from("report_section_feedback")
    .select("section_id, verdict, comment")
    .eq("email", user.email)
    .eq("wave_number", payload.wave);

  const feedbackBySection = new Map<string, ExistingFeedback>();
  (feedbackRows ?? []).forEach((row) => {
    feedbackBySection.set(row.section_id as string, {
      section_id: row.section_id as string,
      verdict: (row.verdict as "up" | "down" | null) ?? null,
      comment: (row.comment as string | null) ?? null,
    });
  });

  return (
    <main className={styles.main}>
      <header className={styles.header}>
        <h1 className={styles.title}>{lang === "sk" ? "SAFE Interaktívna správa" : "SAFE Interactive Report"}</h1>
        <p className={styles.meta}>
          {lang === "sk" ? "Vlna" : "Wave"} {payload.wave}
          {payload.period_label ? ` (${payload.period_label})` : ""}
        </p>
      </header>

      {payload.exec_summary.length > 0 && (
        <section className={styles.execCard}>
          <h2>{lang === "sk" ? "Zhrnutie" : "Executive summary"}</h2>
          <ul>
            {payload.exec_summary.map((item, idx) => (
              <li key={`${item.section_id}-${idx}`}>{item.bullet}</li>
            ))}
          </ul>
        </section>
      )}

      <div className={styles.sections}>
        {payload.sections.map((section) => {
          const initial = feedbackBySection.get(section.section_id);
          const panelCol = section.chart?.panel_col ?? null;
          const bestPanel = section.chart?.best_panel;
          const chartRows = panelCol && bestPanel !== undefined && bestPanel !== null
            ? section.chart.records.filter((row) => String(row[panelCol] ?? "") === String(bestPanel))
            : section.chart.records;
          return (
            <article key={section.section_id} className={styles.sectionCard}>
              <h3 className={styles.sectionFinding}>{section.finding}</h3>
              <p className={styles.sectionTitle}>{section.title}</p>
              <ul className={styles.bulletList}>
                {section.bullets.map((b, idx) => (
                  <li key={`${section.section_id}-${idx}`}>{b}</li>
                ))}
              </ul>

              {section.chart?.records?.length > 0 && section.chart.value_col && section.chart.series_col && (
                <SimpleLineChart
                  rows={chartRows}
                  valueCol={section.chart.value_col}
                  seriesCol={section.chart.series_col}
                />
              )}

              <SectionFeedback
                waveNumber={payload.wave}
                sectionId={section.section_id}
                language={lang}
                initial={{
                  verdict: initial?.verdict ?? null,
                  comment: initial?.comment ?? "",
                }}
                labels={{
                  title: lang === "sk" ? "Spätná väzba k sekcii" : "Section feedback",
                  like: lang === "sk" ? "Páči sa" : "Like",
                  dislike: lang === "sk" ? "Nepáči sa" : "Dislike",
                  comment: lang === "sk" ? "Komentár (voliteľné)" : "Comment (optional)",
                  submit: lang === "sk" ? "Uložiť" : "Save",
                  saving: lang === "sk" ? "Ukladám..." : "Saving...",
                  saved: lang === "sk" ? "Uložené" : "Saved",
                  error: t.somethingWentWrong,
                }}
              />
            </article>
          );
        })}
      </div>
    </main>
  );
}
