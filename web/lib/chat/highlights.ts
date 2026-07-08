// Server-side "what's notable right now" highlights for the chat intro screen —
// computed live from mart_safe__slovakia_kpis so the suggested starting points
// stay accurate wave to wave instead of going stale like a hardcoded example.

import type { DuckDBConnection } from "@duckdb/node-api";

export interface Highlight {
  label: string;
  deltaPp: number;
  latest: number;
}

interface KpiMetric {
  column: string;
  label: string;
  // true = a rising value is the adverse direction (e.g. rejection rate, labour costs)
  higherIsAdverse: boolean;
}

// A representative subset of mart_safe__slovakia_kpis's headline columns —
// see .claude/CLAUDE.md's mart catalogue for the full column list and sign conventions.
const METRICS: KpiMetric[] = [
  { column: "bank_loan_gap", label: "Bank loan financing gap (need − availability)", higherIsAdverse: true },
  { column: "q10a_interest_nb", label: "Bank loan interest rate terms", higherIsAdverse: true },
  { column: "turnover_nb", label: "Turnover", higherIsAdverse: false },
  { column: "profit_nb", label: "Profit", higherIsAdverse: false },
  { column: "labour_cost_nb", label: "Labour costs", higherIsAdverse: true },
  { column: "investment_nb", label: "Investment", higherIsAdverse: false },
  { column: "bank_loan_rej_rate", label: "Bank loan rejection rate", higherIsAdverse: true },
  { column: "bank_loan_disc_rate", label: "Loan application discouragement rate", higherIsAdverse: true },
];

/** Returns up to `limit` metrics with the largest wave-over-wave change,
 * for surfacing as conversation starters. Returns [] on any failure — the
 * intro screen just omits the "trending now" section rather than blocking. */
export async function fetchTrendingHighlights(con: DuckDBConnection, limit = 3): Promise<Highlight[]> {
  try {
    const cols = METRICS.map((m) => m.column).join(", ");
    const reader = await con.runAndReadAll(
      `SELECT wave_number, ${cols} FROM main_safe.mart_safe__slovakia_kpis ORDER BY wave_number DESC LIMIT 2`,
    );
    const rows = reader.getRowsJS();
    if (rows.length < 2) return [];

    const columnNames = reader.columnNames();
    const colIndex = (name: string) => columnNames.indexOf(name);
    const latestRow = rows[0];
    const priorRow = rows[1];

    const highlights: Highlight[] = [];
    for (const metric of METRICS) {
      const idx = colIndex(metric.column);
      if (idx === -1) continue;
      const latestRaw = latestRow[idx];
      const priorRaw = priorRow[idx];
      // Explicit null/undefined check before Number() — Number(null) is 0, which
      // would silently fabricate a "0%" or a fake delta for a metric with no
      // data this wave (this happened for bank_loan_rej_rate in real MotherDuck
      // data, e.g. wave 38 has no rejection-rate figure at all yet).
      if (latestRaw === null || latestRaw === undefined || priorRaw === null || priorRaw === undefined) continue;
      const latest = Number(latestRaw);
      const prior = Number(priorRaw);
      if (Number.isNaN(latest) || Number.isNaN(prior)) continue;
      const deltaPp = latest - prior;
      highlights.push({ label: metric.label, deltaPp, latest });
    }

    highlights.sort((a, b) => Math.abs(b.deltaPp) - Math.abs(a.deltaPp));
    return highlights.slice(0, limit);
  } catch {
    return [];
  }
}
