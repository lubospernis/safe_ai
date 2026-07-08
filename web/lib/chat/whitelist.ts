// SQL safety guards for the chat agent's query_mart tool. Ported from
// reports/db.py (ALLOWED_MART_TABLES, _WRITE_RE, _run_query_tool's validation),
// with additional hardening because this surface takes arbitrary user prompts —
// the Python version only ever sees SQL the LLM wrote against a fixed set of
// report sections, never an adversarial or exploratory end-user question.

// Source of truth: reports/db.py::ALLOWED_MART_TABLES — keep in sync manually.
export const ALLOWED_MART_TABLES = new Set([
  "mart_safe__financing_conditions",
  "mart_safe__financing_purpose",
  "mart_safe__slovakia_kpis",
  "mart_safe__business_problems",
  "mart_safe__financing_factors",
  "mart_safe__loan_applications",
  "mart_safe__business_situation",
  "mart_safe__outlook",
  "mart_safe__availability_expectations",
  "mart_safe__expectations",
  "mart_safe__survey_participants",
  "mart_safe__question_coverage",
  "mart_safe__adhoc_responses",
  "int_safe__core_questions_long",
]);

const WRITE_RE = /\b(INSERT|UPDATE|DELETE|DROP|CREATE|COPY|TRUNCATE|ALTER)\b/i;

// Beyond the Python guard: block statement-level and session-level commands that
// don't write data but could still be abused (attaching another database, changing
// session config, installing extensions) — none of these are needed for read-only
// mart queries, so blocking them outright is free.
const DANGEROUS_KEYWORD_RE = /\b(ATTACH|DETACH|PRAGMA|SET|EXPORT|IMPORT|INSTALL|LOAD)\b/i;

// Reject a second statement stacked after the first via `;` — the Python version
// doesn't need this (report SQL is fixed, never concatenated from user input), but
// a user-driven surface must not let a validated SELECT smuggle a second statement.
const MULTI_STATEMENT_RE = /;\s*\S/;

const MAX_SQL_LENGTH = 4000;
const TABLE_NAME_RE = /(mart_safe__\w+|int_safe__\w+)/g;

export type ValidationResult = { ok: true } | { ok: false; error: string };

export function validateSql(sql: string): ValidationResult {
  if (!sql || !sql.trim()) {
    return { ok: false, error: "Empty SQL." };
  }
  if (sql.length > MAX_SQL_LENGTH) {
    return { ok: false, error: `SQL exceeds max length of ${MAX_SQL_LENGTH} characters.` };
  }
  if (WRITE_RE.test(sql)) {
    return { ok: false, error: "Only SELECT queries are permitted." };
  }
  if (DANGEROUS_KEYWORD_RE.test(sql)) {
    return { ok: false, error: "Query contains a disallowed keyword (session/attach/extension command)." };
  }
  if (MULTI_STATEMENT_RE.test(sql)) {
    return { ok: false, error: "Multi-statement queries are not permitted." };
  }

  const referenced = new Set(Array.from(sql.matchAll(TABLE_NAME_RE), (m) => m[1]));
  const disallowed = Array.from(referenced).filter((t) => !ALLOWED_MART_TABLES.has(t));
  if (disallowed.length > 0) {
    return { ok: false, error: `Table(s) not in whitelist: ${disallowed.join(", ")}` };
  }

  return { ok: true };
}
