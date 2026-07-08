// MotherDuck connection + validated query execution for the chat agent's
// query_mart tool. Ported from reports/db.py::_get_connection / _run_query_tool,
// built on @duckdb/node-api (see web/app/api/smoke-test/route.ts and
// web/next.config.ts's serverExternalPackages for why this package, and the
// bundling fix required to use it on Next/Vercel).

import { DuckDBConnection, DuckDBInstance } from "@duckdb/node-api";
import { validateSql } from "./whitelist";

const MAX_TOOL_ROWS = 30; // rows shown to the LLM — keep small to bound token cost
const MAX_TABLE_ROWS = 200; // rows shown to the user in the UI table
const QUERY_TIMEOUT_MS = 15_000;

export async function getConnection(): Promise<DuckDBConnection> {
  const token = process.env.MOTHERDUCK_TOKEN;
  if (!token) {
    throw new Error("MOTHERDUCK_TOKEN env var not set");
  }
  const instance = await DuckDBInstance.create(`md:my_db?motherduck_token=${token}`);
  return DuckDBConnection.create(instance);
}

function withTimeout<T>(promise: Promise<T>, ms: number, label: string): Promise<T> {
  return Promise.race([
    promise,
    new Promise<T>((_, reject) =>
      setTimeout(() => reject(new Error(`${label} timed out after ${ms}ms`)), ms),
    ),
  ]);
}

function toMarkdownTable(columns: string[], rows: unknown[][], truncated: boolean): string {
  if (rows.length === 0) return "Query returned 0 rows.";
  const header = `| ${columns.join(" | ")} |`;
  const divider = `| ${columns.map(() => "---").join(" | ")} |`;
  const body = rows.map((r) => `| ${r.map((v) => String(v ?? "")).join(" | ")} |`);
  let out = [header, divider, ...body].join("\n");
  if (truncated) out += `\n\n_(truncated to ${MAX_TOOL_ROWS} rows)_`;
  return out;
}

export type QueryToolResult =
  | { ok: true; markdown: string; columns: string[]; rows: unknown[][] }
  | { ok: false; error: string };

/** Validates and executes a tool-use SQL query. Row-capped and token-cheap —
 * used for the LLM-facing tool_result text during the agentic loop. */
export async function runQueryTool(sql: string, con: DuckDBConnection): Promise<QueryToolResult> {
  const validation = validateSql(sql);
  if (!validation.ok) {
    return { ok: false, error: `ERROR: ${validation.error}` };
  }
  try {
    const reader = await withTimeout(con.runAndReadAll(sql), QUERY_TIMEOUT_MS, "Query execution");
    const columns = reader.columnNames();
    const allRows = reader.getRowsJS();
    const truncated = allRows.length > MAX_TOOL_ROWS;
    const rows = allRows.slice(0, MAX_TOOL_ROWS);
    return { ok: true, markdown: toMarkdownTable(columns, rows, truncated), columns, rows };
  } catch (err) {
    return { ok: false, error: `ERROR executing query: ${err instanceof Error ? err.message : String(err)}` };
  }
}

/** Re-runs the same already-validated final query to fetch more rows for the
 * user-facing table, since the UI reader isn't token-cost-constrained the way
 * the LLM's context is. Returns null on any failure (UI just omits the table). */
export async function fetchTableForDisplay(
  sql: string,
  con: DuckDBConnection,
): Promise<{ columns: string[]; rows: unknown[][] } | null> {
  const validation = validateSql(sql);
  if (!validation.ok) return null;
  try {
    const reader = await withTimeout(con.runAndReadAll(sql), QUERY_TIMEOUT_MS, "Query execution");
    const columns = reader.columnNames();
    const rows = reader.getRowsJS().slice(0, MAX_TABLE_ROWS);
    return { columns, rows };
  } catch {
    return null;
  }
}
