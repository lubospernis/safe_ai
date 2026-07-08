// Throwaway smoke test — confirms @duckdb/node-api's native binding bundles and
// connects to MotherDuck correctly on Vercel's Node.js serverless runtime.
// Delete this route once web/lib/chat/duckdb.ts is fully verified in production.

import { NextResponse } from "next/server";
import { getConnection } from "@/lib/chat/duckdb";
import type { DuckDBConnection } from "@duckdb/node-api";

export const runtime = "nodejs";
export const maxDuration = 30;

export async function GET() {
  let connection: DuckDBConnection | undefined;
  try {
    connection = await getConnection();
    const reader = await connection.runAndReadAll(
      "SELECT wave_number, turnover_nb FROM main_safe.mart_safe__slovakia_kpis ORDER BY wave_number DESC LIMIT 1",
    );
    const columns = reader.columnNames();
    const rows = reader.getRowsJS();
    return NextResponse.json({ ok: true, columns, rows });
  } catch (err) {
    return NextResponse.json(
      { ok: false, error: err instanceof Error ? (err.stack ?? err.message) : String(err) },
      { status: 500 },
    );
  } finally {
    connection?.closeSync();
  }
}
