// Throwaway smoke test — confirms @duckdb/node-api's native binding bundles and
// connects to MotherDuck correctly on Vercel's Node.js serverless runtime.
// Delete this route once web/lib/chat/duckdb.ts is built and this has been verified.
//
// STATUS: passed locally (see plan file). Still recommended: a real Vercel
// preview deploy hitting this route before considering the DuckDB approach
// fully de-risked for production.

import { DuckDBConnection, DuckDBInstance } from "@duckdb/node-api";
import { NextResponse } from "next/server";

export const runtime = "nodejs";
export const maxDuration = 30;

export async function GET() {
  const token = process.env.MOTHERDUCK_TOKEN;
  if (!token) {
    return NextResponse.json(
      { ok: false, error: "MOTHERDUCK_TOKEN env var not set" },
      { status: 500 },
    );
  }

  let connection: DuckDBConnection | undefined;
  try {
    const instance = await DuckDBInstance.create(`md:my_db?motherduck_token=${token}`);
    connection = await DuckDBConnection.create(instance);
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
