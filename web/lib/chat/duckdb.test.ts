import { describe, expect, it, vi } from "vitest";
import type { DuckDBConnection } from "@duckdb/node-api";
import { fetchTableForDisplay, runQueryTool } from "./duckdb";

function fakeConnection(columns: string[], rows: unknown[][]): DuckDBConnection {
  return {
    runAndReadAll: vi.fn().mockResolvedValue({
      columnNames: () => columns,
      getRowsJS: () => rows,
    }),
  } as unknown as DuckDBConnection;
}

// Regression tests: a real production bug. DuckDB's getRowsJS() returns
// bigint for columns like wave_number/n_respondents — NextResponse.json()
// throws "Do not know how to serialize a BigInt" if one reaches it
// unconverted, which happened on every successful structured-tool query
// (wave_number is in nearly every SELECT).
describe("runQueryTool bigint handling", () => {
  it("converts bigint values to Number in returned rows", async () => {
    const con = fakeConnection(
      ["wave_number", "problem_label"],
      [[BigInt(38), "Costs of production or labour"]],
    );
    const result = await runQueryTool("SELECT wave_number, problem_label FROM main_safe.mart_safe__business_problems", con);
    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.rows[0][0]).toBe(38);
      expect(typeof result.rows[0][0]).toBe("number");
      expect(() => JSON.stringify(result.rows)).not.toThrow();
    }
  });
});

describe("fetchTableForDisplay bigint handling", () => {
  it("converts bigint values to Number in returned rows", async () => {
    const con = fakeConnection(["wave_number"], [[BigInt(38)]]);
    const result = await fetchTableForDisplay(
      "SELECT wave_number FROM main_safe.mart_safe__business_problems",
      con,
    );
    expect(result).not.toBeNull();
    expect(result?.rows[0][0]).toBe(38);
    expect(() => JSON.stringify(result)).not.toThrow();
  });
});
