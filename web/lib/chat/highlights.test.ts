import { describe, expect, it, vi } from "vitest";
import { fetchTrendingHighlights } from "./highlights";
import type { DuckDBConnection } from "@duckdb/node-api";

function fakeConnection(columns: string[], rows: unknown[][]): DuckDBConnection {
  return {
    runAndReadAll: vi.fn().mockResolvedValue({
      columnNames: () => columns,
      getRowsJS: () => rows,
    }),
  } as unknown as DuckDBConnection;
}

describe("fetchTrendingHighlights", () => {
  it("skips a metric when either wave's value is null (does not fabricate a delta)", async () => {
    const columns = ["wave_number", "bank_loan_rej_rate", "turnover_nb"];
    // wave 38 (latest) has null rejection rate — must not be reported as 0 or as a fake delta.
    const rows = [
      [38, null, 10.5],
      [37, 26.63, 5.2],
    ];
    const con = fakeConnection(columns, rows);
    const highlights = await fetchTrendingHighlights(con);
    expect(highlights.some((h) => h.label === "Bank loan rejection rate")).toBe(false);
    expect(highlights.some((h) => h.label === "Turnover")).toBe(true);
  });

  it("computes a correct delta when both waves have real values", async () => {
    const columns = ["wave_number", "turnover_nb"];
    const rows = [
      [38, 10.5],
      [37, 5.2],
    ];
    const con = fakeConnection(columns, rows);
    const highlights = await fetchTrendingHighlights(con);
    const turnover = highlights.find((h) => h.label === "Turnover");
    expect(turnover?.deltaPp).toBeCloseTo(5.3, 5);
    expect(turnover?.latest).toBe(10.5);
  });

  it("returns [] when fewer than 2 waves are available", async () => {
    const con = fakeConnection(["wave_number", "turnover_nb"], [[38, 10.5]]);
    expect(await fetchTrendingHighlights(con)).toEqual([]);
  });

  it("returns [] on query failure rather than throwing", async () => {
    const con = { runAndReadAll: vi.fn().mockRejectedValue(new Error("connection lost")) } as unknown as DuckDBConnection;
    expect(await fetchTrendingHighlights(con)).toEqual([]);
  });
});
