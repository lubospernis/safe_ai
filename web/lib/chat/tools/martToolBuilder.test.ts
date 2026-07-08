import { describe, expect, it, vi } from "vitest";
import type { DuckDBConnection } from "@duckdb/node-api";
import { buildMartTool } from "./martToolBuilder";
import { BUSINESS_PROBLEMS_SPEC, FINANCING_CONDITIONS_SPEC, MART_TOOL_SPECS } from "./martToolSpecs";
import { validateSql } from "../whitelist";

function fakeConnection(columns: string[] = [], rows: unknown[][] = []): DuckDBConnection {
  return {
    runAndReadAll: vi.fn().mockResolvedValue({
      columnNames: () => columns,
      getRowsJS: () => rows,
    }),
  } as unknown as DuckDBConnection;
}

describe("buildMartTool — business_problems", () => {
  const { run } = buildMartTool(BUSINESS_PROBLEMS_SPEC);

  it("builds default SQL (no args) with default countries and firm_size", async () => {
    const con = fakeConnection(["problem_label", "avg_pressingness_wtd"], []);
    const result = await run({}, con);
    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.sql).toContain("country_code IN ('SK', 'EA', 'DE')");
      expect(result.sql).toContain("firm_size = 'all'");
      expect(result.sql).not.toContain("problem_id =");
      expect(result.sql).not.toContain("reference_period");
    }
  });

  it("builds SQL with an explicit problemId and firmSize", async () => {
    const con = fakeConnection(["problem_label", "avg_pressingness_wtd"], []);
    const result = await run({ problemId: "4", firmSize: "sme", countries: ["SK"] }, con);
    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.sql).toContain("problem_id = '4'");
      expect(result.sql).toContain("firm_size = 'sme'");
      expect(result.sql).toContain("country_code IN ('SK')");
    }
  });

  it("rejects an invalid problemId", async () => {
    const con = fakeConnection();
    const result = await run({ problemId: "99" }, con);
    expect(result.ok).toBe(false);
  });

  it("rejects an unrecognized country code", async () => {
    const con = fakeConnection();
    const result = await run({ countries: ["ZZ"] }, con);
    expect(result.ok).toBe(false);
  });

  it("rejects waveFrom/waveTo combined with lastNWaves", async () => {
    const con = fakeConnection();
    const result = await run({ waveFrom: 30, lastNWaves: 4 }, con);
    expect(result.ok).toBe(false);
  });

  it("builds a lastNWaves filter", async () => {
    const con = fakeConnection(["problem_label", "avg_pressingness_wtd"], []);
    const result = await run({ lastNWaves: 4 }, con);
    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.sql).toContain("wave_number > (SELECT MAX(wave_number) FROM main_safe.mart_safe__business_problems) - 4");
    }
  });
});

describe("buildMartTool — financing_conditions", () => {
  const { run } = buildMartTool(FINANCING_CONDITIONS_SPEC);

  it("requires questionId", async () => {
    const con = fakeConnection();
    const result = await run({}, con);
    expect(result.ok).toBe(false);
    if (!result.ok) expect(result.error).toContain("questionId");
  });

  it("builds SQL with questionId and subItem", async () => {
    const con = fakeConnection(["net_balance_wtd"], []);
    const result = await run({ questionId: "q10", subItem: "a", firmSize: "all" }, con);
    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.sql).toContain("question_id = 'q10'");
      expect(result.sql).toContain("sub_item = 'a'");
    }
  });

  it("rejects an invalid questionId", async () => {
    const con = fakeConnection();
    const result = await run({ questionId: "q99" }, con);
    expect(result.ok).toBe(false);
  });
});

describe("every mart tool spec generates SQL that passes validateSql", () => {
  for (const spec of MART_TOOL_SPECS) {
    it(`${spec.toolName} produces whitelisted SQL`, async () => {
      const { run } = buildMartTool(spec);
      const con = fakeConnection([], []);
      const args: Record<string, unknown> = {};
      for (const dim of spec.dimensions) {
        if (dim.required) args[dim.param] = dim.codes[0].code;
      }
      const result = await run(args, con);
      expect(result.ok).toBe(true);
      if (result.ok && result.sql) {
        expect(validateSql(result.sql)).toEqual({ ok: true });
      }
    });
  }
});

describe("regression: financing_purpose reference_period bug", () => {
  it("business_problems generated SQL never references reference_period", async () => {
    const { run } = buildMartTool(BUSINESS_PROBLEMS_SPEC);
    const con = fakeConnection([], []);
    const result = await run({}, con);
    expect(result.ok).toBe(true);
    if (result.ok) expect(result.sql).not.toContain("reference_period");
  });
});
