import { describe, expect, it } from "vitest";
import { validateSql } from "./whitelist";

describe("validateSql", () => {
  it("allows a SELECT against a whitelisted mart table", () => {
    const result = validateSql(
      "SELECT wave_number, net_balance_wtd FROM main_safe.mart_safe__financing_conditions WHERE country_code = 'SK'",
    );
    expect(result.ok).toBe(true);
  });

  it("rejects an empty query", () => {
    const result = validateSql("");
    expect(result).toEqual({ ok: false, error: "Empty SQL." });
  });

  it("rejects a write keyword", () => {
    const result = validateSql("DELETE FROM main_safe.mart_safe__slovakia_kpis WHERE wave_number = 1");
    expect(result.ok).toBe(false);
  });

  it("rejects a table not in the whitelist", () => {
    const result = validateSql("SELECT * FROM main_safe.mart_safe__not_a_real_table");
    expect(result.ok).toBe(false);
    if (!result.ok) expect(result.error).toContain("not_a_real_table");
  });

  it("rejects a multi-statement payload", () => {
    const result = validateSql(
      "SELECT 1 FROM main_safe.mart_safe__slovakia_kpis; DROP TABLE main_safe.mart_safe__slovakia_kpis",
    );
    expect(result.ok).toBe(false);
  });

  it("rejects a query containing ATTACH", () => {
    const result = validateSql("ATTACH 'md:some_other_db' AS other; SELECT 1 FROM other.t");
    expect(result.ok).toBe(false);
  });

  it("rejects oversized SQL", () => {
    const sql = "SELECT 1 FROM main_safe.mart_safe__slovakia_kpis WHERE " + "1=1 AND ".repeat(1000);
    const result = validateSql(sql);
    expect(result.ok).toBe(false);
  });

  it("allows a query against int_safe__core_questions_long", () => {
    const result = validateSql(
      "SELECT wave_number FROM main_safe.int_safe__core_questions_long WHERE wave_number < 30",
    );
    expect(result.ok).toBe(true);
  });
});
