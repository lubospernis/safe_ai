// Mart schema catalogue injected into the chat agent's system prompt, so the
// model knows table/column names without a "list tables" tool call. Ported from
// reports/db.py::build_mart_catalogue.
//
// KNOWN RISK (see plan): dbt_project/models/marts/schema.yml lives outside web/.
// This is reachable via a relative path in local dev; whether Next's Vercel build
// output-tracing bundles a file outside the app directory into the deployed
// function has not yet been confirmed against a real deploy. If it isn't
// reachable there, this needs a prebuild step that copies/inlines the parsed
// catalogue into a generated file inside web/ instead of reading the YAML live.

import { readFileSync } from "node:fs";
import { join } from "node:path";
import { parse as parseYaml } from "yaml";
import type { DuckDBConnection } from "@duckdb/node-api";

const SCHEMA_YML_PATH = join(process.cwd(), "..", "dbt_project", "models", "marts", "schema.yml");

interface DbtColumn {
  name: string;
}
interface DbtModel {
  name: string;
  columns?: DbtColumn[];
}
interface DbtSchema {
  models?: DbtModel[];
}

let cachedCatalogue: string | null = null;

/** Builds the compact mart catalogue string, verified against the live DB
 * (skips any mart whose table isn't actually queryable, in case schema.yml has
 * drifted from the real database). Cached for the process lifetime — schema.yml
 * doesn't change at runtime, so there's no reason to re-read/re-verify per request. */
export async function buildMartCatalogue(con: DuckDBConnection, schema: string): Promise<string> {
  if (cachedCatalogue) return cachedCatalogue;

  const raw = readFileSync(SCHEMA_YML_PATH, "utf-8");
  const dbtSchema = parseYaml(raw) as DbtSchema;

  const lines: string[] = [
    "Available mart tables (all contain only 3m reference period data, waves 30+).",
    "Default filters: WHERE firm_size = 'all' AND country_code IN ('SK','EA','DE').",
    "EXCEPTION: mart_safe__financing_purpose and mart_safe__business_problems keep",
    "  both periods — always add: AND reference_period = '3m' for those two tables.",
    "",
  ];

  for (const model of dbtSchema.models ?? []) {
    if (!model.name.startsWith("mart_safe__")) continue;
    const fullName = `${schema}.${model.name}`;
    try {
      await con.run(`SELECT 1 FROM ${fullName} LIMIT 1`);
    } catch {
      continue;
    }
    const cols = (model.columns ?? []).map((c) => c.name);
    lines.push(fullName);
    for (let i = 0; i < cols.length; i += 6) {
      lines.push("  " + cols.slice(i, i + 6).join(", "));
    }
    lines.push("");
  }

  lines.push(
    "main_safe.int_safe__core_questions_long",
    "  permid, wave_number, country_code, question_id, sub_item, response_raw,",
    "  response_rec, response_3m, weight_common, is_nonresponse, employee_band_code, is_sme",
    "  (26M rows — use ONLY for pre-wave-30 history or raw microdata drill-downs)",
  );

  cachedCatalogue = lines.join("\n");
  return cachedCatalogue;
}
