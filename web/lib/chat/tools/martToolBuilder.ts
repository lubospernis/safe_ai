// Builds a Mistral function-calling Tool + its executor from a MartToolSpec.
// The model supplies typed, validated arguments (countries, wave range,
// firm_size, dimension codes) — this module is the only thing that ever
// writes the actual SQL, so a hallucinated column name is no longer possible
// for any mart with a spec here.

import type { Tool } from "@mistralai/mistralai/models/components";
import { runQueryTool, type QueryToolResult } from "../duckdb";
import type { DuckDBConnection } from "@duckdb/node-api";
import { isAllowedCountry, type MartToolSpec } from "./martToolSpecs";

const DEFAULT_COUNTRIES = ["SK", "EA", "DE"];

export interface BuiltMartTool {
  tool: Tool & { type: "function" };
  /** Builds and runs the SQL for this tool call. Never throws — validation
   * failures come back as a QueryToolResult error, same shape as a failed
   * query_mart call, so the agent loop can treat every tool uniformly. */
  run: (args: Record<string, unknown>, con: DuckDBConnection) => Promise<QueryToolResult & { sql?: string }>;
}

function quoteList(values: string[]): string {
  return values.map((v) => `'${v.replace(/'/g, "''")}'`).join(", ");
}

export function buildMartTool(spec: MartToolSpec): BuiltMartTool {
  const properties: Record<string, unknown> = {
    countries: {
      type: "array",
      items: { type: "string" },
      description: `Country codes to include, e.g. ["SK","EA"]. Defaults to ${JSON.stringify(DEFAULT_COUNTRIES)} if omitted.`,
    },
    waveFrom: { type: "integer", description: "First wave number to include (inclusive). Omit for no lower bound." },
    waveTo: { type: "integer", description: "Last wave number to include (inclusive). Omit for no upper bound." },
    lastNWaves: {
      type: "integer",
      description: "Alternative to waveFrom/waveTo: only the most recent N waves. Do not combine with waveFrom/waveTo.",
    },
  };
  const required: string[] = [];

  if (spec.firmSizeValues) {
    properties.firmSize = {
      type: "string",
      enum: spec.firmSizeValues,
      description: `Firm size filter. One of: ${spec.firmSizeValues.join(", ")}. Defaults to "all" if omitted.`,
    };
  }

  for (const dim of spec.dimensions) {
    properties[dim.param] = {
      type: "string",
      enum: dim.codes.map((c) => c.code),
      description:
        `${dim.column} code. Valid values: ` + dim.codes.map((c) => `${c.code}=${c.label}`).join("; ") + ".",
    };
    if (dim.required) required.push(dim.param);
  }

  const tool: Tool & { type: "function" } = {
    type: "function",
    function: {
      name: spec.toolName,
      description: spec.description,
      parameters: { type: "object", properties, required },
    },
  };

  async function run(args: Record<string, unknown>, con: DuckDBConnection): Promise<QueryToolResult & { sql?: string }> {
    if (args.waveFrom !== undefined || args.waveTo !== undefined) {
      if (args.lastNWaves !== undefined) {
        return { ok: false, error: "ERROR: supply either waveFrom/waveTo or lastNWaves, not both." };
      }
    }

    const rawCountries = Array.isArray(args.countries) ? (args.countries as unknown[]).map(String) : DEFAULT_COUNTRIES;
    const countries = rawCountries.length > 0 ? rawCountries : DEFAULT_COUNTRIES;
    const badCountries = countries.filter((c) => !isAllowedCountry(c));
    if (badCountries.length > 0) {
      return { ok: false, error: `ERROR: unrecognized country code(s): ${badCountries.join(", ")}` };
    }

    const where: string[] = [`country_code IN (${quoteList(countries)})`];

    if (spec.firmSizeValues) {
      const firmSize = typeof args.firmSize === "string" ? args.firmSize : "all";
      if (!spec.firmSizeValues.includes(firmSize as never)) {
        return {
          ok: false,
          error: `ERROR: invalid firmSize "${firmSize}" — must be one of: ${spec.firmSizeValues.join(", ")}`,
        };
      }
      where.push(`firm_size = '${firmSize}'`);
    }

    for (const dim of spec.dimensions) {
      const value = args[dim.param];
      if (value === undefined || value === null || value === "") {
        if (dim.required) {
          return {
            ok: false,
            error: `ERROR: ${dim.param} is required — one of: ${dim.codes.map((c) => c.code).join(", ")}`,
          };
        }
        continue;
      }
      const codeStr = String(value);
      if (!dim.codes.some((c) => c.code === codeStr)) {
        return {
          ok: false,
          error: `ERROR: invalid ${dim.param} "${codeStr}" — must be one of: ${dim.codes.map((c) => c.code).join(", ")}`,
        };
      }
      where.push(`${dim.column} = '${codeStr.replace(/'/g, "''")}'`);
    }

    where.push(...spec.extraWhere);

    if (args.lastNWaves !== undefined) {
      const n = Number(args.lastNWaves);
      if (!Number.isInteger(n) || n <= 0) {
        return { ok: false, error: "ERROR: lastNWaves must be a positive integer." };
      }
      where.push(
        `wave_number > (SELECT MAX(wave_number) FROM main_safe.${spec.martTable}) - ${n}`,
      );
    } else {
      if (args.waveFrom !== undefined) {
        const n = Number(args.waveFrom);
        if (!Number.isInteger(n)) return { ok: false, error: "ERROR: waveFrom must be an integer." };
        where.push(`wave_number >= ${n}`);
      }
      if (args.waveTo !== undefined) {
        const n = Number(args.waveTo);
        if (!Number.isInteger(n)) return { ok: false, error: "ERROR: waveTo must be an integer." };
        where.push(`wave_number <= ${n}`);
      }
    }

    const sql =
      `SELECT ${spec.selectColumns.join(", ")}\n` +
      `FROM main_safe.${spec.martTable}\n` +
      `WHERE ${where.join("\n  AND ")}\n` +
      `ORDER BY ${spec.orderBy}`;

    const result = await runQueryTool(sql, con);
    return { ...result, sql };
  }

  return { tool, run };
}

