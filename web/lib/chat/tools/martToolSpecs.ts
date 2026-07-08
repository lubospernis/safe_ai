// Declarative specs for structured, purpose-built query tools — one per mart.
// Ground truth for every field below is the mart's actual .sql model under
// dbt_project/models/marts/ (NOT schema.yml or .claude/CLAUDE.md, both of
// which have been found stale in places) — re-verify against the .sql file
// whenever a mart changes.
//
// Why this exists: the chat agent's original design offered only one generic
// tool (query_mart: "write a SELECT yourself"), guided by prose in the system
// prompt. That repeatedly caused the model to hallucinate column names (e.g.
// writing "survey_period" when the real column is "reference_period", or
// inventing a reference_period filter for a mart that has no such column at
// all). A structured tool takes typed arguments and builds the exact,
// pre-verified SQL itself — the model can no longer misname a column for
// whatever mart has a spec here.

export type FirmSizeValue = "all" | "sme" | "large";

export interface DimensionCode {
  code: string;
  label: string;
}

export interface DimensionSpec {
  /** SQL column name, e.g. "sub_item", "question_id", "problem_id". */
  column: string;
  /** Parameter name exposed to the model, e.g. "subItem", "questionId". */
  param: string;
  /** Whether the model must always supply this dimension (true for
   * financing_conditions' question_id, since sub_item codes are ambiguous
   * without knowing which question they belong to). */
  required: boolean;
  codes: DimensionCode[];
}

export interface MartToolSpec {
  /** Function name exposed to the model, e.g. "get_business_problems". */
  toolName: string;
  /** Tool-calling description — when to use this vs. other tools. */
  description: string;
  martTable: string;
  /** Columns to project in the SELECT, in order. */
  selectColumns: string[];
  /** Valid firm_size values for this mart, or null if the mart has no firm_size dimension. */
  firmSizeValues: FirmSizeValue[] | null;
  /** Filterable dimension columns beyond country/wave/firm_size (0, 1, or 2 of them). */
  dimensions: DimensionSpec[];
  /** Extra WHERE clauses always applied, verified against the mart's actual behavior
   * (e.g. business_problems bakes reference_period='3m' into the mart itself already —
   * this is for anything a caller still needs to add, which today is nothing for
   * either spec below; kept for future marts that might need one). */
  extraWhere: string[];
  orderBy: string;
}

const ALLOWED_COUNTRIES = new Set(["SK", "EA", "DE", "AT", "CZ", "HU", "PL", "FR", "IT", "ES", "NL", "BE"]);

export function isAllowedCountry(code: string): boolean {
  return ALLOWED_COUNTRIES.has(code);
}

// Verified against dbt_project/models/marts/mart_safe__business_problems.sql:
// firm_size IS a plain 'all'/'sme' column built directly by the mart (not
// employee_band_code/firm_size_en as earlier, unverified notes assumed — the
// mart's own CTEs already do that mapping and expose a clean 'all'/'sme'
// column). reference_period='3m' is filtered INSIDE the mart's source CTEs
// and never appears in the final output columns — callers must NOT filter on
// it; doing so is exactly the bug that motivated this file (the model was
// told to add "AND reference_period = '3m'" and it doesn't exist in the
// output, so it started guessing at plausible-sounding alternative names).
export const BUSINESS_PROBLEMS_SPEC: MartToolSpec = {
  toolName: "get_business_problems",
  description:
    "Get pressingness scores (1-10 scale) for the 7 business problems SAFE asks about " +
    "(finding customers, competition, access to finance, costs, skilled staff, regulation, other), " +
    "by country and wave. Use this for any question about which problems are most/least pressing " +
    "for firms, or how a problem's pressingness has changed over time.",
  martTable: "mart_safe__business_problems",
  selectColumns: ["wave_number", "country_code", "firm_size", "problem_label", "avg_pressingness_wtd", "n_respondents"],
  firmSizeValues: ["all", "sme"],
  dimensions: [
    {
      column: "problem_id",
      param: "problemId",
      required: false,
      codes: [
        { code: "1", label: "Finding customers" },
        { code: "2", label: "Competition" },
        { code: "3", label: "Access to finance" },
        { code: "4", label: "Costs of production or labour" },
        { code: "5", label: "Availability of skilled staff or experienced managers" },
        { code: "6", label: "Regulation" },
        { code: "7", label: "Other" },
      ],
    },
  ],
  extraWhere: [],
  orderBy: "wave_number, country_code, firm_size, problem_id",
};

// Verified against dbt_project/models/marts/financing_conditions/mart_safe__financing_conditions.sql:
// firm_size is 'all'/'sme'/'large' (large = NOT is_sme, i.e. employee_band_code
// 4). No reference_period column or filter needed — the mart already restricts
// to wave_number >= 30 and response_3m internally. question_id/sub_item codes
// and labels below are taken verbatim from the mart's own header comment and
// its "labeled" CTE's CASE statements, not from CLAUDE.md. question_id is
// REQUIRED because sub_item codes are reused with different meanings across
// Q5/Q9 vs Q10 (e.g. sub_item 'a' = bank loans for Q5/Q9 but "interest rates"
// for Q10) — a caller must disambiguate which question a sub_item refers to.
export const FINANCING_CONDITIONS_SPEC: MartToolSpec = {
  toolName: "get_financing_conditions",
  description:
    "Get ECB-methodology net balances for financing need (Q5), availability (Q9), and bank loan " +
    "terms (Q10), by country, wave, and instrument. Positive net balance for Q5 = more firms report " +
    "increased need; for Q9 = availability improved; for Q10 = bank tightened terms (adverse). " +
    "Use this for any question about credit demand, credit availability, the financing gap, or " +
    "interest rates/collateral/loan terms set by banks.",
  martTable: "mart_safe__financing_conditions",
  selectColumns: [
    "wave_number",
    "country_code",
    "firm_size",
    "question_id",
    "question_label",
    "sub_item_label",
    "net_balance_wtd",
    "financing_gap_wtd",
    "n_respondents",
  ],
  firmSizeValues: ["all", "sme", "large"],
  dimensions: [
    {
      column: "question_id",
      param: "questionId",
      required: true,
      codes: [
        { code: "q5", label: "Financing need — has your need for external financing changed?" },
        { code: "q9", label: "Financing availability — has availability of external financing changed?" },
        { code: "q10", label: "Bank loan terms — interest rates, costs, collateral, maturity, size" },
      ],
    },
    {
      column: "sub_item",
      param: "subItem",
      required: false,
      codes: [
        { code: "a", label: "Bank loans (Q5/Q9) or level of interest rates (Q10)" },
        { code: "b", label: "Trade credit (Q5/Q9) or non-interest financing costs (Q10)" },
        { code: "c", label: "Equity capital (Q5/Q9) or available loan/credit line size (Q10)" },
        { code: "d", label: "Debt securities issued (Q5/Q9) or available loan maturity (Q10)" },
        { code: "e", label: "Collateral requirements (Q10 only)" },
        { code: "f", label: "Credit line/bank overdraft/credit card overdraft (Q5/Q9) or other terms (Q10)" },
        { code: "g", label: "Leasing or hire-purchase (Q5/Q9 only)" },
        { code: "h", label: "Other loan (Q5/Q9 only)" },
      ],
    },
  ],
  extraWhere: [],
  orderBy: "wave_number, country_code, firm_size, question_id, sub_item",
};

export const MART_TOOL_SPECS: MartToolSpec[] = [BUSINESS_PROBLEMS_SPEC, FINANCING_CONDITIONS_SPEC];
