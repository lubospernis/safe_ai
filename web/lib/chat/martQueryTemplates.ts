// Query templates for query_mart — the fallback tool for marts that don't
// (yet) have a structured tool in web/lib/chat/tools/. Ported from
// reports/db.py::MART_QUERY_TEMPLATES, with two corrections found while
// building the structured tools (both verified against the marts' actual
// .sql models under dbt_project/models/marts/, not schema.yml or CLAUDE.md):
//
// 1. business_problems and financing_conditions templates REMOVED — both now
//    have a structured tool (get_business_problems, get_financing_conditions)
//    that builds correct SQL directly; the business_problems template here
//    used to instruct "AND reference_period = '3m'", but that column does not
//    exist in the mart's output at all (it's filtered inside the mart's own
//    source CTEs) — this was a real, live bug that caused the model to guess
//    at alternative column names ("survey_period") when the query failed.
// 2. financing_purpose's template dropped the same "AND reference_period =
//    '3m'" filter for the same reason — mart_safe__financing_purpose has no
//    reference_period column either. Confirmed by reading the mart's .sql
//    directly: it has no reference_period column, only 3m-suffixed source
//    columns consumed internally.
export const MART_QUERY_TEMPLATES = `
Query templates — fill in the UPPER_CASE placeholders, do not change the rest:

-- Historical trend for a net-balance mart (business_situation, outlook,
-- availability_expectations, financing_factors):
SELECT wave_number, country_code, net_balance_wtd, n_respondents
FROM main_safe.mart_safe__MART_NAME
WHERE country_code IN ('SK', 'EA', 'DE')
  AND firm_size = 'all'
  AND sub_item = 'SUB_ITEM_CODE'
ORDER BY wave_number, country_code;

-- Loan application / rejection rates:
SELECT wave_number, country_code, application_rate_wtd, rejection_rate_wtd,
       discouragement_rate_wtd, financing_gap_wtd
FROM main_safe.mart_safe__loan_applications
WHERE country_code IN ('SK', 'EA', 'DE')
  AND firm_size = 'all'
  AND sub_item = 'a'
ORDER BY wave_number, country_code;

-- Financing purpose:
SELECT wave_number, country_code, purpose_label, pct_cited_wtd
FROM main_safe.mart_safe__financing_purpose
WHERE country_code IN ('SK', 'EA', 'DE')
  AND firm_size = 'all'
ORDER BY wave_number, country_code, purpose_id;

-- Expectations (Q31 mean / Q33 net balance / Q34 pct):
SELECT wave_number, country_code, question_id, sub_item_label,
       mean_wtd, net_balance_wtd, pct_upside_wtd, pct_downside_wtd
FROM main_safe.mart_safe__expectations
WHERE country_code IN ('SK', 'EA', 'DE')
  AND firm_size = 'all'
  AND question_id = 'QUESTION_ID'
ORDER BY wave_number, country_code;

-- Historical microdata (pre-wave-30 only):
SELECT wave_number, country_code,
       AVG(CASE WHEN response_3m IN (1,2,3) THEN 1.0 ELSE 0.0 END) AS pct_relevant
FROM main_safe.int_safe__core_questions_long
WHERE country_code IN ('SK', 'EA')
  AND question_id = 'QUESTION_ID'
  AND sub_item = 'SUB_ITEM_CODE'
  AND wave_number < 30
  AND is_nonresponse = false
GROUP BY wave_number, country_code
ORDER BY wave_number;
`.trim();
