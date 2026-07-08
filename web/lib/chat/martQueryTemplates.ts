// Verbatim TS port of reports/db.py::MART_QUERY_TEMPLATES — same template SQL
// strings, since the underlying mart schema is identical for chat use. Injected
// into the agent's system prompt to bias it toward safe, correct queries rather
// than writing SQL from scratch.

export const MART_QUERY_TEMPLATES = `
Query templates — fill in the UPPER_CASE placeholders, do not change the rest:

-- Historical trend for a net-balance mart (financing_conditions, business_situation,
-- outlook, availability_expectations, financing_factors):
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

-- Business problems pressingness (note: reference_period filter required):
SELECT wave_number, country_code, problem_label, avg_pressingness_wtd
FROM main_safe.mart_safe__business_problems
WHERE country_code IN ('SK', 'EA', 'DE')
  AND firm_size = 'all'
  AND reference_period = '3m'
ORDER BY wave_number, avg_pressingness_wtd DESC;

-- Financing purpose (note: reference_period filter required):
SELECT wave_number, country_code, purpose_label, pct_cited_wtd
FROM main_safe.mart_safe__financing_purpose
WHERE country_code IN ('SK', 'EA', 'DE')
  AND firm_size = 'all'
  AND reference_period = '3m'
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
