-- Chart 1: Changes in terms and conditions of bank financing (Q10)
-- Countries: Slovakia (SK), Euro Area (EA), Austria (AT), Germany (DE)
-- All firm sizes combined (firm_size = 'all'), last 4 waves
--
-- net_balance_wtd: positive = net tightening (adverse for firms)
--                 negative = net easing (favourable for firms)
-- Sub-items:
--   a = Level of interest rates
--   b = Non-interest financing costs (charges, fees, commissions)
--   c = Available size of loan or credit line
--   d = Available maturity of the loan
--   e = Collateral requirements
--   f = Other terms (guarantees, covenants, procedures)

WITH latest_waves AS (
    SELECT DISTINCT wave_number
    FROM main_safe.mart_safe__financing_conditions
    WHERE question_id = 'q10'
    ORDER BY wave_number DESC
    LIMIT 4
)

SELECT
    f.wave_number,
    f.survey_period_label,
    f.country_code,
    f.sub_item,
    f.sub_item_label,
    f.net_balance_wtd,
    f.pct_improved_wtd,
    f.pct_deteriorated_wtd,
    f.pct_unchanged_wtd,
    f.n_respondents
FROM main_safe.mart_safe__financing_conditions f
WHERE f.question_id = 'q10'
  AND f.country_code IN ('SK', 'EA', 'DE')
  AND f.firm_size = 'all'
  AND f.wave_number IN (SELECT wave_number FROM latest_waves)
ORDER BY f.sub_item, f.country_code, f.wave_number
