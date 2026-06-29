-- Chart 1: Changes in terms and conditions of bank financing (Q10)
-- Countries: Slovakia (SK), Euro Area (EA), Austria (AT), Germany (DE)
-- All firm sizes combined (firm_size = 'all'), wave 30 onward
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

SELECT
    wave_number,
    survey_period_label,
    country_code,
    sub_item,
    sub_item_label,
    net_balance_wtd,
    pct_improved_wtd,
    pct_deteriorated_wtd,
    pct_unchanged_wtd,
    n_respondents
FROM main_safe.mart_safe__financing_conditions
WHERE question_id = 'q10'
  AND country_code IN ('SK', 'EA', 'AT', 'DE')
  AND firm_size = 'all'
ORDER BY sub_item, country_code, wave_number
