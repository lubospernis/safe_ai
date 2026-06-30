-- Q23: Expected availability of external financing over the next quarter
-- Countries: Slovakia (SK), Euro Area (EA), Germany (DE)
-- All firms (firm_size = 'all'), last 4 waves (dynamic)
--
-- net_balance_wtd: % expecting improvement minus % expecting deterioration
-- Positive = more firms expect availability to IMPROVE (FAVOURABLE)
-- Negative = more firms expect availability to DETERIORATE (ADVERSE)
--
-- Sub-items (matching Q5/Q9 instruments):
--   b = Bank loans
--   d = Trade credit
--   g = Credit lines and bank overdraft

WITH latest_waves AS (
    SELECT DISTINCT wave_number
    FROM main_safe.mart_safe__availability_expectations
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
    f.pct_improve_wtd,
    f.pct_deteriorate_wtd,
    f.n_respondents
FROM main_safe.mart_safe__availability_expectations f
WHERE f.country_code IN ('SK', 'EA', 'DE')
  AND f.firm_size = 'all'
  AND f.n_respondents >= 10
  AND f.wave_number IN (SELECT wave_number FROM latest_waves)
ORDER BY f.sub_item, f.country_code, f.wave_number
