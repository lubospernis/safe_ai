-- Q31/Q34: Quantitative expectations — inflation rate (Q31) and expected % changes
-- in selling prices, input costs, wages, employment (Q34).
-- Countries: Slovakia (SK), Euro Area (EA), Germany (DE)
-- All firms (firm_size = 'all'), last 4 waves (dynamic)
--
-- mean_wtd: weighted mean expected % change (NOT a net balance)
-- Positive = firms expect an increase; negative = firms expect a decrease.
--
-- Sub-items:
--   Q31_a = Expected inflation rate in 12 months
--   Q34_a = Average selling price of products/services
--   Q34_b = Average prices of production inputs (non-labour)
--   Q34_c = Average wage of employees
--   Q34_d = Number of employees
--
-- panel_id = question_id || '_' || sub_item to disambiguate Q31/Q34 sub_item 'a'

WITH latest_waves AS (
    SELECT DISTINCT wave_number
    FROM main_safe.mart_safe__expectations
    WHERE question_id = 'q34'
    ORDER BY wave_number DESC
    LIMIT 4
)

SELECT
    f.wave_number,
    f.survey_period_label,
    f.country_code,
    f.question_id,
    f.sub_item,
    f.question_id || '_' || f.sub_item                          AS panel_id,
    f.sub_item_label,
    f.mean_wtd,
    f.p25_unwtd,
    f.median_unwtd,
    f.p75_unwtd,
    f.n_respondents
FROM main_safe.mart_safe__expectations f
WHERE f.country_code IN ('SK', 'EA', 'DE')
  AND f.firm_size = 'all'
  AND f.question_id IN ('q31', 'q34')
  AND f.sub_item IN ('a', 'b', 'c', 'd')
  AND f.mean_wtd IS NOT NULL
  AND f.n_respondents >= 10
  AND f.wave_number IN (SELECT wave_number FROM latest_waves)
ORDER BY f.question_id, f.sub_item, f.country_code, f.wave_number
