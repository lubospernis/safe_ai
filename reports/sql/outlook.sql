-- Q26: Expected changes in turnover and investments over the next two quarters
-- Countries: Slovakia (SK), Euro Area (EA), Germany (DE)
-- All firms (firm_size = 'all'), last 4 waves (dynamic)
--
-- net_balance_wtd: positive = more firms expect increase (FAVOURABLE)
--                 negative = more firms expect decrease (ADVERSE)
-- Sub-items:
--   a = Company's turnover
--   b = Investments in property, plant or equipment

WITH latest_waves AS (
    SELECT DISTINCT wave_number
    FROM main_safe.mart_safe__outlook
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
    f.pct_increase_wtd,
    f.pct_decrease_wtd,
    f.n_respondents
FROM main_safe.mart_safe__outlook f
WHERE f.country_code IN ('SK', 'EA', 'DE')
  AND f.firm_size = 'all'
  AND f.wave_number IN (SELECT wave_number FROM latest_waves)
ORDER BY f.sub_item, f.country_code, f.wave_number
