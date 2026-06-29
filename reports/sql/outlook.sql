-- Q26: Expected changes in turnover and investment over next 2 quarters
-- Countries: Slovakia (SK), Euro Area (EA), Germany (DE)
-- SMEs only, last 4 waves (dynamic)
--
-- sub_items: a=Turnover, b=Investment
-- Sign: positive = more firms expect increase; negative = more firms expect decrease

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
  AND f.wave_number IN (SELECT wave_number FROM latest_waves)
ORDER BY f.sub_item, f.country_code, f.wave_number
