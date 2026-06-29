-- Q2: Business situation indicators (turnover, labour costs, profit, investment, employees)
-- Countries: Slovakia (SK), Euro Area (EA), Germany (DE)
-- SMEs only, last 4 waves (dynamic)
--
-- sub_items: a=Turnover, b=Labour costs, e=Profit, g=Investment, i=Employees
-- Sign: positive = indicator rising; labour costs positive = costs rising (adverse)

WITH latest_waves AS (
    SELECT DISTINCT wave_number
    FROM main_safe.mart_safe__business_situation
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
    f.pct_increased_wtd,
    f.pct_decreased_wtd,
    f.n_respondents
FROM main_safe.mart_safe__business_situation f
WHERE f.country_code IN ('SK', 'EA', 'DE')
  AND f.wave_number IN (SELECT wave_number FROM latest_waves)
ORDER BY f.sub_item, f.country_code, f.wave_number
