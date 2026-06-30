-- Q33: Main risk to 5-year inflation outlook (ordinal net balance)
-- Countries: Slovakia (SK), Euro Area (EA), Germany (DE)
-- All firms (firm_size = 'all'), last 4 waves (dynamic)
--
-- net_balance_wtd: % seeing upside inflation risk minus % seeing downside risk
-- Positive = more firms see inflation as a risk to the upside
-- Negative = more firms see inflation risk to the downside

WITH latest_waves AS (
    SELECT DISTINCT wave_number
    FROM main_safe.mart_safe__expectations
    WHERE question_id = 'q33'
    ORDER BY wave_number DESC
    LIMIT 4
)

SELECT
    f.wave_number,
    f.survey_period_label,
    f.country_code,
    f.pct_downside_wtd,
    f.pct_balanced_wtd,
    f.pct_upside_wtd,
    f.net_balance_wtd,
    f.n_respondents
FROM main_safe.mart_safe__expectations f
WHERE f.country_code IN ('SK', 'EA', 'DE')
  AND f.firm_size = 'all'
  AND f.question_id = 'q33'
  AND f.n_respondents >= 10
  AND f.wave_number IN (SELECT wave_number FROM latest_waves)
ORDER BY f.country_code, f.wave_number
