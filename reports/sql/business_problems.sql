-- Q0B: Most pressing business problems
-- Countries: Slovakia (SK), Euro Area (EA), Germany (DE)
-- All firms (firm_size = 'all'), last 4 waves (dynamic)

WITH latest_waves AS (
    SELECT DISTINCT wave_number
    FROM main_safe.mart_safe__business_problems
    ORDER BY wave_number DESC
    LIMIT 4
)

SELECT
    f.wave_number,
    f.survey_period_label,
    f.country_code,
    f.problem_id,
    f.problem_label,
    f.avg_pressingness_wtd,
    f.n_respondents
FROM main_safe.mart_safe__business_problems f
WHERE f.country_code IN ('SK', 'EA', 'DE')
  AND f.firm_size = 'all'
  AND f.wave_number IN (SELECT wave_number FROM latest_waves)
ORDER BY f.problem_id, f.country_code, f.wave_number
