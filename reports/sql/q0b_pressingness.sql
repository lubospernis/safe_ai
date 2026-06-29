-- Q0B: Most pressing business problems
-- Countries: Slovakia (SK), Euro Area (EA), Germany (DE)
-- SMEs only, last 4 waves (dynamic)

WITH latest_waves AS (
    SELECT DISTINCT wave_number
    FROM main_safe.mart_safe__q0b_pressingness
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
FROM main_safe.mart_safe__q0b_pressingness f
WHERE f.country_code IN ('SK', 'EA', 'DE')
  AND f.wave_number IN (SELECT wave_number FROM latest_waves)
ORDER BY f.problem_id, f.country_code, f.wave_number
