-- Q6A: Purpose of financing (multi-select)
-- pct_cited_wtd = % of firms citing each purpose (can sum > 100% as multi-select)
-- purposes: 1=Fixed investment, 2=Inventory/working capital, 3=Hiring/training,
--           4=New products/services, 5=Refinancing, 6=Other
-- Countries: Slovakia (SK), Euro Area (EA), Germany (DE)
-- All firms (firm_size = 'all'), last 4 waves (dynamic)

WITH latest_waves AS (
    SELECT DISTINCT wave_number
    FROM main_safe.mart_safe__financing_purpose
    ORDER BY wave_number DESC
    LIMIT 4
)

SELECT
    f.wave_number,
    f.survey_period_label,
    f.country_code,
    f.purpose_id,
    f.purpose_label,
    f.pct_cited_wtd,
    f.n_respondents
FROM main_safe.mart_safe__financing_purpose f
WHERE f.country_code IN ('SK', 'EA', 'DE')
  AND f.firm_size = 'all'
  AND f.wave_number IN (SELECT wave_number FROM latest_waves)
ORDER BY f.purpose_id, f.country_code, f.wave_number
