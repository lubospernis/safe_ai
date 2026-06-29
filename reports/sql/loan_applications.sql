-- Q7A/Q7B: Bank loan applications, discouragement, rejection and financing gap
-- Countries: Slovakia (SK), Euro Area (EA), Germany (DE)
-- SMEs only, bank loans (sub_item='a'), last 4 waves (dynamic)
--
-- financing_gap_wtd = discouragement + rejection share (ECB headline access indicator)
-- Higher financing_gap = worse access to bank loans

WITH latest_waves AS (
    SELECT DISTINCT wave_number
    FROM main_safe.mart_safe__loan_applications
    WHERE sub_item = 'a'
    ORDER BY wave_number DESC
    LIMIT 4
)

SELECT
    f.wave_number,
    f.survey_period_label,
    f.country_code,
    f.application_rate_wtd,
    f.discouragement_rate_wtd,
    f.rejection_rate_wtd,
    f.financing_gap_wtd,
    f.n_total AS n_respondents
FROM main_safe.mart_safe__loan_applications f
WHERE f.country_code IN ('SK', 'EA', 'DE')
  AND f.sub_item = 'a'
  AND f.wave_number IN (SELECT wave_number FROM latest_waves)
ORDER BY f.country_code, f.wave_number
