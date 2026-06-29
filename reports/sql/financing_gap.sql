-- Financing need vs availability gap (Q5/Q9) from mart_safe__financing_conditions
-- financing_gap_wtd = Q5 net balance (need) minus Q9 net balance (availability)
-- Positive gap = need exceeds availability (adverse for firms)
-- Countries: Slovakia (SK), Euro Area (EA), Germany (DE)
-- All firms (firm_size = 'all'), bank loans sub_item='a', last 4 waves (dynamic)

WITH latest_waves AS (
    SELECT DISTINCT wave_number
    FROM main_safe.mart_safe__financing_conditions
    WHERE question_id = 'q9'
    ORDER BY wave_number DESC
    LIMIT 4
)

SELECT
    f.wave_number,
    f.survey_period_label,
    f.country_code,
    f.sub_item,
    f.sub_item_label,
    f.financing_gap_wtd,
    f.net_balance_wtd,
    f.n_respondents
FROM main_safe.mart_safe__financing_conditions f
WHERE f.question_id = 'q9'
  AND f.country_code IN ('SK', 'EA', 'DE')
  AND f.firm_size = 'all'
  AND f.wave_number IN (SELECT wave_number FROM latest_waves)
ORDER BY f.sub_item, f.country_code, f.wave_number
