-- Financing need vs availability gap (Q5/Q9) from mart_safe__financing_conditions
-- financing_gap_wtd = Q5 net balance (need) minus Q9 net balance (availability)
-- Positive gap = need exceeds availability (adverse for firms)
-- Countries: Slovakia (SK), Euro Area (EA), Germany (DE)
-- All firms (firm_size = 'all'), bank loans sub_item='a', last 4 waves (dynamic)
--
-- Returns one row per country × wave with need, availability, and gap columns
-- so the chart can show bars (need/availability) + line (gap) in a single query.

WITH latest_waves AS (
    SELECT DISTINCT wave_number
    FROM main_safe.mart_safe__financing_conditions
    WHERE question_id = 'q9'
    ORDER BY wave_number DESC
    LIMIT 4
),

q5 AS (
    SELECT wave_number, survey_period_label, country_code, sub_item, sub_item_label,
           net_balance_wtd AS need_nb, n_respondents AS n_respondents_need
    FROM main_safe.mart_safe__financing_conditions
    WHERE question_id = 'q5'
      AND country_code IN ('SK', 'EA', 'DE')
      AND firm_size = 'all'
      AND wave_number IN (SELECT wave_number FROM latest_waves)
),

q9 AS (
    SELECT wave_number, country_code, sub_item,
           net_balance_wtd        AS availability_nb,
           financing_gap_wtd,
           n_respondents          AS n_respondents_avail
    FROM main_safe.mart_safe__financing_conditions
    WHERE question_id = 'q9'
      AND country_code IN ('SK', 'EA', 'DE')
      AND firm_size = 'all'
      AND wave_number IN (SELECT wave_number FROM latest_waves)
)

SELECT
    q5.wave_number,
    q5.survey_period_label,
    q5.country_code,
    q5.sub_item,
    q5.sub_item_label,
    q5.need_nb,
    q9.availability_nb,
    q9.financing_gap_wtd,
    q5.n_respondents_need,
    q9.n_respondents_avail
FROM q5
JOIN q9 USING (wave_number, country_code, sub_item)
ORDER BY q5.sub_item, q5.country_code, q5.wave_number
