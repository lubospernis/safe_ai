-- Financing need vs availability gap (Q5/Q9) from mart_safe__financing_conditions
-- financing_gap_wtd = Q5 net balance (need) minus Q9 net balance (availability)
-- Positive gap = need exceeds availability (adverse for firms)
-- Countries: Slovakia (SK), Euro Area (EA), Germany (DE)
-- All firms (firm_size = 'all'), last 4 waves (dynamic)
--
-- Returns two result sets unioned:
--   type='main'  — bank loans (sub_item='a') for SK/EA/DE — used for the main grouped-bar chart
--   type='sk_all' — all major instruments for SK only — used for the SK instrument breakdown chart

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
),

joined AS (
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
)

-- Main chart: bank loans (a), credit lines (f), trade credit (b) for SK/EA/DE
SELECT *, 'main' AS chart_type
FROM joined
WHERE sub_item IN ('a', 'f', 'b')

UNION ALL

-- SK instrument breakdown: all instruments for Slovakia only
SELECT *, 'sk_all' AS chart_type
FROM joined
WHERE country_code = 'SK'
  AND sub_item IN ('a', 'b', 'f', 'g', 'h')

ORDER BY chart_type, sub_item, country_code, wave_number
