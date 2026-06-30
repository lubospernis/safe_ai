-- Q11: Self-assessed factors affecting availability of external financing
-- Net balance = % reporting improvement minus % reporting deterioration
-- Positive = factor improving (favourable for financing access)
-- Negative = factor deteriorating (adverse / potential credit supply constraint)
--
-- Key sub-items for credit supply analysis:
--   f = Willingness of banks to provide credit  (direct bank supply signal)
--   a = General economic outlook                (macro drag on supply)
--   b = Access to public financial support      (policy/guarantee channel)
--
-- Countries: Slovakia (SK), Euro Area (EA), Germany (DE)
-- All firms (firm_size = 'all'), last 4 waves (dynamic)

WITH latest_waves AS (
    SELECT DISTINCT wave_number
    FROM main_safe.mart_safe__financing_factors
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
    f.pct_improved_wtd,
    f.pct_deteriorated_wtd,
    f.n_respondents
FROM main_safe.mart_safe__financing_factors f
WHERE f.country_code IN ('SK', 'EA', 'DE')
  AND f.firm_size = 'all'
  AND f.sub_item IN ('f', 'a', 'b')
  AND f.wave_number IN (SELECT wave_number FROM latest_waves)
ORDER BY f.sub_item, f.country_code, f.wave_number
