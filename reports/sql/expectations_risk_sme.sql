-- SME-vs-all-firms comparison for Q33 inflation risk direction, Slovakia only.
-- Separate, minimal query so expectations_risk.sql (all-firms only — used for
-- charts, interest checks, and the main section df) is untouched; this feeds
-- ONLY _sme_divergence_note (llm.py).

WITH latest_waves AS (
    SELECT DISTINCT wave_number
    FROM main_safe.mart_safe__expectations
    WHERE question_id = 'q33'
    ORDER BY wave_number DESC
    LIMIT 4
)

SELECT
    f.wave_number,
    f.country_code,
    f.firm_size,
    f.net_balance_wtd
FROM main_safe.mart_safe__expectations f
WHERE f.country_code = 'SK'
  AND f.firm_size IN ('all', 'sme')
  AND f.question_id = 'q33'
  AND f.wave_number IN (SELECT wave_number FROM latest_waves)
ORDER BY f.wave_number, f.firm_size
