-- SME-vs-all-firms comparison for Q31/Q34 quantitative expectations, Slovakia only.
-- Separate, minimal query so expectations_quantitative.sql (all-firms only —
-- used for charts, interest checks, and the main section df) is untouched;
-- this feeds ONLY _sme_divergence_note (llm.py). panel_id mirrors the main
-- query so value_col/panel_col line up with config.py's section definition.
--
-- Sub-items: Q31_a=inflation rate in 12mo; Q34_a=selling price, Q34_b=input
-- prices, Q34_c=wages, Q34_d=employees.

WITH latest_waves AS (
    SELECT DISTINCT wave_number
    FROM main_safe.mart_safe__expectations
    WHERE question_id = 'q34'
    ORDER BY wave_number DESC
    LIMIT 4
)

SELECT
    f.wave_number,
    f.country_code,
    f.firm_size,
    f.question_id || '_' || f.sub_item                          AS panel_id,
    f.mean_wtd
FROM main_safe.mart_safe__expectations f
WHERE f.country_code = 'SK'
  AND f.firm_size IN ('all', 'sme')
  AND f.question_id IN ('q31', 'q34')
  AND f.sub_item IN ('a', 'b', 'c', 'd')
  AND f.mean_wtd IS NOT NULL
  AND f.wave_number IN (SELECT wave_number FROM latest_waves)
ORDER BY f.wave_number, f.sub_item, f.firm_size
