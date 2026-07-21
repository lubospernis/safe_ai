-- SME-vs-all-firms comparison for Q2 business situation, Slovakia only.
-- Separate, minimal query so business_situation.sql (all-firms only — used for
-- charts, interest checks, and the main section df) is untouched; this feeds
-- ONLY _sme_divergence_note (llm.py), which computes SK all-vs-SME divergence
-- from the latest wave present in this df.
--
-- sub_items: a=Turnover, b=Labour costs, c=Other costs, d=Interest expenses,
-- e=Profit, g=Investment, h=Working capital, i=Employees, j=Debt/assets.

WITH latest_waves AS (
    SELECT DISTINCT wave_number
    FROM main_safe.mart_safe__business_situation
    ORDER BY wave_number DESC
    LIMIT 4
)

SELECT
    f.wave_number,
    f.country_code,
    f.firm_size,
    f.sub_item,
    f.net_balance_wtd
FROM main_safe.mart_safe__business_situation f
WHERE f.country_code = 'SK'
  AND f.firm_size IN ('all', 'sme')
  AND f.wave_number IN (SELECT wave_number FROM latest_waves)
ORDER BY f.wave_number, f.sub_item, f.firm_size
