-- Adhoc module spotlight query
-- Returns response distribution for the focal module_id, current wave, SK and EA only.
-- Parameters (Python .format()): {wave_number}, {module_id}, {schema}
--
-- pct_wtd: weighted % of valid respondents giving each response code, within country.
-- Used by build_adhoc_spotlight() to construct the data table injected into the LLM prompt.

SELECT
    country_code,
    sub_item,
    response_raw,
    n_firms,
    n_firms_wtd,
    n_total_wtd,
    pct_wtd
FROM {schema}.mart_safe__adhoc_responses
WHERE wave_number = {wave_number}
  AND module_id   = '{module_id}'
  AND country_code IN ('SK', 'EA')
ORDER BY country_code, sub_item, response_raw
