{{
  config(
    materialized = 'table' if target.name == 'prod' else 'view'
  )
}}

/*
  Aggregates adhoc module responses (QA/QB series) to
  wave × country × module_id × sub_item × response_raw level.

  One row per unique combination; pct_wtd is the weighted share of valid
  respondents who gave that response code within the country × module × sub_item.

  Excludes non-response codes (-1, -2, -99, 7, 99).
  Joins to int_safe__firm_survey_responses for country_code and weight_common.

  Covers all waves where adhoc module columns are populated.
  Use period_asked to filter to a specific survey wave label (e.g. '2026Q1').
*/

with base as (

    select
        r.permid,
        r.wave_number,
        r.module_id,
        r.sub_item,
        r.period_asked,
        r.response_raw,
        f.country_code,
        f.weight_common

    from {{ ref('int_safe__adhoc_modules_long') }} r
    join {{ ref('int_safe__firm_survey_responses') }} f
        on r.permid = f.permid
        and r.wave_number = f.wave_number

    where not r.is_nonresponse
        and f.weight_common > 0

),

aggregated as (

    select
        wave_number,
        period_asked,
        module_id,
        sub_item,
        country_code,
        response_raw,
        count(*)           as n_firms,
        sum(weight_common) as n_firms_wtd

    from base
    group by all

),

-- Compute denominator via a separate aggregation and join back.
-- Avoids a window function over the full dataset, which OOMs on small MotherDuck tiers.
totals as (

    select
        wave_number,
        module_id,
        sub_item,
        country_code,
        sum(n_firms_wtd) as n_total_wtd

    from aggregated
    group by all

)

select
    a.wave_number,
    a.period_asked,
    a.module_id,
    a.sub_item,
    a.country_code,
    a.response_raw,
    a.n_firms,
    a.n_firms_wtd,
    t.n_total_wtd,
    round(a.n_firms_wtd / nullif(t.n_total_wtd, 0) * 100, 1) as pct_wtd

from aggregated a
join totals t
    on  a.wave_number  = t.wave_number
    and a.module_id    = t.module_id
    and a.sub_item     = t.sub_item
    and a.country_code = t.country_code

order by a.wave_number desc, a.module_id, a.sub_item, a.country_code, a.response_raw
