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

with_totals as (

    select
        *,
        sum(n_firms_wtd) over (
            partition by wave_number, module_id, sub_item, country_code
        ) as n_total_wtd

    from aggregated

)

select
    wave_number,
    period_asked,
    module_id,
    sub_item,
    country_code,
    response_raw,
    n_firms,
    n_firms_wtd,
    n_total_wtd,
    round(n_firms_wtd / nullif(n_total_wtd, 0) * 100, 1) as pct_wtd

from with_totals
order by wave_number desc, module_id, sub_item, country_code, response_raw
