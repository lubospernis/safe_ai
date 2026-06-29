{{
  config(
    materialized = 'table'
  )
}}

/*
  Firm-level participation counts by wave × country × firm_size.
  Three-month reference period only (wave 30 / 2024Q1 onward).

  Includes an 'EA' pseudo-country row aggregating all euro-area countries.

  FIRM SIZE:
    'all'   = all firms in scope.
    'sme'   = employee_band_code 1–3 (up to 249 employees).
    'large' = employee_band_code 4 (250+ employees).

  n_firms     = unweighted headcount of survey respondents.
  n_firms_wtd = sum of weight_common (ECB-weighted total).
  pct_sme     = share of SMEs among all firms (meaningful on firm_size = 'all' rows only).
*/

with base as (

    select
        wave_number,
        survey_year,
        survey_period,
        survey_period_label,
        country_code,
        country_name_en,
        is_sme,
        weight_common
    from {{ ref('int_safe__firm_survey_responses') }}
    where wave_number >= 30

),

sized as (

    select s.*, sc.firm_size
    from base s
    cross join (values ('all'), ('sme'), ('large')) as sc(firm_size)
    where
        sc.firm_size = 'all'
        or (sc.firm_size = 'sme'   and s.is_sme)
        or (sc.firm_size = 'large' and not s.is_sme)

),

aggregated as (

    select
        wave_number,
        survey_year,
        survey_period,
        survey_period_label,
        country_code,
        country_name_en,
        firm_size,

        count(*)                                                    as n_firms,
        round(sum(weight_common), 1)                                as n_firms_wtd,

        -- SME/large share among all respondents (meaningful on firm_size='all' rows)
        round(
            100.0 * count(*) filter (where is_sme)
            / nullif(count(*), 0)
        , 1)                                                        as pct_sme,

        round(
            100.0 * count(*) filter (where not is_sme)
            / nullif(count(*), 0)
        , 1)                                                        as pct_large

    from sized
    group by
        wave_number, survey_year, survey_period, survey_period_label,
        country_code, country_name_en, firm_size

)

select *
from aggregated
order by wave_number, country_code, firm_size
