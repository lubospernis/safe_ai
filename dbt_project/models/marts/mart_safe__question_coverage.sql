{{
  config(
    materialized = 'table'
  )
}}

/*
  Response coverage for all survey questions by wave × country × firm_size.
  Three-month reference period only (wave 30 / 2024Q1 onward).

  Grain: wave_number × country_code × question_id × sub_item × firm_size.

  Covers all questions in int_safe__core_questions_long (Q2, Q5, Q6A, Q7A, Q7B,
  Q9, Q10, Q11, Q23, Q26, Q31, Q33, Q34, Q4rec) plus Q0B from its own intermediate.

  Includes an 'EA' pseudo-country row aggregating all euro-area countries.

  FIRM SIZE:
    'all'   = all firms.
    'sme'   = employee_band_code 1–3.
    'large' = employee_band_code 4.

  n_total         = firms for whom the question was asked (response_3m is not null).
  n_valid         = firms giving a codeable answer (non-response codes excluded).
  n_nonresponse   = firms answering DK/NA/not applicable.
  response_rate   = n_valid / n_total * 100 (weighted).
*/

with core as (

    select
        wave_number,
        question_id,
        sub_item,
        response_3m,
        response_3m in (-1, -2, -99, 7, 9, 99)         as is_nonresponse,
        survey_year,
        survey_period,
        survey_period_label,
        country_code,
        country_name_en,
        is_sme,
        weight_common
    from {{ ref('int_safe__core_questions_long') }}
    where wave_number >= 30
      and response_3m is not null

),

q0b as (

    select
        wave_number,
        'q0b'                                           as question_id,
        problem_id::varchar                             as sub_item,
        pressingness_score                              as response_3m,
        is_nonresponse,
        survey_year,
        survey_period,
        survey_period_label,
        country_code,
        country_name_en,
        (employee_band_code between 1 and 3)            as is_sme,
        weight_common
    from {{ ref('int_safe__q0b_pressingness') }}
    where wave_number >= 30
      and reference_period = '3m'

),

all_questions as (

    select * from core
    union all
    select * from q0b

),

sized as (

    select s.*, sc.firm_size
    from all_questions s
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
        question_id,
        sub_item,
        firm_size,

        count(*)                                                    as n_total,
        count(*) filter (where not is_nonresponse)                  as n_valid,
        count(*) filter (where is_nonresponse)                      as n_nonresponse,

        round(
            100.0 * sum(weight_common) filter (where not is_nonresponse)
            / nullif(sum(weight_common), 0)
        , 1)                                                        as response_rate_wtd,

        round(
            100.0 * count(*) filter (where not is_nonresponse)
            / nullif(count(*), 0)
        , 1)                                                        as response_rate_unwtd

    from sized
    group by
        wave_number, survey_year, survey_period, survey_period_label,
        country_code, country_name_en, question_id, sub_item, firm_size

)

select *
from aggregated
order by wave_number, country_code, question_id, sub_item, firm_size
