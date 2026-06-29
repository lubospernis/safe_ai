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
        q.wave_number,
        q.question_id,
        q.sub_item,
        q.response_3m,
        q.response_3m in (-1, -2, -99, 7, 9, 99)       as is_nonresponse,
        f.survey_year,
        f.survey_period,
        f.survey_period_label,
        f.country_code,
        f.country_name_en,
        f.is_sme,
        f.is_euro_area,
        f.weight_common
    from {{ ref('int_safe__core_questions_long') }} q
    join {{ ref('int_safe__firm_survey_responses') }} f
        using (permid, wave_number)
    where q.wave_number >= 30
      and q.response_3m is not null

),

q0b as (

    select
        p.wave_number,
        'q0b'                                           as question_id,
        p.problem_id::varchar                           as sub_item,
        p.pressingness_score                            as response_3m,
        p.is_nonresponse,
        p.survey_year,
        p.survey_period,
        p.survey_period_label,
        p.country_code,
        p.country_name_en,
        (p.employee_band_code between 1 and 3)          as is_sme,
        f.is_euro_area,
        p.weight_common
    from {{ ref('int_safe__q0b_pressingness') }} p
    join {{ ref('int_safe__firm_survey_responses') }} f
        using (permid, wave_number)
    where p.wave_number >= 30
      and p.reference_period = '3m'

),

all_questions as (

    select * from core
    union all
    select * from q0b

),

with_ea as (

    select * from all_questions

    union all

    select wave_number, question_id, sub_item, response_3m, is_nonresponse,
           survey_year, survey_period, survey_period_label,
           'EA'        as country_code,
           'Euro Area' as country_name_en,
           is_sme, is_euro_area, weight_common
    from all_questions
    where is_euro_area

),

sized as (

    select s.*, sc.firm_size
    from with_ea s
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
