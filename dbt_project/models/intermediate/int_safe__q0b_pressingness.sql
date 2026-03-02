{{
  config(
    materialized = 'table'
  )
}}

/*
  Long-format model for Q0B: "How pressing are the following problems for your firm?"
  (scale 1–10, where 1 = not pressing at all, 10 = extremely pressing)

  One row per firm (permid) × wave × problem category (7 categories).

  Problem categories (from ECB SAFE questionnaire annex):
    1 = Finding customers
    2 = Competition
    3 = Access to finance
    4 = Costs of production or labour
    5 = Availability of skilled staff or experienced managers
    6 = Regulation
    7 = Other (please specify — see q0b_7_open)

  Non-response codes:
    -1  = Not applicable
    -2  = Don't know / refused
    99  = Not asked (question not administered in that wave/sub-sample)

  Firm context (country, size, sector, survey period) is joined from
  int_safe__firm_survey_responses.
*/

with stg as (

    select * from {{ ref('stg_safe__microdata') }}

),

firm as (

    select
        permid,
        wave_number,
        country_code,
        country_name_en                 as country_name,
        employee_band_code              as firm_size_code,
        firm_size_en                    as firm_size_label,
        sector_code,
        sector_en                       as sector_label,
        survey_year,
        survey_period,
        survey_period_label,
        weight_common
    from {{ ref('int_safe__firm_survey_responses') }}

),

unpivoted as (

    select permid, wave_number,
        1                               as problem_id,
        'Finding customers'             as problem_label,
        q0b_1                           as pressingness_score,
        null::varchar                   as q0b_open
    from stg where q0b_1 is not null

    union all

    select permid, wave_number,
        2,
        'Competition',
        q0b_2,
        null::varchar
    from stg where q0b_2 is not null

    union all

    select permid, wave_number,
        3,
        'Access to finance',
        q0b_3,
        null::varchar
    from stg where q0b_3 is not null

    union all

    select permid, wave_number,
        4,
        'Costs of production or labour',
        q0b_4,
        null::varchar
    from stg where q0b_4 is not null

    union all

    select permid, wave_number,
        5,
        'Availability of skilled staff or experienced managers',
        q0b_5,
        null::varchar
    from stg where q0b_5 is not null

    union all

    select permid, wave_number,
        6,
        'Regulation',
        q0b_6,
        null::varchar
    from stg where q0b_6 is not null

    union all

    select permid, wave_number,
        7,
        'Other',
        q0b_7,
        q0b_7_open
    from stg where q0b_7 is not null

),

final as (

    select
        u.permid,
        u.wave_number,
        u.problem_id,
        u.problem_label,
        u.pressingness_score,
        u.pressingness_score in (-1, -2, 99)    as is_nonresponse,
        u.q0b_open,

        -- Firm context
        f.country_code,
        f.country_name,
        f.firm_size_code,
        f.firm_size_label,
        f.sector_code,
        f.sector_label,
        f.survey_year,
        f.survey_period,
        f.survey_period_label,
        f.weight_common

    from unpivoted u
    left join firm f
        on u.permid = f.permid
        and u.wave_number = f.wave_number

)

select * from final
