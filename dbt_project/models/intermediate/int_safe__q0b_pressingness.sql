{{
  config(
    materialized = 'table'
  )
}}

/*
  Long-format model for Q0B: "How pressing are the following problems for your firm?"
  (scale 1–10, where 1 = not pressing at all, 10 = extremely pressing)

  One row per firm (permid) × wave × problem category × reference period.

  Problem categories (from ECB SAFE questionnaire annex):
    1 = Finding customers
    2 = Competition
    3 = Access to finance
    4 = Costs of production or labour
    5 = Availability of skilled staff or experienced managers
    6 = Regulation
    7 = Other (please specify — see q0b_open)

  Reference period:
    '6m' = six-month questionnaire (source: q0b_*)
    '3m' = three-month questionnaire (source: q0b_*_3m / raw: q0b_*_g1)
           Only populated in wave 30 (2024H1) and wave 37 (2025Q4).

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
        country_name_en,
        employee_band_code,
        firm_size_en,
        sector_code,
        sector_en,
        survey_year,
        survey_period,
        survey_period_label,
        weight_common
    from {{ ref('int_safe__firm_survey_responses') }}

),

unpivoted as (

    -- -------------------------------------------------------------------------
    -- 6-month reference period (q0b_*)
    -- -------------------------------------------------------------------------
    select permid, wave_number, '6m' as reference_period,
        1 as problem_id, 'Finding customers' as problem_label,
        q0b_1 as pressingness_score, null::varchar as q0b_open
    from stg where q0b_1 is not null
    union all
    select permid, wave_number, '6m', 2, 'Competition', q0b_2, null::varchar
    from stg where q0b_2 is not null
    union all
    select permid, wave_number, '6m', 3, 'Access to finance', q0b_3, null::varchar
    from stg where q0b_3 is not null
    union all
    select permid, wave_number, '6m', 4, 'Costs of production or labour', q0b_4, null::varchar
    from stg where q0b_4 is not null
    union all
    select permid, wave_number, '6m', 5, 'Availability of skilled staff or experienced managers', q0b_5, null::varchar
    from stg where q0b_5 is not null
    union all
    select permid, wave_number, '6m', 6, 'Regulation', q0b_6, null::varchar
    from stg where q0b_6 is not null
    union all
    select permid, wave_number, '6m', 7, 'Other', q0b_7, q0b_7_open
    from stg where q0b_7 is not null

    -- -------------------------------------------------------------------------
    -- 3-month reference period (q0b_*_3m / source: q0b_*_g1)
    -- Populated in wave 30 (2024H1) and wave 37 (2025Q4) only
    -- -------------------------------------------------------------------------
    union all
    select permid, wave_number, '3m', 1, 'Finding customers', q0b_1_3m, null::varchar
    from stg where q0b_1_3m is not null
    union all
    select permid, wave_number, '3m', 2, 'Competition', q0b_2_3m, null::varchar
    from stg where q0b_2_3m is not null
    union all
    select permid, wave_number, '3m', 3, 'Access to finance', q0b_3_3m, null::varchar
    from stg where q0b_3_3m is not null
    union all
    select permid, wave_number, '3m', 4, 'Costs of production or labour', q0b_4_3m, null::varchar
    from stg where q0b_4_3m is not null
    union all
    select permid, wave_number, '3m', 5, 'Availability of skilled staff or experienced managers', q0b_5_3m, null::varchar
    from stg where q0b_5_3m is not null
    union all
    select permid, wave_number, '3m', 6, 'Regulation', q0b_6_3m, null::varchar
    from stg where q0b_6_3m is not null
    union all
    select permid, wave_number, '3m', 7, 'Other', q0b_7_3m, null::varchar
    from stg where q0b_7_3m is not null

),

final as (

    select
        u.permid,
        u.wave_number,
        u.reference_period,
        u.problem_id,
        u.problem_label,
        u.pressingness_score,
        u.pressingness_score in (-1, -2, 99)    as is_nonresponse,
        u.q0b_open,

        -- Firm context
        f.country_code,
        f.country_name_en,
        f.employee_band_code,
        f.firm_size_en,
        f.sector_code,
        f.sector_en,
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
