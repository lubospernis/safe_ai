{{
  config(
    materialized = 'table'
  )
}}

/*
  Aggregated pressingness scores for Q0B by country × wave × problem.

  All codes are decoded to human-readable labels so that AI tools querying
  MotherDuck do not need a separate codebook.

  Metrics:
    - n_respondents          : count of valid (non-missing) responses
    - n_nonresponse          : count of non-responses (-1, -2, 99) excluded from averages
    - total_weight           : sum of weight_common for valid respondents
    - avg_pressingness_wtd   : weighted average pressingness score (1–10)
    - avg_pressingness_unwtd : unweighted average for reference
    - pct_high_pressing      : % of valid weighted responses with score >= 7
    - pct_low_pressing       : % of valid weighted responses with score <= 3

  Scope: SMEs only (employee_band_code 1–3: micro, small, medium).
  Source: int_safe__q0b_pressingness
*/

with int_q0b as (

    select
        p.*,
        f.is_euro_area
    from {{ ref('int_safe__q0b_pressingness') }} p
    join {{ ref('int_safe__firm_survey_responses') }} f
        using (permid, wave_number)
    where p.employee_band_code between 1 and 3
      and p.wave_number >= 30
      and p.reference_period = '3m'

),

with_ea as (

    select * from int_q0b

    union all

    select wave_number, permid, reference_period, problem_id, problem_label,
           pressingness_score, is_nonresponse, q0b_open,
           'EA' as country_code, 'Euro Area' as country_name_en,
           employee_band_code, firm_size_en, sector_code, sector_en,
           survey_year, survey_period, survey_period_label, weight_common, is_euro_area
    from int_q0b
    where is_euro_area

),

aggregated as (

    select
        country_code,
        country_name_en,
        wave_number,
        survey_year,
        survey_period,
        survey_period_label,
        problem_id,
        problem_label,

        count(*) filter (where not is_nonresponse
                           and weight_common is not null)        as n_respondents,
        count(*) filter (where is_nonresponse)                  as n_nonresponse,
        sum(weight_common) filter (where not is_nonresponse)    as total_weight,

        round(
            sum(pressingness_score * weight_common)
                filter (where not is_nonresponse)
            / nullif(sum(weight_common) filter (where not is_nonresponse), 0),
        2)                                                      as avg_pressingness_wtd,

        round(
            avg(pressingness_score) filter (where not is_nonresponse),
        2)                                                      as avg_pressingness_unwtd,

        round(
            100.0 * sum(weight_common)
                filter (where not is_nonresponse and pressingness_score >= 7)
            / nullif(sum(weight_common) filter (where not is_nonresponse), 0),
        1)                                                      as pct_high_pressing,

        round(
            100.0 * sum(weight_common)
                filter (where not is_nonresponse and pressingness_score <= 3)
            / nullif(sum(weight_common) filter (where not is_nonresponse), 0),
        1)                                                      as pct_low_pressing

    from with_ea
    group by all

)

select * from aggregated
order by wave_number, country_code, problem_id
