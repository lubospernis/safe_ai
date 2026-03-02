{{
  config(
    materialized = 'table'
  )
}}

/*
  Aggregated pressingness scores for Q0B by country × wave × problem × firm size.

  All codes are decoded to human-readable labels so that AI tools querying
  MotherDuck do not need a separate codebook.

  Metrics:
    - n_respondents          : count of valid (non-missing) responses
    - n_nonresponse          : count of non-responses (-1, -2, 99) excluded from averages
    - total_weight           : sum of weight_common for valid respondents
    - avg_pressingness_wtd   : weighted average pressingness score (1–10)
    - avg_pressingness_unwtd : unweighted average for reference
    - pct_high_pressing      : % of valid responses with score >= 7 (highly pressing)
    - pct_low_pressing       : % of valid responses with score <= 3 (not very pressing)

  Source: int_safe__q0b_pressingness (valid responses only, is_nonresponse = false)
*/

with int_q0b as (

    select * from {{ ref('int_safe__q0b_pressingness') }}

),

valid as (

    select * from int_q0b
    where  weight_common is not null and is_nonresponse = false
    -- and employee_band_code BETWEEN 1 AND 3

),

aggregated as (

    select
        country_code,
        country_name_en,
        wave_number,
        survey_year,
        survey_period,
        survey_period_label,
        reference_period,
        problem_id,
        problem_label,
        count(*)                                                as n_respondents,
        sum(case when is_nonresponse then 1 else 0 end)        as n_nonresponse,
        sum(weight_common)                                      as total_weight,

        -- Weighted average pressingness (1–10 scale)
        round(
            sum(pressingness_score * weight_common)
            / nullif(sum(weight_common), 0),
        2)                                                      as avg_pressingness_wtd,

        -- Unweighted average for reference
        round(avg(pressingness_score), 2)                      as avg_pressingness_unwtd,

        -- % highly pressing (score 7–10)
        round(
            100.0 * sum(case when pressingness_score >= 7 then weight_common else 0 end)
            / nullif(sum(weight_common), 0),
        1)                                                      as pct_high_pressing,

        -- % not very pressing (score 1–3)
        round(
            100.0 * sum(case when pressingness_score <= 3 then weight_common else 0 end)
            / nullif(sum(weight_common), 0),
        1)                                                      as pct_low_pressing

    from valid
    group by all

)

select * from aggregated
order by wave_number, country_code, problem_id
