{{
  config(
    materialized = 'table'
  )
}}

/*
  Aggregated pressingness scores for Q0B by country × wave × problem.
  (Renamed from mart_safe__q0b_pressingness for domain-naming consistency.)

  All codes are decoded to human-readable labels so that AI tools querying
  MotherDuck do not need a separate codebook.

  Scale: 1 (not pressing at all) → 10 (extremely pressing).
  Non-response codes: -1, -2, 99.

  Problems (problem_id):
    1 = Finding customers
    2 = Competition
    3 = Access to finance
    4 = Costs of production or labour
    5 = Availability of skilled staff or experienced managers
    6 = Regulation
    7 = Other

  Metrics:
    - n_respondents          : count of valid (non-missing) responses
    - n_nonresponse          : count of non-responses excluded from averages
    - total_weight           : sum of weight_common for valid respondents
    - avg_pressingness_wtd   : weighted average pressingness score (1–10)
    - avg_pressingness_unwtd : unweighted average for reference
    - pct_high_pressing      : % of valid weighted responses with score >= 7
    - pct_low_pressing       : % of valid weighted responses with score <= 3

  firm_size: 'all' = all respondents; 'sme' = employee_band_code 1–3.
  Three-month reference period only (wave 30 / 2024Q1 onward).
  Source: int_safe__q0b_pressingness
*/

with int_q0b_all as (

    select
        *,
        'all' as firm_size
    from {{ ref('int_safe__q0b_pressingness') }}
    where wave_number >= 30
      and reference_period = '3m'

),

int_q0b_sme as (

    select
        *,
        'sme' as firm_size
    from {{ ref('int_safe__q0b_pressingness') }}
    where employee_band_code between 1 and 3
      and wave_number >= 30
      and reference_period = '3m'

),

combined as (

    select * from int_q0b_all
    union all
    select * from int_q0b_sme

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
        firm_size,

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

    from combined
    group by all

)

select * from aggregated
order by wave_number, country_code, problem_id, firm_size
