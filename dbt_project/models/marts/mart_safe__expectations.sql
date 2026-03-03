{{
  config(
    materialized = 'table'
  )
}}

/*
  Aggregated expectation statistics for Q31, Q33, and Q34.

  --- Q31: Euro area inflation rate expectations (ECB-only) ---
  Question: "What do you think the euro area inflation rate will be at the following
  points in time? Please provide your answer as an annual percentage rate."

  Type: Continuous numeric (annual %). Non-response sentinel: -9999.
  Sub-items (verified from annex.xlsx Q31):
    a = In 12 months
    b = In three years (change in consumer prices in y+3 vs y+2)
    c = In five years  (change in consumer prices in y+5 vs y+4)

  --- Q33: Main risk to 5-year inflation outlook (ECB-only) ---
  Question: "How do you see the main risk to the outlook for inflation in five years' time?"

  Response scale (verified from annex.xlsx Q33):
    1 = A risk to the downside (inflation lower than expected)
    2 = Risks are broadly balanced
    3 = A risk to the upside (inflation higher than expected)
    9 = DK (non-response)
  Single response, no sub-items (sub_item = '').

  --- Q34: Expected % change over next 12 months ---
  Question: "Looking ahead, by how much do you expect the following to increase or
  decrease over the next 12 months? Please provide your answer as a percentage change."

  Type: Continuous numeric (% change). Non-response sentinel: -9999.
  Sub-items (verified from annex.xlsx Q34):
    a = Average selling price of products/services in main markets
    b = Average prices of production inputs (non-labour: materials, energy)
    c = Average wage of current employees (excl. bonuses/overtime, FTE basis)
    d = Number of employees
  Note: _rec column contains confirmed values for responses originally >50%
  (survey read-back validation). Use response_rec when available (non-null).

  Scope: SMEs only (employee_band_code 1–3: micro, small, medium).
  Large firms (band 4, 250+ employees) are excluded for comparability with
  the ECB's published SAFE data warehouse, which reports SME aggregates.

  Aggregation: wave × country × question_id × sub_item.

  Two metric patterns are used:
  1. Q31, Q34 (continuous): weighted mean, p25, median (p50), p75 of valid responses.
  2. Q33 (ordinal): weighted % downside / balanced / upside and net balance (upside − downside).
*/

with source as (

    select
        wave_number,
        survey_year,
        survey_period,
        survey_period_label,
        country_code,
        country_name_en,
        question_id,
        sub_item,
        -- For Q34, prefer confirmed value (response_rec) over raw when available
        case
            when question_id = 'q34' and response_rec is not null
                then response_rec
            else response_raw
        end                                                         as response_value,
        weight_common,
        is_nonresponse
    from {{ ref('int_safe__core_questions_long') }}
    where question_id in ('q31', 'q33', 'q34')
      and employee_band_code between 1 and 3

),

labels as (

    select
        *,
        case question_id
            when 'q31' then 'Euro area inflation rate expectations'
            when 'q33' then 'Main risk to 5-year inflation outlook'
            when 'q34' then 'Expected % change over next 12 months'
        end                                                         as question_label,

        case
            when question_id = 'q31' and sub_item = 'a' then 'In 12 months'
            when question_id = 'q31' and sub_item = 'b' then 'In three years'
            when question_id = 'q31' and sub_item = 'c' then 'In five years'
            when question_id = 'q33' and sub_item = ''  then 'Overall risk direction'
            when question_id = 'q34' and sub_item = 'a' then 'Average selling price'
            when question_id = 'q34' and sub_item = 'b' then 'Production input prices (non-labour)'
            when question_id = 'q34' and sub_item = 'c' then 'Average wage of employees'
            when question_id = 'q34' and sub_item = 'd' then 'Number of employees'
        end                                                         as sub_item_label

    from source

),

-- Q31 and Q34: weighted distribution statistics for continuous numeric responses
continuous_stats as (

    select
        country_code,
        country_name_en,
        wave_number,
        survey_year,
        survey_period,
        survey_period_label,
        question_id,
        question_label,
        sub_item,
        sub_item_label,

        count(*) filter (where not is_nonresponse)                  as n_respondents,
        count(*) filter (where is_nonresponse)                      as n_nonresponse,
        sum(weight_common) filter (where not is_nonresponse)        as total_weight,

        -- Weighted mean
        round(
            sum(response_value * weight_common) filter (where not is_nonresponse)
            / nullif(sum(weight_common) filter (where not is_nonresponse), 0),
            2
        )                                                           as mean_wtd,

        -- Unweighted mean for reference
        round(
            avg(response_value) filter (where not is_nonresponse),
            2
        )                                                           as mean_unwtd,

        -- Weighted percentiles (approximate)
        round(
            percentile_cont(0.25) within group (order by response_value)
            filter (where not is_nonresponse),
            2
        )                                                           as p25_unwtd,

        round(
            percentile_cont(0.50) within group (order by response_value)
            filter (where not is_nonresponse),
            2
        )                                                           as median_unwtd,

        round(
            percentile_cont(0.75) within group (order by response_value)
            filter (where not is_nonresponse),
            2
        )                                                           as p75_unwtd,

        -- Proportion expecting positive / zero / negative (for context)
        round(
            100.0 * count(*) filter (where not is_nonresponse and response_value > 0)
            / nullif(count(*) filter (where not is_nonresponse), 0),
            1
        )                                                           as pct_positive,

        round(
            100.0 * count(*) filter (where not is_nonresponse and response_value = 0)
            / nullif(count(*) filter (where not is_nonresponse), 0),
            1
        )                                                           as pct_zero,

        round(
            100.0 * count(*) filter (where not is_nonresponse and response_value < 0)
            / nullif(count(*) filter (where not is_nonresponse), 0),
            1
        )                                                           as pct_negative,

        -- Placeholders so UNION ALL schema matches ordinal stats CTE
        null::double                                                as pct_downside_wtd,
        null::double                                                as pct_balanced_wtd,
        null::double                                                as pct_upside_wtd,
        null::double                                                as net_balance_wtd

    from labels
    where question_id in ('q31', 'q34')
    group by all

),

-- Q33: weighted % distribution for ordinal risk direction question
ordinal_stats as (

    select
        country_code,
        country_name_en,
        wave_number,
        survey_year,
        survey_period,
        survey_period_label,
        question_id,
        question_label,
        sub_item,
        sub_item_label,

        count(*) filter (where not is_nonresponse)                  as n_respondents,
        count(*) filter (where is_nonresponse)                      as n_nonresponse,
        sum(weight_common) filter (where not is_nonresponse)        as total_weight,

        -- Placeholders so UNION ALL schema matches continuous stats CTE
        null::double                                                as mean_wtd,
        null::double                                                as mean_unwtd,
        null::double                                                as p25_unwtd,
        null::double                                                as median_unwtd,
        null::double                                                as p75_unwtd,
        null::double                                                as pct_positive,
        null::double                                                as pct_zero,
        null::double                                                as pct_negative,

        round(
            100.0 * sum(weight_common) filter (where response_value = 1 and not is_nonresponse)
            / nullif(sum(weight_common) filter (where not is_nonresponse), 0),
            2
        )                                                           as pct_downside_wtd,

        round(
            100.0 * sum(weight_common) filter (where response_value = 2 and not is_nonresponse)
            / nullif(sum(weight_common) filter (where not is_nonresponse), 0),
            2
        )                                                           as pct_balanced_wtd,

        round(
            100.0 * sum(weight_common) filter (where response_value = 3 and not is_nonresponse)
            / nullif(sum(weight_common) filter (where not is_nonresponse), 0),
            2
        )                                                           as pct_upside_wtd,

        -- Net balance = % upside risk − % downside risk
        round(
            100.0 * (
                sum(weight_common) filter (where response_value = 3 and not is_nonresponse)
                - sum(weight_common) filter (where response_value = 1 and not is_nonresponse)
            ) / nullif(sum(weight_common) filter (where not is_nonresponse), 0),
            2
        )                                                           as net_balance_wtd

    from labels
    where question_id = 'q33'
    group by all

)

select * from continuous_stats
union all
select * from ordinal_stats
order by wave_number, country_code, question_id, sub_item
