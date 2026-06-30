{{
  config(
    materialized = 'table'
  )
}}

/*
  Expected availability of external financing over the next quarter (Q23).

  Question (verified from annex.xlsx Q23): "Looking ahead, for each of the following
  types of financing available to your enterprise, please indicate whether you think
  their availability will improve, deteriorate or remain unchanged over the next
  [two quarters / quarter]."

  Response scale (verified from annex.xlsx Q23):
    1 = Will improve
    2 = Will remain unchanged
    3 = Will deteriorate
    7 = Not applicable (excluded from denominator)
    9 = DK (excluded from denominator)

  Sub-items included (verified from annex.xlsx Q23, matching Q5/Q9 instruments):
    b = Bank loans (excluding overdraft and credit lines)
    d = Trade credit
    g = Credit line, bank overdraft or credit cards overdraft

  Net balance = % will improve − % will deteriorate.
  Positive = more firms expect availability to improve (FAVOURABLE).
  Negative = more firms expect availability to deteriorate (ADVERSE).

  firm_size: 'all' = all respondents; 'sme' = employee_band_code 1–3.
  Three-month reference period only (wave 30 / 2024Q1 onward).
  Aggregation: wave × country × sub_item × firm_size.
*/

with source_all as (

    select
        wave_number,
        survey_year,
        survey_period,
        survey_period_label,
        country_code,
        country_name_en,
        sub_item,
        response_3m                                                 as response_raw,
        response_3m in (-1, -2, -99, 7, 9, 99)                     as is_nonresponse,
        weight_common,
        'all'                                                       as firm_size
    from {{ ref('int_safe__core_questions_long') }}
    where question_id = 'q23'
      and sub_item in ('b', 'd', 'g')
      and wave_number >= 30
      and response_3m is not null

),

source_sme as (

    select
        wave_number,
        survey_year,
        survey_period,
        survey_period_label,
        country_code,
        country_name_en,
        sub_item,
        response_3m                                                 as response_raw,
        response_3m in (-1, -2, -99, 7, 9, 99)                     as is_nonresponse,
        weight_common,
        'sme'                                                       as firm_size
    from {{ ref('int_safe__core_questions_long') }}
    where question_id = 'q23'
      and sub_item in ('b', 'd', 'g')
      and employee_band_code between 1 and 3
      and wave_number >= 30
      and response_3m is not null

),

source as (

    select * from source_all
    union all
    select * from source_sme

),

labels as (

    select
        *,
        case sub_item
            when 'b' then 'Bank loans'
            when 'd' then 'Trade credit'
            when 'g' then 'Credit lines and bank overdraft'
        end                                                         as sub_item_label

    from source

),

aggregated as (

    select
        country_code,
        country_name_en,
        wave_number,
        survey_year,
        survey_period,
        survey_period_label,
        sub_item,
        sub_item_label,
        firm_size,

        count(*)                                                    as n_total,
        count(*) filter (where not is_nonresponse)                  as n_respondents,
        count(*) filter (where is_nonresponse)                      as n_nonresponse,
        sum(weight_common) filter (where not is_nonresponse)        as total_weight,

        round(
            100.0 * sum(weight_common) filter (where response_raw = 1 and not is_nonresponse)
            / nullif(sum(weight_common) filter (where not is_nonresponse), 0),
            2
        )                                                           as pct_improve_wtd,

        round(
            100.0 * sum(weight_common) filter (where response_raw = 2 and not is_nonresponse)
            / nullif(sum(weight_common) filter (where not is_nonresponse), 0),
            2
        )                                                           as pct_unchanged_wtd,

        round(
            100.0 * sum(weight_common) filter (where response_raw = 3 and not is_nonresponse)
            / nullif(sum(weight_common) filter (where not is_nonresponse), 0),
            2
        )                                                           as pct_deteriorate_wtd,

        -- Net balance (% improve − % deteriorate)
        round(
            100.0 * (
                sum(weight_common) filter (where response_raw = 1 and not is_nonresponse)
                - sum(weight_common) filter (where response_raw = 3 and not is_nonresponse)
            ) / nullif(sum(weight_common) filter (where not is_nonresponse), 0),
            2
        )                                                           as net_balance_wtd,

        round(
            100.0 * (
                count(*) filter (where response_raw = 1 and not is_nonresponse)
                - count(*) filter (where response_raw = 3 and not is_nonresponse)
            ) / nullif(count(*) filter (where not is_nonresponse), 0),
            2
        )                                                           as net_balance_unwtd

    from labels
    group by all

)

select * from aggregated
order by wave_number, country_code, sub_item, firm_size
