{{
  config(
    materialized = 'table'
  )
}}

/*
  Aggregated net balances for the business situation question (annex Q2 / question_id = 'q2').

  Question: "Have the following company indicators decreased, remained unchanged or increased
  during the past six months (or quarter)?"

  Response scale (verified from annex.xlsx Q2):
    1 = Increased
    2 = Remained unchanged
    3 = Decreased
    7 = Not applicable (routing / non-response)
    9 = DK/NA (non-response)

  Sub-items (verified from annex.xlsx Q2):
    a = Turnover
    b = Labour costs (including social contributions)
    c = Other costs (materials, energy, other)
    d = Interest expenses (net)
    e = Profit
    f = Profit margin — removed from questionnaire, older rounds only
    g = Investments in property, plant or equipment
    h = Inventories and other working capital
    i = Number of employees
    j = Debt compared to assets

  Net balance = % increased − % decreased (standard ECB definition).

  Scope: SMEs only (employee_band_code 1–3: micro, small, medium).
  Large firms (band 4, 250+ employees) are excluded for comparability with
  the ECB's published SAFE data warehouse, which reports SME aggregates.

  Aggregation: wave × country × sub_item.
  Non-responses (response_raw in -1, -2, -99, 7, 99) excluded from aggregation metrics
  but counted in n_nonresponse.
*/

with source as (

    select
        wave_number,
        survey_year,
        survey_period,
        survey_period_label,
        country_code,
        country_name_en,
        sub_item,
        coalesce(response_raw, response_3m)                         as response_raw,
        weight_common,
        is_nonresponse,
        employee_band_code
    from {{ ref('int_safe__core_questions_long') }}
    where question_id = 'q2' and employee_band_code BETWEEN 1 and 3

),

labels as (

    select
        *,
        case sub_item
            when 'a' then 'Turnover'
            when 'b' then 'Labour costs (including social contributions)'
            when 'c' then 'Other costs (materials, energy, other)'
            when 'd' then 'Interest expenses (net)'
            when 'e' then 'Profit'
            when 'f' then 'Profit margin (legacy, older rounds only)'
            when 'g' then 'Investments in property, plant or equipment'
            when 'h' then 'Inventories and other working capital'
            when 'i' then 'Number of employees'
            when 'j' then 'Debt compared to assets'
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

        count(*)                                                    as n_total,
        count(*) filter (where not is_nonresponse)                  as n_respondents,
        count(*) filter (where is_nonresponse)                      as n_nonresponse,
        sum(weight_common) filter (where not is_nonresponse)        as total_weight,

        -- Weighted percentages
        round(
            100.0 * sum(weight_common) filter (where response_raw = 1 and not is_nonresponse)
            / nullif(sum(weight_common) filter (where not is_nonresponse), 0),
            2
        )                                                           as pct_increased_wtd,

        round(
            100.0 * sum(weight_common) filter (where response_raw = 2 and not is_nonresponse)
            / nullif(sum(weight_common) filter (where not is_nonresponse), 0),
            2
        )                                                           as pct_unchanged_wtd,

        round(
            100.0 * sum(weight_common) filter (where response_raw = 3 and not is_nonresponse)
            / nullif(sum(weight_common) filter (where not is_nonresponse), 0),
            2
        )                                                           as pct_decreased_wtd,

        -- Net balance (% increased − % decreased)
        round(
            100.0 * (
                sum(weight_common) filter (where response_raw = 1 and not is_nonresponse)
                - sum(weight_common) filter (where response_raw = 3 and not is_nonresponse)
            ) / nullif(sum(weight_common) filter (where not is_nonresponse), 0),
            2
        )                                                           as net_balance_wtd,

        -- Unweighted net balance for reference
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
order by wave_number, country_code, sub_item
