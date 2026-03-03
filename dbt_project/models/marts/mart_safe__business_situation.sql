{{
  config(
    materialized = 'table'
  )
}}

/*
  Aggregated net balances for the business situation question (Q2 in annex / question_id = 'q1' in data).

  Question: "Have the following company indicators decreased, remained unchanged or increased
  during the past six months (or quarter)?"

  Response scale (verified from annex.xlsx Q2):
    1 = Increased
    2 = Remained unchanged
    3 = Decreased
    7 = Not applicable (routing / non-response)
    9 = DK/NA (non-response)

  Sub-items in the data (raw column names q1_a through q1_d):
    a = Turnover
    b = Labour costs (including social contributions)
    c = Interest expenses (net)
    d = Profit

  Net balance = % increased − % decreased (standard ECB definition).
  Positive net balance = indicator is rising on balance; interpretation depends on the item
  (e.g. rising turnover = positive, rising interest expenses = negative for firms).

  Aggregation: wave × country × sub_item (all firm sizes combined).
  Non-responses (response_raw in -1, -2, -99, 7, 99) excluded from aggregation metrics
  but counted in n_nonresponse.
*/

with q1 as (

    select
        wave_number,
        survey_year,
        survey_period,
        survey_period_label,
        country_code,
        country_name_en,
        sub_item,
        response_raw,
        weight_common,
        is_nonresponse
    from {{ ref('int_safe__core_questions_long') }}
    where question_id = 'q1'

),

labels as (

    select
        *,
        case sub_item
            when 'a' then 'Turnover'
            when 'b' then 'Labour costs (including social contributions)'
            when 'c' then 'Interest expenses (net)'
            when 'd' then 'Profit'
        end                                                         as sub_item_label

    from q1

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
