{{
  config(
    materialized = 'table'
  )
}}

/*
  Aggregated net balances for the business situation question (annex Q2).

  In the data this question appears as two question_ids:
    - 'q1': short block (sub-items a–d only), asked in common/short questionnaire rounds
    - 'q2': extended block (sub-items a–j), asked in the full questionnaire rounds
  Both use the same response scale and the sub-items a–d are identical in meaning.

  Question: "Have the following company indicators decreased, remained unchanged or increased
  during the past six months (or quarter)?"

  Response scale (verified from annex.xlsx Q2):
    1 = Increased
    2 = Remained unchanged
    3 = Decreased
    7 = Not applicable (routing / non-response)
    9 = DK/NA (non-response)

  Sub-items:
    a = Turnover
    b = Labour costs (including social contributions)
    c = Other costs (q2 only) / Interest expenses net (q1)
    d = Interest expenses (q2 only) / Profit (q1)
    e = Profit (q2 only)
    f = Profit margin — removed from questionnaire, older rounds only (q2 only)
    g = Investments in property, plant or equipment (q2 only)
    h = Inventories and other working capital (q2 only)
    i = Number of employees (q2 only)
    j = Debt compared to assets (q2 only)

  Net balance = % increased − % decreased (standard ECB definition).

  Aggregation: wave × country × question_id × sub_item (all firm sizes combined).
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
        question_id,
        sub_item,
        coalesce(response_raw, response_3m)                         as response_raw,
        weight_common,
        is_nonresponse
    from {{ ref('int_safe__core_questions_long') }}
    where question_id in ('q1', 'q2')

),

labels as (

    select
        *,
        case
            -- q1 sub-items (short questionnaire)
            when question_id = 'q1' and sub_item = 'a' then 'Turnover'
            when question_id = 'q1' and sub_item = 'b' then 'Labour costs (including social contributions)'
            when question_id = 'q1' and sub_item = 'c' then 'Interest expenses (net)'
            when question_id = 'q1' and sub_item = 'd' then 'Profit'
            -- q2 sub-items (extended questionnaire)
            when question_id = 'q2' and sub_item = 'a' then 'Turnover'
            when question_id = 'q2' and sub_item = 'b' then 'Labour costs (including social contributions)'
            when question_id = 'q2' and sub_item = 'c' then 'Other costs (materials, energy, other)'
            when question_id = 'q2' and sub_item = 'd' then 'Interest expenses (net)'
            when question_id = 'q2' and sub_item = 'e' then 'Profit'
            when question_id = 'q2' and sub_item = 'f' then 'Profit margin (legacy, older rounds only)'
            when question_id = 'q2' and sub_item = 'g' then 'Investments in property, plant or equipment'
            when question_id = 'q2' and sub_item = 'h' then 'Inventories and other working capital'
            when question_id = 'q2' and sub_item = 'i' then 'Number of employees'
            when question_id = 'q2' and sub_item = 'j' then 'Debt compared to assets'
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
        question_id,
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
order by wave_number, country_code, question_id, sub_item
