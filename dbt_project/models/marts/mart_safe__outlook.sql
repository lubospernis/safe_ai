{{
  config(
    materialized = 'table'
  )
}}

/*
  Aggregated net balances for expected changes in turnover and investments (Q26).

  Question: "Looking ahead, please indicate whether you think the following will
  increase, decrease or remain unchanged over the next two quarters."

  Response scale (verified from annex.xlsx Q26):
    1 = Will increase
    2 = Will remain unchanged
    3 = Will decrease
    9 = DK (non-response)

  Sub-items (verified from annex.xlsx Q26):
    a = Your company's turnover
    b = Investments in property, plant or equipment (fixed investment)

  Net balance = % will increase − % will decrease (standard ECB definition).

  Scope: SMEs only (employee_band_code 1–3: micro, small, medium).
  Large firms (band 4, 250+ employees) are excluded for comparability with
  the ECB's published SAFE data warehouse, which reports SME aggregates.

  Aggregation: wave × country × sub_item.
*/

with source as (

    select
        q.wave_number,
        f.survey_year,
        f.survey_period,
        f.survey_period_label,
        f.country_code,
        f.country_name_en,
        q.sub_item,
        q.response_3m                                                       as response_raw,
        q.weight_common,
        q.is_nonresponse,
        f.is_euro_area
    from {{ ref('int_safe__core_questions_long') }} q
    join {{ ref('int_safe__firm_survey_responses') }} f
        using (permid, wave_number)
    where q.question_id = 'q26'
      and f.employee_band_code between 1 and 3
      and q.wave_number >= 30
      and q.response_3m is not null

),

with_ea as (

    select * from source

    union all

    select wave_number, survey_year, survey_period, survey_period_label,
           'EA' as country_code, 'Euro Area' as country_name_en,
           sub_item, response_raw, weight_common, is_nonresponse, is_euro_area
    from source
    where is_euro_area

),

labels as (

    select
        *,
        case sub_item
            when 'a' then 'Turnover'
            when 'b' then 'Investments in property, plant or equipment'
        end                                                         as sub_item_label

    from with_ea

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
        )                                                           as pct_increase_wtd,

        round(
            100.0 * sum(weight_common) filter (where response_raw = 2 and not is_nonresponse)
            / nullif(sum(weight_common) filter (where not is_nonresponse), 0),
            2
        )                                                           as pct_unchanged_wtd,

        round(
            100.0 * sum(weight_common) filter (where response_raw = 3 and not is_nonresponse)
            / nullif(sum(weight_common) filter (where not is_nonresponse), 0),
            2
        )                                                           as pct_decrease_wtd,

        -- Net balance (% will increase − % will decrease)
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
