{{
  config(
    materialized = 'table'
  )
}}

/*
  Aggregated net balances for financing availability (Q9 in annex / question_id = 'q9') and
  bank loan terms and conditions (Q10 in annex / question_id = 'q10').

  --- Q9: Availability of financing ---
  Question: "For each type of financing, has availability improved, remained unchanged or
  deteriorated for your enterprise during the past period?"

  Response scale (verified from annex.xlsx Q9):
    1 = Improved
    2 = Remained unchanged
    3 = Deteriorated
    7 = Not applicable (routing / non-response)
    9 = DK (non-response)

  Sub-items (verified from annex.xlsx Q9):
    a = Bank loans (excluding overdraft and credit lines)
    b = Trade credit
    c = Equity capital (including venture capital / business angels)
    d = Debt securities issued
    e = Other (e.g. loans from related company, leasing, factoring, grants)
    f = Credit line, bank overdraft or credit cards overdraft
    g = Leasing or hire-purchase
    h = Other loan (from family, friends, related enterprise)

  --- Q10: Terms and conditions of bank financing ---
  Question: "Please indicate whether the following items were increased, remained unchanged
  or decreased by the bank (for bank loans, overdrafts and credit lines)."

  Response scale (verified from annex.xlsx Q10):
    1 = Was increased by the bank
    2 = Remained unchanged
    3 = Was decreased by the bank
    9 = DK/NA (non-response)

  Sub-items (verified from annex.xlsx Q10):
    a = Level of interest rates
    b = Level of cost of financing other than interest rates (charges, fees, commissions)
    c = Available size of loan or credit line
    d = Available maturity of the loan
    e = Collateral requirements
    f = Other (required guarantees, information requirements, loan covenants, procedures)

  Net balance = % improved − % deteriorated (Q9) or % increased − % decreased (Q10).

  Scope: SMEs only (employee_band_code 1–3: micro, small, medium).
  Large firms (band 4, 250+ employees) are excluded for comparability with
  the ECB's published SAFE data warehouse, which reports SME aggregates.

  Aggregation: wave × country × question_id × sub_item.
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
        coalesce(response_raw, response_3m)         as response_raw,
        weight_common,
        is_nonresponse
    from {{ ref('int_safe__core_questions_long') }}
    where question_id in ('q9', 'q10')
      and employee_band_code between 1 and 3

),

labels as (

    select
        *,
        case question_id
            when 'q9'  then 'Availability of external financing'
            when 'q10' then 'Terms and conditions of bank financing'
        end                                                         as question_label,

        case
            -- Q9 sub-items
            when question_id = 'q9' and sub_item = 'a' then 'Bank loans (excl. overdraft and credit lines)'
            when question_id = 'q9' and sub_item = 'b' then 'Trade credit'
            when question_id = 'q9' and sub_item = 'c' then 'Equity capital'
            when question_id = 'q9' and sub_item = 'd' then 'Debt securities issued'
            when question_id = 'q9' and sub_item = 'e' then 'Other financing'
            when question_id = 'q9' and sub_item = 'f' then 'Credit line, bank overdraft or credit cards overdraft'
            when question_id = 'q9' and sub_item = 'g' then 'Leasing or hire-purchase'
            when question_id = 'q9' and sub_item = 'h' then 'Other loan'
            -- Q10 sub-items
            when question_id = 'q10' and sub_item = 'a' then 'Level of interest rates'
            when question_id = 'q10' and sub_item = 'b' then 'Non-interest financing costs (charges, fees, commissions)'
            when question_id = 'q10' and sub_item = 'c' then 'Available size of loan or credit line'
            when question_id = 'q10' and sub_item = 'd' then 'Available maturity of the loan'
            when question_id = 'q10' and sub_item = 'e' then 'Collateral requirements'
            when question_id = 'q10' and sub_item = 'f' then 'Other terms (guarantees, covenants, procedures)'
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
        question_label,
        sub_item,
        sub_item_label,

        count(*)                                                    as n_total,
        count(*) filter (where not is_nonresponse)                  as n_respondents,
        count(*) filter (where is_nonresponse)                      as n_nonresponse,
        sum(weight_common) filter (where not is_nonresponse)        as total_weight,

        -- Weighted percentages (improved / increased = code 1)
        round(
            100.0 * sum(weight_common) filter (where response_raw = 1 and not is_nonresponse)
            / nullif(sum(weight_common) filter (where not is_nonresponse), 0),
            2
        )                                                           as pct_improved_wtd,

        round(
            100.0 * sum(weight_common) filter (where response_raw = 2 and not is_nonresponse)
            / nullif(sum(weight_common) filter (where not is_nonresponse), 0),
            2
        )                                                           as pct_unchanged_wtd,

        round(
            100.0 * sum(weight_common) filter (where response_raw = 3 and not is_nonresponse)
            / nullif(sum(weight_common) filter (where not is_nonresponse), 0),
            2
        )                                                           as pct_deteriorated_wtd,

        -- Net balance (% improved/increased − % deteriorated/decreased)
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
