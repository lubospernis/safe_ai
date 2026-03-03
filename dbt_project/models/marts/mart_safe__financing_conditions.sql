{{
  config(
    materialized = 'table'
  )
}}

/*
  Aggregated net balances for financing needs (Q5), financing availability (Q9), and
  bank loan terms and conditions (Q10).

  Each question uses raw response codes for net balance: % code-1 − % code-3.
  Signs differ by question — read net_balance_wtd in context:

  --- Q5: Change in need for external financing ---
  Question: "Over the past six months, has your need for the following types of
  external financing increased, remained unchanged or decreased?"

  Response scale (verified from annex.xlsx Q5):
    1 = Increased need   → pct_improved_wtd (code 1, "first response")
    2 = Remained unchanged
    3 = Decreased need   → pct_deteriorated_wtd (code 3, "third response")
    7 = Not applicable, 9 = DK (non-response)

  Net balance for Q5 = % increased need − % decreased need.
  Positive = net increase in financing need (pressure building).

  Sub-items (verified from annex.xlsx Q5):
    a = Bank loans (excl. overdraft and credit lines)
    b = Trade credit
    c = Equity capital
    d = Debt securities issued
    e = Other (legacy, removed in later rounds)
    f = Credit line, bank overdraft or credit cards overdraft
    g = Leasing or hire-purchase
    h = Other loan (family, friends, related enterprise, shareholders)

  --- Q9: Availability of financing ---
  Question: "For each type of financing, has availability improved, remained unchanged or
  deteriorated for your enterprise during the past period?"

  Response scale (verified from annex.xlsx Q9):
    1 = Improved            → pct_improved_wtd
    2 = Remained unchanged
    3 = Deteriorated        → pct_deteriorated_wtd
    7 = Not applicable, 9 = DK (non-response)

  Net balance for Q9 = % improved − % deteriorated.
  Negative = net deterioration in financing availability.

  Sub-items (verified from annex.xlsx Q9):
    a = Bank loans (excluding overdraft and credit lines)
    b = Trade credit
    c = Equity capital (including venture capital / business angels)
    d = Debt securities issued
    e = Other (e.g. loans from related company, leasing, factoring, grants)
    f = Credit line, bank overdraft or credit cards overdraft
    g = Leasing or hire-purchase
    h = Other loan (from family, friends, related enterprise)

  --- Financing gap (Q5 vs Q9, same instrument) ---
  financing_gap_wtd = net_balance_wtd(Q5) − net_balance_wtd(Q9)
    = (% need increased − % need decreased) − (% availability improved − % availability deteriorated)
    Positive gap = needs rising faster than availability (credit crunch signal).
    Negative gap = availability improving relative to needs (easing conditions).
  Only populated for rows where question_id = 'q9' (the availability side).
  NULL for Q5 and Q10 rows.

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

  Note: for Q10, pct_improved_wtd = % where bank decreased (loosened) the term,
  pct_deteriorated_wtd = % where bank increased (tightened) the term.
  Net balance negative = net tightening of bank lending terms.

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
    where question_id in ('q5', 'q9', 'q10')
      and employee_band_code between 1 and 3
      -- Q5 sub_item 'e' is a legacy catch-all removed in later rounds
      and not (question_id = 'q5' and sub_item = 'e')

),

labels as (

    select
        *,
        case question_id
            when 'q5'  then 'Change in need for external financing'
            when 'q9'  then 'Availability of external financing'
            when 'q10' then 'Terms and conditions of bank financing'
        end                                                         as question_label,

        case
            -- Q5 sub-items (same instruments as Q9)
            when question_id = 'q5' and sub_item = 'a' then 'Bank loans (excl. overdraft and credit lines)'
            when question_id = 'q5' and sub_item = 'b' then 'Trade credit'
            when question_id = 'q5' and sub_item = 'c' then 'Equity capital'
            when question_id = 'q5' and sub_item = 'd' then 'Debt securities issued'
            when question_id = 'q5' and sub_item = 'f' then 'Credit line, bank overdraft or credit cards overdraft'
            when question_id = 'q5' and sub_item = 'g' then 'Leasing or hire-purchase'
            when question_id = 'q5' and sub_item = 'h' then 'Other loan'
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

        -- pct_improved_wtd = % code 1:
        --   Q5:  need increased   Q9: availability improved   Q10: term increased by bank
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

        -- pct_deteriorated_wtd = % code 3:
        --   Q5:  need decreased   Q9: availability deteriorated   Q10: term decreased by bank
        round(
            100.0 * sum(weight_common) filter (where response_raw = 3 and not is_nonresponse)
            / nullif(sum(weight_common) filter (where not is_nonresponse), 0),
            2
        )                                                           as pct_deteriorated_wtd,

        -- Net balance = % code1 − % code3.
        -- Q5:  positive = net increase in need (pressure rising)
        -- Q9:  positive = net improvement in availability
        -- Q10: positive = net tightening by bank (e.g. rates rising, collateral rising)
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

),

-- Financing gap on Q9 rows: needs_nb − avail_nb.
-- Positive = needs rising faster than availability (gap opened, credit crunch signal).
-- Negative = availability improving relative to needs (conditions easing).
-- NULL for Q5 and Q10 rows.
with_gap as (

    select
        a.*,
        case
            when a.question_id = 'q9' then
                round(q5.net_balance_wtd - a.net_balance_wtd, 2)
            else null
        end                                                         as financing_gap_wtd
    from aggregated a
    left join aggregated q5
        on  q5.country_code  = a.country_code
        and q5.wave_number   = a.wave_number
        and q5.sub_item      = a.sub_item
        and q5.question_id   = 'q5'

)

select * from with_gap
order by wave_number, country_code, question_id, sub_item
