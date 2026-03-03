{{
  config(
    materialized = 'table'
  )
}}

/*
  Loan application outcomes by instrument (Q7A + Q7B in annex / question_ids 'q7a' and 'q7b').

  --- Q7A: Application status ---
  Question: "Have you applied for the following types of financing during the past period?"

  Response codes (verified from annex.xlsx Q7A):
    1 = Applied
    2 = Did not apply — fear of rejection (DISCOURAGED)
    3 = Did not apply — sufficient internal funds
    4 = Did not apply — other reasons
    9 = DK/NA (non-response)

  --- Q7B: Outcome of application ---
  Question: "If you applied, what was the outcome?"
  Note: Only populated for firms where Q7A = 1 (applied). Null for non-applicants.

  Response codes (verified from annex.xlsx Q7B):
    1 = Received everything applied for
    2 = Applied but only got part (used up to 2010H1, then split into 5 and 6)
    3 = Refused — cost was too high
    4 = Was rejected by the lender
    5 = Received most of what was applied for (new from 2010H1)
    6 = Received only a limited part (new from 2010H1)
    8 = Application still pending (new from 2014H1)
    9 = DK (non-response)

  Sub-items (same for both Q7A and Q7B, verified from annex.xlsx):
    a = Bank loan (excluding overdraft and credit lines)
    b = Trade credit
    c = Other external financing
    d = Credit line, bank overdraft or credit cards overdraft

  Key metrics computed:
    - application_rate: % of valid respondents that applied (Q7A = 1)
    - discouragement_rate: % of valid respondents that were discouraged (Q7A = 2)
    - success_rate: % of applicants that received all or most (Q7B in 1, 5)
    - partial_rate: % of applicants that received a limited part (Q7B in 2, 6)
    - rejection_rate: % of applicants that were rejected (Q7B = 4)
    - financing_gap: discouragement_rate + rejection_rate (ECB headline indicator)
      expressed as % of all valid respondents

  Scope: SMEs only (employee_band_code 1–3: micro, small, medium).
  Large firms (band 4, 250+ employees) are excluded for comparability with
  the ECB's published SAFE data warehouse, which reports SME aggregates.

  Aggregation: wave × country × instrument.
*/

with q7a as (

    select
        permid,
        wave_number,
        survey_year,
        survey_period,
        survey_period_label,
        country_code,
        country_name_en,
        sub_item,
        coalesce(response_raw, response_3m)                         as q7a_response,
        weight_common,
        is_nonresponse                                              as q7a_nonresponse
    from {{ ref('int_safe__core_questions_long') }}
    where question_id = 'q7a' and employee_band_code BETWEEN 1 and 3

),

q7b as (

    select
        permid,
        wave_number,
        sub_item,
        coalesce(response_raw, response_3m)                         as q7b_response,
        coalesce(response_raw, response_3m) in (-1, -2, -99, 7, 9, 99) as q7b_nonresponse
    from {{ ref('int_safe__core_questions_long') }}
    where question_id = 'q7b' and employee_band_code BETWEEN 1 and 3
),

combined as (

    select
        a.wave_number,
        a.survey_year,
        a.survey_period,
        a.survey_period_label,
        a.country_code,
        a.country_name_en,
        a.sub_item,

        case a.sub_item
            when 'a' then 'Bank loan (excl. overdraft and credit lines)'
            when 'b' then 'Trade credit'
            when 'c' then 'Other external financing'
            when 'd' then 'Credit line, bank overdraft or credit cards overdraft'
        end                                                         as instrument_label,

        a.q7a_response,
        a.q7a_nonresponse,
        b.q7b_response,
        b.q7b_nonresponse,
        a.weight_common

    from q7a a
    left join q7b b using (permid, wave_number, sub_item)

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
        instrument_label,

        -- Counts
        count(*)                                                        as n_total,
        count(*) filter (where not q7a_nonresponse)                     as n_valid_q7a,
        count(*) filter (where q7a_response = 1)                        as n_applied,
        count(*) filter (where q7a_response = 2)                        as n_discouraged,
        count(*) filter (where q7a_response = 3)                        as n_no_need,
        count(*) filter (where q7a_response = 4)                        as n_other_no_apply,
        count(*) filter (where q7a_response = 1
                         and q7b_response in (1, 5))                    as n_success_full,
        count(*) filter (where q7a_response = 1
                         and q7b_response in (2, 6))                    as n_success_partial,
        count(*) filter (where q7a_response = 1
                         and q7b_response = 3)                          as n_too_costly,
        count(*) filter (where q7a_response = 1
                         and q7b_response = 4)                          as n_rejected,
        count(*) filter (where q7a_response = 1
                         and q7b_response = 8)                          as n_pending,

        -- Weighted total for valid Q7A respondents
        sum(weight_common) filter (where not q7a_nonresponse)           as total_weight,

        -- Rates as % of all valid Q7A respondents (weighted)
        round(
            100.0 * sum(weight_common) filter (where q7a_response = 1)
            / nullif(sum(weight_common) filter (where not q7a_nonresponse), 0),
            2
        )                                                               as application_rate_wtd,

        round(
            100.0 * sum(weight_common) filter (where q7a_response = 2)
            / nullif(sum(weight_common) filter (where not q7a_nonresponse), 0),
            2
        )                                                               as discouragement_rate_wtd,

        -- Rates as % of applicants (weighted)
        round(
            100.0 * sum(weight_common) filter (where q7a_response = 1
                                                and q7b_response in (1, 5))
            / nullif(sum(weight_common) filter (where q7a_response = 1), 0),
            2
        )                                                               as success_full_rate_wtd,

        round(
            100.0 * sum(weight_common) filter (where q7a_response = 1
                                                and q7b_response in (2, 6))
            / nullif(sum(weight_common) filter (where q7a_response = 1), 0),
            2
        )                                                               as success_partial_rate_wtd,

        round(
            100.0 * sum(weight_common) filter (where q7a_response = 1
                                                and q7b_response = 4)
            / nullif(sum(weight_common) filter (where q7a_response = 1), 0),
            2
        )                                                               as rejection_rate_wtd,

        -- Financing gap = % discouraged + % rejected (both as % of all valid respondents)
        -- Rejection share of all respondents = rejection_rate × application_rate / 100
        round(
            100.0 * (
                sum(weight_common) filter (where q7a_response = 2)
                + sum(weight_common) filter (where q7a_response = 1 and q7b_response = 4)
            ) / nullif(sum(weight_common) filter (where not q7a_nonresponse), 0),
            2
        )                                                               as financing_gap_wtd

    from combined
    group by all

)

select * from aggregated
order by wave_number, country_code, sub_item
