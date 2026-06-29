{{
  config(
    materialized = 'table'
  )
}}

/*
  Net balances for financing needs (Q5), availability (Q9), and bank loan
  terms (Q10). Three-month reference period only (wave 30 / 2024Q1 onward).

  ECB methodology: all firms in scope; codes 7 (not applicable) and 9 (DK)
  excluded from both numerator and denominator.

  NET BALANCE: % code-1 − % code-3 (weighted by weight_common).
    Q5:  1=Increased need,       3=Decreased need.    Positive = more need.
    Q9:  1=Improved,             3=Deteriorated.      Positive = better availability.
    Q10: 1=Increased by bank,    3=Decreased by bank. Positive = tightening (adverse).

  FIRM SIZE:
    'all'   = all respondents — matches ECB published figures.
    'sme'   = employee_band_code 1–3 (up to 249 employees).
    'large' = employee_band_code 4 (250+ employees).

  Q5/Q9 sub-items: a=bank loans, b=trade credit, c=equity, d=debt securities,
    f=credit line/overdraft, g=leasing, h=other loan.
  Q10 sub-items: a=interest rates, b=non-interest costs, c=loan size,
    d=maturity, e=collateral, f=other terms.

  financing_gap_wtd (Q9 rows only) = Q5.net_balance − Q9.net_balance.
    Positive = needs rising faster than availability (credit crunch signal).
    NULL for Q5 and Q10 rows.

  Aggregation: wave × country × question_id × sub_item × firm_size.
*/

with source as (

    select
        f.wave_number,
        f.survey_year,
        f.survey_period,
        f.survey_period_label,
        f.country_code,
        f.country_name_en,
        f.is_sme,
        f.is_euro_area,
        q.question_id,
        q.sub_item,
        q.response_3m                                   as response,
        q.response_3m in (-1, -2, -99, 7, 9, 99)       as is_nonresponse,
        f.weight_common

    from {{ ref('int_safe__core_questions_long') }} q
    join {{ ref('int_safe__firm_survey_responses') }} f
        using (permid, wave_number)
    where q.question_id in ('q5', 'q9', 'q10')
      and not (q.question_id = 'q5' and q.sub_item = 'e')
      and q.response_3m is not null
      and f.wave_number >= 30

),

with_ea as (

    select * from source

    union all

    select wave_number, survey_year, survey_period, survey_period_label,
           'EA' as country_code, 'Euro Area' as country_name_en,
           is_sme, is_euro_area, question_id, sub_item, response, is_nonresponse, weight_common
    from source
    where is_euro_area

),

source_sized as (

    select s.*, sc.firm_size
    from with_ea s
    cross join (values ('all'), ('sme'), ('large')) as sc(firm_size)
    where
        sc.firm_size = 'all'
        or (sc.firm_size = 'sme'   and s.is_sme)
        or (sc.firm_size = 'large' and not s.is_sme)

),

aggregated as (

    select
        wave_number,
        survey_year,
        survey_period,
        survey_period_label,
        country_code,
        country_name_en,
        question_id,
        sub_item,
        firm_size,

        count(*) filter (where not is_nonresponse)      as n_respondents,
        count(*) filter (where is_nonresponse)          as n_nonresponse,
        sum(weight_common) filter (where not is_nonresponse) as total_weight,

        round(
            100.0 * sum(weight_common) filter (where response = 1 and not is_nonresponse)
            / nullif(sum(weight_common) filter (where not is_nonresponse), 0)
        , 2)                                            as pct_improved_wtd,

        round(
            100.0 * sum(weight_common) filter (where response = 2 and not is_nonresponse)
            / nullif(sum(weight_common) filter (where not is_nonresponse), 0)
        , 2)                                            as pct_unchanged_wtd,

        round(
            100.0 * sum(weight_common) filter (where response = 3 and not is_nonresponse)
            / nullif(sum(weight_common) filter (where not is_nonresponse), 0)
        , 2)                                            as pct_deteriorated_wtd,

        round(
            100.0 * (
                sum(weight_common) filter (where response = 1 and not is_nonresponse)
                - sum(weight_common) filter (where response = 3 and not is_nonresponse)
            ) / nullif(sum(weight_common) filter (where not is_nonresponse), 0)
        , 1)                                            as net_balance_wtd,

        round(
            100.0 * (
                count(*) filter (where response = 1 and not is_nonresponse)
                - count(*) filter (where response = 3 and not is_nonresponse)
            ) / nullif(count(*) filter (where not is_nonresponse), 0)
        , 1)                                            as net_balance_unwtd

    from source_sized
    group by
        wave_number, survey_year, survey_period, survey_period_label,
        country_code, country_name_en, question_id, sub_item, firm_size

),

labeled as (

    select
        *,
        case question_id
            when 'q5'  then 'Change in need for external financing'
            when 'q9'  then 'Availability of external financing'
            when 'q10' then 'Terms and conditions of bank financing'
        end                                             as question_label,

        case
            when question_id in ('q5', 'q9') and sub_item = 'a' then 'Bank loans (excl. overdraft and credit lines)'
            when question_id in ('q5', 'q9') and sub_item = 'b' then 'Trade credit'
            when question_id in ('q5', 'q9') and sub_item = 'c' then 'Equity capital'
            when question_id in ('q5', 'q9') and sub_item = 'd' then 'Debt securities issued'
            when question_id in ('q5', 'q9') and sub_item = 'f' then 'Credit line, bank overdraft or credit cards overdraft'
            when question_id in ('q5', 'q9') and sub_item = 'g' then 'Leasing or hire-purchase'
            when question_id in ('q5', 'q9') and sub_item = 'h' then 'Other loan'
            when question_id = 'q10' and sub_item = 'a' then 'Level of interest rates'
            when question_id = 'q10' and sub_item = 'b' then 'Non-interest financing costs (charges, fees, commissions)'
            when question_id = 'q10' and sub_item = 'c' then 'Available size of loan or credit line'
            when question_id = 'q10' and sub_item = 'd' then 'Available maturity of the loan'
            when question_id = 'q10' and sub_item = 'e' then 'Collateral requirements'
            when question_id = 'q10' and sub_item = 'f' then 'Other terms (guarantees, covenants, procedures)'
        end                                             as sub_item_label

    from aggregated
    where n_respondents > 0

),

with_gap as (

    select
        a.*,
        case
            when a.question_id = 'q9' then
                round(q5.net_balance_wtd - a.net_balance_wtd, 1)
            else null
        end                                             as financing_gap_wtd
    from labeled a
    left join labeled q5
        on  q5.country_code  = a.country_code
        and q5.wave_number   = a.wave_number
        and q5.sub_item      = a.sub_item
        and q5.firm_size     = a.firm_size
        and q5.question_id   = 'q5'

)

select *
from with_gap
order by wave_number, country_code, question_id, sub_item, firm_size
