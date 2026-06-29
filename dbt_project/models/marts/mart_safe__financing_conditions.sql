{{
  config(
    materialized = 'table'
  )
}}

/*
  Net balances for financing needs (Q5), availability (Q9), and bank loan
  terms (Q10), following ECB methodology.

  METHODOLOGY (verified against ECB Table 1, wave 38 / 2026Q1):
  ─────────────────────────────────────────────────────────────────────
  1. SCOPE: three firm_size cuts per row — see FIRM SIZE below.

  2. REFERENCE PERIOD: 6m and 3m kept separate — one row each per wave.
       reference_period = '6m': six-month questionnaire (ECB white panel).
       reference_period = '3m': three-month questionnaire (ECB grey panel).
     Waves 1–29: only '6m' rows. Waves 30+ may have both per wave.
     To match ECB Table 1, use reference_period = '3m' for Q2/Q4 waves
     and for the grey panel in Q1/Q3 waves.

  3. NON-RESPONSE: codes 7 (not applicable / routing skip) and 9 (DK/NA)
     excluded from both numerator and denominator.
     Code 7 is NOT "unchanged" — it means the firm does not use that instrument.

  4. NET BALANCE: % code-1 − % code-3 (weighted by weight_common).
     Q5:  1=Increased need,       3=Decreased need.    Positive = more need.
     Q9:  1=Improved,             3=Deteriorated.      Positive = better availability.
     Q10: 1=Increased by bank,    3=Decreased by bank. Positive = tightening (adverse).

  FIRM SIZE (firm_size column):
  ─────────────────────────────────────────────────────────────────────
    'all'   = all respondents regardless of size — matches ECB Table 1.
    'sme'   = micro, small, medium (employee_band_code 1–3, up to 249 employees).
    'large' = large firms (employee_band_code 4, 250+ employees).
  To replicate ECB published figures, always filter firm_size = 'all'.

  QUESTIONS AND SUB-ITEMS:
  ─────────────────────────────────────────────────────────────────────
  Q5 — Change in need for external financing
    a = Bank loans (excl. overdraft and credit lines)
    b = Trade credit
    c = Equity capital
    d = Debt securities issued
    f = Credit line, bank overdraft or credit cards overdraft
    g = Leasing or hire-purchase
    h = Other loan

  Q9 — Availability of external financing
    a = Bank loans (excl. overdraft and credit lines)
    b = Trade credit
    c = Equity capital
    d = Debt securities issued
    f = Credit line, bank overdraft or credit cards overdraft
    g = Leasing or hire-purchase
    h = Other loan

  Q10 — Terms and conditions of bank financing
    a = Level of interest rates
    b = Non-interest financing costs (charges, fees, commissions)
    c = Available size of loan or credit line
    d = Available maturity of the loan
    e = Collateral requirements
    f = Other terms (guarantees, covenants, procedures)
    Note: Q10 is not published in ECB Table 1 but follows the same methodology.

  financing_gap_wtd (Q9 rows only) = Q5.net_balance − Q9.net_balance for
  the same wave × country × sub_item × reference_period × firm_size.
  Positive = needs rising faster than availability (credit crunch signal).
  NULL for Q5 and Q10 rows.

  Aggregation: wave × country × question_id × sub_item × reference_period × firm_size.
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
        q.question_id,
        q.sub_item,
        q.response_raw,
        q.is_nonresponse,
        q.response_3m,
        case
            when q.response_3m is not null
                then q.response_3m in (-1, -2, -99, 7, 9, 99)
            else null
        end                                             as is_nonresponse_3m,
        f.weight_common

    from {{ ref('int_safe__core_questions_long') }} q
    join {{ ref('int_safe__firm_survey_responses') }} f
        using (permid, wave_number)
    where q.question_id in ('q5', 'q9', 'q10')
      and not (q.question_id = 'q5' and q.sub_item = 'e')

),

-- Cross-join source with the three size cuts so each row appears once per cut.
-- size_filter: true=all, true=sme-only, false=large-only — applied via CASE below.
source_sized as (

    select
        s.*,
        sc.firm_size
    from source s
    cross join (values ('all'), ('sme'), ('large')) as sc(firm_size)
    where
        sc.firm_size = 'all'
        or (sc.firm_size = 'sme'   and s.is_sme)
        or (sc.firm_size = 'large' and not s.is_sme)

),

agg_6m as (

    select
        wave_number,
        survey_year,
        survey_period,
        survey_period_label,
        country_code,
        country_name_en,
        question_id,
        sub_item,
        '6m'                                            as reference_period,
        firm_size,

        count(*) filter (where not is_nonresponse
                           and response_raw is not null) as n_respondents,
        count(*) filter (where is_nonresponse
                           and response_raw is not null) as n_nonresponse,
        sum(weight_common) filter (where not is_nonresponse
                                     and response_raw is not null) as total_weight,

        round(
            100.0 * sum(weight_common) filter (where response_raw = 1 and not is_nonresponse)
            / nullif(sum(weight_common) filter (where not is_nonresponse and response_raw is not null), 0)
        , 2)                                            as pct_improved_wtd,

        round(
            100.0 * sum(weight_common) filter (where response_raw = 2 and not is_nonresponse)
            / nullif(sum(weight_common) filter (where not is_nonresponse and response_raw is not null), 0)
        , 2)                                            as pct_unchanged_wtd,

        round(
            100.0 * sum(weight_common) filter (where response_raw = 3 and not is_nonresponse)
            / nullif(sum(weight_common) filter (where not is_nonresponse and response_raw is not null), 0)
        , 2)                                            as pct_deteriorated_wtd,

        round(
            100.0 * (
                sum(weight_common) filter (where response_raw = 1 and not is_nonresponse)
                - sum(weight_common) filter (where response_raw = 3 and not is_nonresponse)
            ) / nullif(sum(weight_common) filter (where not is_nonresponse and response_raw is not null), 0)
        , 1)                                            as net_balance_wtd,

        round(
            100.0 * (
                count(*) filter (where response_raw = 1 and not is_nonresponse)
                - count(*) filter (where response_raw = 3 and not is_nonresponse)
            ) / nullif(count(*) filter (where not is_nonresponse and response_raw is not null), 0)
        , 1)                                            as net_balance_unwtd

    from source_sized
    where response_raw is not null
    group by
        wave_number, survey_year, survey_period, survey_period_label,
        country_code, country_name_en, question_id, sub_item, firm_size

),

agg_3m as (

    select
        wave_number,
        survey_year,
        survey_period,
        survey_period_label,
        country_code,
        country_name_en,
        question_id,
        sub_item,
        '3m'                                            as reference_period,
        firm_size,

        count(*) filter (where not is_nonresponse_3m
                           and response_3m is not null) as n_respondents,
        count(*) filter (where is_nonresponse_3m
                           and response_3m is not null) as n_nonresponse,
        sum(weight_common) filter (where not is_nonresponse_3m
                                     and response_3m is not null) as total_weight,

        round(
            100.0 * sum(weight_common) filter (where response_3m = 1 and not is_nonresponse_3m)
            / nullif(sum(weight_common) filter (where not is_nonresponse_3m and response_3m is not null), 0)
        , 2)                                            as pct_improved_wtd,

        round(
            100.0 * sum(weight_common) filter (where response_3m = 2 and not is_nonresponse_3m)
            / nullif(sum(weight_common) filter (where not is_nonresponse_3m and response_3m is not null), 0)
        , 2)                                            as pct_unchanged_wtd,

        round(
            100.0 * sum(weight_common) filter (where response_3m = 3 and not is_nonresponse_3m)
            / nullif(sum(weight_common) filter (where not is_nonresponse_3m and response_3m is not null), 0)
        , 2)                                            as pct_deteriorated_wtd,

        round(
            100.0 * (
                sum(weight_common) filter (where response_3m = 1 and not is_nonresponse_3m)
                - sum(weight_common) filter (where response_3m = 3 and not is_nonresponse_3m)
            ) / nullif(sum(weight_common) filter (where not is_nonresponse_3m and response_3m is not null), 0)
        , 1)                                            as net_balance_wtd,

        round(
            100.0 * (
                count(*) filter (where response_3m = 1 and not is_nonresponse_3m)
                - count(*) filter (where response_3m = 3 and not is_nonresponse_3m)
            ) / nullif(count(*) filter (where not is_nonresponse_3m and response_3m is not null), 0)
        , 1)                                            as net_balance_unwtd

    from source_sized
    where response_3m is not null
    group by
        wave_number, survey_year, survey_period, survey_period_label,
        country_code, country_name_en, question_id, sub_item, firm_size

),

combined as (
    select * from agg_6m
    union all
    select * from agg_3m
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

    from combined
    where n_respondents > 0

),

-- financing_gap = Q5.net_balance − Q9.net_balance for same instrument/wave/period/size.
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
        on  q5.country_code      = a.country_code
        and q5.wave_number       = a.wave_number
        and q5.sub_item          = a.sub_item
        and q5.reference_period  = a.reference_period
        and q5.firm_size         = a.firm_size
        and q5.question_id       = 'q5'

)

select *
from with_gap
order by wave_number, country_code, question_id, sub_item, reference_period, firm_size
