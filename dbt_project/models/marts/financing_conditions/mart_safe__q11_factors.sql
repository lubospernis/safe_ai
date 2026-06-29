{{
  config(
    materialized = 'table'
  )
}}

/*
  Net balances for Q11 — self-assessed factors affecting availability of external financing.
  Three-month reference period only (wave 30 / 2024Q1 onward).

  ECB methodology: codes 7 (N/A) and 9 (DK) excluded from denominator.
  NET BALANCE: % code-1 (improved) − % code-3 (deteriorated), weighted by weight_common.
  Positive = net improvement in this factor (favourable for financing access).
  Negative = net deterioration (adverse signal).

  Sub-items:
    a = General economic outlook (macro supply drag)
    b = Access to public financial support, including guarantees
    c = Enterprise-specific outlook (sales/profitability)
    d = Enterprise's own capital
    e = Enterprise's credit history
    f = Willingness of banks to provide credit  ← key credit supply indicator
    g = Willingness of business partners to provide trade credit
    h = Willingness of investors to invest in the enterprise
    i = Willingness to extend credit to customers (wave 29+)

  FIRM SIZE:
    'all' = all respondents
    'sme' = employee_band_code 1–3 (up to 249 employees)
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
        q.sub_item,
        q.response_3m                                   as response,
        q.response_3m in (-1, -2, -99, 7, 9, 99)       as is_nonresponse,
        f.weight_common

    from {{ ref('int_safe__core_questions_long') }} q
    join {{ ref('int_safe__firm_survey_responses') }} f
        using (permid, wave_number)
    where q.question_id = 'q11'
      and q.response_3m is not null
      and f.wave_number >= 30

),

source_sized as (

    select s.*, sc.firm_size
    from source s
    cross join (values ('all'), ('sme')) as sc(firm_size)
    where
        sc.firm_size = 'all'
        or (sc.firm_size = 'sme' and s.is_sme)

),

aggregated as (

    select
        wave_number,
        survey_year,
        survey_period,
        survey_period_label,
        country_code,
        country_name_en,
        sub_item,
        firm_size,

        count(*) filter (where not is_nonresponse)      as n_respondents,
        count(*) filter (where is_nonresponse)          as n_nonresponse,

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
        , 1)                                            as net_balance_wtd

    from source_sized
    group by
        wave_number, survey_year, survey_period, survey_period_label,
        country_code, country_name_en, sub_item, firm_size

),

labeled as (

    select
        *,
        'Factors affecting availability of external financing' as question_label,
        case sub_item
            when 'a' then 'General economic outlook'
            when 'b' then 'Access to public financial support (incl. guarantees)'
            when 'c' then 'Enterprise-specific outlook (sales/profitability)'
            when 'd' then 'Enterprise''s own capital'
            when 'e' then 'Enterprise''s credit history'
            when 'f' then 'Willingness of banks to provide credit'
            when 'g' then 'Willingness of business partners (trade credit)'
            when 'h' then 'Willingness of investors'
            when 'i' then 'Willingness to extend credit to customers'
        end                                                     as sub_item_label

    from aggregated
    where n_respondents > 0

)

select *
from labeled
order by wave_number, country_code, sub_item, firm_size
