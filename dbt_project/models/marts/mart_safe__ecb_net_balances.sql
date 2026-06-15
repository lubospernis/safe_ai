{{
  config(
    materialized = 'table'
  )
}}

/*
  ECB-methodology net balances for Q5 (financing need) and Q9 (availability).

  REPLICATION RULE (verified against ECB Table 1, wave 38 / 2026Q1):
  ─────────────────────────────────────────────────────────────────────
  1. SCOPE: all firms (no SME restriction).  ECB publishes country aggregates
     across all respondents, not SME-only.

  2. REFERENCE PERIOD: each wave uses exactly one questionnaire version.
     • Waves 1–29 (H1/H2, biannual): 6-month questionnaire → use response_raw.
     • Waves 30+ (quarterly):
         Q2/Q4 waves (31,33,35,37,...): 3-month only → use response_3m.
         Q1/Q3 waves (30,32,34,36,38,...): BOTH versions fielded in same wave.
           The ECB publishes them as separate "white panel" (6m) and "grey panel"
           (3m) data points.  This mart emits one row per wave × reference_period
           so both are available; join on reference_period = '6m' or '3m' as needed.

  3. NON-RESPONSE: exclude codes 7 (not applicable) and 9 (DK/NA) from
     both numerator and denominator.  Code 7 = firm does not use this
     instrument (routing skip) — it is NOT a substantive "unchanged" answer.

  4. NET BALANCE: % code-1 − % code-3 (weighted by weight_common).
     Q5: 1=Increased need,    3=Decreased need.    Positive = more need.
     Q9: 1=Improved,          3=Deteriorated.      Positive = better availability.

  SUB-ITEMS covered:
    a = Bank loans (excl. overdraft and credit lines)
    f = Credit line, bank overdraft or credit cards overdraft

  These are the two instruments published in ECB Table 1.  Add further
  sub-items (b,c,d,g,h) below if needed for other ECB tables.

  Aggregation: wave × country × question_id × sub_item × reference_period.
*/

with source as (

    select
        f.wave_number,
        f.survey_year,
        f.survey_period,
        f.survey_period_label,
        f.country_code,
        f.country_name_en,
        q.question_id,
        q.sub_item,
        -- 6m response
        q.response_raw,
        q.is_nonresponse,
        -- 3m response (only populated in waves 30+)
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
    where q.question_id in ('q5', 'q9')
      and q.sub_item in ('a', 'f')
      -- exclude legacy Q5 sub_item 'e' catch-all
      and not (q.question_id = 'q5' and q.sub_item = 'e')

),

-- Compute net balance separately for each reference period
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
        count(*) filter (where not is_nonresponse and response_raw is not null)
                                                        as n_respondents,
        round(
            100.0 * sum(weight_common) filter (where response_raw = 1 and not is_nonresponse)
            / nullif(sum(weight_common) filter (
                where not is_nonresponse and response_raw is not null
            ), 0)
          - 100.0 * sum(weight_common) filter (where response_raw = 3 and not is_nonresponse)
            / nullif(sum(weight_common) filter (
                where not is_nonresponse and response_raw is not null
            ), 0)
        , 1)                                            as net_balance_wtd,
        round(
            100.0 * count(*) filter (where response_raw = 1 and not is_nonresponse)
            / nullif(count(*) filter (
                where not is_nonresponse and response_raw is not null
            ), 0)
          - 100.0 * count(*) filter (where response_raw = 3 and not is_nonresponse)
            / nullif(count(*) filter (
                where not is_nonresponse and response_raw is not null
            ), 0)
        , 1)                                            as net_balance_unwtd

    from source
    where response_raw is not null
    group by
        wave_number, survey_year, survey_period, survey_period_label,
        country_code, country_name_en, question_id, sub_item

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
        count(*) filter (where not is_nonresponse_3m and response_3m is not null)
                                                        as n_respondents,
        round(
            100.0 * sum(weight_common) filter (where response_3m = 1 and not is_nonresponse_3m)
            / nullif(sum(weight_common) filter (
                where not is_nonresponse_3m and response_3m is not null
            ), 0)
          - 100.0 * sum(weight_common) filter (where response_3m = 3 and not is_nonresponse_3m)
            / nullif(sum(weight_common) filter (
                where not is_nonresponse_3m and response_3m is not null
            ), 0)
        , 1)                                            as net_balance_wtd,
        round(
            100.0 * count(*) filter (where response_3m = 1 and not is_nonresponse_3m)
            / nullif(count(*) filter (
                where not is_nonresponse_3m and response_3m is not null
            ), 0)
          - 100.0 * count(*) filter (where response_3m = 3 and not is_nonresponse_3m)
            / nullif(count(*) filter (
                where not is_nonresponse_3m and response_3m is not null
            ), 0)
        , 1)                                            as net_balance_unwtd

    from source
    where response_3m is not null
    group by
        wave_number, survey_year, survey_period, survey_period_label,
        country_code, country_name_en, question_id, sub_item

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
            when 'q5' then 'Change in need for external financing'
            when 'q9' then 'Availability of external financing'
        end                                             as question_label,
        case
            when sub_item = 'a' then 'Bank loans (excl. overdraft and credit lines)'
            when sub_item = 'f' then 'Credit line, bank overdraft or credit cards overdraft'
        end                                             as sub_item_label

    from combined

)

select *
from labeled
where n_respondents > 0
order by wave_number, country_code, question_id, sub_item, reference_period
