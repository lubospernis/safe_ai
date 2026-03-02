{{ config(materialized = 'table') }}

/*
  Weighted aggregation of financing availability and need by country × wave × firm size.

  Covers:
    Q4  : change in AVAILABILITY of financing instruments (bank loans, credit lines, etc.)
    Q5  : change in NEED for financing
    Q10 : change in TERMS AND CONDITIONS (interest rates, collateral, maturity, etc.)

  Net balance = weighted % "Increased" minus weighted % "Decreased"
  This is the ECB's standard published metric for SAFE results.
  Positive = conditions improving; Negative = conditions deteriorating.

  Response scale for Q4, Q5, Q10 (using _rec harmonised codes):
    1 = Increased / Improved
    2 = Decreased / Deteriorated
    3 = Remained unchanged
   -1 = Not applicable
   -2 = Don't know

  sub_item maps to financing instrument:
    Q4/Q5: a=bank loans, b=credit lines, c=trade credit, d=other loans,
           e=equity, f=leasing, g=factoring, h=public support
    Q10:   a=interest rates, b=other costs, c=loan size, d=collateral,
           e=maturity, f=covenants
*/

with responses as (

    select
        f.country,
        f.krajina,
        f.wave_number,
        f.survey_period_label,
        f.survey_year,
        f.firm_size,
        f.velkost_firmy,
        f.is_sme,
        f.is_euro_area,
        q.question_id,
        q.sub_item,
        -- Use recoded response where available, fall back to raw
        coalesce(q.response_rec, q.response_raw)    as response,
        f.weight_common
    from {{ ref('int_safe__core_questions_long') }} q
    inner join {{ ref('int_safe__firm_survey_responses') }} f
        on q.permid = f.permid
        and q.wave_number = f.wave_number
    where q.question_id in ('q4', 'q5', 'q10')
      and not q.is_nonresponse
      and f.weight_common is not null

),

aggregated as (

    select
        country,
        krajina,
        wave_number,
        survey_period_label,
        survey_year,
        firm_size,
        velkost_firmy,
        is_sme,
        is_euro_area,
        question_id,
        sub_item,

        -- Instrument label (EN)
        case
            when question_id in ('q4','q5') then
                case sub_item
                    when 'a' then 'Bank loans'
                    when 'b' then 'Bank overdraft / credit line'
                    when 'c' then 'Trade credit'
                    when 'd' then 'Other loans'
                    when 'e' then 'Equity'
                    when 'f' then 'Leasing / hire purchase'
                    when 'g' then 'Factoring'
                    when 'h' then 'Public financial support'
                    else sub_item
                end
            when question_id = 'q10' then
                case sub_item
                    when 'a' then 'Interest rates'
                    when 'b' then 'Other financing costs'
                    when 'c' then 'Available loan size / amount'
                    when 'd' then 'Collateral requirements'
                    when 'e' then 'Maturity'
                    when 'f' then 'Covenants'
                    else sub_item
                end
        end                                                         as instrument_label,

        -- Instrument label (SK)
        case
            when question_id in ('q4','q5') then
                case sub_item
                    when 'a' then 'Bankové úvery'
                    when 'b' then 'Bankový kontokorent / úverová linka'
                    when 'c' then 'Obchodný úver'
                    when 'd' then 'Iné úvery'
                    when 'e' then 'Vlastné imanie'
                    when 'f' then 'Lízing / nájom'
                    when 'g' then 'Faktoring'
                    when 'h' then 'Verejná finančná podpora'
                    else sub_item
                end
            when question_id = 'q10' then
                case sub_item
                    when 'a' then 'Úrokové sadzby'
                    when 'b' then 'Iné náklady financovania'
                    when 'c' then 'Dostupná výška úveru'
                    when 'd' then 'Požiadavky na zabezpečenie'
                    when 'e' then 'Splatnosť'
                    when 'f' then 'Úverové podmienky (kovenants)'
                    else sub_item
                end
        end                                                         as instrument_label_sk,

        -- Question topic label (EN + SK)
        case question_id
            when 'q4'  then 'Change in availability of financing'
            when 'q5'  then 'Change in need for financing'
            when 'q10' then 'Change in terms and conditions of financing'
        end                                                         as topic,
        case question_id
            when 'q4'  then 'Zmena dostupnosti financovania'
            when 'q5'  then 'Zmena potreby financovania'
            when 'q10' then 'Zmena podmienok financovania'
        end                                                         as tema,

        count(*)                                                    as n_respondents,
        sum(weight_common)                                          as total_weight,
        sum(case when response = 1 then weight_common else 0 end)   as weight_increased,
        sum(case when response = 2 then weight_common else 0 end)   as weight_decreased,
        sum(case when response = 3 then weight_common else 0 end)   as weight_unchanged

    from responses
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15

),

with_metrics as (

    select
        *,
        round(weight_increased / nullif(total_weight, 0) * 100, 1)  as pct_increased,
        round(weight_decreased / nullif(total_weight, 0) * 100, 1)  as pct_decreased,
        round(weight_unchanged / nullif(total_weight, 0) * 100, 1)  as pct_unchanged,
        round(
            (weight_increased - weight_decreased)
            / nullif(total_weight, 0) * 100
        , 1)                                                         as net_balance

    from aggregated

),

with_change as (

    select
        *,
        -- Change in net balance vs previous wave (same country/firm_size/question/instrument)
        net_balance - lag(net_balance) over (
            partition by country, firm_size, question_id, sub_item
            order by wave_number
        )                                                            as net_balance_change_vs_prev_wave

    from with_metrics

)

select * from with_change
