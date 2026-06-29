{{
  config(
    materialized = 'table'
  )
}}

/*
  Weighted share of firms citing each financing purpose (Q6A).
  Three-month reference period only (wave 30 / 2024Q1 onward).

  Q6A is multi-select — firms can cite multiple purposes simultaneously.
  Each purpose is a separate binary item: 1=Yes, 2=No, 99/null=DK/NA (excluded).

  Purposes (purpose_id):
    1 = Fixed investment
    2 = Inventory and working capital
    3 = Hiring and training of employees
    4 = Developing and launching new products or services
    5 = Refinancing or paying off obligations
    6 = Other

  METRIC: pct_cited_wtd = weighted % of valid respondents (codes 1 or 2)
    who cited the purpose (code=1). Percentages across purposes sum to >100%
    because firms can select multiple.

  FIRM SIZE:
    'all'   = all firms.
    'sme'   = employee_band_code 1–3 (up to 249 employees).
    'large' = employee_band_code 4 (250+ employees).

  Aggregation: wave × country × purpose_id × firm_size.
*/

with source as (

    select
        f.permid,
        f.wave_number,
        f.survey_year,
        f.survey_period,
        f.survey_period_label,
        f.country_code,
        f.country_name_en,
        f.weight_common,
        f.is_sme,
        f.is_euro_area,
        s.q6a_1_3m,
        s.q6a_2_3m,
        s.q6a_3_3m,
        s.q6a_4_3m,
        s.q6a_5_3m,
        s.q6a_6_3m

    from {{ ref('stg_safe__microdata') }} s
    join {{ ref('int_safe__firm_survey_responses') }} f
        using (permid, wave_number)
    where f.wave_number >= 30

),

unpivoted as (

    select
        permid,
        wave_number,
        survey_year,
        survey_period,
        survey_period_label,
        country_code,
        country_name_en,
        weight_common,
        is_sme,
        is_euro_area,
        purpose_id,
        response
    from source
    unpivot (response for purpose_id in (
        q6a_1_3m as '1',
        q6a_2_3m as '2',
        q6a_3_3m as '3',
        q6a_4_3m as '4',
        q6a_5_3m as '5',
        q6a_6_3m as '6'
    ))
    where response is not null

),

with_ea as (

    select * from unpivoted

    union all

    select permid, wave_number, survey_year, survey_period, survey_period_label,
           'EA' as country_code, 'Euro Area' as country_name_en,
           weight_common, is_sme, is_euro_area, purpose_id, response
    from unpivoted
    where is_euro_area

),

sized as (

    select u.*, sc.firm_size
    from with_ea u
    cross join (values ('all'), ('sme'), ('large')) as sc(firm_size)
    where
        sc.firm_size = 'all'
        or (sc.firm_size = 'sme'   and u.is_sme)
        or (sc.firm_size = 'large' and not u.is_sme)

),

aggregated as (

    select
        wave_number,
        survey_year,
        survey_period,
        survey_period_label,
        country_code,
        country_name_en,
        purpose_id::integer                                     as purpose_id,
        firm_size,

        count(*) filter (where response in (1, 2))              as n_respondents,
        count(*) filter (where response = 99)                   as n_nonresponse,

        round(
            100.0 * sum(weight_common) filter (where response = 1)
            / nullif(sum(weight_common) filter (where response in (1, 2)), 0)
        , 1)                                                    as pct_cited_wtd,

        round(
            100.0 * count(*) filter (where response = 1)
            / nullif(count(*) filter (where response in (1, 2)), 0)
        , 1)                                                    as pct_cited_unwtd

    from sized
    group by
        wave_number, survey_year, survey_period, survey_period_label,
        country_code, country_name_en, purpose_id, firm_size

),

labeled as (

    select
        *,
        case purpose_id
            when 1 then 'Fixed investment'
            when 2 then 'Inventory and working capital'
            when 3 then 'Hiring and training of employees'
            when 4 then 'Developing and launching new products or services'
            when 5 then 'Refinancing or paying off obligations'
            when 6 then 'Other'
        end                                                     as purpose_label

    from aggregated
    where n_respondents > 0

)

select *
from labeled
order by wave_number, country_code, purpose_id, firm_size
