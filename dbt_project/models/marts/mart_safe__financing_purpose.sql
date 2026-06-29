{{
  config(
    materialized = 'table'
  )
}}

/*
  Weighted share of firms citing each financing purpose (Q6A).

  Q6A: "For what purpose was financing used by your enterprise during the past
  six months?" (multi-select — firms can cite multiple purposes simultaneously).

  Each purpose is a separate binary item:
    purpose_id 1 = Fixed investment
    purpose_id 2 = Inventory and working capital
    purpose_id 3 = Hiring and training of employees
    purpose_id 4 = Developing and launching new products or services
    purpose_id 5 = Refinancing or paying off obligations
    purpose_id 6 = Other

  Answer codes per item:
    1   = Yes (purpose was cited)
    2   = No (purpose was not cited)
    99  = DK/NA (excluded from denominator)
    null = not asked / routing skip (excluded)

  Non-response handling note: prior to 2017H2, firms that answered "not
  applicable — I have not used any financing" had all items set to null.
  From 2017H2 onward, 99 is assigned per item individually. In both cases,
  null and 99 are excluded from the denominator.

  METRIC:
    pct_cited_wtd = SUM(weight_common WHERE code=1)
                  / SUM(weight_common WHERE code IN (1,2)) × 100
    Interpretation: weighted % of firms that used financing for this purpose,
    among those who gave a valid response to this item.

  FIRM SIZE (firm_size column):
    'all'   = all respondents regardless of size — matches ECB published aggregates.
    'sme'   = micro, small, medium (employee_band_code 1–3, up to 249 employees).
    'large' = large firms (employee_band_code 4, 250+ employees).

  REFERENCE PERIOD: 6m and 3m kept separate (one row per reference period).
    reference_period = '6m': six-month questionnaire (waves 1+).
    reference_period = '3m': three-month questionnaire (waves 30+).
    Waves 1–29: only '6m' rows. Waves 30+ may have both.

  Aggregation: wave × country × purpose_id × reference_period × firm_size.
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

        -- 6m purpose columns
        s.q6a_1,
        s.q6a_2,
        s.q6a_3,
        s.q6a_4,
        s.q6a_5,
        s.q6a_6,

        -- 3m purpose columns (waves 30+ only)
        s.q6a_1_3m,
        s.q6a_2_3m,
        s.q6a_3_3m,
        s.q6a_4_3m,
        s.q6a_5_3m,
        s.q6a_6_3m

    from {{ ref('stg_safe__microdata') }} s
    join {{ ref('int_safe__firm_survey_responses') }} f
        using (permid, wave_number)

),

-- Unpivot 6m items into long format: one row per firm × purpose
unpivoted_6m as (

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
        purpose_id,
        response
    from source
    unpivot (response for purpose_id in (
        q6a_1 as '1',
        q6a_2 as '2',
        q6a_3 as '3',
        q6a_4 as '4',
        q6a_5 as '5',
        q6a_6 as '6'
    ))
    where response is not null

),

-- Unpivot 3m items into long format
unpivoted_3m as (

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

-- Cross-join unpivoted rows with size cuts
sized_6m as (

    select u.*, sc.firm_size
    from unpivoted_6m u
    cross join (values ('all'), ('sme'), ('large')) as sc(firm_size)
    where
        sc.firm_size = 'all'
        or (sc.firm_size = 'sme'   and u.is_sme)
        or (sc.firm_size = 'large' and not u.is_sme)

),

sized_3m as (

    select u.*, sc.firm_size
    from unpivoted_3m u
    cross join (values ('all'), ('sme'), ('large')) as sc(firm_size)
    where
        sc.firm_size = 'all'
        or (sc.firm_size = 'sme'   and u.is_sme)
        or (sc.firm_size = 'large' and not u.is_sme)

),

agg_6m as (

    select
        wave_number,
        survey_year,
        survey_period,
        survey_period_label,
        country_code,
        country_name_en,
        purpose_id::integer                                     as purpose_id,
        '6m'                                                    as reference_period,
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

    from sized_6m
    group by
        wave_number, survey_year, survey_period, survey_period_label,
        country_code, country_name_en, purpose_id, firm_size

),

agg_3m as (

    select
        wave_number,
        survey_year,
        survey_period,
        survey_period_label,
        country_code,
        country_name_en,
        purpose_id::integer                                     as purpose_id,
        '3m'                                                    as reference_period,
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

    from sized_3m
    group by
        wave_number, survey_year, survey_period, survey_period_label,
        country_code, country_name_en, purpose_id, firm_size

),

combined as (
    select * from agg_6m
    union all
    select * from agg_3m
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

    from combined
    where n_respondents > 0

)

select *
from labeled
order by wave_number, country_code, purpose_id, reference_period, firm_size
