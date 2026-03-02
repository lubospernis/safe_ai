{{
  config(
    materialized = 'table'
  )
}}

/*
  Spine table: one row per firm (permid) × wave.
  Decodes firm classification codes to EN labels and derives survey period metadata.

  Source codes verified against annex.xlsx (Questionnaire sheet).
*/

with stg as (

    select * from {{ ref('stg_safe__microdata') }}

),

with_period as (

    select
        *,

        case
            when wave_number = 1 then 2009
            when interview_date is not null then year(interview_date)
        end                                                         as survey_year,

        -- Biannual (waves 1–29): H1 = fieldwork Feb–Apr; H2 = fieldwork Aug–Oct
        -- Quarterly (waves 30+): Q1=Feb-Mar, Q2=May-Jun, Q3=Aug-Oct, Q4=Nov-Dec
        case
            when wave_number = 1     then 'H1'
            when interview_date is null then null
            when wave_number < 30 then
                case
                    when month(interview_date) between 2 and 4  then 'H1'
                    when month(interview_date) between 8 and 10 then 'H2'
                end
            else
                case
                    when month(interview_date) between 1 and 4  then 'Q1'
                    when month(interview_date) between 5 and 7  then 'Q2'
                    when month(interview_date) between 8 and 10 then 'Q3'
                    when month(interview_date) between 11 and 12 then 'Q4'
                end
        end                                                         as survey_period,

        case
            when wave_number = 1 then '2009H1'
            when interview_date is null then null
            when wave_number < 30 then
                cast(year(interview_date) as varchar)
                || case
                    when month(interview_date) between 2 and 4  then 'H1'
                    when month(interview_date) between 8 and 10 then 'H2'
                end
            else
                cast(year(interview_date) as varchar)
                || case
                    when month(interview_date) between 1 and 4  then 'Q1'
                    when month(interview_date) between 5 and 7  then 'Q2'
                    when month(interview_date) between 8 and 10 then 'Q3'
                    when month(interview_date) between 11 and 12 then 'Q4'
                end
        end                                                         as survey_period_label

    from stg

),

decoded as (

    select

        ------------------------------------------------------------------------
        -- IDENTIFIERS & PERIOD
        ------------------------------------------------------------------------
        permid,
        wave_number,
        survey_year,
        survey_period,
        survey_period_label,
        interview_date,

        wave_number = max(wave_number) over ()                      as is_latest_wave,
        zone = '1'                                                  as is_euro_area,

        ------------------------------------------------------------------------
        -- COUNTRY (D0 — ISO alpha-2, assigned from sample register)
        ------------------------------------------------------------------------
        country_code,
        case country_code
            when 'AT' then 'Austria'        when 'BE' then 'Belgium'
            when 'BG' then 'Bulgaria'       when 'CY' then 'Cyprus'
            when 'CZ' then 'Czechia'        when 'DE' then 'Germany'
            when 'DK' then 'Denmark'        when 'EE' then 'Estonia'
            when 'ES' then 'Spain'          when 'FI' then 'Finland'
            when 'FR' then 'France'         when 'GR' then 'Greece'
            when 'HR' then 'Croatia'        when 'HU' then 'Hungary'
            when 'IE' then 'Ireland'        when 'IT' then 'Italy'
            when 'LT' then 'Lithuania'      when 'LU' then 'Luxembourg'
            when 'LV' then 'Latvia'         when 'MT' then 'Malta'
            when 'NL' then 'Netherlands'    when 'PL' then 'Poland'
            when 'PT' then 'Portugal'       when 'RO' then 'Romania'
            when 'SE' then 'Sweden'         when 'SI' then 'Slovenia'
            when 'SK' then 'Slovakia'       when 'AL' then 'Albania'
            when 'BA' then 'Bosnia and Herzegovina'
            when 'CH' then 'Switzerland'    when 'IL' then 'Israel'
            when 'IS' then 'Iceland'        when 'LI' then 'Liechtenstein'
            when 'MD' then 'Moldova'        when 'ME' then 'Montenegro'
            when 'MK' then 'North Macedonia' when 'NO' then 'Norway'
            when 'RS' then 'Serbia'         when 'TR' then 'Turkey'
            when 'UA' then 'Ukraine'        when 'UK' then 'United Kingdom'
            when 'XK' then 'Kosovo'
            else country_code
        end                                                         as country_name_en,

        ------------------------------------------------------------------------
        -- FIRM SIZE (D1_rec)
        -- 1 = 1–9 employees (micro)
        -- 2 = 10–49 employees (small)
        -- 3 = 50–249 employees (medium)
        -- 4 = 250+ employees (large)
        ------------------------------------------------------------------------
        employee_band_code,
        case employee_band_code
            when 1 then 'Micro (1–9 employees)'
            when 2 then 'Small (10–49 employees)'
            when 3 then 'Medium (50–249 employees)'
            when 4 then 'Large (250+ employees)'
        end                                                         as firm_size_en,

        employee_band_code between 1 and 3                         as is_sme,

        ------------------------------------------------------------------------
        -- SECTOR (D3_rec)
        -- 2  = Construction
        -- 4  = Wholesale or retail trade
        -- 5  = Transport
        -- 12 = Industry (mining, manufacturing, energy, water — merged 2014H1)
        -- 13 = Other services (hotels, IT, real estate, professional — 2014H1)
        ------------------------------------------------------------------------
        sector_code,
        case sector_code
            when 2  then 'Construction'
            when 4  then 'Wholesale or retail trade'
            when 5  then 'Transport'
            when 12 then 'Industry'
            when 13 then 'Other services'
        end                                                         as sector_en,

        ------------------------------------------------------------------------
        -- FIRM AGE (D5_rec)  — note: code 1 = OLDEST category
        -- 1 = 10 years or more
        -- 2 = 5 to less than 10 years
        -- 3 = 2 to less than 5 years
        -- 4 = Less than 2 years
        ------------------------------------------------------------------------
        firm_age_code,
        case firm_age_code
            when 1 then '10 years or more'
            when 2 then '5 to less than 10 years'
            when 3 then '2 to less than 5 years'
            when 4 then 'Less than 2 years'
        end                                                         as firm_age_en,

        ------------------------------------------------------------------------
        -- OWNERSHIP (D6)
        -- 1 = Public shareholders (listed on stock market)
        -- 2 = Family or entrepreneurs (more than one owner)
        -- 3 = Other enterprises or business associates
        -- 4 = Venture capital or business angels
        -- 5 = Single owner (natural person / sole proprietor)
        -- 7 = Other
        ------------------------------------------------------------------------
        ownership_code,
        case ownership_code
            when 1 then 'Public shareholders (listed)'
            when 2 then 'Family or entrepreneurs'
            when 3 then 'Other enterprises or business associates'
            when 4 then 'Venture capital or business angels'
            when 5 then 'Single owner'
            when 7 then 'Other'
        end                                                         as ownership_en,

        ------------------------------------------------------------------------
        -- ANNUAL TURNOVER (D4)
        -- 5 = Up to €500,000
        -- 6 = €500,000 to €1 million
        -- 7 = €1 million to €2 million
        -- 2 = €2 million to €10 million
        -- 3 = €10 million to €50 million
        -- 4 = More than €50 million
        ------------------------------------------------------------------------
        turnover_code,
        case turnover_code
            when 5 then 'Up to €500,000'
            when 6 then '€500,000 to €1 million'
            when 7 then '€1 million to €2 million'
            when 2 then '€2 million to €10 million'
            when 3 then '€10 million to €50 million'
            when 4 then 'More than €50 million'
        end                                                         as turnover_en,

        ------------------------------------------------------------------------
        -- FIRM HEALTH (profitable_6m / vulnerable_6m from staging)
        -- 1 = flag is true, 0 = false
        ------------------------------------------------------------------------
        profitable_6m,
        vulnerable_6m,

        ------------------------------------------------------------------------
        -- SURVEY WEIGHTS
        ------------------------------------------------------------------------
        weight_common,
        weight_enterprise

    from with_period

)

select * from decoded
