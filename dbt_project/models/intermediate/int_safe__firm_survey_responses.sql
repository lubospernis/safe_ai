{{
  config(
    materialized = 'table'
  )
}}

/*
  Spine table: one row per firm (permid) × wave.
  
  Decodes all firm classification codes to human-readable labels (EN + SK).
  Derives survey period metadata from interview dates — no seed required,
  the dates make the H1/H2/Q cadence fully deterministic.

  Wave cadence observed in data:
    Waves  1–30  : biannual (H1 spring ~11k respondents, H2 autumn ~15-18k)
    Waves 31+    : ECB switched to quarterly — large biannual waves continue
                   alongside smaller quarterly waves (~5-6k)
  Wave 1 has no interview dates (pilot wave, 2009H1).
*/

with stg as (

    select * from {{ ref('stg_safe__microdata') }}

),

with_period as (

    select
        *,

        -- Derive calendar year from interview date; wave 1 hardcoded
        case
            when wave_number = 1  then 2009
            when interview_date is not null then year(interview_date)
        end                                                         as survey_year,

        -- Derive half-year / quarter from interview month
        -- H1 = fieldwork Feb–Apr, H2 = fieldwork Aug–Oct
        -- Quarterly waves (31+): Q1=Feb-Mar, Q2=May-Jun, Q3=Aug-Oct, Q4=Nov-Dec
        case
            when wave_number = 1 then 'H1'
            when interview_date is null then null
            when month(interview_date) between 1  and 4  then 'H1'
            when month(interview_date) between 5  and 7  then 'Q2'
            when month(interview_date) between 8  and 10 then 'H2'
            when month(interview_date) between 11 and 12 then 'Q4'
        end                                                         as survey_period,

        -- Convenience label e.g. "2023H1", "2024Q3"
        case
            when wave_number = 1 then '2009H1'
            when interview_date is null then null
            else
                cast(
                    case
                        when wave_number = 1 then 2009
                        else year(interview_date)
                    end
                as varchar)
                || case
                    when month(interview_date) between 1  and 4  then 'H1'
                    when month(interview_date) between 5  and 7  then 'Q2'
                    when month(interview_date) between 8  and 10 then 'H2'
                    when month(interview_date) between 11 and 12 then 'Q4'
                end
        end                                                         as survey_period_label

    from stg

),

decoded as (

    select

        ------------------------------------------------------------------------
        -- IDENTIFIERS & WAVE METADATA
        ------------------------------------------------------------------------
        permid,
        wave_number,
        survey_year,
        survey_period,
        survey_period_label,
        interview_date,
        survey_version,
        zone,

        -- Flag: is this the most recent wave in the dataset?
        wave_number = max(wave_number) over ()                      as is_latest_wave,

        -- Flag: is the firm in the euro area?
        -- zone = '1' indicates euro area membership in ECB SAFE coding
        zone = '1'                                                  as is_euro_area,

        ------------------------------------------------------------------------
        -- COUNTRY
        -- ISO 3166-1 alpha-2 codes; 42 countries in dataset
        ------------------------------------------------------------------------
        country_code,
        case country_code
            when 'AT' then 'Austria'
            when 'BE' then 'Belgium'
            when 'BG' then 'Bulgaria'
            when 'CH' then 'Switzerland'
            when 'CY' then 'Cyprus'
            when 'CZ' then 'Czechia'
            when 'DE' then 'Germany'
            when 'DK' then 'Denmark'
            when 'EE' then 'Estonia'
            when 'ES' then 'Spain'
            when 'FI' then 'Finland'
            when 'FR' then 'France'
            when 'GR' then 'Greece'
            when 'HR' then 'Croatia'
            when 'HU' then 'Hungary'
            when 'IE' then 'Ireland'
            when 'IL' then 'Israel'
            when 'IS' then 'Iceland'
            when 'IT' then 'Italy'
            when 'LI' then 'Liechtenstein'
            when 'LT' then 'Lithuania'
            when 'LU' then 'Luxembourg'
            when 'LV' then 'Latvia'
            when 'MT' then 'Malta'
            when 'NL' then 'Netherlands'
            when 'NO' then 'Norway'
            when 'PL' then 'Poland'
            when 'PT' then 'Portugal'
            when 'RO' then 'Romania'
            when 'SE' then 'Sweden'
            when 'SI' then 'Slovenia'
            when 'SK' then 'Slovakia'
            -- Candidate / neighbourhood countries
            when 'AL' then 'Albania'
            when 'BA' then 'Bosnia and Herzegovina'
            when 'MD' then 'Moldova'
            when 'ME' then 'Montenegro'
            when 'MK' then 'North Macedonia'
            when 'RS' then 'Serbia'
            when 'TR' then 'Turkey'
            when 'UA' then 'Ukraine'
            when 'UK' then 'United Kingdom'
            when 'XK' then 'Kosovo'
            else country_code
        end                                                         as country,

        case country_code
            when 'AT' then 'Rakúsko'
            when 'BE' then 'Belgicko'
            when 'BG' then 'Bulharsko'
            when 'CH' then 'Švajčiarsko'
            when 'CY' then 'Cyprus'
            when 'CZ' then 'Česko'
            when 'DE' then 'Nemecko'
            when 'DK' then 'Dánsko'
            when 'EE' then 'Estónsko'
            when 'ES' then 'Španielsko'
            when 'FI' then 'Fínsko'
            when 'FR' then 'Francúzsko'
            when 'GR' then 'Grécko'
            when 'HR' then 'Chorvátsko'
            when 'HU' then 'Maďarsko'
            when 'IE' then 'Írsko'
            when 'IL' then 'Izrael'
            when 'IS' then 'Island'
            when 'IT' then 'Taliansko'
            when 'LI' then 'Lichtenštajnsko'
            when 'LT' then 'Litva'
            when 'LU' then 'Luxembursko'
            when 'LV' then 'Lotyšsko'
            when 'MT' then 'Malta'
            when 'NL' then 'Holandsko'
            when 'NO' then 'Nórsko'
            when 'PL' then 'Poľsko'
            when 'PT' then 'Portugalsko'
            when 'RO' then 'Rumunsko'
            when 'SE' then 'Švédsko'
            when 'SI' then 'Slovinsko'
            when 'SK' then 'Slovensko'
            when 'AL' then 'Albánsko'
            when 'BA' then 'Bosna a Hercegovina'
            when 'MD' then 'Moldavsko'
            when 'ME' then 'Čierna Hora'
            when 'MK' then 'Severné Macedónsko'
            when 'RS' then 'Srbsko'
            when 'TR' then 'Turecko'
            when 'UA' then 'Ukrajina'
            when 'UK' then 'Spojené kráľovstvo'
            when 'XK' then 'Kosovo'
            else country_code
        end                                                         as krajina,

        ------------------------------------------------------------------------
        -- FIRM SIZE  (d1_rec)
        -- 1=Micro, 2=Small, 3=Medium, 4=Large
        ------------------------------------------------------------------------
        firm_size_code,
        case firm_size_code
            when 1 then 'Micro (1-9 employees)'
            when 2 then 'Small (10-49 employees)'
            when 3 then 'Medium (50-249 employees)'
            when 4 then 'Large (250+ employees)'
        end                                                         as firm_size,
        case firm_size_code
            when 1 then 'Mikro (1–9 zamestnancov)'
            when 2 then 'Malá (10–49 zamestnancov)'
            when 3 then 'Stredná (50–249 zamestnancov)'
            when 4 then 'Veľká (250+ zamestnancov)'
        end                                                         as velkost_firmy,

        -- Convenience SME flag (micro + small + medium)
        firm_size_code between 1 and 3                              as is_sme,

        employee_band_code,

        ------------------------------------------------------------------------
        -- SECTOR  (d3_rec)
        -- 1=Industry, 2=Construction, 3=Trade, 4=Services
        ------------------------------------------------------------------------
        sector_code,
        case sector_code
            when 1 then 'Industry'
            when 2 then 'Construction'
            when 3 then 'Trade'
            when 4 then 'Services'
        end                                                         as sector,
        case sector_code
            when 1 then 'Priemysel'
            when 2 then 'Stavebníctvo'
            when 3 then 'Obchod'
            when 4 then 'Služby'
        end                                                         as sektor,

        ------------------------------------------------------------------------
        -- FIRM AGE  (d4)
        -- 1=Up to 2 years, 2=2-5 years, 3=5-10 years, 4=More than 10 years
        ------------------------------------------------------------------------
        firm_age_code,
        case firm_age_code
            when 1 then 'Up to 2 years'
            when 2 then '2 to 5 years'
            when 3 then '5 to 10 years'
            when 4 then 'More than 10 years'
        end                                                         as firm_age,
        case firm_age_code
            when 1 then 'Do 2 rokov'
            when 2 then '2 až 5 rokov'
            when 3 then '5 až 10 rokov'
            when 4 then 'Viac ako 10 rokov'
        end                                                         as vek_firmy,

        ------------------------------------------------------------------------
        -- OWNERSHIP  (d5_rec)
        -- 1=Family/owner-managed, 2=Other private, 3=Listed, 4=Public
        ------------------------------------------------------------------------
        ownership_code,
        case ownership_code
            when 1 then 'Family or owner-managed'
            when 2 then 'Other privately owned'
            when 3 then 'Listed on stock exchange'
            when 4 then 'Publicly owned'
        end                                                         as ownership_type,
        case ownership_code
            when 1 then 'Rodinná alebo vlastnícky riadená'
            when 2 then 'Iná súkromná'
            when 3 then 'Kótovaná na burze'
            when 4 then 'Verejná'
        end                                                         as typ_vlastnictva,

        ------------------------------------------------------------------------
        -- AUTONOMY  (d6_rec)
        -- 1=Standalone, 2=Subsidiary/branch, 3=Head office of group
        ------------------------------------------------------------------------
        autonomy_code,
        case autonomy_code
            when 1 then 'Standalone enterprise'
            when 2 then 'Subsidiary or branch'
            when 3 then 'Head office of a group'
        end                                                         as autonomy,
        case autonomy_code
            when 1 then 'Samostatný podnik'
            when 2 then 'Dcérska spoločnosť alebo pobočka'
            when 3 then 'Sídlo skupiny'
        end                                                         as autonómia,

        ------------------------------------------------------------------------
        -- ANNUAL TURNOVER  (d7_rec)
        -- 1=Up to €2m, 2=€2m-10m, 3=€10m-50m, 4=€50m+
        ------------------------------------------------------------------------
        turnover_code,
        case turnover_code
            when 1 then 'Up to €2 million'
            when 2 then '€2 million to €10 million'
            when 3 then '€10 million to €50 million'
            when 4 then '€50 million or more'
        end                                                         as turnover_band,
        case turnover_code
            when 1 then 'Do 2 mil. €'
            when 2 then '2 až 10 mil. €'
            when 3 then '10 až 50 mil. €'
            when 4 then '50 mil. € a viac'
        end                                                         as pasmo_obratu,

        ------------------------------------------------------------------------
        -- SURVEY WEIGHTS
        ------------------------------------------------------------------------
        weight_common,
        weight_enterprise,
        weight_old_common,
        weight_old_enterprise,

        ------------------------------------------------------------------------
        -- ECB COMPOSITE INDICATORS
        -- These are pre-computed by ECB; values are continuous (0–1 range)
        -- NULL = firm not in scope for that indicator in that wave
        ------------------------------------------------------------------------
        ind_a,          -- change in external financing needs
        ind_a_grouped,
        ind_b,          -- change in availability of bank loans
        ind_b_grouped,
        ind_c,          -- change in availability of credit lines
        ind_c_grouped,
        ind_d,          -- change in availability of trade credit
        ind_d_grouped,
        ind_f,          -- change in interest rates on bank loans
        ind_f_grouped,
        ind_fgap,       -- financing gap (ECB headline indicator)
        ind_fgap_grouped,
        ind_fgap_broad, -- broad financing gap
        ind_fgap_broad_grouped,
        ind_g,          -- change in collateral requirements
        ind_g_grouped,
        ind_h,          -- change in other loan terms & conditions
        ind_h_grouped,

        ------------------------------------------------------------------------
        -- FIRM HEALTH FLAGS
        -- profitable_code: 0=not profitable, 1=profitable
        -- vulnerable_code: composite flag derived by ECB
        ------------------------------------------------------------------------
        profitable_code,
        case profitable_code
            when 1 then 'Profitable'
            when 0 then 'Not profitable'
        end                                                         as profitability_status,
        case profitable_code
            when 1 then 'Zisková'
            when 0 then 'Nezisková'
        end                                                         as stav_ziskovosti,

        vulnerable_code,
        case vulnerable_code
            when 1 then 'Vulnerable'
            when 0 then 'Not vulnerable'
        end                                                         as vulnerability_status,
        case vulnerable_code
            when 1 then 'Zraniteľná'
            when 0 then 'Nezraniteľná'
        end                                                         as stav_zranitelnosti,

        ------------------------------------------------------------------------
        -- PIPELINE METADATA
        ------------------------------------------------------------------------
        _dlt_load_id,
        _dlt_id,
        _dbt_loaded_at

    from with_period

)

select * from decoded
