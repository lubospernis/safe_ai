{{
  config(
    materialized = 'view'
  )
}}

/*
  Unpivots all wave-stamped ad-hoc module columns (qa* / qb*) into long format.

  Target schema:
    permid | wave_number | module_id | sub_item | period_asked | response_raw | is_nonresponse

  The column name encodes three things: module, sub_item, and the period in which
  that module was fielded. We parse these explicitly here so downstream consumers
  never need to know column names.

  period_asked uses the survey period label format: "2023H1", "2024Q3" etc.
  This is independent of the respondent's wave_number — a firm in wave 28 (2023H1)
  answers a module labelled _2023h1 in the column name.

  Non-response codes: same as core questions (-1, -2, -99, 7, 99).

  Module reference (what each qa/qb series covers):
    qa1 / qa1a / qa1b / qa1c  : Digital transformation and technology adoption
    qa2 / qa2dec / qa2inc      : Green transition / energy / sustainability
    qa3                        : Supply chain disruptions / resilience
    qa4 / qa41                 : AI adoption (recent waves); earlier waves: other topics
    qa5                        : Geopolitical risk (2024Q4)
    qa6                        : Energy costs (2024Q4)
    qa21 / qa22                : Additional digitalisation questions (2024Q3)
    qb1 / qb2                  : New topic blocks introduced 2025Q2+
*/

with stg as (

    select * from {{ ref('stg_safe__microdata') }}

),

unpivoted as (

    -- =========================================================================
    -- QA1 SERIES — Digital transformation / technology adoption
    -- =========================================================================

    -- Single-item waves (biannual)
    select permid, wave_number, 'qa1' as module_id, '' as sub_item, '2015H2' as period_asked, qa1_2015h2 as response_raw from stg where qa1_2015h2 is not null
    union all
    select permid, wave_number, 'qa1', '', '2016H2', qa1_2016h2 from stg where qa1_2016h2 is not null
    union all
    select permid, wave_number, 'qa1', '', '2017H2', qa1_2017h2 from stg where qa1_2017h2 is not null
    union all
    select permid, wave_number, 'qa1', '', '2018H2', qa1_2018h2 from stg where qa1_2018h2 is not null
    union all
    select permid, wave_number, 'qa1', '', '2021H2', qa1_2021h2 from stg where qa1_2021h2 is not null
    union all
    select permid, wave_number, 'qa1', '', '2024Q2', qa1_2024q2 from stg where qa1_2024q2 is not null
    union all
    select permid, wave_number, 'qa1', '', '2024Q4', qa1_2024q4 from stg where qa1_2024q4 is not null
    union all
    select permid, wave_number, 'qa1', '', '2025Q2', qa1_2025q2 from stg where qa1_2025q2 is not null
    union all
    select permid, wave_number, 'qa1', '', '2025Q4', qa1_2025q4 from stg where qa1_2025q4 is not null

    -- 2022H2: multi-item version
    union all
    select permid, wave_number, 'qa1', '1',  '2022H2', qa1_1_2022h2  from stg where qa1_1_2022h2 is not null
    union all
    select permid, wave_number, 'qa1', '2',  '2022H2', qa1_2_2022h2  from stg where qa1_2_2022h2 is not null
    union all
    select permid, wave_number, 'qa1', 'c1', '2022H2', qa1_c1_2022h2 from stg where qa1_c1_2022h2 is not null

    -- 2019H2 lettered sub-items
    union all
    select permid, wave_number, 'qa1', 'a', '2019H2', qa1_a_2019h2 from stg where qa1_a_2019h2 is not null
    union all
    select permid, wave_number, 'qa1', 'b', '2019H2', qa1_b_2019h2 from stg where qa1_b_2019h2 is not null
    union all
    select permid, wave_number, 'qa1', 'c', '2019H2', qa1_c_2019h2 from stg where qa1_c_2019h2 is not null
    union all
    select permid, wave_number, 'qa1', 'd', '2019H2', qa1_d_2019h2 from stg where qa1_d_2019h2 is not null
    union all
    select permid, wave_number, 'qa1', 'e', '2019H2', qa1_e_2019h2 from stg where qa1_e_2019h2 is not null
    union all
    select permid, wave_number, 'qa1', 'f', '2019H2', qa1_f_2019h2 from stg where qa1_f_2019h2 is not null
    union all
    select permid, wave_number, 'qa1', 'g', '2019H2', qa1_g_2019h2 from stg where qa1_g_2019h2 is not null

    -- 2020H2 lettered sub-items
    union all
    select permid, wave_number, 'qa1', 'a', '2020H2', qa1_a_2020h2 from stg where qa1_a_2020h2 is not null
    union all
    select permid, wave_number, 'qa1', 'b', '2020H2', qa1_b_2020h2 from stg where qa1_b_2020h2 is not null
    union all
    select permid, wave_number, 'qa1', 'c', '2020H2', qa1_c_2020h2 from stg where qa1_c_2020h2 is not null

    -- =========================================================================
    -- QA1A SERIES — Technology adoption sub-questions (H1 waves + 2024Q3)
    -- =========================================================================
    union all
    select permid, wave_number, 'qa1a', '1',  '2019H1', qa1a_1_2019h1  from stg where qa1a_1_2019h1 is not null
    union all
    select permid, wave_number, 'qa1a', '2',  '2019H1', qa1a_2_2019h1  from stg where qa1a_2_2019h1 is not null
    union all
    select permid, wave_number, 'qa1a', '3',  '2019H1', qa1a_3_2019h1  from stg where qa1a_3_2019h1 is not null
    union all
    select permid, wave_number, 'qa1a', '1',  '2020H1', qa1a_1_2020h1  from stg where qa1a_1_2020h1 is not null
    union all
    select permid, wave_number, 'qa1a', '2',  '2020H1', qa1a_2_2020h1  from stg where qa1a_2_2020h1 is not null
    union all
    select permid, wave_number, 'qa1a', '3',  '2020H1', qa1a_3_2020h1  from stg where qa1a_3_2020h1 is not null
    union all
    select permid, wave_number, 'qa1a', '10', '2020H1', qa1a_10_2020h1 from stg where qa1a_10_2020h1 is not null
    union all
    select permid, wave_number, 'qa1a', '1',  '2021H1', qa1a_1_2021h1  from stg where qa1a_1_2021h1 is not null
    union all
    select permid, wave_number, 'qa1a', '2',  '2021H1', qa1a_2_2021h1  from stg where qa1a_2_2021h1 is not null
    union all
    select permid, wave_number, 'qa1a', '3',  '2021H1', qa1a_3_2021h1  from stg where qa1a_3_2021h1 is not null
    union all
    select permid, wave_number, 'qa1a', '10', '2021H1', qa1a_10_2021h1 from stg where qa1a_10_2021h1 is not null
    union all
    select permid, wave_number, 'qa1a', '1',  '2022H1', qa1a_1_2022h1  from stg where qa1a_1_2022h1 is not null
    union all
    select permid, wave_number, 'qa1a', '2',  '2022H1', qa1a_2_2022h1  from stg where qa1a_2_2022h1 is not null
    union all
    select permid, wave_number, 'qa1a', '3',  '2022H1', qa1a_3_2022h1  from stg where qa1a_3_2022h1 is not null
    union all
    select permid, wave_number, 'qa1a', '10', '2022H1', qa1a_10_2022h1 from stg where qa1a_10_2022h1 is not null
    union all
    select permid, wave_number, 'qa1a', '1',  '2023H1', qa1a_1_2023h1  from stg where qa1a_1_2023h1 is not null
    union all
    select permid, wave_number, 'qa1a', '2',  '2023H1', qa1a_2_2023h1  from stg where qa1a_2_2023h1 is not null
    union all
    select permid, wave_number, 'qa1a', '3',  '2023H1', qa1a_3_2023h1  from stg where qa1a_3_2023h1 is not null
    union all
    select permid, wave_number, 'qa1a', '10', '2023H1', qa1a_10_2023h1 from stg where qa1a_10_2023h1 is not null
    union all
    select permid, wave_number, 'qa1a', '1',  '2024Q3', qa1a_1_2024q3  from stg where qa1a_1_2024q3 is not null
    union all
    select permid, wave_number, 'qa1a', '2',  '2024Q3', qa1a_2_2024q3  from stg where qa1a_2_2024q3 is not null
    union all
    select permid, wave_number, 'qa1a', '3',  '2024Q3', qa1a_3_2024q3  from stg where qa1a_3_2024q3 is not null
    union all
    select permid, wave_number, 'qa1a', '10', '2024Q3', qa1a_10_2024q3 from stg where qa1a_10_2024q3 is not null

    -- =========================================================================
    -- QA1B / QA1C SERIES
    -- =========================================================================
    union all
    select permid, wave_number, 'qa1b', '', '2019H1', qa1b_2019h1 from stg where qa1b_2019h1 is not null
    union all
    select permid, wave_number, 'qa1b', '', '2020H1', qa1b_2020h1 from stg where qa1b_2020h1 is not null
    union all
    select permid, wave_number, 'qa1b', '', '2021H1', qa1b_2021h1 from stg where qa1b_2021h1 is not null
    union all
    select permid, wave_number, 'qa1b', '', '2022H1', qa1b_2022h1 from stg where qa1b_2022h1 is not null
    union all
    select permid, wave_number, 'qa1b', '', '2023H1', qa1b_2023h1 from stg where qa1b_2023h1 is not null
    union all
    select permid, wave_number, 'qa1b', '', '2024Q3', qa1b_2024q3 from stg where qa1b_2024q3 is not null
    union all
    select permid, wave_number, 'qa1c', '4', '2019H1', qa1c_4_2019h1 from stg where qa1c_4_2019h1 is not null
    union all
    select permid, wave_number, 'qa1c', '5', '2019H1', qa1c_5_2019h1 from stg where qa1c_5_2019h1 is not null
    union all
    select permid, wave_number, 'qa1c', '6', '2019H1', qa1c_6_2019h1 from stg where qa1c_6_2019h1 is not null
    union all
    select permid, wave_number, 'qa1c', '7', '2019H1', qa1c_7_2019h1 from stg where qa1c_7_2019h1 is not null
    union all
    select permid, wave_number, 'qa1c', '8', '2019H1', qa1c_8_2019h1 from stg where qa1c_8_2019h1 is not null
    union all
    select permid, wave_number, 'qa1c', '9', '2019H1', qa1c_9_2019h1 from stg where qa1c_9_2019h1 is not null
    union all
    select permid, wave_number, 'qa1c', '4', '2020H1', qa1c_4_2020h1 from stg where qa1c_4_2020h1 is not null
    union all
    select permid, wave_number, 'qa1c', '5', '2020H1', qa1c_5_2020h1 from stg where qa1c_5_2020h1 is not null
    union all
    select permid, wave_number, 'qa1c', '6', '2020H1', qa1c_6_2020h1 from stg where qa1c_6_2020h1 is not null
    union all
    select permid, wave_number, 'qa1c', '7', '2020H1', qa1c_7_2020h1 from stg where qa1c_7_2020h1 is not null
    union all
    select permid, wave_number, 'qa1c', '8', '2020H1', qa1c_8_2020h1 from stg where qa1c_8_2020h1 is not null
    union all
    select permid, wave_number, 'qa1c', '9', '2020H1', qa1c_9_2020h1 from stg where qa1c_9_2020h1 is not null
    union all
    select permid, wave_number, 'qa1c', '4', '2021H1', qa1c_4_2021h1 from stg where qa1c_4_2021h1 is not null
    union all
    select permid, wave_number, 'qa1c', '5', '2021H1', qa1c_5_2021h1 from stg where qa1c_5_2021h1 is not null
    union all
    select permid, wave_number, 'qa1c', '6', '2021H1', qa1c_6_2021h1 from stg where qa1c_6_2021h1 is not null
    union all
    select permid, wave_number, 'qa1c', '7', '2021H1', qa1c_7_2021h1 from stg where qa1c_7_2021h1 is not null
    union all
    select permid, wave_number, 'qa1c', '8', '2021H1', qa1c_8_2021h1 from stg where qa1c_8_2021h1 is not null
    union all
    select permid, wave_number, 'qa1c', '9', '2021H1', qa1c_9_2021h1 from stg where qa1c_9_2021h1 is not null
    union all
    select permid, wave_number, 'qa1c', '4', '2022H1', qa1c_4_2022h1 from stg where qa1c_4_2022h1 is not null
    union all
    select permid, wave_number, 'qa1c', '5', '2022H1', qa1c_5_2022h1 from stg where qa1c_5_2022h1 is not null
    union all
    select permid, wave_number, 'qa1c', '6', '2022H1', qa1c_6_2022h1 from stg where qa1c_6_2022h1 is not null
    union all
    select permid, wave_number, 'qa1c', '7', '2022H1', qa1c_7_2022h1 from stg where qa1c_7_2022h1 is not null
    union all
    select permid, wave_number, 'qa1c', '8', '2022H1', qa1c_8_2022h1 from stg where qa1c_8_2022h1 is not null
    union all
    select permid, wave_number, 'qa1c', '9', '2022H1', qa1c_9_2022h1 from stg where qa1c_9_2022h1 is not null
    union all
    select permid, wave_number, 'qa1c', '4', '2023H1', qa1c_4_2023h1 from stg where qa1c_4_2023h1 is not null
    union all
    select permid, wave_number, 'qa1c', '5', '2023H1', qa1c_5_2023h1 from stg where qa1c_5_2023h1 is not null
    union all
    select permid, wave_number, 'qa1c', '6', '2023H1', qa1c_6_2023h1 from stg where qa1c_6_2023h1 is not null
    union all
    select permid, wave_number, 'qa1c', '7', '2023H1', qa1c_7_2023h1 from stg where qa1c_7_2023h1 is not null
    union all
    select permid, wave_number, 'qa1c', '8', '2023H1', qa1c_8_2023h1 from stg where qa1c_8_2023h1 is not null
    union all
    select permid, wave_number, 'qa1c', '9', '2023H1', qa1c_9_2023h1 from stg where qa1c_9_2023h1 is not null
    union all
    select permid, wave_number, 'qa1c', '4', '2024Q3', qa1c_4_2024q3 from stg where qa1c_4_2024q3 is not null
    union all
    select permid, wave_number, 'qa1c', '5', '2024Q3', qa1c_5_2024q3 from stg where qa1c_5_2024q3 is not null
    union all
    select permid, wave_number, 'qa1c', '6', '2024Q3', qa1c_6_2024q3 from stg where qa1c_6_2024q3 is not null
    union all
    select permid, wave_number, 'qa1c', '7', '2024Q3', qa1c_7_2024q3 from stg where qa1c_7_2024q3 is not null
    union all
    select permid, wave_number, 'qa1c', '8', '2024Q3', qa1c_8_2024q3 from stg where qa1c_8_2024q3 is not null
    union all
    select permid, wave_number, 'qa1c', '9', '2024Q3', qa1c_9_2024q3 from stg where qa1c_9_2024q3 is not null

    -- =========================================================================
    -- QA2 SERIES — Green transition / sustainability
    -- =========================================================================
    union all
    select permid, wave_number, 'qa2dec', '1', '2015H2', qa2dec_1_2015h2 from stg where qa2dec_1_2015h2 is not null
    union all
    select permid, wave_number, 'qa2dec', '2', '2015H2', qa2dec_2_2015h2 from stg where qa2dec_2_2015h2 is not null
    union all
    select permid, wave_number, 'qa2dec', '3', '2015H2', qa2dec_3_2015h2 from stg where qa2dec_3_2015h2 is not null
    union all
    select permid, wave_number, 'qa2dec', '4', '2015H2', qa2dec_4_2015h2 from stg where qa2dec_4_2015h2 is not null
    union all
    select permid, wave_number, 'qa2dec', '5', '2015H2', qa2dec_5_2015h2 from stg where qa2dec_5_2015h2 is not null
    union all
    select permid, wave_number, 'qa2dec', '6', '2015H2', qa2dec_6_2015h2 from stg where qa2dec_6_2015h2 is not null
    union all
    select permid, wave_number, 'qa2dec', '7', '2015H2', qa2dec_7_2015h2 from stg where qa2dec_7_2015h2 is not null
    union all
    select permid, wave_number, 'qa2inc', '1', '2015H2', qa2inc_1_2015h2 from stg where qa2inc_1_2015h2 is not null
    union all
    select permid, wave_number, 'qa2inc', '2', '2015H2', qa2inc_2_2015h2 from stg where qa2inc_2_2015h2 is not null
    union all
    select permid, wave_number, 'qa2inc', '3', '2015H2', qa2inc_3_2015h2 from stg where qa2inc_3_2015h2 is not null
    union all
    select permid, wave_number, 'qa2inc', '4', '2015H2', qa2inc_4_2015h2 from stg where qa2inc_4_2015h2 is not null
    union all
    select permid, wave_number, 'qa2inc', '5', '2015H2', qa2inc_5_2015h2 from stg where qa2inc_5_2015h2 is not null
    union all
    select permid, wave_number, 'qa2inc', '6', '2015H2', qa2inc_6_2015h2 from stg where qa2inc_6_2015h2 is not null
    union all
    select permid, wave_number, 'qa2inc', '7', '2015H2', qa2inc_7_2015h2 from stg where qa2inc_7_2015h2 is not null
    union all
    select permid, wave_number, 'qa2', '1', '2016H2', qa2_1_2016h2 from stg where qa2_1_2016h2 is not null
    union all
    select permid, wave_number, 'qa2', '2', '2016H2', qa2_2_2016h2 from stg where qa2_2_2016h2 is not null
    union all
    select permid, wave_number, 'qa2', '3', '2016H2', qa2_3_2016h2 from stg where qa2_3_2016h2 is not null
    union all
    select permid, wave_number, 'qa2', '4', '2016H2', qa2_4_2016h2 from stg where qa2_4_2016h2 is not null
    union all
    select permid, wave_number, 'qa2', '5', '2016H2', qa2_5_2016h2 from stg where qa2_5_2016h2 is not null
    union all
    select permid, wave_number, 'qa2', '6', '2016H2', qa2_6_2016h2 from stg where qa2_6_2016h2 is not null
    union all
    select permid, wave_number, 'qa2', '7', '2016H2', qa2_7_2016h2 from stg where qa2_7_2016h2 is not null
    union all
    select permid, wave_number, 'qa2', '',  '2017H2', qa2_2017h2    from stg where qa2_2017h2 is not null
    union all
    select permid, wave_number, 'qa2cat', '', '2017H2', qa2cat_2017h2 from stg where qa2cat_2017h2 is not null
    union all
    select permid, wave_number, 'qa2', '',  '2018H2', qa2_2018h2    from stg where qa2_2018h2 is not null
    union all
    select permid, wave_number, 'qa2', 'a', '2019H2', qa2_a_2019h2  from stg where qa2_a_2019h2 is not null
    union all
    select permid, wave_number, 'qa2', 'b', '2019H2', qa2_b_2019h2  from stg where qa2_b_2019h2 is not null
    union all
    select permid, wave_number, 'qa2', 'c', '2019H2', qa2_c_2019h2  from stg where qa2_c_2019h2 is not null
    union all
    select permid, wave_number, 'qa2', 'd', '2019H2', qa2_d_2019h2  from stg where qa2_d_2019h2 is not null
    union all
    select permid, wave_number, 'qa2', 'e', '2019H2', qa2_e_2019h2  from stg where qa2_e_2019h2 is not null
    union all
    select permid, wave_number, 'qa2', 'f', '2019H2', qa2_f_2019h2  from stg where qa2_f_2019h2 is not null
    union all
    select permid, wave_number, 'qa2', 'g', '2019H2', qa2_g_2019h2  from stg where qa2_g_2019h2 is not null
    union all
    select permid, wave_number, 'qa2', 'a', '2020H2', qa2_a_2020h2  from stg where qa2_a_2020h2 is not null
    union all
    select permid, wave_number, 'qa2', 'b', '2020H2', qa2_b_2020h2  from stg where qa2_b_2020h2 is not null
    union all
    select permid, wave_number, 'qa2', 'c', '2020H2', qa2_c_2020h2  from stg where qa2_c_2020h2 is not null
    union all
    select permid, wave_number, 'qa2', '',  '2019H1', qa2_2019h1    from stg where qa2_2019h1 is not null
    union all
    select permid, wave_number, 'qa2', '',  '2020H1', qa2_2020h1    from stg where qa2_2020h1 is not null
    union all
    select permid, wave_number, 'qa2', '',  '2021H1', qa2_2021h1    from stg where qa2_2021h1 is not null
    union all
    select permid, wave_number, 'qa2', '',  '2021H2', qa2_2021h2    from stg where qa2_2021h2 is not null
    union all
    select permid, wave_number, 'qa2', '',  '2022H1', qa2_2022h1    from stg where qa2_2022h1 is not null
    union all
    select permid, wave_number, 'qa2', '1', '2022H2', qa2_1_2022h2  from stg where qa2_1_2022h2 is not null
    union all
    select permid, wave_number, 'qa2', '2', '2022H2', qa2_2_2022h2  from stg where qa2_2_2022h2 is not null
    union all
    select permid, wave_number, 'qa2', '3', '2022H2', qa2_3_2022h2  from stg where qa2_3_2022h2 is not null
    union all
    select permid, wave_number, 'qa2', '4', '2022H2', qa2_4_2022h2  from stg where qa2_4_2022h2 is not null
    union all
    select permid, wave_number, 'qa2', '5', '2022H2', qa2_5_2022h2  from stg where qa2_5_2022h2 is not null
    union all
    select permid, wave_number, 'qa2', '6', '2022H2', qa2_6_2022h2  from stg where qa2_6_2022h2 is not null
    union all
    select permid, wave_number, 'qa2', '7', '2022H2', qa2_7_2022h2  from stg where qa2_7_2022h2 is not null
    union all
    select permid, wave_number, 'qa2', '',  '2023H1', qa2_2023h1    from stg where qa2_2023h1 is not null
    union all
    select permid, wave_number, 'qa2', '1', '2024Q2', qa2_1_2024q2  from stg where qa2_1_2024q2 is not null
    union all
    select permid, wave_number, 'qa2', '2', '2024Q2', qa2_2_2024q2  from stg where qa2_2_2024q2 is not null
    union all
    select permid, wave_number, 'qa2', '3', '2024Q2', qa2_3_2024q2  from stg where qa2_3_2024q2 is not null
    union all
    select permid, wave_number, 'qa2', '4', '2024Q2', qa2_4_2024q2  from stg where qa2_4_2024q2 is not null
    union all
    select permid, wave_number, 'qa2', '5', '2024Q2', qa2_5_2024q2  from stg where qa2_5_2024q2 is not null
    union all
    select permid, wave_number, 'qa2', '',  '2024Q4', qa2_2024q4    from stg where qa2_2024q4 is not null
    union all
    select permid, wave_number, 'qa2', '',  '2025Q2', qa2_2025q2    from stg where qa2_2025q2 is not null
    union all
    select permid, wave_number, 'qa2', 'a', '2025Q4', qa2_2025q4_a  from stg where qa2_2025q4_a is not null
    union all
    select permid, wave_number, 'qa2', 'b', '2025Q4', qa2_2025q4_b  from stg where qa2_2025q4_b is not null

    -- =========================================================================
    -- QA3 SERIES — Supply chain resilience
    -- =========================================================================
    union all
    select permid, wave_number, 'qa3', '',  '2017H2', qa3_2017h2    from stg where qa3_2017h2 is not null
    union all
    select permid, wave_number, 'qa3', '',  '2020H2', qa3_2020h2    from stg where qa3_2020h2 is not null
    union all
    select permid, wave_number, 'qa3', 'a', '2021H2', qa3_a_2021h2  from stg where qa3_a_2021h2 is not null
    union all
    select permid, wave_number, 'qa3', 'b', '2021H2', qa3_b_2021h2  from stg where qa3_b_2021h2 is not null
    union all
    select permid, wave_number, 'qa3', 'c', '2021H2', qa3_c_2021h2  from stg where qa3_c_2021h2 is not null
    union all
    select permid, wave_number, 'qa3', 'd', '2021H2', qa3_d_2021h2  from stg where qa3_d_2021h2 is not null
    union all
    select permid, wave_number, 'qa3', 'e', '2021H2', qa3_e_2021h2  from stg where qa3_e_2021h2 is not null
    union all
    select permid, wave_number, 'qa3', 'f', '2021H2', qa3_f_2021h2  from stg where qa3_f_2021h2 is not null
    union all
    select permid, wave_number, 'qa3', 'g', '2021H2', qa3_g_2021h2  from stg where qa3_g_2021h2 is not null
    union all
    select permid, wave_number, 'qa3', 'c1','2022H2', qa3_c1_2022h2 from stg where qa3_c1_2022h2 is not null
    union all
    select permid, wave_number, 'qa3', '1', '2019H1', qa3_1_2019h1  from stg where qa3_1_2019h1 is not null
    union all
    select permid, wave_number, 'qa3', '2', '2019H1', qa3_2_2019h1  from stg where qa3_2_2019h1 is not null
    union all
    select permid, wave_number, 'qa3', '3', '2019H1', qa3_3_2019h1  from stg where qa3_3_2019h1 is not null
    union all
    select permid, wave_number, 'qa3', '4', '2019H1', qa3_4_2019h1  from stg where qa3_4_2019h1 is not null
    union all
    select permid, wave_number, 'qa3', '1', '2020H1', qa3_1_2020h1  from stg where qa3_1_2020h1 is not null
    union all
    select permid, wave_number, 'qa3', '2', '2020H1', qa3_2_2020h1  from stg where qa3_2_2020h1 is not null
    union all
    select permid, wave_number, 'qa3', '3', '2020H1', qa3_3_2020h1  from stg where qa3_3_2020h1 is not null
    union all
    select permid, wave_number, 'qa3', '4', '2020H1', qa3_4_2020h1  from stg where qa3_4_2020h1 is not null
    union all
    select permid, wave_number, 'qa3', '1', '2021H1', qa3_1_2021h1  from stg where qa3_1_2021h1 is not null
    union all
    select permid, wave_number, 'qa3', '2', '2021H1', qa3_2_2021h1  from stg where qa3_2_2021h1 is not null
    union all
    select permid, wave_number, 'qa3', '3', '2021H1', qa3_3_2021h1  from stg where qa3_3_2021h1 is not null
    union all
    select permid, wave_number, 'qa3', '1', '2022H1', qa3_1_2022h1  from stg where qa3_1_2022h1 is not null
    union all
    select permid, wave_number, 'qa3', '2', '2022H1', qa3_2_2022h1  from stg where qa3_2_2022h1 is not null
    union all
    select permid, wave_number, 'qa3', '3', '2022H1', qa3_3_2022h1  from stg where qa3_3_2022h1 is not null
    union all
    select permid, wave_number, 'qa3', '4', '2022H1', qa3_4_2022h1  from stg where qa3_4_2022h1 is not null
    union all
    select permid, wave_number, 'qa3', '1', '2022H2', qa3_1_2022h2  from stg where qa3_1_2022h2 is not null
    union all
    select permid, wave_number, 'qa3', '2', '2022H2', qa3_2_2022h2  from stg where qa3_2_2022h2 is not null
    union all
    select permid, wave_number, 'qa3', '1', '2023H1', qa3_1_2023h1  from stg where qa3_1_2023h1 is not null
    union all
    select permid, wave_number, 'qa3', '2', '2023H1', qa3_2_2023h1  from stg where qa3_2_2023h1 is not null
    union all
    select permid, wave_number, 'qa3', '3', '2023H1', qa3_3_2023h1  from stg where qa3_3_2023h1 is not null
    union all
    select permid, wave_number, 'qa3', '4', '2023H1', qa3_4_2023h1  from stg where qa3_4_2023h1 is not null
    union all
    select permid, wave_number, 'qa3', '1', '2024Q3', qa3_1_2024q3  from stg where qa3_1_2024q3 is not null
    union all
    select permid, wave_number, 'qa3', '2', '2024Q3', qa3_2_2024q3  from stg where qa3_2_2024q3 is not null
    union all
    select permid, wave_number, 'qa3', '3', '2024Q3', qa3_3_2024q3  from stg where qa3_3_2024q3 is not null
    union all
    select permid, wave_number, 'qa3', '4', '2024Q3', qa3_4_2024q3  from stg where qa3_4_2024q3 is not null
    union all
    select permid, wave_number, 'qa3', '',  '2024Q4', qa3_2024q4    from stg where qa3_2024q4 is not null
    union all
    select permid, wave_number, 'qa3', 'a', '2025Q2', qa3_2025q2_a  from stg where qa3_2025q2_a is not null
    union all
    select permid, wave_number, 'qa3', 'b', '2025Q2', qa3_2025q2_b  from stg where qa3_2025q2_b is not null
    union all
    select permid, wave_number, 'qa3', 'c', '2025Q2', qa3_2025q2_c  from stg where qa3_2025q2_c is not null
    union all
    select permid, wave_number, 'qa3', 'd', '2025Q2', qa3_2025q2_d  from stg where qa3_2025q2_d is not null
    union all
    select permid, wave_number, 'qa3', 'e', '2025Q2', qa3_2025q2_e  from stg where qa3_2025q2_e is not null
    union all
    select permid, wave_number, 'qa3', 'a', '2025Q4', qa3_2025q4_a  from stg where qa3_2025q4_a is not null
    union all
    select permid, wave_number, 'qa3', 'b', '2025Q4', qa3_2025q4_b  from stg where qa3_2025q4_b is not null

    -- =========================================================================
    -- QA4 SERIES — AI adoption (2024Q3+); earlier: qa41 = legacy topic
    -- =========================================================================
    union all
    select permid, wave_number, 'qa41a', '',    '2014H2', qa41a_2014h2     from stg where qa41a_2014h2 is not null
    union all
    select permid, wave_number, 'qa41b', '1',   '2014H2', qa41b_1_2014h2   from stg where qa41b_1_2014h2 is not null
    union all
    select permid, wave_number, 'qa41b', '2',   '2014H2', qa41b_2_2014h2   from stg where qa41b_2_2014h2 is not null
    union all
    select permid, wave_number, 'qa41b', '3',   '2014H2', qa41b_3_2014h2   from stg where qa41b_3_2014h2 is not null
    union all
    select permid, wave_number, 'qa41b', '4',   '2014H2', qa41b_4_2014h2   from stg where qa41b_4_2014h2 is not null
    union all
    select permid, wave_number, 'qa41b', '5',   '2014H2', qa41b_5_2014h2   from stg where qa41b_5_2014h2 is not null
    union all
    select permid, wave_number, 'qa41c', '',    '2014H2', qa41c_2014h2     from stg where qa41c_2014h2 is not null
    union all
    select permid, wave_number, 'qa41c', 'rec', '2014H2', qa41c_rec_2014h2 from stg where qa41c_rec_2014h2 is not null
    union all
    select permid, wave_number, 'qa4', '1', '2024Q3', qa4_1_2024q3 from stg where qa4_1_2024q3 is not null
    union all
    select permid, wave_number, 'qa4', '2', '2024Q3', qa4_2_2024q3 from stg where qa4_2_2024q3 is not null
    union all
    select permid, wave_number, 'qa4', '3', '2024Q3', qa4_3_2024q3 from stg where qa4_3_2024q3 is not null
    union all
    select permid, wave_number, 'qa4', '4', '2024Q3', qa4_4_2024q3 from stg where qa4_4_2024q3 is not null
    union all
    select permid, wave_number, 'qa4', '',  '2024Q4', qa4_2024q4   from stg where qa4_2024q4 is not null
    union all
    select permid, wave_number, 'qa4', '',  '2025Q4', qa4_2025q4   from stg where qa4_2025q4 is not null

    -- =========================================================================
    -- QA5 / QA6 — Geopolitical risk / Energy costs (2024Q4)
    -- =========================================================================
    union all
    select permid, wave_number, 'qa5', '',  '2024Q4', qa5_2024q4   from stg where qa5_2024q4 is not null
    union all
    select permid, wave_number, 'qa6', 'a', '2024Q4', qa6_a_2024q4 from stg where qa6_a_2024q4 is not null
    union all
    select permid, wave_number, 'qa6', 'b', '2024Q4', qa6_b_2024q4 from stg where qa6_b_2024q4 is not null
    union all
    select permid, wave_number, 'qa6', 'c', '2024Q4', qa6_c_2024q4 from stg where qa6_c_2024q4 is not null

    -- =========================================================================
    -- QA21 / QA22 — Additional digitalisation questions (2024Q3)
    -- =========================================================================
    union all
    select permid, wave_number, 'qa21', '', '2024Q3', qa21_2024q3 from stg where qa21_2024q3 is not null
    union all
    select permid, wave_number, 'qa22', '', '2024Q3', qa22_2024q3 from stg where qa22_2024q3 is not null

    -- =========================================================================
    -- QB SERIES — New topic blocks (2025Q2+)
    -- =========================================================================
    union all
    select permid, wave_number, 'qb1', '',  '2025Q2', qb1_2025q2      from stg where qb1_2025q2 is not null
    union all
    select permid, wave_number, 'qb1', 'a', '2025Q4', qb1_2025q4_a    from stg where qb1_2025q4_a is not null
    union all
    select permid, wave_number, 'qb1', 'b', '2025Q4', qb1_2025q4_b    from stg where qb1_2025q4_b is not null
    union all
    select permid, wave_number, 'qb2', '',  '2025Q2', qb2_2025q2      from stg where qb2_2025q2 is not null
    union all
    select permid, wave_number, 'qb2', 'a_g3', '2025Q4', qb2_2025q4_a_g3 from stg where qb2_2025q4_a_g3 is not null
    union all
    select permid, wave_number, 'qb2', 'a_g4', '2025Q4', qb2_2025q4_a_g4 from stg where qb2_2025q4_a_g4 is not null
    union all
    select permid, wave_number, 'qb2', 'b_g3', '2025Q4', qb2_2025q4_b_g3 from stg where qb2_2025q4_b_g3 is not null
    union all
    select permid, wave_number, 'qb2', 'b_g4', '2025Q4', qb2_2025q4_b_g4 from stg where qb2_2025q4_b_g4 is not null

),

final as (

    select
        permid,
        wave_number,
        module_id,
        sub_item,
        period_asked,
        response_raw,
        response_raw in (-1, -2, -99, 7, 99)   as is_nonresponse
    from unpivoted

)

select * from final
