{{
  config(
    materialized = 'table' if target.name == 'prod' else 'view'
  )
}}

/*
  AI adoption spotlight mart — wave 37 (2025Q4) ECB SAFE adhoc block.

  Pre-joins all six AI-related adhoc modules with decoded response labels,
  question text, and routing context. Filters to period_asked = '2025Q4'
  where the full AI battery was fielded.

  Modules:
    qa1  — AI use intensity in own firm (categorical, all firms)
    qa2  — Main reasons for USING AI (multi-select a/b, asked to QA1 ≥ 2 only)
    qa3  — Main reasons for NOT using AI more (multi-select a/b, asked to QA1 = 1 or 2)
    qa4  — Expected % of total investment in AI next 12 months (continuous 0–100, all firms)
    qb1  — % of peer firms estimated to have invested in AI today (continuous, sub_item a/b)
    qb2  — % of peer firms expected to invest in AI next 12 months (continuous, sub_items a_g3/a_g4/b_g3/b_g4)

  Non-response codes (-9999, -1, -2, -99, 7, 9, 99) excluded from pct_wtd denominator
  in the upstream mart_safe__adhoc_responses. Additional range filter (0–100) applied
  here for continuous modules to strip any remaining sentinel values.

  is_continuous: true for qa4/qb1/qb2 (open numeric estimates), false for qa1/qa2/qa3.
  routing_note: who was asked this question.
  response_label: short decoded label for categorical modules; NULL for continuous.
  sub_item_label: human-readable sub_item for QB2's g3/g4 scheme.
*/

with

-- -------------------------------------------------------------------------
-- Label lookup tables (hardcoded from ECB annex for short chart labels)
-- -------------------------------------------------------------------------

qa1_labels (response_raw, response_label) as (
    values
        (1, 'Not in use'),
        (2, 'Infrequent / pilot'),
        (3, 'Moderate use'),
        (4, 'Significant use')
),

qa2_labels (response_raw, response_label) as (
    values
        (1, 'Improve core processes'),
        (2, 'Improve non-core processes'),
        (3, 'Expand products / services'),
        (4, 'Reduce personnel costs'),
        (5, 'Support R&D / innovation')
),

qa3_labels (response_raw, response_label) as (
    values
        (1, 'AI not useful for firm'),
        (2, 'Costs exceed benefits'),
        (3, 'Systems incompatibility'),
        (4, 'Data / privacy concerns'),
        (5, 'Lack of AI skills'),
        (6, 'Distrust of AI results')
),

-- Sub-item labels for QB2 (g3 = SMEs, g4 = large firms)
qb2_sub_labels (sub_item, sub_item_label) as (
    values
        ('a_g3', 'SME peers — your country'),
        ('a_g4', 'Large firm peers — your country'),
        ('b_g3', 'SME peers — DE / FR / IT'),
        ('b_g4', 'Large firm peers — DE / FR / IT')
),

-- -------------------------------------------------------------------------
-- Base: pull from mart_safe__adhoc_responses, filtered to AI block
-- -------------------------------------------------------------------------

base as (

    select
        wave_number,
        period_asked,
        module_id,
        sub_item,
        country_code,
        response_raw,
        n_firms,
        n_firms_wtd,
        n_total_wtd,
        pct_wtd

    from {{ ref('mart_safe__adhoc_responses') }}

    where period_asked = '2025Q4'
      and module_id in ('qa1', 'qa2', 'qa3', 'qa4', 'qb1', 'qb2')
      -- Strip non-response sentinels not caught upstream
      and response_raw >= 0
      -- Cap continuous responses at 100 (qa4/qb1/qb2 are 0–100 % estimates)
      and not (module_id in ('qa4', 'qb1', 'qb2') and response_raw > 100)

),

-- -------------------------------------------------------------------------
-- Join labels and add metadata columns
-- -------------------------------------------------------------------------

final as (

    select
        b.wave_number,
        b.period_asked,
        b.module_id,
        b.sub_item,

        -- Decoded sub_item label
        coalesce(
            qs.sub_item_label,
            case
                when b.module_id in ('qa2', 'qa3') and b.sub_item = 'a' then '1st choice'
                when b.module_id in ('qa2', 'qa3') and b.sub_item = 'b' then '2nd choice'
                when b.module_id = 'qb1' and b.sub_item = 'a' then 'In your country'
                when b.module_id = 'qb1' and b.sub_item = 'b' then 'In DE / FR / IT'
                else nullif(b.sub_item, '')
            end
        ) as sub_item_label,

        b.country_code,
        b.response_raw,

        -- Decoded response label (categorical modules only)
        case b.module_id
            when 'qa1' then l1.response_label
            when 'qa2' then l2.response_label
            when 'qa3' then l3.response_label
            else null
        end as response_label,

        b.n_firms,
        b.n_firms_wtd,
        b.n_total_wtd,
        b.pct_wtd,

        -- Question text (short form for prompts and chart titles)
        case b.module_id
            when 'qa1' then 'AI use intensity in own firm'
            when 'qa2' then 'Main reasons for using AI (AI users only)'
            when 'qa3' then 'Main barriers to greater AI use (non / light users)'
            when 'qa4' then '% of total investment expected in AI (next 12 months)'
            when 'qb1' then '% of similar firms estimated to have invested in AI (today)'
            when 'qb2' then '% of similar firms expected to invest in AI (next 12 months)'
        end as question_text,

        -- Flag for continuous vs categorical
        module_id in ('qa4', 'qb1', 'qb2') as is_continuous,

        -- Routing context for LLM prompts
        case b.module_id
            when 'qa1' then 'All firms'
            when 'qa2' then 'Firms using AI (QA1 = 2, 3, or 4 only)'
            when 'qa3' then 'Firms not using AI or using infrequently (QA1 = 1 or 2)'
            when 'qa4' then 'All firms'
            when 'qb1' then 'All firms; sub_item a = in your country, b = in DE / FR / IT'
            when 'qb2' then 'All firms; g3 = SME-sized peers, g4 = large firm peers'
        end as routing_note

    from base b

    left join qa1_labels l1
        on b.module_id = 'qa1' and b.response_raw = l1.response_raw

    left join qa2_labels l2
        on b.module_id = 'qa2' and b.response_raw = l2.response_raw

    left join qa3_labels l3
        on b.module_id = 'qa3' and b.response_raw = l3.response_raw

    left join qb2_sub_labels qs
        on b.module_id = 'qb2' and b.sub_item = qs.sub_item

)

select * from final
order by module_id, sub_item, country_code, response_raw
