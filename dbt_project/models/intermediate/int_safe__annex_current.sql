{{
  config(
    materialized = 'table'
  )
}}

/*
  Resolves the ECB SAFE annex to one row per
  (module_id, question_period, element, answer_code, item_letter).

  Two distinct disambiguation problems, both handled here:

  1. Wording drift across waves for the SAME question (e.g. Q5 text has been
     reworded slightly over the years). For question_item values that carry no
     period suffix (stable Common/ECB questions like q5), question_period is
     NULL and we keep only the most recent waves wording via ROW_NUMBER().

  2. Module_id reuse for entirely different questions across eras (e.g. qa1
     meant a price-expectations question in 2021H2 and means the AI-adoption
     question in 2025Q4 -- confirmed by inspecting the raw annex). For
     period-suffixed question_item values (all adhoc QA/QB modules),
     question_period is the real disambiguating key and is kept as-is --
     callers must filter to their target waves period, exactly as the prior
     live Python code did via question_item = module_period_suffix.
     Collapsing across question_period for these would silently serve the wrong
     questions answer codes for a retrospective (--wave N) report run.

  Within a single (module_id, question_period), the same row can still appear
  in more than one wave_label if the ECB re-published unchanged wording in a
  later refresh -- ROW_NUMBER() over wave_label DESC keeps the freshest text.

  element=item rows have no distinguishing code column in the source XLSX --
  the sub-item letter is embedded as an a)/b) prefix inside the text itself,
  and multiple item rows for the same question share an identical
  (module_id, question_period, element, answer_code=empty) key. item_letter is
  parsed from that prefix so multiple item rows for one question dont collapse
  into one during the ROW_NUMBER() dedup below.
*/

with parsed as (

    select
        *,
        case
            when element = 'item'
                then lower(regexp_extract(text, '^\s*([a-z])\)', 1))
        end as item_letter

    from {{ ref('stg_safe__annex') }}

),

ranked as (

    select
        *,
        row_number() over (
            partition by module_id, question_period, element, answer_code, item_letter
            order by wave_label desc
        ) as rn

    from parsed

)

select
    module_id,
    question_period,
    element,
    answer_code,
    item_letter,
    sample,
    notes,
    wave_label as current_wave_label,
    case
        -- item rows: strip the leading "a) "/"b) " prefix (already parsed into item_letter above)
        when element = 'item' then trim(regexp_replace(text, '^\s*[a-z]\)\s*', ''))
        -- answer rows: strip the leading "- " / "- [" bullet formatting and a trailing "]"
        when element = 'answer' then trim(regexp_replace(regexp_replace(text, '^-\s*\[?', ''), '\]$', ''))
        -- question rows: some source rows carry a leading "- " bullet artifact too
        when element = 'question' then trim(regexp_replace(text, '^-\s*', ''))
        else text
    end as text

from ranked
where rn = 1
