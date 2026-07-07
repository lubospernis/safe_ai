{{
  config(
    materialized = 'view'
  )
}}

/*
  Staging pass over the long-format ECB SAFE questionnaire annex
  (raw.main_safe.ref_safe__annex_long — unpivoted by the safe_annex dlt pipeline).

  Cleans/normalises without collapsing wave history: derives a bare module_id by
  stripping the trailing wave/period suffix (_YYYYQn / _YYYYHn) from question_item,
  and (separately) a question_period column holding that suffix in the same
  YYYYQn/YYYYHn uppercase format as int_safe__firm_survey_responses.survey_period_label
  -- this lets downstream models join annex rows to an exact survey wave rather than
  only ever resolving to whichever wording is most recent, which matters for
  retrospective/--wave N report runs against a wave that is not the latest.

  Adhoc (QA/QB series) module_ids get reused across eras for entirely different
  questions (e.g. qa1 meant something else in 2021 than it does in 2025Q4), so
  question_period is not optional metadata here -- it is the actual disambiguating
  key alongside module_id.

  One row per (module_id, question_period, element, answer_code, wave_label).
*/

with source as (

    select * from {{ source('main_safe', 'ref_safe__annex_long') }}

),

staged as (

    select
        lower(trim(element))                                          as element,
        lower(trim(question_item))                                    as question_item,
        regexp_replace(
            lower(trim(question_item)), '_\d{4}(q[1-4]|h[1-2])$', ''
        )                                                              as module_id,
        upper(regexp_extract(
            lower(trim(question_item)), '_(\d{4}(?:q[1-4]|h[1-2]))$', 1
        ))                                                              as question_period,
        nullif(trim(answer), '')                                       as answer_code,
        nullif(trim(sample), '')                                       as sample,
        nullif(trim(notes), '')                                        as notes,
        wave_label,
        trim(text)                                                     as text

    from source
    where nullif(trim(text), '') is not null

)

select * from staged
