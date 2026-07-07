{{
  config(
    materialized = 'table' if target.name == 'prod' else 'view'
  )
}}

/*
  One row per (module_id, question_period): the ECB SAFE questionnaire text for
  each question code, with its sample/module type (e.g. 'ad-hoc ECB', 'Common').

  question_period is NULL for stable Common/ECB questions whose question_item
  carries no wave suffix (e.g. 'q5'); populated (e.g. '2025Q4') for adhoc
  QA/QB-series modules, which reuse module_id across eras for different
  questions -- always filter to a specific question_period when looking up an
  adhoc module's text (see int_safe__annex_current for why).
*/

select
    module_id,
    question_period,
    text as question_text,
    sample,
    notes

from {{ ref('int_safe__annex_current') }}
where element = 'question'
