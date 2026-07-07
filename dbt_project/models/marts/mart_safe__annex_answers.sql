{{
  config(
    materialized = 'table' if target.name == 'prod' else 'view'
  )
}}

/*
  One row per (module_id, question_period, answer_code): the label text for
  each response/answer code of a question (e.g. qa1 code 1 = "Not currently in
  use"). Fixes chart x-axis tick labels and response-code descriptions that
  previously came only from a fragile live PDF parse
  (reports/questionnaire.py::fetch_adhoc_response_labels) -- the PDF parse is
  kept as a fallback only, applied in Python where the report builds prompts
  and charts, not here.

  Always filter to a specific question_period for adhoc QA/QB modules -- see
  int_safe__annex_current for why module_id alone is not a safe key.
*/

select
    module_id,
    question_period,
    answer_code,
    text as answer_label

from {{ ref('int_safe__annex_current') }}
where element = 'answer'
  and answer_code is not null
