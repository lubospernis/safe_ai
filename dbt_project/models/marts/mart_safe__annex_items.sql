{{
  config(
    materialized = 'table' if target.name == 'prod' else 'view'
  )
}}

/*
  One row per (module_id, question_period, item_key): the sub-item label for
  multi-part questions (e.g. qb1.a = "In your country", qb1.b = "In Germany,
  France and Italy"). Fixes chart panel titles that previously fell back to a
  bare sub-item code ("a"/"b") whenever the annex-derived question_texts dict
  wasn't populated for a sibling adhoc module.

  The source XLSX encodes item identity three different ways depending on the
  question: some (e.g. qb1/qb2) prefix the item text itself with "a)"/"b)" and
  leave answer_code blank; others (e.g. qa6, the inflation-horizon question)
  leave the text unprefixed and instead populate answer_code (1/2/3) as the
  item's identifier; and for many stable Common/ECB questions (e.g. q2_d, q5_f)
  the sub-item letter is already baked into module_id itself (there is exactly
  one item row per module_id × question_period in this case, confirmed empty
  by construction). item_key normalises the first two shapes into one join key
  (item_letter when the text was letter-prefixed, else the raw answer_code) and
  falls back to an empty string for the third shape, where module_id alone is
  already the unique sub-item identifier.

  Coverage is inherently sparse: only questions whose source XLSX rows use
  element='item' get a row here. A question with no row in this mart at all
  should NOT be assumed to have no sub-items -- check reports/adhoc.py's
  pick-order fallback (e.g. QA2's "a"/"b" are a microdata pick-order artifact --
  first/second reason chosen -- with no annex representation whatsoever, not a
  gap this mart can fill).

  Always filter to a specific question_period for adhoc QA/QB modules -- see
  int_safe__annex_current for why module_id alone is not a safe key.
*/

select
    module_id,
    question_period,
    coalesce(nullif(item_letter, ''), answer_code, '') as item_key,
    text as item_label

from {{ ref('int_safe__annex_current') }}
where element = 'item'
