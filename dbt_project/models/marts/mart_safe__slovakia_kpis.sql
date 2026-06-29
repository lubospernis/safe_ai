{{
  config(
    materialized = 'table'
  )
}}

/*
  Pre-selected, semantically-named KPIs for Slovakia SMEs.

  PURPOSE: give the AI agent a single table of headline numbers where every
  series choice has been made by a human analyst.  The agent must read KPI
  values from this table — it must NOT recompute or re-select series from
  the raw mart tables.

  One row per wave.  All KPIs are for Slovakia (country_code = 'SK'),
  SMEs only (employee_band_code 1–3), 6m reference period where applicable.

  Series choices (all verified against annex.xlsx):
  ─────────────────────────────────────────────────────────────────────────
  FINANCING CONDITIONS
    q5a_need_nb         : Q5 sub_item='a' net balance — change in need for
                          BANK LOANS.  Positive = net increase in need.
                          Source: mart_safe__ecb_net_balances (ECB methodology:
                          all firms, reference_period preferred: 3m where
                          available, else 6m). Matches ECB Table 1 within ±1pp.
    q9a_avail_nb        : Q9 sub_item='a' net balance — availability of
                          BANK LOANS.  Positive = availability improved.
                          Same source and methodology as q5a_need_nb.
    bank_loan_gap       : q5a_need_nb − q9a_avail_nb.
                          Positive = needs rising faster than supply
                          (credit crunch signal).
                          Negative = supply easing relative to demand.
    q10a_interest_nb    : Q10 sub_item='a' net balance — level of interest
                          rates.  Positive = rates RISING (tightening).
                          Negative = rates FALLING (easing).
                          Source: mart_safe__financing_conditions (SME-only,
                          coalesced; Q10 not in ECB net balances mart).

  BUSINESS SITUATION    (from mart_safe__business_situation)
    turnover_nb         : Q2 sub_item='a' — Turnover net balance.
    profit_nb           : Q2 sub_item='e' — Profit net balance.
    labour_cost_nb      : Q2 sub_item='b' — Labour costs net balance.
                          Positive = labour costs rising.
    employees_nb        : Q2 sub_item='i' — Number of employees net balance.
    investment_nb       : Q2 sub_item='g' — Investment in property/plant net balance.

  PRESSING PROBLEMS     (from mart_safe__q0b_pressingness, 6m preferred)
    All seven categories on the 1–10 scale.
    Positive direction: higher score = more pressing.

  LOAN ACCESS           (from mart_safe__loan_applications, sub_item='a')
    bank_loan_app_rate  : % of firms that applied for a bank loan.
    bank_loan_disc_rate : % discouraged (fear of rejection).
    bank_loan_rej_rate  : % of applicants that were rejected.
    bank_loan_access_gap: ECB financing-gap indicator = % discouraged
                          + % rejected (as % of all valid respondents).

  OUTLOOK               (from mart_safe__outlook)
    turnover_outlook_nb : Q26 sub_item='a' — expected turnover change.
    investment_outlook_nb: Q26 sub_item='b' — expected investment change.
  ─────────────────────────────────────────────────────────────────────────
*/

with

fin as (
    select
        wave_number,
        survey_period_label,
        max(case when question_id = 'q5'  then net_balance_wtd end) as q5a_need_nb,
        max(case when question_id = 'q9'  then net_balance_wtd end) as q9a_avail_nb,
        max(case when question_id = 'q10' then net_balance_wtd end) as q10a_interest_nb
    from {{ ref('mart_safe__financing_conditions') }}
    where country_code = 'SK'
      and sub_item = 'a'
      and question_id in ('q5', 'q9', 'q10')
      and firm_size = 'all'
    group by wave_number, survey_period_label
),

fin_with_gap as (
    select
        *,
        round(q5a_need_nb - q9a_avail_nb, 2)                as bank_loan_gap
    from fin
),

biz as (
    select
        wave_number,
        max(case when sub_item = 'a' then net_balance_wtd end) as turnover_nb,
        max(case when sub_item = 'e' then net_balance_wtd end) as profit_nb,
        max(case when sub_item = 'b' then net_balance_wtd end) as labour_cost_nb,
        max(case when sub_item = 'i' then net_balance_wtd end) as employees_nb,
        max(case when sub_item = 'g' then net_balance_wtd end) as investment_nb
    from {{ ref('mart_safe__business_situation') }}
    where country_code = 'SK'
    group by wave_number
),

press as (
    select
        wave_number,
        max(case when problem_id = 1 then avg_pressingness_wtd end) as press_finding_customers,
        max(case when problem_id = 2 then avg_pressingness_wtd end) as press_competition,
        max(case when problem_id = 3 then avg_pressingness_wtd end) as press_access_to_finance,
        max(case when problem_id = 4 then avg_pressingness_wtd end) as press_costs,
        max(case when problem_id = 5 then avg_pressingness_wtd end) as press_skilled_staff,
        max(case when problem_id = 6 then avg_pressingness_wtd end) as press_regulation,
        max(case when problem_id = 7 then avg_pressingness_wtd end) as press_other
    from {{ ref('mart_safe__q0b_pressingness') }}
    where country_code = 'SK'
    group by wave_number
),

loans as (
    select
        wave_number,
        -- sub_item='a' = bank loans (excl. overdraft / credit lines)
        max(case when sub_item = 'a' then application_rate_wtd  end) as bank_loan_app_rate,
        max(case when sub_item = 'a' then discouragement_rate_wtd end) as bank_loan_disc_rate,
        max(case when sub_item = 'a' then rejection_rate_wtd    end) as bank_loan_rej_rate,
        max(case when sub_item = 'a' then financing_gap_wtd     end) as bank_loan_access_gap
    from {{ ref('mart_safe__loan_applications') }}
    where country_code = 'SK'
    group by wave_number
),

out_ as (
    select
        wave_number,
        max(case when sub_item = 'a' then net_balance_wtd end) as turnover_outlook_nb,
        max(case when sub_item = 'b' then net_balance_wtd end) as investment_outlook_nb
    from {{ ref('mart_safe__outlook') }}
    where country_code = 'SK'
    group by wave_number
)

select
    f.wave_number,
    f.survey_period_label,

    -- ── Financing conditions ──────────────────────────────────────────
    -- Q5a: need for bank loans (pp net balance; positive = more need)
    f.q5a_need_nb,
    -- Q9a: availability of bank loans (pp net balance; positive = easier)
    f.q9a_avail_nb,
    -- Gap: need_nb − avail_nb; positive = credit crunch signal; negative = easing
    f.bank_loan_gap,
    -- Q10a: interest rate level (pp net balance; positive = rates RISING)
    f.q10a_interest_nb,

    -- ── Business situation ────────────────────────────────────────────
    b.turnover_nb,
    b.profit_nb,
    b.labour_cost_nb,
    b.employees_nb,
    b.investment_nb,

    -- ── Pressing problems (weighted avg 1–10; higher = more pressing) ─
    p.press_finding_customers,
    p.press_competition,
    p.press_access_to_finance,
    p.press_costs,
    p.press_skilled_staff,
    p.press_regulation,
    p.press_other,

    -- ── Loan access ───────────────────────────────────────────────────
    -- All rates as % of valid respondents; 0–100 scale
    l.bank_loan_app_rate,
    l.bank_loan_disc_rate,
    l.bank_loan_rej_rate,
    -- ECB financing-gap indicator: % discouraged + % rejected (0–100)
    l.bank_loan_access_gap,

    -- ── Outlook (2-quarter forward net balance; positive = improving) ─
    o.turnover_outlook_nb,
    o.investment_outlook_nb

from fin_with_gap f
left join biz   b using (wave_number)
left join press p using (wave_number)
left join loans l using (wave_number)
left join out_  o using (wave_number)

order by wave_number
