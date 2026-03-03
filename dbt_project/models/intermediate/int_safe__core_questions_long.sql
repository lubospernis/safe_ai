{{
  config(
    materialized = 'table'
  )
}}

/*
  Unpivots all recurring core survey questions (q1–q34) into long format.

  Target schema:
    permid | wave_number | question_id | sub_item | response_raw
           | response_rec | response_grouped | response_grouped_rec
           | is_nonresponse

  Non-response codes in this dataset:
    -1  = Not applicable
    -2  = Don't know / refused
    -99 = Refused (observed in q8b and rate fields)
    7   = Not asked / not applicable (routing code used in some question blocks)
    99  = Not asked (routing code)

  is_nonresponse = true for any of the above codes.
  These rows are retained (not dropped) so that response rates can be analysed.

  One row per question sub-item per firm-wave. Where a question has _rec and/or
  _g1 variants, those are additional columns on the same row — not separate rows.
*/

with stg as (

    select * from {{ ref('stg_safe__microdata') }}

),

firm as (

    select
        permid,
        wave_number,
        country_code,
        country_name_en,
        employee_band_code,
        firm_size_en,
        is_sme,
        sector_code,
        sector_en,
        survey_year,
        survey_period,
        survey_period_label,
        weight_common
    from {{ ref('int_safe__firm_survey_responses') }}

),

unpivoted as (

    --------------------------------------------------------------------------
    -- Q1 (annex Q2): Business situation change over past 6 months/quarter
    --   (decreased / remained unchanged / increased; 1=increased, 2=unchanged, 3=decreased)
    --   Core 4-item block asked in all common rounds:
    --   a=Turnover, b=Labour costs (incl. social contributions),
    --   c=Interest expenses, d=Profit
    -- The extended Q2 block (data q2, same annex Q2) adds items e–j.
    --------------------------------------------------------------------------
    select permid, wave_number, 'q1' as question_id, 'a' as sub_item,
        q1_a as response_raw, null::integer as response_rec,
        null::integer as response_grouped, null::integer as response_grouped_rec
    from stg where q1_a is not null
    union all
    select permid, wave_number, 'q1', 'b', q1_b, null, null, null
    from stg where q1_b is not null
    union all
    select permid, wave_number, 'q1', 'c', q1_c, null, null, null
    from stg where q1_c is not null
    union all
    select permid, wave_number, 'q1', 'd', q1_d, null, null, null
    from stg where q1_d is not null

    --------------------------------------------------------------------------
    -- Q2 (annex Q2): Business situation — extended sub-item block
    --   (decreased / remained unchanged / increased)
    --   a=Turnover, b=Labour costs (incl. social contributions),
    --   c=Other costs (materials, energy, other), d=Interest expenses,
    --   e=Profit, f=Profit margin (removed from questionnaire, older rounds only),
    --   g=Investments in property, plant or equipment,
    --   h=Inventories and other working capital, i=Number of employees,
    --   j=Debt compared to assets
    -- This is the full Q2 column set; data q1 holds only sub-items a–d.
    --------------------------------------------------------------------------
    union all
    select permid, wave_number, 'q2', 'a', q2_a, null, q2_a_grouped, null
    from stg where q2_a is not null
    union all
    select permid, wave_number, 'q2', 'b', q2_b, null, q2_b_grouped, null
    from stg where q2_b is not null
    union all
    select permid, wave_number, 'q2', 'c', q2_c, null, q2_c_grouped, null
    from stg where q2_c is not null
    union all
    select permid, wave_number, 'q2', 'd', q2_d, null, q2_d_grouped, null
    from stg where q2_d is not null
    union all
    select permid, wave_number, 'q2', 'e', q2_e, null, q2_e_grouped, null
    from stg where q2_e is not null
    union all
    select permid, wave_number, 'q2', 'f', q2_f, null, null, null
    from stg where q2_f is not null
    union all
    select permid, wave_number, 'q2', 'g', q2_g, null, q2_g_grouped, null
    from stg where q2_g is not null
    union all
    select permid, wave_number, 'q2', 'h', q2_h, null, q2_h_grouped, null
    from stg where q2_h is not null
    union all
    select permid, wave_number, 'q2', 'i', q2_i, null, q2_i_grouped, null
    from stg where q2_i is not null
    union all
    select permid, wave_number, 'q2', 'j', q2_j, q2_j_rec, q2_j_grouped, q2_j_grouped_rec
    from stg where q2_j is not null

    --------------------------------------------------------------------------
    -- Q3 (annex Q3): Debt compared to assets — decreased/unchanged/increased
    --   over past 6 months (single response, no sub-items).
    --   Note: from later waves this item was folded into Q2 (as Q2_j).
    --------------------------------------------------------------------------
    union all
    select permid, wave_number, 'q3', '', q3, null, null, null
    from stg where q3 is not null

    --------------------------------------------------------------------------
    -- Q4 (annex Q4): Financing sources relevant/used — whether each source was
    --   used or considered (1=relevant/used, 2=not used, 3=not relevant).
    --   Sub-items (verified from annex.xlsx Q4):
    --   a=Retained earnings or sale of assets
    --   b=Grants or subsidised bank loans
    --   c=Credit line, bank overdraft or credit cards overdraft
    --   d=Bank loan (excl. subsidised loans, overdrafts and credit lines)
    --   e=Trade credit
    --   f=Other loan (family, friends, related enterprise, shareholders)
    --   g=(legacy, empty in current questionnaire)
    --   h=Debt securities issued
    --   i=(legacy, empty in current questionnaire)
    --   j=Equity capital
    --   k=(legacy, empty in current questionnaire)
    --   l=(legacy, empty in current questionnaire)
    --   m=Leasing or hire-purchase
    --   p=Other sources of financing (subordinated debt, peer-to-peer, crowdfunding, etc.)
    --   r=Factoring
    --------------------------------------------------------------------------
    union all
    select permid, wave_number, 'q4', 'a', q4_a, q4_a_rec, null, q4_a_grouped_rec
    from stg where q4_a is not null
    union all
    select permid, wave_number, 'q4', 'b', q4_b, q4_b_rec, null, q4_b_grouped_rec
    from stg where q4_b is not null
    union all
    select permid, wave_number, 'q4', 'c', q4_c, q4_c_rec, null, q4_c_grouped_rec
    from stg where q4_c is not null
    union all
    select permid, wave_number, 'q4', 'd', q4_d, q4_d_rec, null, q4_d_grouped_rec
    from stg where q4_d is not null
    union all
    select permid, wave_number, 'q4', 'e', q4_e, q4_e_rec, null, q4_e_grouped_rec
    from stg where q4_e is not null
    union all
    select permid, wave_number, 'q4', 'f', q4_f, q4_f_rec, null, q4_f_grouped_rec
    from stg where q4_f is not null
    union all
    select permid, wave_number, 'q4', 'g', q4_g, q4_g_rec, null, q4_g_grouped_rec
    from stg where q4_g is not null
    union all
    select permid, wave_number, 'q4', 'h', q4_h, q4_h_rec, null, q4_h_grouped_rec
    from stg where q4_h is not null
    union all
    select permid, wave_number, 'q4', 'i', q4_i, null, null, null
    from stg where q4_i is not null
    union all
    select permid, wave_number, 'q4', 'j', q4_j, q4_j_rec, null, q4_j_grouped_rec
    from stg where q4_j is not null
    union all
    select permid, wave_number, 'q4', 'k', q4_k, null, null, null
    from stg where q4_k is not null
    union all
    select permid, wave_number, 'q4', 'l', q4_l, null, null, null
    from stg where q4_l is not null
    union all
    select permid, wave_number, 'q4', 'm', q4_m, q4_m_rec, null, q4_m_grouped_rec
    from stg where q4_m is not null
    union all
    select permid, wave_number, 'q4', 'p', q4_p, q4_p_rec, null, q4_p_grouped_rec
    from stg where q4_p is not null
    union all
    select permid, wave_number, 'q4', 'r', q4_r, q4_r_rec, null, q4_r_grouped_rec
    from stg where q4_r is not null

    --------------------------------------------------------------------------
    -- Q4A (annex Q4A): Change in availability of each financing source over
    --   past 6 months (improved/unchanged/deteriorated).
    --   Sub-items mirror the Q4 instrument list (same letter codes).
    --------------------------------------------------------------------------
    union all
    select permid, wave_number, 'q4a', 'a', q4a_a, null, q4a_a_grouped, null
    from stg where q4a_a is not null
    union all
    select permid, wave_number, 'q4a', 'b', q4a_b, null, q4a_b_grouped, null
    from stg where q4a_b is not null
    union all
    select permid, wave_number, 'q4a', 'c', q4a_c, null, q4a_c_grouped, null
    from stg where q4a_c is not null
    union all
    select permid, wave_number, 'q4a', 'd', q4a_d, null, q4a_d_grouped, null
    from stg where q4a_d is not null
    union all
    select permid, wave_number, 'q4a', 'e', q4a_e, null, q4a_e_grouped, null
    from stg where q4a_e is not null
    union all
    select permid, wave_number, 'q4a', 'f', q4a_f, null, q4a_f_grouped, null
    from stg where q4a_f is not null
    union all
    select permid, wave_number, 'q4a', 'h', q4a_h, null, q4a_h_grouped, null
    from stg where q4a_h is not null
    union all
    select permid, wave_number, 'q4a', 'j', q4a_j, null, q4a_j_grouped, null
    from stg where q4a_j is not null
    union all
    select permid, wave_number, 'q4a', 'm', q4a_m, null, q4a_m_grouped, null
    from stg where q4a_m is not null
    union all
    select permid, wave_number, 'q4a', 'p', q4a_p, null, q4a_p_grouped, null
    from stg where q4a_p is not null
    union all
    select permid, wave_number, 'q4a', 'r', q4a_r, null, q4a_r_grouped, null
    from stg where q4a_r is not null

    --------------------------------------------------------------------------
    -- Q5 (annex Q5): Change in need for external financing over past 6 months
    --   (increased/unchanged/decreased per instrument type)
    --   a=Bank loans (excl. overdraft and credit lines)
    --   b=Trade credit
    --   c=Equity capital
    --   d=Debt securities issued
    --   e=(removed in later rounds)
    --   f=Credit line, bank overdraft or credit cards overdraft
    --   g=Leasing or hire-purchase
    --   h=Other loan (family, friends, related enterprise, shareholders)
    --------------------------------------------------------------------------
    union all
    select permid, wave_number, 'q5', 'a', q5_a, q5_a_rec, q5_a_grouped, q5_a_grouped_rec
    from stg where q5_a is not null
    union all
    select permid, wave_number, 'q5', 'b', q5_b, q5_b_rec, q5_b_grouped, q5_b_grouped_rec
    from stg where q5_b is not null
    union all
    select permid, wave_number, 'q5', 'c', q5_c, q5_c_rec, q5_c_grouped, q5_c_grouped_rec
    from stg where q5_c is not null
    union all
    select permid, wave_number, 'q5', 'd', q5_d, q5_d_rec, q5_d_grouped, q5_d_grouped_rec
    from stg where q5_d is not null
    union all
    select permid, wave_number, 'q5', 'e', q5_e, q5_e_rec, null, null
    from stg where q5_e is not null
    union all
    select permid, wave_number, 'q5', 'f', q5_f, q5_f_rec, q5_f_grouped, q5_f_grouped_rec
    from stg where q5_f is not null
    union all
    select permid, wave_number, 'q5', 'g', q5_g, q5_g_rec, q5_g_grouped, q5_g_grouped_rec
    from stg where q5_g is not null
    union all
    select permid, wave_number, 'q5', 'h', q5_h, q5_h_rec, q5_h_grouped, q5_h_grouped_rec
    from stg where q5_h is not null

    --------------------------------------------------------------------------
    -- Q6 (annex Q6, ECB-only): Factors affecting firm's need for external
    --   financing — increased/decreased/no impact over past 6 months
    --   a=Fixed Investment
    --   b=Inventories and working capital
    --   c=Availability of internal funds (Internal funds)
    --   d=Mergers & Acquisitions and corporate restructuring
    --------------------------------------------------------------------------
    union all
    select permid, wave_number, 'q6', 'a', q6_a, null, null, null
    from stg where q6_a is not null
    union all
    select permid, wave_number, 'q6', 'b', q6_b, null, null, null
    from stg where q6_b is not null
    union all
    select permid, wave_number, 'q6', 'c', q6_c, null, null, null
    from stg where q6_c is not null
    union all
    select permid, wave_number, 'q6', 'd', q6_d, null, null, null
    from stg where q6_d is not null

    --------------------------------------------------------------------------
    -- Q6A (annex Q6A): Purpose of external financing used in past 6 months
    --   1=Investments in property, plant or equipment
    --   2=Inventory and other working capital
    --   3=Hiring and training of employees
    --   4=Developing and launching new products or services
    --   5=Refinancing or paying off obligations
    --   6=Other
    --------------------------------------------------------------------------
    union all
    select permid, wave_number, 'q6a', '1', q6a_1, null, q6a_1_grouped, null
    from stg where q6a_1 is not null
    union all
    select permid, wave_number, 'q6a', '2', q6a_2, null, q6a_2_grouped, null
    from stg where q6a_2 is not null
    union all
    select permid, wave_number, 'q6a', '3', q6a_3, null, q6a_3_grouped, null
    from stg where q6a_3 is not null
    union all
    select permid, wave_number, 'q6a', '4', q6a_4, null, q6a_4_grouped, null
    from stg where q6a_4 is not null
    union all
    select permid, wave_number, 'q6a', '5', q6a_5, null, q6a_5_grouped, null
    from stg where q6a_5 is not null
    union all
    select permid, wave_number, 'q6a', '6', q6a_6, null, q6a_6_grouped, null
    from stg where q6a_6 is not null

    --------------------------------------------------------------------------
    -- Q7A (annex Q7A): Application status for each financing instrument
    --   1=Applied, 2=Did not apply — fear of rejection (discouraged),
    --   3=Did not apply — sufficient internal funds,
    --   4=Did not apply — other reasons, 9=DK/NA
    -- Q7B (annex Q7B): Outcome of application (for applicants only)
    --   1=Received all applied for, 2=Got part (old), 3=Refused — too costly,
    --   4=Rejected by lender, 5=Received most (new), 6=Received limited part (new),
    --   8=Still pending, 9=DK
    --   Sub-items for both Q7A and Q7B:
    --   a=Bank loan (excl. overdraft and credit lines)
    --   b=Trade credit
    --   c=Other external financing
    --   d=Credit line, bank overdraft or credit cards overdraft
    --------------------------------------------------------------------------
    union all
    select permid, wave_number, 'q7a', 'a', q7a_a, q7a_a_rec, q7a_a_grouped, q7a_a_grouped_rec
    from stg where q7a_a is not null
    union all
    select permid, wave_number, 'q7a', 'b', q7a_b, q7a_b_rec, q7a_b_grouped, q7a_b_grouped_rec
    from stg where q7a_b is not null
    union all
    select permid, wave_number, 'q7a', 'c', q7a_c, q7a_c_rec, q7a_c_grouped, q7a_c_grouped_rec
    from stg where q7a_c is not null
    union all
    select permid, wave_number, 'q7a', 'd', q7a_d, q7a_d_rec, q7a_d_grouped, q7a_d_grouped_rec
    from stg where q7a_d is not null
    union all
    select permid, wave_number, 'q7b', 'a', q7b_a, q7b_a_rec, q7b_a_grouped, q7b_a_grouped_rec
    from stg where q7b_a is not null
    union all
    select permid, wave_number, 'q7b', 'b', q7b_b, q7b_b_rec, q7b_b_grouped, q7b_b_grouped_rec
    from stg where q7b_b is not null
    union all
    select permid, wave_number, 'q7b', 'c', q7b_c, q7b_c_rec, q7b_c_grouped, q7b_c_grouped_rec
    from stg where q7b_c is not null
    union all
    select permid, wave_number, 'q7b', 'd', q7b_d, q7b_d_rec, q7b_d_grouped, q7b_d_grouped_rec
    from stg where q7b_d is not null

    --------------------------------------------------------------------------
    -- Q8A (annex Q8A): Size bracket of the last bank loan obtained.
    --   Single response (no sub-items). Replaces old Q12 from 2014H1 onwards.
    -- Q8B (interest rate on credit line/overdraft) is a numeric field kept in
    --   staging as a float; not included in this long-format model.
    --------------------------------------------------------------------------
    union all
    select permid, wave_number, 'q8a', '', q8a, q8a_rec, null, null
    from stg where q8a is not null

    --------------------------------------------------------------------------
    -- Q9 (annex Q9): Availability of each financing type — has it improved,
    --   remained unchanged or deteriorated over past 6 months?
    --   1=Improved, 2=Unchanged, 3=Deteriorated, 7=Not applicable, 9=DK
    --   a=Bank loans (excl. overdraft and credit lines)
    --   b=Trade credit
    --   c=Equity capital
    --   d=Debt securities issued
    --   e=(legacy, empty in current questionnaire)
    --   f=Credit line, bank overdraft or credit cards overdraft
    --   g=Leasing or hire-purchase
    --   h=Other loan (family, friends, related enterprise, shareholders)
    --------------------------------------------------------------------------
    union all
    select permid, wave_number, 'q9', 'a', q9_a, q9_a_rec, q9_a_grouped, q9_a_grouped_rec
    from stg where q9_a is not null
    union all
    select permid, wave_number, 'q9', 'b', q9_b, q9_b_rec, q9_b_grouped, q9_b_grouped_rec
    from stg where q9_b is not null
    union all
    select permid, wave_number, 'q9', 'c', q9_c, q9_c_rec, q9_c_grouped, q9_c_grouped_rec
    from stg where q9_c is not null
    union all
    select permid, wave_number, 'q9', 'd', q9_d, q9_d_rec, q9_d_grouped, q9_d_grouped_rec
    from stg where q9_d is not null
    union all
    select permid, wave_number, 'q9', 'e', q9_e, q9_e_rec, null, null
    from stg where q9_e is not null
    union all
    select permid, wave_number, 'q9', 'f', q9_f, q9_f_rec, q9_f_grouped, q9_f_grouped_rec
    from stg where q9_f is not null
    union all
    select permid, wave_number, 'q9', 'g', q9_g, q9_g_rec, q9_g_grouped, q9_g_grouped_rec
    from stg where q9_g is not null
    union all
    select permid, wave_number, 'q9', 'h', q9_h, q9_h_rec, q9_h_grouped, q9_h_grouped_rec
    from stg where q9_h is not null

    --------------------------------------------------------------------------
    -- Q10 (annex Q10): Terms and conditions of bank financing — increased,
    --   unchanged or decreased by the bank over past 6 months?
    --   1=Increased, 2=Unchanged, 3=Decreased, 9=DK/NA
    --   a=Level of interest rates
    --   b=Level of cost of financing other than interest rates (charges, fees, commissions)
    --   c=Available size of loan or credit line
    --   d=Available maturity of the loan
    --   e=Collateral requirements
    --   f=Other terms (guarantees, information requirements, procedures, covenants)
    --------------------------------------------------------------------------
    union all
    select permid, wave_number, 'q10', 'a', q10_a, q10_a_rec, q10_a_grouped, q10_a_grouped_rec
    from stg where q10_a is not null
    union all
    select permid, wave_number, 'q10', 'b', q10_b, q10_b_rec, q10_b_grouped, q10_b_grouped_rec
    from stg where q10_b is not null
    union all
    select permid, wave_number, 'q10', 'c', q10_c, q10_c_rec, q10_c_grouped, q10_c_grouped_rec
    from stg where q10_c is not null
    union all
    select permid, wave_number, 'q10', 'd', q10_d, q10_d_rec, q10_d_grouped, q10_d_grouped_rec
    from stg where q10_d is not null
    union all
    select permid, wave_number, 'q10', 'e', q10_e, q10_e_rec, q10_e_grouped, q10_e_grouped_rec
    from stg where q10_e is not null
    union all
    select permid, wave_number, 'q10', 'f', q10_f, q10_f_rec, q10_f_grouped, q10_f_grouped_rec
    from stg where q10_f is not null

    --------------------------------------------------------------------------
    -- Q11 (annex Q11): Factors affecting availability of external financing —
    --   improved, unchanged or deteriorated over past 6 months?
    --   1=Improved, 2=Unchanged, 3=Deteriorated, 9=DK
    --   a=General economic outlook
    --   b=Access to public financial support, including guarantees
    --   c=Enterprise-specific outlook (sales, profitability, business plan)
    --   d=Enterprise's own capital
    --   e=Enterprise's credit history
    --   f=Willingness of banks to provide credit
    --   g=Willingness of business partners to provide trade credit
    --   h=Willingness of investors to invest in the enterprise
    --   i=(used for sub-item with grouped coding)
    --   j=Willingness to extend credit to customers (accounts receivable)
    --------------------------------------------------------------------------
    union all
    select permid, wave_number, 'q11', 'a', q11_a, null, q11_a_grouped, null
    from stg where q11_a is not null
    union all
    select permid, wave_number, 'q11', 'b', q11_b, null, q11_b_grouped, null
    from stg where q11_b is not null
    union all
    select permid, wave_number, 'q11', 'c', q11_c, null, q11_c_grouped, null
    from stg where q11_c is not null
    union all
    select permid, wave_number, 'q11', 'd', q11_d, null, q11_d_grouped, null
    from stg where q11_d is not null
    union all
    select permid, wave_number, 'q11', 'e', q11_e, null, q11_e_grouped, null
    from stg where q11_e is not null
    union all
    select permid, wave_number, 'q11', 'f', q11_f, q11_f_rec, q11_f_grouped, q11_f_grouped_rec
    from stg where q11_f is not null
    union all
    select permid, wave_number, 'q11', 'g', q11_g, q11_g_rec, q11_g_grouped, q11_g_grouped_rec
    from stg where q11_g is not null
    union all
    select permid, wave_number, 'q11', 'h', q11_h, q11_h_rec, q11_h_grouped, q11_h_grouped_rec
    from stg where q11_h is not null
    union all
    select permid, wave_number, 'q11', 'i', q11_i, null, q11_i_grouped, null
    from stg where q11_i is not null

    --------------------------------------------------------------------------
    -- Q12 (annex Q12, EC-only): Size of last loan of any kind obtained in
    --   past 2 years (single response). Replaced by Q8A from 2014H1 onwards.
    -- Q13 (annex Q13, EC-only): Provider of last loan (single response).
    --   Removed from later rounds.
    -- Q14 (annex Q14, EC-only): Purpose of last loan (single response).
    --   Replaced by Q6A from 2014H1 onwards.
    -- Q16 (annex Q16, EC-only): Average annual turnover growth over past 3 years
    --   a=in 12 months (past year), b=past 3 years avg
    -- Q17 (annex Q17, EC-only): Expected turnover growth over next 2–3 years
    --   (single response)
    --------------------------------------------------------------------------
    union all
    select permid, wave_number, 'q12', '', q12, null, null, null
    from stg where q12 is not null
    union all
    select permid, wave_number, 'q13', '', q13, null, null, null
    from stg where q13 is not null
    union all
    select permid, wave_number, 'q14', 'a', q14_a, null, null, null
    from stg where q14_a is not null
    union all
    select permid, wave_number, 'q14', 'b', q14_b, null, null, null
    from stg where q14_b is not null
    union all
    select permid, wave_number, 'q14', 'c', q14_c, null, null, null
    from stg where q14_c is not null
    union all
    select permid, wave_number, 'q14', 'd', q14_d, null, null, null
    from stg where q14_d is not null
    union all
    select permid, wave_number, 'q14', 'e', q14_e, null, null, null
    from stg where q14_e is not null
    union all
    select permid, wave_number, 'q14', 'f', q14_f, null, null, null
    from stg where q14_f is not null
    union all
    select permid, wave_number, 'q14', 'g', q14_g, null, null, null
    from stg where q14_g is not null
    union all
    select permid, wave_number, 'q14', 'h', q14_h, null, null, null
    from stg where q14_h is not null
    union all
    select permid, wave_number, 'q16', 'a', q16_a, null, null, null
    from stg where q16_a is not null
    union all
    select permid, wave_number, 'q16', 'b', q16_b, null, null, null
    from stg where q16_b is not null
    union all
    select permid, wave_number, 'q17', '', q17, null, null, null
    from stg where q17 is not null

    --------------------------------------------------------------------------
    -- Q19: Government support usage (a=applied, b=received)
    -- Q20: Enterprise size (number of employees bracket)
    -- Q21: Enterprise age bracket
    -- Q22: Turnover bracket (q22=main, q22_a/b=sub-items)
    -- Q23 (annex Q23): Expected availability of financing over next 6 months
    --   (improve/unchanged/deteriorate per instrument)
    --   a=Retained earnings or sale of assets
    --   b=Bank loans (excl. overdraft and credit lines)
    --   c=Equity capital
    --   d=Trade credit
    --   e=Debt securities issued
    --   f=(legacy, empty in current questionnaire)
    --   g=Credit line, bank overdraft or credit cards overdraft
    --   i=Leasing or hire-purchase
    --   j=Other loan (family, friends, related enterprise, shareholders)
    -- Q24 (annex Q24, EC-only): Importance of financing factors for future
    --   (scale 1–10, sub-items a–f)
    -- Q25 (annex Q25, EC-only): Main obstacle to stock market listing
    -- Q26 (annex Q26): Expected change over next 6 months (increase/unchanged/decrease)
    --   a=Company's turnover, b=Investments in property, plant or equipment
    --------------------------------------------------------------------------
    union all
    select permid, wave_number, 'q19', 'a', q19_a, null, null, null
    from stg where q19_a is not null
    union all
    select permid, wave_number, 'q19', 'b', q19_b, null, null, null
    from stg where q19_b is not null
    union all
    select permid, wave_number, 'q20', '', q20, null, null, null
    from stg where q20 is not null
    union all
    select permid, wave_number, 'q21', '', q21, null, null, null
    from stg where q21 is not null
    union all
    select permid, wave_number, 'q22', '', q22, null, q22_rec, null
    from stg where q22 is not null
    union all
    select permid, wave_number, 'q22', 'a', q22_a, null, null, null
    from stg where q22_a is not null
    union all
    select permid, wave_number, 'q22', 'b', q22_b, null, null, null
    from stg where q22_b is not null
    union all
    select permid, wave_number, 'q23', 'a', q23_a, q23_a_rec, q23_a_grouped, q23_a_grouped_rec
    from stg where q23_a is not null
    union all
    select permid, wave_number, 'q23', 'b', q23_b, q23_b_rec, q23_b_grouped, q23_b_grouped_rec
    from stg where q23_b is not null
    union all
    select permid, wave_number, 'q23', 'c', q23_c, q23_c_rec, q23_c_grouped, q23_c_grouped_rec
    from stg where q23_c is not null
    union all
    select permid, wave_number, 'q23', 'd', q23_d, q23_d_rec, q23_d_grouped, q23_d_grouped_rec
    from stg where q23_d is not null
    union all
    select permid, wave_number, 'q23', 'e', q23_e, q23_e_rec, q23_e_grouped, q23_e_grouped_rec
    from stg where q23_e is not null
    union all
    select permid, wave_number, 'q23', 'f', q23_f, q23_f_rec, null, null
    from stg where q23_f is not null
    union all
    select permid, wave_number, 'q23', 'g', q23_g, q23_g_rec, q23_g_grouped, q23_g_grouped_rec
    from stg where q23_g is not null
    union all
    select permid, wave_number, 'q23', 'i', q23_i, q23_i_rec, q23_i_grouped, q23_i_grouped_rec
    from stg where q23_i is not null
    union all
    select permid, wave_number, 'q23', 'j', q23_j, q23_j_rec, q23_j_grouped, q23_j_grouped_rec
    from stg where q23_j is not null
    union all
    select permid, wave_number, 'q24', '', q24, null, null, null
    from stg where q24 is not null
    union all
    select permid, wave_number, 'q24', 'a', q24_a, null, null, null
    from stg where q24_a is not null
    union all
    select permid, wave_number, 'q24', 'b', q24_b, null, null, null
    from stg where q24_b is not null
    union all
    select permid, wave_number, 'q24', 'c', q24_c, null, null, null
    from stg where q24_c is not null
    union all
    select permid, wave_number, 'q24', 'd', q24_d, null, null, null
    from stg where q24_d is not null
    union all
    select permid, wave_number, 'q24', 'e', q24_e, null, null, null
    from stg where q24_e is not null
    union all
    select permid, wave_number, 'q24', 'f', q24_f, null, null, null
    from stg where q24_f is not null
    union all
    select permid, wave_number, 'q25', '', q25, null, null, null
    from stg where q25 is not null
    union all
    select permid, wave_number, 'q26', '', q26, null, null, null
    from stg where q26 is not null
    union all
    select permid, wave_number, 'q26', 'a', q26_a, null, q26_a_grouped, null
    from stg where q26_a is not null
    union all
    select permid, wave_number, 'q26', 'b', q26_b, null, q26_b_grouped, null
    from stg where q26_b is not null

    --------------------------------------------------------------------------
    -- Q31 (annex Q31): Expected turnover growth in future periods
    --   a=in 12 months, b=in three years, c=in five years
    -- Q32 (annex Q32): Most important reason bank loans are not relevant
    --   (single response)
    -- Q33 (annex Q33): Supplementary question (varies by round/country)
    -- Q34 (annex Q34): Price and wage expectations (increase/unchanged/decrease)
    --   a=Average selling price, a_1=selling price (numeric variant)
    --   b=Average prices of production inputs, b_1=production inputs (numeric)
    --   c=Average wage of current employees, c_1=wage (numeric)
    --   d=Number of employees, d_1=employees (numeric)
    --------------------------------------------------------------------------
    union all
    select permid, wave_number, 'q31', 'a', q31_a, null, null, null
    from stg where q31_a is not null
    union all
    select permid, wave_number, 'q31', 'b', q31_b, null, null, null
    from stg where q31_b is not null
    union all
    select permid, wave_number, 'q31', 'c', q31_c, null, null, null
    from stg where q31_c is not null
    union all
    select permid, wave_number, 'q32', '', q32, null, null, null
    from stg where q32 is not null
    union all
    select permid, wave_number, 'q33', '', q33, null, null, null
    from stg where q33 is not null
    union all
    select permid, wave_number, 'q34', 'a',   q34_a,   q34_a_rec,   null, null
    from stg where q34_a is not null
    union all
    select permid, wave_number, 'q34', 'a_1', q34_a_1, null,        null, null
    from stg where q34_a_1 is not null
    union all
    select permid, wave_number, 'q34', 'b',   q34_b,   q34_b_rec,   null, null
    from stg where q34_b is not null
    union all
    select permid, wave_number, 'q34', 'b_1', q34_b_1, null,        null, null
    from stg where q34_b_1 is not null
    union all
    select permid, wave_number, 'q34', 'c',   q34_c,   q34_c_rec,   null, null
    from stg where q34_c is not null
    union all
    select permid, wave_number, 'q34', 'c_1', q34_c_1, null,        null, null
    from stg where q34_c_1 is not null
    union all
    select permid, wave_number, 'q34', 'd',   q34_d,   q34_d_rec,   null, null
    from stg where q34_d is not null
    union all
    select permid, wave_number, 'q34', 'd_1', q34_d_1, null,        null, null
    from stg where q34_d_1 is not null

    --------------------------------------------------------------------------
    -- Q0: Screener — whether enterprise obtains external financing (yes/no)
    -- Q0B: Pressingness of business problems (scale 1–10, 7 problem categories)
    --   1=Finding customers, 2=Competition, 3=Access to finance,
    --   4=Costs of production or labour,
    --   5=Availability of skilled staff or experienced managers,
    --   6=Regulation, 7=Other
    --   The _3m variant (response_grouped) = 3-month reference period version
    --   (present in wave 30/2024H1 and wave 37/2025Q4 only)
    -- Q0C: Routing / classification question
    --------------------------------------------------------------------------
    union all
    select permid, wave_number, 'q0', '',  q0,  null, null, null
    from stg where q0 is not null
    union all
    select permid, wave_number, 'q0b', '1', q0b_1, null, q0b_1_3m, null
    from stg where q0b_1 is not null
    union all
    select permid, wave_number, 'q0b', '2', q0b_2, null, q0b_2_3m, null
    from stg where q0b_2 is not null
    union all
    select permid, wave_number, 'q0b', '3', q0b_3, null, q0b_3_3m, null
    from stg where q0b_3 is not null
    union all
    select permid, wave_number, 'q0b', '4', q0b_4, null, q0b_4_3m, null
    from stg where q0b_4 is not null
    union all
    select permid, wave_number, 'q0b', '5', q0b_5, null, q0b_5_3m, null
    from stg where q0b_5 is not null
    union all
    select permid, wave_number, 'q0b', '6', q0b_6, null, q0b_6_3m, null
    from stg where q0b_6 is not null
    union all
    select permid, wave_number, 'q0b', '7', q0b_7, null, q0b_7_3m, null
    from stg where q0b_7 is not null
    union all
    select permid, wave_number, 'q0c', '', q0c, null, null, null
    from stg where q0c is not null

),

final as (

    select
        u.permid,
        u.wave_number,
        f.country_code,
        f.country_name_en,
        f.employee_band_code,
        f.firm_size_en,
        f.is_sme,
        f.sector_code,
        f.sector_en,
        f.survey_year,
        f.survey_period,
        f.survey_period_label,
        f.weight_common,
        u.question_id,
        u.sub_item,
        u.response_raw,
        u.response_rec,
        u.response_grouped,
        u.response_grouped_rec,

        -- Non-response flag
        -- Covers: -1 (N/A), -2 (don't know), -99 (refused),
        --         7 (not asked - routing), 99 (not asked - routing)
        u.response_raw in (-1, -2, -99, 7, 99)                     as is_nonresponse

    from unpivoted u
    left join firm f using (permid, wave_number)

)

select * from final
