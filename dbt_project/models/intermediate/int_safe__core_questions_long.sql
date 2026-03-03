{{
  config(
    materialized = 'table'
  )
}}

/*
  Unpivots core survey questions into long format.
  Sections and question IDs included:
    Section 2 (Business situation): q2
    Section 3 (Financing):          q4rec, q5, q6a, q7a, q7b, q32
    Section 4 (Availability):       q9, q10, q11, q23
    Section 5 (Expectations):       q26, q31, q33, q34

  Target schema:
    permid | wave_number | question_id | sub_item | response_raw
           | response_rec | response_3m | response_3m_rec
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
  _3m variants (3-month reference period questionnaire, raw suffix _g1), those
  are additional columns on the same row — not separate rows.
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
    -- Q2 (annex Q2): Business situation — extended sub-item block
    --   (decreased / remained unchanged / increased)
    --   a=Turnover, b=Labour costs (incl. social contributions),
    --   c=Other costs (materials, energy, other), d=Interest expenses,
    --   e=Profit, f=Profit margin (removed from questionnaire, older rounds only),
    --   g=Investments in property, plant or equipment,
    --   h=Inventories and other working capital, i=Number of employees,
    --   j=Debt compared to assets
    --------------------------------------------------------------------------
    select permid, wave_number, 'q2' as question_id, 'a' as sub_item, q2_a as response_raw, null as response_rec, q2_a_3m as response_3m, null as response_3m_rec
    from stg where (q2_a is not null or q2_a_3m is not null)
    union all
    select permid, wave_number, 'q2', 'b', q2_b, null, q2_b_3m, null
    from stg where (q2_b is not null or q2_b_3m is not null)
    union all
    select permid, wave_number, 'q2', 'c', q2_c, null, q2_c_3m, null
    from stg where (q2_c is not null or q2_c_3m is not null)
    union all
    select permid, wave_number, 'q2', 'd', q2_d, null, q2_d_3m, null
    from stg where (q2_d is not null or q2_d_3m is not null)
    union all
    select permid, wave_number, 'q2', 'e', q2_e, null, q2_e_3m, null
    from stg where (q2_e is not null or q2_e_3m is not null)
    union all
    select permid, wave_number, 'q2', 'f', q2_f, null, null, null
    from stg where q2_f is not null
    union all
    select permid, wave_number, 'q2', 'g', q2_g, null, q2_g_3m, null
    from stg where (q2_g is not null or q2_g_3m is not null)
    union all
    select permid, wave_number, 'q2', 'h', q2_h, null, q2_h_3m, null
    from stg where (q2_h is not null or q2_h_3m is not null)
    union all
    select permid, wave_number, 'q2', 'i', q2_i, null, q2_i_3m, null
    from stg where (q2_i is not null or q2_i_3m is not null)
    union all
    select permid, wave_number, 'q2', 'j', q2_j, q2_j_rec, q2_j_3m, q2_j_3m_rec
    from stg where (q2_j is not null or q2_j_3m is not null)



    --------------------------------------------------------------------------
    -- Q4rec (annex Q4): Financing sources — combined relevance/usage recode.
    --
    --   Q4 asks per instrument: "Is this source relevant to your enterprise?"
    --     3 = Yes, relevant
    --     7 = Not relevant
    --     9 = DK
    --   If relevant (code 3), a follow-up (Q4A in raw: q4a_*) asks:
    --     "Have you used it in the past 6 months?"
    --     1 = Yes (used)
    --     2 = No (relevant but not used)
    --     99 = DK on follow-up
    --
    --   The recode (q4_*_rec / q4_*_3m_rec) collapses both into one variable:
    --     1  = Used in the past 6 months  (Q4=3 AND Q4A=1)
    --     2  = Relevant but not used       (Q4=3 AND Q4A=2)
    --     7  = Not relevant                (Q4=7)
    --     9  = DK (at Q4 level)            (Q4=9)
    --     99 = DK (at Q4A follow-up level) (Q4=3, Q4A=99)
    --
    --   Non-response codes: 9, 99
    --
    --   Sub-items (verified from annex.xlsx Q4):
    --     a = Retained earnings or sale of assets
    --     b = Grants or subsidised bank loans
    --     c = Credit line, bank overdraft or credit cards overdraft
    --     d = Bank loan (excl. subsidised loans, overdrafts and credit lines)
    --     e = Trade credit
    --     f = Other loan (family, friends, related enterprise, shareholders)
    --     g = (legacy combined leasing+factoring, pre-2014H1 only)
    --     h = Debt securities issued
    --     i = (legacy mezzanine, removed)
    --     j = Equity capital
    --     m = Leasing or hire-purchase
    --     p = Other sources (subordinated debt, peer-to-peer, crowdfunding, etc.)
    --     r = Factoring
    --
    --   response_raw  = q4_*_rec  (6-month questionnaire)
    --   response_3m   = q4_*_3m_rec (3-month questionnaire, waves 30–37 only)
    --------------------------------------------------------------------------
    union all
    select permid, wave_number, 'q4rec', 'a', q4_a_rec, null, q4_a_3m_rec, null
    from stg where (q4_a_rec is not null or q4_a_3m_rec is not null)
    union all
    select permid, wave_number, 'q4rec', 'b', q4_b_rec, null, q4_b_3m_rec, null
    from stg where (q4_b_rec is not null or q4_b_3m_rec is not null)
    union all
    select permid, wave_number, 'q4rec', 'c', q4_c_rec, null, q4_c_3m_rec, null
    from stg where (q4_c_rec is not null or q4_c_3m_rec is not null)
    union all
    select permid, wave_number, 'q4rec', 'd', q4_d_rec, null, q4_d_3m_rec, null
    from stg where (q4_d_rec is not null or q4_d_3m_rec is not null)
    union all
    select permid, wave_number, 'q4rec', 'e', q4_e_rec, null, q4_e_3m_rec, null
    from stg where (q4_e_rec is not null or q4_e_3m_rec is not null)
    union all
    select permid, wave_number, 'q4rec', 'f', q4_f_rec, null, q4_f_3m_rec, null
    from stg where (q4_f_rec is not null or q4_f_3m_rec is not null)
    union all
    select permid, wave_number, 'q4rec', 'g', q4_g_rec, null, q4_g_3m_rec, null
    from stg where (q4_g_rec is not null or q4_g_3m_rec is not null)
    union all
    select permid, wave_number, 'q4rec', 'h', q4_h_rec, null, q4_h_3m_rec, null
    from stg where (q4_h_rec is not null or q4_h_3m_rec is not null)
    union all
    select permid, wave_number, 'q4rec', 'j', q4_j_rec, null, q4_j_3m_rec, null
    from stg where (q4_j_rec is not null or q4_j_3m_rec is not null)
    union all
    select permid, wave_number, 'q4rec', 'm', q4_m_rec, null, q4_m_3m_rec, null
    from stg where (q4_m_rec is not null or q4_m_3m_rec is not null)
    union all
    select permid, wave_number, 'q4rec', 'p', q4_p_rec, null, q4_p_3m_rec, null
    from stg where (q4_p_rec is not null or q4_p_3m_rec is not null)
    union all
    select permid, wave_number, 'q4rec', 'r', q4_r_rec, null, q4_r_3m_rec, null
    from stg where (q4_r_rec is not null or q4_r_3m_rec is not null)

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
    select permid, wave_number, 'q5', 'a', q5_a, q5_a_rec, q5_a_3m, q5_a_3m_rec
    from stg where (q5_a is not null or q5_a_3m is not null)
    union all
    select permid, wave_number, 'q5', 'b', q5_b, q5_b_rec, q5_b_3m, q5_b_3m_rec
    from stg where (q5_b is not null or q5_b_3m is not null)
    union all
    select permid, wave_number, 'q5', 'c', q5_c, q5_c_rec, q5_c_3m, q5_c_3m_rec
    from stg where (q5_c is not null or q5_c_3m is not null)
    union all
    select permid, wave_number, 'q5', 'd', q5_d, q5_d_rec, q5_d_3m, q5_d_3m_rec
    from stg where (q5_d is not null or q5_d_3m is not null)
    union all
    select permid, wave_number, 'q5', 'e', q5_e, q5_e_rec, null, null
    from stg where q5_e is not null
    union all
    select permid, wave_number, 'q5', 'f', q5_f, q5_f_rec, q5_f_3m, q5_f_3m_rec
    from stg where (q5_f is not null or q5_f_3m is not null)
    union all
    select permid, wave_number, 'q5', 'g', q5_g, q5_g_rec, q5_g_3m, q5_g_3m_rec
    from stg where (q5_g is not null or q5_g_3m is not null)
    union all
    select permid, wave_number, 'q5', 'h', q5_h, q5_h_rec, q5_h_3m, q5_h_3m_rec
    from stg where (q5_h is not null or q5_h_3m is not null)


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
    select permid, wave_number, 'q6a', '1', q6a_1, null, q6a_1_3m, null
    from stg where (q6a_1 is not null or q6a_1_3m is not null)
    union all
    select permid, wave_number, 'q6a', '2', q6a_2, null, q6a_2_3m, null
    from stg where (q6a_2 is not null or q6a_2_3m is not null)
    union all
    select permid, wave_number, 'q6a', '3', q6a_3, null, q6a_3_3m, null
    from stg where (q6a_3 is not null or q6a_3_3m is not null)
    union all
    select permid, wave_number, 'q6a', '4', q6a_4, null, q6a_4_3m, null
    from stg where (q6a_4 is not null or q6a_4_3m is not null)
    union all
    select permid, wave_number, 'q6a', '5', q6a_5, null, q6a_5_3m, null
    from stg where (q6a_5 is not null or q6a_5_3m is not null)
    union all
    select permid, wave_number, 'q6a', '6', q6a_6, null, q6a_6_3m, null
    from stg where (q6a_6 is not null or q6a_6_3m is not null)

    --------------------------------------------------------------------------
    -- Q7A (annex Q7A): Application status for each financing instrument
    --   1=Applied, 2=Did not apply — fear of rejection (discouraged),
    --   3=Did not apply — sufficient internal funds,
    --   4=Did not apply — other reasons, 9=DK/NA
    -- Q7B (annex Q7B): Outcome of application (for applicants only)
    --   1=Received everything, 2=Got part (old code, pre-2010H1),
    --   3=Refused — cost too high, 4=Rejected by lender,
    --   5=Received 75% and above (new, from 2010H1),
    --   6=Received below 75% (new, from 2010H1),
    --   8=Still pending (added later), 9=DK
    --   Sub-items for both Q7A and Q7B (verified from annex.xlsx):
    --   a=Bank loan (excl. overdraft and credit lines)
    --   b=Trade credit
    --   c=Other external financing
    --   d=Credit line, bank overdraft or credit cards overdraft
    --------------------------------------------------------------------------
    union all
    select permid, wave_number, 'q7a', 'a', q7a_a, q7a_a_rec, q7a_a_3m, q7a_a_3m_rec
    from stg where (q7a_a is not null or q7a_a_3m is not null)
    union all
    select permid, wave_number, 'q7a', 'b', q7a_b, q7a_b_rec, q7a_b_3m, q7a_b_3m_rec
    from stg where (q7a_b is not null or q7a_b_3m is not null)
    union all
    select permid, wave_number, 'q7a', 'c', q7a_c, q7a_c_rec, q7a_c_3m, q7a_c_3m_rec
    from stg where (q7a_c is not null or q7a_c_3m is not null)
    union all
    select permid, wave_number, 'q7a', 'd', q7a_d, q7a_d_rec, q7a_d_3m, q7a_d_3m_rec
    from stg where (q7a_d is not null or q7a_d_3m is not null)
    union all
    select permid, wave_number, 'q7b', 'a', q7b_a, q7b_a_rec, q7b_a_3m, q7b_a_3m_rec
    from stg where (q7b_a is not null or q7b_a_3m is not null)
    union all
    select permid, wave_number, 'q7b', 'b', q7b_b, q7b_b_rec, q7b_b_3m, q7b_b_3m_rec
    from stg where (q7b_b is not null or q7b_b_3m is not null)
    union all
    select permid, wave_number, 'q7b', 'c', q7b_c, q7b_c_rec, q7b_c_3m, q7b_c_3m_rec
    from stg where (q7b_c is not null or q7b_c_3m is not null)
    union all
    select permid, wave_number, 'q7b', 'd', q7b_d, q7b_d_rec, q7b_d_3m, q7b_d_3m_rec
    from stg where (q7b_d is not null or q7b_d_3m is not null)


    --------------------------------------------------------------------------
    -- Q9 (annex Q9): Availability of each financing type — has it improved,
    --   remained unchanged or deteriorated over past 6 months?
    --   1=Improved, 2=Unchanged, 3=Deteriorated, 7=Not applicable, 9=DK
    --   a=Bank loans (excl. overdraft and credit lines)
    --   b=Trade credit
    --   c=Equity capital
    --   d=Debt securities issued
    --   e=Other (removed in later rounds — same note as Q5_e)
    --   f=Credit line, bank overdraft or credit cards overdraft
    --   g=Leasing or hire-purchase
    --   h=Other loan (family, friends, related enterprise, shareholders)
    --------------------------------------------------------------------------
    union all
    select permid, wave_number, 'q9', 'a', q9_a, q9_a_rec, q9_a_3m, q9_a_3m_rec
    from stg where (q9_a is not null or q9_a_3m is not null)
    union all
    select permid, wave_number, 'q9', 'b', q9_b, q9_b_rec, q9_b_3m, q9_b_3m_rec
    from stg where (q9_b is not null or q9_b_3m is not null)
    union all
    select permid, wave_number, 'q9', 'c', q9_c, q9_c_rec, q9_c_3m, q9_c_3m_rec
    from stg where (q9_c is not null or q9_c_3m is not null)
    union all
    select permid, wave_number, 'q9', 'd', q9_d, q9_d_rec, q9_d_3m, q9_d_3m_rec
    from stg where (q9_d is not null or q9_d_3m is not null)
    union all
    select permid, wave_number, 'q9', 'e', q9_e, q9_e_rec, null, null
    from stg where q9_e is not null
    union all
    select permid, wave_number, 'q9', 'f', q9_f, q9_f_rec, q9_f_3m, q9_f_3m_rec
    from stg where (q9_f is not null or q9_f_3m is not null)
    union all
    select permid, wave_number, 'q9', 'g', q9_g, q9_g_rec, q9_g_3m, q9_g_3m_rec
    from stg where (q9_g is not null or q9_g_3m is not null)
    union all
    select permid, wave_number, 'q9', 'h', q9_h, q9_h_rec, q9_h_3m, q9_h_3m_rec
    from stg where (q9_h is not null or q9_h_3m is not null)

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
    select permid, wave_number, 'q10', 'a', q10_a, q10_a_rec, q10_a_3m, q10_a_3m_rec
    from stg where (q10_a is not null or q10_a_3m is not null)
    union all
    select permid, wave_number, 'q10', 'b', q10_b, q10_b_rec, q10_b_3m, q10_b_3m_rec
    from stg where (q10_b is not null or q10_b_3m is not null)
    union all
    select permid, wave_number, 'q10', 'c', q10_c, q10_c_rec, q10_c_3m, q10_c_3m_rec
    from stg where (q10_c is not null or q10_c_3m is not null)
    union all
    select permid, wave_number, 'q10', 'd', q10_d, q10_d_rec, q10_d_3m, q10_d_3m_rec
    from stg where (q10_d is not null or q10_d_3m is not null)
    union all
    select permid, wave_number, 'q10', 'e', q10_e, q10_e_rec, q10_e_3m, q10_e_3m_rec
    from stg where (q10_e is not null or q10_e_3m is not null)
    union all
    select permid, wave_number, 'q10', 'f', q10_f, q10_f_rec, q10_f_3m, q10_f_3m_rec
    from stg where (q10_f is not null or q10_f_3m is not null)

    --------------------------------------------------------------------------
    -- Q11 (annex Q11): Factors affecting availability of external financing —
    --   improved, unchanged or deteriorated over past 6 months?
    --   1=Improved, 2=Unchanged, 3=Deteriorated, 7=Not applicable, 9=DK
    --   a=General economic outlook (insofar as it affects availability of ext. financing)
    --   b=Access to public financial support, including guarantees
    --   c=Enterprise-specific outlook (sales, profitability, business plan)
    --   d=Enterprise's own capital
    --   e=Enterprise's credit history
    --   f=Willingness of banks to provide credit to your enterprise
    --   g=Willingness of business partners to provide trade credit
    --   h=Willingness of investors to invest in your enterprise
    --   i=(no annex definition; exists in data; likely a grouped/derived code)
    --   j=Willingness to extend credit to customers (accounts receivable)
    --------------------------------------------------------------------------
    union all
    select permid, wave_number, 'q11', 'a', q11_a, null, q11_a_3m, null
    from stg where (q11_a is not null or q11_a_3m is not null)
    union all
    select permid, wave_number, 'q11', 'b', q11_b, null, q11_b_3m, null
    from stg where (q11_b is not null or q11_b_3m is not null)
    union all
    select permid, wave_number, 'q11', 'c', q11_c, null, q11_c_3m, null
    from stg where (q11_c is not null or q11_c_3m is not null)
    union all
    select permid, wave_number, 'q11', 'd', q11_d, null, q11_d_3m, null
    from stg where (q11_d is not null or q11_d_3m is not null)
    union all
    select permid, wave_number, 'q11', 'e', q11_e, null, q11_e_3m, null
    from stg where (q11_e is not null or q11_e_3m is not null)
    union all
    select permid, wave_number, 'q11', 'f', q11_f, q11_f_rec, q11_f_3m, q11_f_3m_rec
    from stg where (q11_f is not null or q11_f_3m is not null)
    union all
    select permid, wave_number, 'q11', 'g', q11_g, q11_g_rec, q11_g_3m, q11_g_3m_rec
    from stg where (q11_g is not null or q11_g_3m is not null)
    union all
    select permid, wave_number, 'q11', 'h', q11_h, q11_h_rec, q11_h_3m, q11_h_3m_rec
    from stg where (q11_h is not null or q11_h_3m is not null)
    union all
    select permid, wave_number, 'q11', 'i', q11_i, null, q11_i_3m, null
    from stg where (q11_i is not null or q11_i_3m is not null)

    --------------------------------------------------------------------------
    -- Q23 (annex Q23): Expected availability of financing over next 6 months
    --   1=Will improve, 2=Will remain unchanged, 3=Will deteriorate,
    --   7=Not applicable, 9=DK
    --   a=Retained earnings or sale of assets
    --   b=Bank loans (excl. overdraft and credit lines)
    --   c=Equity capital
    --   d=Trade credit
    --   e=Debt securities issued
    --   f=Other (removed in later rounds — same note as Q5_e/Q9_e)
    --   g=Credit line, bank overdraft or credit cards overdraft
    --   i=Leasing or hire-purchase
    --   j=Other loan (family, friends, related enterprise, shareholders)
    --------------------------------------------------------------------------
    union all
    select permid, wave_number, 'q23', 'a', q23_a, q23_a_rec, q23_a_3m, q23_a_3m_rec
    from stg where (q23_a is not null or q23_a_3m is not null)
    union all
    select permid, wave_number, 'q23', 'b', q23_b, q23_b_rec, q23_b_3m, q23_b_3m_rec
    from stg where (q23_b is not null or q23_b_3m is not null)
    union all
    select permid, wave_number, 'q23', 'c', q23_c, q23_c_rec, q23_c_3m, q23_c_3m_rec
    from stg where (q23_c is not null or q23_c_3m is not null)
    union all
    select permid, wave_number, 'q23', 'd', q23_d, q23_d_rec, q23_d_3m, q23_d_3m_rec
    from stg where (q23_d is not null or q23_d_3m is not null)
    union all
    select permid, wave_number, 'q23', 'e', q23_e, q23_e_rec, q23_e_3m, q23_e_3m_rec
    from stg where (q23_e is not null or q23_e_3m is not null)
    union all
    select permid, wave_number, 'q23', 'f', q23_f, q23_f_rec, null, null
    from stg where q23_f is not null
    union all
    select permid, wave_number, 'q23', 'g', q23_g, q23_g_rec, q23_g_3m, q23_g_3m_rec
    from stg where (q23_g is not null or q23_g_3m is not null)
    union all
    select permid, wave_number, 'q23', 'i', q23_i, q23_i_rec, q23_i_3m, q23_i_3m_rec
    from stg where (q23_i is not null or q23_i_3m is not null)
    union all
    select permid, wave_number, 'q23', 'j', q23_j, q23_j_rec, q23_j_3m, q23_j_3m_rec
    from stg where (q23_j is not null or q23_j_3m is not null)

    --------------------------------------------------------------------------
    -- Q32 (annex Q32, Section 3): Main reason bank loans are not relevant.
    --   1=Insufficient collateral/guarantee, 2=Interest/price too high,
    --   3=Reduced control, 4=No bank loans available, 5=Other,
    --   6=Too much paperwork, 8=Do not need this type, 9=DK (single response)
    --------------------------------------------------------------------------
    union all
    select permid, wave_number, 'q32', '', q32, null, null, null
    from stg where q32 is not null

    --------------------------------------------------------------------------
    -- Q26 (annex Q26): Expected change in turnover and fixed investments
    --   over the next two quarters.
    --   1=will increase, 2=will remain unchanged, 3=will decrease, 9=DK
    --   a=Company's turnover
    --   b=Investments in property, plant or equipment (fixed investment)
    --   No 3m variant; no _rec suffix (recode not produced by ECB).
    --------------------------------------------------------------------------
    union all
    select permid, wave_number, 'q26', 'a', q26_a, null, null, null
    from stg where q26_a is not null
    union all
    select permid, wave_number, 'q26', 'b', q26_b, null, null, null
    from stg where q26_b is not null

    --------------------------------------------------------------------------
    -- Q31 (annex Q31, ECB-only): Euro area HICP inflation rate expectations.
    --   Continuous numeric variable (annual %, e.g. 3 = 3%).
    --   Non-response sentinel value: -9999 (DK / refused).
    --   Sub-items:
    --     a = in 12 months
    --     b = in three years (change in consumer prices in y+3 vs y+2)
    --     c = in five years  (change in consumer prices in y+5 vs y+4)
    --   No 3m variant; no _rec suffix.
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

    --------------------------------------------------------------------------
    -- Q33 (annex Q33, ECB-only): Main risk to 5-year inflation outlook.
    --   1=Risk to the downside (inflation lower than expected)
    --   2=Risks broadly balanced
    --   3=Risk to the upside (inflation higher than expected)
    --   9=DK
    --   Single response, no sub-items.
    --------------------------------------------------------------------------
    union all
    select permid, wave_number, 'q33', '', q33, null, null, null
    from stg where q33 is not null

    --------------------------------------------------------------------------
    -- Q34 (annex Q34): Expected % change over the next 12 months.
    --   Continuous numeric variable (% change).
    --   Non-response sentinel value: -9999 (DK / refused).
    --   Validation: responses >50% trigger a read-back confirmation in the survey.
    --   Sub-items:
    --     a = Average selling price of products/services in main markets
    --     b = Average prices of production inputs (non-labour: materials, energy)
    --     c = Average wage of current employees (excl. bonuses, overtime, FTE basis)
    --     d = Number of employees
    --   _rec suffix = values confirmed after >50% validation (stored alongside raw).
    --   No 3m variant.
    --------------------------------------------------------------------------
    union all
    select permid, wave_number, 'q34', 'a', q34_a, q34_a_rec, null, null
    from stg where q34_a is not null
    union all
    select permid, wave_number, 'q34', 'b', q34_b, q34_b_rec, null, null
    from stg where q34_b is not null
    union all
    select permid, wave_number, 'q34', 'c', q34_c, q34_c_rec, null, null
    from stg where q34_c is not null
    union all
    select permid, wave_number, 'q34', 'd', q34_d, q34_d_rec, null, null
    from stg where q34_d is not null

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
        u.response_3m,
        u.response_3m_rec,

        -- Non-response flag based on whichever response column is populated.
        -- For most questions: -1 (N/A), -2 (don't know), -99 (refused),
        --   7 (not asked/routing), 99 (not asked/routing).
        -- Exception — q4rec: code 7 = "not relevant" is a valid substantive answer;
        --   only 9 and 99 are non-response.
        -- Exception — q31, q34: continuous numeric variables; non-response is -9999.
        case
            when u.question_id = 'q4rec'
                then coalesce(u.response_raw, u.response_3m) in (9, 99)
            when u.question_id in ('q31', 'q34')
                then coalesce(u.response_raw, u.response_3m) = -9999
            else
                coalesce(u.response_raw, u.response_3m) in (-1, -2, -99, 7, 99)
        end                                                             as is_nonresponse

    from unpivoted u
    left join firm f using (permid, wave_number)

)

select * from final
