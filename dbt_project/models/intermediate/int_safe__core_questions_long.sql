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

unpivoted as (

    --------------------------------------------------------------------------
    -- Q1: Business situation change over past 6 months
    --   a=turnover, b=labour costs, c=interest expenses, d=profits
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
    -- Q2: Sources of financing used in past 6 months
    --   a=retained earnings, b=bank overdraft/credit line, c=bank loan,
    --   d=trade credit, e=other loans, f=subsidised bank loan,
    --   g=subordinated debt/hybrid, h=equity issuance, i=other,
    --   j=not needed external financing
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
    -- Q3: Most pressing problem facing firm (single-choice)
    --------------------------------------------------------------------------
    union all
    select permid, wave_number, 'q3', '', q3, null, null, null
    from stg where q3 is not null

    --------------------------------------------------------------------------
    -- Q4: Change in availability of financing instruments over past 6 months
    --   a=bank loans, b=bank overdraft/credit line, c=trade credit,
    --   d=other loans, e=equity, f=leasing/hire purchase,
    --   g=factoring, h=public financial support, i=debt securities,
    --   j=subordinated debt, k=internal funds, l=grants,
    --   m=other, p=private equity/venture capital, r=crowdfunding
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
    -- Q4A: Reasons for change in availability (mirrors q4 sub-items)
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
    -- Q5: Change in need for financing over past 6 months
    --   Same instrument sub-items as q4 (a–h)
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
    -- Q6: Bank relationship
    --   a=main bank, b=number of banks, c=bank switched, d=duration
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
    -- Q6A: Bank relationship sub-items (1–6)
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
    -- Q7A/Q7B: Obstacles to obtaining financing
    --   a=sufficient collateral, b=credit history, c=too small/young,
    --   d=business prospects
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
    -- Q8A: Bank loan amount bracket
    -- Q8A only — q8b and rate sub-fields are kept in staging as floats
    -- and handled separately in mart_safe__loan_applications
    --------------------------------------------------------------------------
    union all
    select permid, wave_number, 'q8a', '', q8a, q8a_rec, null, null
    from stg where q8a is not null

    --------------------------------------------------------------------------
    -- Q9: Applied for / outcome of applications (by instrument)
    --   a=bank loan, b=bank overdraft/credit line, c=trade credit,
    --   d=other loans, e=subordinated debt, f=equity, g=grants,
    --   h=other
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
    -- Q10: Change in terms and conditions of financing
    --   a=interest rates, b=other financing costs, c=available size/amount,
    --   d=collateral requirements, e=maturity, f=covenants
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
    -- Q11: Most important obstacle to business activity
    --   a=finding customers, b=access to finance, c=costs of production,
    --   d=availability of skilled staff, e=competition,
    --   f=regulation, g=economic outlook, h=other, i=none
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
    -- Q12–Q17: Future needs, innovation financing, trade credit
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
    -- Q19–Q26: Government support, expectations, other
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
    -- Q31–Q34: Supplementary / country-specific questions
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
    -- Q0 / Q0B / Q0C: Screener and routing questions
    --------------------------------------------------------------------------
    union all
    select permid, wave_number, 'q0', '',  q0,  null, null, null
    from stg where q0 is not null
    union all
    select permid, wave_number, 'q0b', '1', q0b_1, null, q0b_1_grouped, null
    from stg where q0b_1 is not null
    union all
    select permid, wave_number, 'q0b', '2', q0b_2, null, q0b_2_grouped, null
    from stg where q0b_2 is not null
    union all
    select permid, wave_number, 'q0b', '3', q0b_3, null, q0b_3_grouped, null
    from stg where q0b_3 is not null
    union all
    select permid, wave_number, 'q0b', '4', q0b_4, null, q0b_4_grouped, null
    from stg where q0b_4 is not null
    union all
    select permid, wave_number, 'q0b', '5', q0b_5, null, q0b_5_grouped, null
    from stg where q0b_5 is not null
    union all
    select permid, wave_number, 'q0b', '6', q0b_6, null, q0b_6_grouped, null
    from stg where q0b_6 is not null
    union all
    select permid, wave_number, 'q0b', '7', q0b_7, null, q0b_7_grouped, null
    from stg where q0b_7 is not null
    union all
    select permid, wave_number, 'q0c', '', q0c, null, null, null
    from stg where q0c is not null

),

final as (

    select
        permid,
        wave_number,
        question_id,
        sub_item,
        response_raw,
        response_rec,
        response_grouped,
        response_grouped_rec,

        -- Non-response flag
        -- Covers: -1 (N/A), -2 (don't know), -99 (refused),
        --         7 (not asked - routing), 99 (not asked - routing)
        response_raw in (-1, -2, -99, 7, 99)                       as is_nonresponse

    from unpivoted

)

select * from final
