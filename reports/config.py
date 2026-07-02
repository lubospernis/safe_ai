"""
Section registry for the SAFE automated report.

Each entry defines one report section:
  id             — unique slug
  sql_file       — filename under reports/sql/
  title          — human-readable section heading
  sign_note      — how to interpret positive/negative values (injected into bullet prompt)
  value_col      — numeric column to plot
  panel_col      — column whose unique values become subplot panels (None = single panel)
  panel_label_col— human-readable label column for panel_col (can equal panel_col)
  series_col     — column that determines which line/bar per panel (usually country_code)
  pinned_panels  — panel values always included regardless of interest check
  max_panels     — hard cap on number of panels shown (≤ 2 recommended)
  always_include — if True, section shown even if interest check returns False
  focus          — country/topic focus injected into bullet prompt
  routed         — if True, a methodology footnote is added explaining that only
                   firms for which the instrument is relevant are asked (Q5/Q9/Q10)
  has_missingness_caveat — if True, a footnote is added about sparse data exclusion
"""

SECTIONS = [
    {
        "id": "bank_loan_terms",
        "sql_file": "bank_loan_terms.sql",
        "group": "Financing Conditions",
        "title": "Changes in Terms and Conditions of Bank Financing (Q10)",
        "question_ids": ["q10"],
        "sign_note": (
            "positive net balance = net tightening (more firms report worse/tighter conditions, ADVERSE); "
            "negative net balance = net easing (more firms report better/easier conditions, FAVOURABLE). "
            "NEVER say 'smoothing' or imply positive is good."
        ),
        "value_col": "net_balance_wtd",
        "panel_col": "sub_item",
        "panel_label_col": "sub_item_label",
        "series_col": "country_code",
        "pinned_panels": ["a"],
        "max_panels": 2,
        "always_include": True,
        "routed": True,
        "focus": "Lead with Slovakia (SK). Compare to EA and DE primarily.",
    },
    {
        "id": "financing_gap",
        "sql_file": "financing_gap.sql",
        "group": "Financing Conditions",
        "title": "Financing Need vs Availability Gap (Q5/Q9)",
        "question_ids": ["q5", "q9"],
        "sign_note": (
            "need_nb = Q5 net balance: positive = more firms report increased financing need. "
            "availability_nb = Q9 net balance: positive = more firms report improved availability (FAVOURABLE). "
            "financing_gap_wtd = need_nb minus availability_nb: positive = need exceeds availability (ADVERSE). "
            "CRITICAL for comparisons: a LARGER positive gap = MORE stressed (worse). "
            "A SMALLER positive gap = LESS stressed (better). "
            "Example: SK gap +4.8pp vs EA gap +6.6pp means SK is LESS stressed than EA — NOT tighter. "
            "Always state which country is more/less stressed when comparing gaps. "
            "All values are net balances in percentage points."
        ),
        "value_col": "financing_gap_wtd",
        "panel_col": "sub_item",
        "panel_label_col": "sub_item_label",
        "series_col": "country_code",
        "pinned_panels": ["a"],
        "max_panels": 1,
        "always_include": False,
        "routed": True,
        "focus": (
            "Cover bank loans (a), credit lines (f), and trade credit (b) for Slovakia vs EA. "
            "Use one bullet per instrument. For each: state the gap level and direction, then "
            "diagnose WHICH component drove it — need or availability or both — with exact pp values. "
            "e.g. 'The bank loan gap narrowed to -7.6pp as need fell (-6.2pp) while availability "
            "turned positive (+1.4pp).' "
            "CRITICALLY: compare bank loans vs credit lines explicitly — if the credit line gap "
            "diverges from the bank loan gap (e.g. tight credit lines but easy bank loans), name it "
            "directly: 'While bank loan conditions eased, the credit line gap remained wide at Xpp, "
            "driven by rising need rather than tighter supply.' "
            "This cross-instrument comparison is the most analytically important part of the section."
        ),
    },
    {
        "id": "loan_applications",
        "sql_file": "loan_applications.sql",
        "group": "Financing Conditions",
        "title": "Bank Loan Applications and Access Conditions (Q7A/Q7B)",
        "question_ids": ["q7a", "q7b"],
        "sign_note": (
            "financing_gap_wtd = share of firms who wanted bank credit but couldn't get it "
            "(discouragement_rate_wtd + rejection share of all respondents). Higher = worse credit access. "
            "THIS IS NOT the Q5/Q9 need-vs-availability gap — it measures credit-seeking behaviour. "
            "application_rate_wtd = % that applied. discouragement_rate_wtd = % that didn't apply "
            "fearing rejection. rejection_rate_wtd = % of applicants rejected. All are % of respondents."
        ),
        "value_col": "financing_gap_wtd",
        "panel_col": None,
        "panel_label_col": None,
        "series_col": "country_code",
        "pinned_panels": [],
        "max_panels": 1,
        "always_include": False,
        "routed": True,
        "focus": (
            "WHAT financing_gap_wtd MEANS HERE: this is NOT the need-vs-availability gap from Q5/Q9. "
            "Here, financing_gap_wtd = discouragement_rate_wtd + rejection_share_of_all_respondents. "
            "It measures the share of firms who WANTED bank credit but either did not apply (feared "
            "rejection) or applied and were turned down. Explain this clearly in the bullets: "
            "e.g. 'X% of Slovak firms seeking bank loans either did not apply out of fear of rejection "
            "(discouragement) or were turned down, compared with Y% in the EA (n=...).' "
            "Then break it down: "
            "application_rate_wtd: share of all respondents that actually applied; compare SK vs EA and prior wave. "
            "discouragement_rate_wtd: share who did not apply due to fear of rejection; compare SK vs EA and prior wave. "
            "rejection_rate_wtd: share of applicants rejected; compare SK vs EA and prior wave. "
            "Always cite n_respondents next to every rate. All figures are for bank loans only (sub_item='a'). "
            "Never say 'surged' or 'collapsed' — write 'rose from X% (wave N-1) to Y% (wave N)'."
        ),
    },
    {
        "id": "availability_expectations",
        "sql_file": "availability_expectations.sql",
        "group": "Financing Conditions",
        "title": "Expected Availability of External Financing (Q23)",
        "question_ids": ["q23"],
        "sign_note": (
            "positive net balance = more firms expect availability to IMPROVE (FAVOURABLE). "
            "negative net balance = more firms expect availability to DETERIORATE (ADVERSE). "
            "CRITICAL sign rule: net_balance_wtd is already signed. When it is negative, write "
            "'a net X% of firms expected availability to deteriorate' where X = absolute value. "
            "NEVER write 'a net -X%' — the word 'deteriorate' already captures the direction. "
            "This is a forward-looking complement to Q9 (actual availability changes)."
        ),
        "value_col": "net_balance_wtd",
        "panel_col": "sub_item",
        "panel_label_col": "sub_item_label",
        "series_col": "country_code",
        "pinned_panels": ["b"],
        "max_panels": 2,
        "always_include": False,
        "routed": True,
        "has_missingness_caveat": True,
        "focus": (
            "Focus on bank loans (b) for Slovakia vs EA. "
            "Always include n_respondents and the instrument name (e.g. 'bank loans'). "
            "When describing SK vs EA divergence, always state the exact pp gap between them. "
            "Only use 'notable' if the SK–EA gap ≥ 5 pp. Only use 'sharp' if the wave-over-wave change "
            "≥ 5 pp. For smaller changes, use 'marginally' or 'slightly'. "
            "If SK forward expectation diverges from the EA trend, explain what that signals — "
            "e.g. 'Slovak firms expect availability to deteriorate while EA firms expect improvement, "
            "suggesting persistent local supply tightness not yet visible in EA-wide data.'"
        ),
    },
    {
        "id": "financing_purpose",
        "sql_file": "financing_purpose.sql",
        "group": "Financing Conditions",
        "title": "Purpose of Financing (Q6A)",
        "question_ids": ["q6a"],
        "sign_note": (
            "pct_cited_wtd = % of firms that cited this purpose (multi-select, so percentages can sum > 100%). "
            "Not a net balance. Higher = more firms used financing for this purpose."
        ),
        "value_col": "pct_cited_wtd",
        "panel_col": "purpose_id",
        "panel_label_col": "purpose_label",
        "series_col": "country_code",
        "pinned_panels": ["2"],
        "max_panels": 2,
        "always_include": False,
        "focus": "Focus on Slovakia vs EA. Highlight where SK financing purpose mix differs from the euro area.",
    },
    {
        "id": "financing_factors",
        "sql_file": "financing_factors.sql",
        "group": "Financing Conditions",
        "title": "Factors Affecting Access to External Financing (Q11)",
        "question_ids": ["q11"],
        "sign_note": (
            "positive net balance = factor IMPROVED (more firms say better than worse — FAVOURABLE for financing access). "
            "negative net balance = factor DETERIORATED (more firms say worse — ADVERSE, potential credit supply constraint). "
            "Key sub-items: f=willingness of banks (direct supply signal), "
            "a=general economic outlook (macro drag), b=access to public support/guarantees."
        ),
        "value_col": "net_balance_wtd",
        "panel_col": "sub_item",
        "panel_label_col": "sub_item_label",
        "series_col": "country_code",
        "pinned_panels": ["f"],
        "max_panels": 2,
        "always_include": False,
        "focus": (
            "Lead with Q11f (willingness of banks) for Slovakia vs EA — this is the key credit supply indicator. "
            "If Q11a (economic outlook) is also deteriorating, note that as a macro-level drag on supply. "
            "STRICT RULE: Only write mechanism language ('signalling', 'suggesting', 'indicating') if the "
            "mechanism follows directly from the data or is supported by published ECB/NBS methodology. "
            "Do NOT write phrases like 'tightening in non-bank financing buffers' without a published source. "
            "Default: describe what the data shows — 'a net X% of firms reported Y deteriorated'."
        ),
    },
    {
        "id": "business_situation",
        "sql_file": "business_situation.sql",
        "group": "Economic Situation of Firms",
        "title": "Business Situation Indicators (Q2)",
        "question_ids": ["q2"],
        "sign_note": (
            "positive net balance = indicator rising (more firms report increase than decrease). "
            "For labour costs (sub_item b), positive = costs rising = ADVERSE for firms. "
            "For turnover/profit, positive = improving = FAVOURABLE."
        ),
        "value_col": "net_balance_wtd",
        "panel_col": "sub_item",
        "panel_label_col": "sub_item_label",
        "series_col": "country_code",
        "pinned_panels": ["a"],
        "max_panels": 2,
        "always_include": False,
        "focus": "Lead with Slovakia turnover trend. Compare profit and cost indicators to EA.",
    },
    {
        "id": "outlook",
        "sql_file": "outlook.sql",
        "group": "Economic Situation of Firms",
        "title": "Expected Changes in Turnover and Investment (Q26)",
        "question_ids": ["q26"],
        "sign_note": (
            "positive net balance = more firms expect the indicator to INCREASE (FAVOURABLE). "
            "negative net balance = more firms expect the indicator to DECREASE (ADVERSE). "
            "Sub-items: a=Turnover, b=Fixed investment (property, plant, equipment)."
        ),
        "value_col": "net_balance_wtd",
        "panel_col": "sub_item",
        "panel_label_col": "sub_item_label",
        "series_col": "country_code",
        "pinned_panels": ["a"],
        "max_panels": 2,
        "always_include": False,
        "focus": (
            "Lead with turnover outlook (sub_item='a') for Slovakia vs EA. "
            "If SK expectation diverges significantly from EA — e.g. EA firms optimistic but SK pessimistic — "
            "flag that as a signal of diverging economic trajectories."
        ),
    },
    {
        "id": "expectations_quantitative",
        "sql_file": "expectations_quantitative.sql",
        "group": "Economic Situation of Firms",
        "title": "Price, Wage and Employment Expectations (Q31/Q34)",
        "question_ids": ["q31", "q34"],
        "sign_note": (
            "mean_wtd is a weighted mean expected % change over the next 12 months. "
            "This is NOT a net balance — it is a level in percent (e.g. 3.5 means firms expect +3.5% change). "
            "Positive = firms expect an increase. Negative = firms expect a decrease. "
            "panel_id combines question_id and sub_item: q31_a=expected inflation rate in 12 months, "
            "q34_a=average selling price, q34_b=production input prices (non-labour), "
            "q34_c=average wages, q34_d=number of employees."
        ),
        "value_col": "mean_wtd",
        "panel_col": "panel_id",
        "panel_label_col": "sub_item_label",
        "series_col": "country_code",
        "pinned_panels": ["q34_a"],
        "max_panels": 2,
        "always_include": False,
        "has_missingness_caveat": True,
        "focus": (
            "Lead with selling price expectations (q34_a) for Slovakia vs EA. "
            "If SK wage expectations (q34_c) are also high — relative to EA — flag the margin compression risk: "
            "'Slovak firms anticipate X% wage growth alongside Y% input price rises, compressing margins.' "
            "Cross-reference with current labour cost net balance from the business situation section if elevated."
        ),
    },
    {
        "id": "expectations_risk",
        "sql_file": "expectations_risk.sql",
        "group": "Economic Situation of Firms",
        "title": "Inflation Risk Outlook (Q33)",
        "question_ids": ["q33"],
        "sign_note": (
            "net_balance_wtd = % of firms seeing upside inflation risk minus % seeing downside risk. "
            "Positive = more firms see inflation risks to the upside (inflation higher than expected). "
            "Negative = more firms see inflation risks to the downside. "
            "Also available: pct_downside_wtd, pct_balanced_wtd, pct_upside_wtd."
        ),
        "value_col": "net_balance_wtd",
        "panel_col": None,
        "panel_label_col": None,
        "series_col": "country_code",
        "pinned_panels": [],
        "max_panels": 1,
        "always_include": False,
        "has_missingness_caveat": True,
        "focus": (
            "Compare SK vs EA on the inflation risk balance. "
            "If SK firms disproportionately see upside risk vs EA — note that as a signal of local price persistence. "
            "Cross-reference with Q34 selling price expectations if both are elevated."
        ),
    },
    {
        "id": "business_problems",
        "sql_file": "business_problems.sql",
        "group": "Economic Situation of Firms",
        "title": "Most Pressing Business Problems (Q0B)",
        "question_ids": ["q0b"],
        "sign_note": (
            "values are average pressingness scores on a 1–10 scale (NOT net balances). "
            "Higher score = problem is more pressing for firms. "
            "This is not a net balance — do not say 'net X% of firms'."
        ),
        "value_col": "avg_pressingness_wtd",
        "panel_col": "problem_id",
        "panel_label_col": "problem_label",
        "series_col": "country_code",
        "pinned_panels": ["3"],
        "max_panels": 2,
        "always_include": False,
        "focus": (
            "Report the top 3 most pressing problems for Slovakia by avg_pressingness_wtd score. "
            "CRITICAL: always contextualise the absolute score — the scale is 1–10 where 1 = not pressing "
            "at all, 10 = extremely pressing. A score of 3.9 = low priority; 6 = moderate; 8 = very pressing. "
            "Do NOT describe any problem as 'most pressing' in isolation — always say 'the most pressing "
            "among the seven tested'. Never label a problem 'pressing' if its score is below 5. "
            "If all scores are below 5, open with: 'Slovak firms rate all seven business problems as "
            "relatively low priority (all scores below 5/10).' "
            "Order bullets by descending avg_pressingness_wtd. "
            "Compare SK vs EA rank order where they diverge — e.g. if access to finance ranks higher "
            "for SK than EA despite both scoring low in absolute terms."
        ),
    },
]
