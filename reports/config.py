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
"""

SECTIONS = [
    {
        "id": "q10_terms",
        "sql_file": "q10.sql",
        "group": "Financing Conditions",
        "title": "Changes in Terms and Conditions of Bank Financing (Q10)",
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
        "id": "q0b_pressingness",
        "sql_file": "q0b_pressingness.sql",
        "group": "Economic Situation of Firms",
        "title": "Most Pressing Business Problems (Q0B)",
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
            "Focus on Slovakia. Highlight where SK 'access to finance' score (problem_id=3) "
            "diverges from EA. Compare to the most pressing problem overall."
        ),
    },
    {
        "id": "business_situation",
        "sql_file": "business_situation.sql",
        "group": "Economic Situation of Firms",
        "title": "Business Situation Indicators (Q2)",
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
        "id": "financing_gap",
        "sql_file": "financing_gap.sql",
        "group": "Financing Conditions",
        "title": "Financing Need vs Availability Gap (Q5/Q9)",
        "sign_note": (
            "need_nb = Q5 net balance: positive = more firms report increased financing need. "
            "availability_nb = Q9 net balance: positive = more firms report improved availability (FAVOURABLE). "
            "financing_gap_wtd = need_nb minus availability_nb: positive = need exceeds availability (ADVERSE). "
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
        "id": "financing_purpose",
        "sql_file": "financing_purpose.sql",
        "group": "Financing Conditions",
        "title": "Purpose of Financing (Q6A)",
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
        "id": "q11_factors",
        "sql_file": "q11_factors.sql",
        "group": "Financing Conditions",
        "title": "Factors Affecting Access to External Financing (Q11)",
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
            "If Q11a (economic outlook) is also deteriorating, note that as a macro-level drag on supply."
        ),
    },
]
