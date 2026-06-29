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
"""

SECTIONS = [
    {
        "id": "q10_terms",
        "sql_file": "q10.sql",
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
        "focus": "Lead with Slovakia (SK). Compare to EA and DE primarily.",
    },
    {
        "id": "q0b_pressingness",
        "sql_file": "q0b_pressingness.sql",
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
        "id": "outlook",
        "sql_file": "outlook.sql",
        "title": "Expected Changes in Turnover and Investment (Q26)",
        "sign_note": (
            "positive net balance = more firms expect increase over next 2 quarters; "
            "negative = more firms expect decrease."
        ),
        "value_col": "net_balance_wtd",
        "panel_col": "sub_item",
        "panel_label_col": "sub_item_label",
        "series_col": "country_code",
        "pinned_panels": ["a", "b"],
        "max_panels": 2,
        "always_include": False,
        "focus": "Focus on Slovakia vs EA outlook for investment and turnover.",
    },
    {
        "id": "loan_applications",
        "sql_file": "loan_applications.sql",
        "title": "Bank Loan Access: Applications and Financing Gap (Q7A/Q7B)",
        "sign_note": (
            "financing_gap_wtd = share of firms that are discouraged or rejected (ECB headline access indicator). "
            "Higher = worse access to bank loans. Values are % of firms, not net balances."
        ),
        "value_col": "financing_gap_wtd",
        "panel_col": None,
        "panel_label_col": None,
        "series_col": "country_code",
        "pinned_panels": [],
        "max_panels": 1,
        "always_include": False,
        "focus": "Focus on Slovakia financing gap vs EA. Note any divergence in rejection vs discouragement.",
    },
]
