import sys
from pathlib import Path

# Ensure reports/ is importable
sys.path.insert(0, str(Path(__file__).parent.parent / "reports"))

import matplotlib
matplotlib.use("Agg")
matplotlib.rcParams.update({"font.family": "DejaVu Sans"})

import pandas as pd
import pytest


@pytest.fixture
def sample_cost_tracker():
    return {"input_tokens": 0, "output_tokens": 0, "usd": 0.0, "calls": 0, "by_model": {}}


@pytest.fixture
def net_balance_df():
    """Minimal SK/EA/DE net balance dataframe for two waves."""
    rows = []
    for wave in [37, 38]:
        for country, val in [("SK", 5.0), ("EA", 3.0), ("DE", 1.5)]:
            rows.append({
                "wave_number": wave,
                "country_code": country,
                "net_balance_wtd": val + (wave - 37) * 2,
                "n_respondents": 80,
                "firm_size": "all",
            })
    return pd.DataFrame(rows)


@pytest.fixture
def section_stub():
    return {
        "id": "bank_loan_terms",
        "title": "Bank loan terms",
        "sql_file": "bank_loan_terms.sql",
        "value_col": "net_balance_wtd",
        "panel_col": "sub_item",
        "panel_label_col": "sub_item_label",
        "series_col": "country_code",
        "pinned_panels": ["a"],
        "always_include": False,
        "sign_note": "positive = tightening (adverse)",
        "focus": "Slovakia vs EA",
        "group": "Financing Conditions",
        "routed": True,
        "has_missingness_caveat": False,
        "question_ids": ["q10"],
    }
