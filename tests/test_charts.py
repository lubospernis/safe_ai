import pandas as pd
import pytest

# _is_continuous lives in adhoc.py
from adhoc import _is_continuous
from charts import _select_panels


# ── _is_continuous ────────────────────────────────────────────────────────────

def _df_with_distinct(n: int) -> pd.DataFrame:
    """Create a df where SK has n distinct response_raw values for sub_item 'a'."""
    return pd.DataFrame({
        "response_raw": list(range(n)),
        "country_code": ["SK"] * n,
        "sub_item": ["a"] * n,
    })


def test_is_continuous_above_threshold():
    df = _df_with_distinct(11)
    assert _is_continuous(df) is True


def test_is_continuous_at_boundary():
    # exactly 10 distinct values → NOT continuous
    df = _df_with_distinct(10)
    assert _is_continuous(df) is False


def test_is_continuous_below_threshold():
    df = _df_with_distinct(4)
    assert _is_continuous(df) is False


def test_is_continuous_empty_df():
    df = pd.DataFrame({"response_raw": [], "country_code": [], "sub_item": []})
    assert _is_continuous(df) is False


# ── _select_panels ────────────────────────────────────────────────────────────

def _make_panel_df(panels, countries=("SK", "EA", "DE"), wave=38):
    rows = []
    for p in panels:
        for c in countries:
            rows.append({
                "sub_item": p,
                "sub_item_label": f"Label {p}",
                "country_code": c,
                "wave_number": wave,
                "net_balance_wtd": 5.0,
                "n_respondents": 50,
            })
    return pd.DataFrame(rows)


def _make_section(pinned, panel_col="sub_item", max_panels=4):
    return {
        "pinned_panels": pinned,
        "panel_col": panel_col,
        "value_col": "net_balance_wtd",
        "series_col": "country_code",
        "max_panels": max_panels,
    }


def test_select_panels_includes_pinned():
    df = _make_panel_df(["a", "b", "c"])
    sec = _make_section(pinned=["a"])
    panels = _select_panels(sec, df, best_panel=None)
    assert "a" in panels


def test_select_panels_respects_max():
    df = _make_panel_df(["a", "b", "c", "d", "e", "f"])
    sec = _make_section(pinned=["a"], max_panels=3)
    panels = _select_panels(sec, df, best_panel="b")
    assert len(panels) <= 3


def test_select_panels_adds_best_panel():
    df = _make_panel_df(["a", "b", "c"])
    sec = _make_section(pinned=["a"])
    panels = _select_panels(sec, df, best_panel="c")
    assert "c" in panels


def test_select_panels_no_panel_col():
    df = pd.DataFrame([{"wave_number": 38, "country_code": "SK", "net_balance_wtd": 5.0}])
    sec = {
        "pinned_panels": [],
        "panel_col": None,
        "value_col": "net_balance_wtd",
        "series_col": "country_code",
        "max_panels": 4,
    }
    panels = _select_panels(sec, df, best_panel=None)
    # No panel_col → returns [None] sentinel
    assert panels == [None]
