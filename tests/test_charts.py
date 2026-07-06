import pandas as pd
import pytest

# _is_continuous lives in adhoc.py
from adhoc import _is_continuous
from charts import _select_panels, build_chart, build_financing_gap_chart


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


# ── build_chart with chart_title / chart_question ────────────────────────────

_PNG_MAGIC = b"\x89PNG\r\n\x1a\n"


def _make_chart_df(panels, countries=("SK", "EA", "DE"), waves=(37, 38)):
    rows = []
    for wave in waves:
        for p in panels:
            for c in countries:
                rows.append({
                    "sub_item": p,
                    "sub_item_label": f"Label {p}",
                    "country_code": c,
                    "wave_number": wave,
                    "survey_period_label": f"W{wave}",
                    "net_balance_wtd": 5.0,
                    "n_respondents": 50,
                })
    return pd.DataFrame(rows)


def test_build_chart_single_panel_with_title_and_question():
    df = _make_chart_df(["a"])
    sec = _make_section(pinned=["a"], max_panels=1)
    png = build_chart(sec, df, "line", None,
                       chart_title="Slovak firms see persistent pressure",
                       chart_question="Which problems are pressing for you?")
    assert isinstance(png, bytes)
    assert png[:8] == _PNG_MAGIC


def test_build_chart_multi_panel_with_title_and_question():
    df = _make_chart_df(["a", "b"])
    sec = _make_section(pinned=["a", "b"], max_panels=2)
    png = build_chart(sec, df, "line", None,
                       chart_title="Slovak firms see persistent pressure",
                       chart_question="Which problems are pressing for you?")
    assert isinstance(png, bytes)
    assert png[:8] == _PNG_MAGIC


def test_build_chart_without_title_or_question_still_renders():
    df = _make_chart_df(["a"])
    sec = _make_section(pinned=["a"], max_panels=1)
    png = build_chart(sec, df, "line", None)
    assert isinstance(png, bytes)
    assert png[:8] == _PNG_MAGIC


# ── build_financing_gap_chart with chart_title / chart_question ─────────────

def _make_financing_gap_df():
    rows = []
    for wave in (37, 38):
        for c in ("SK", "EA", "DE"):
            rows.append({
                "chart_type": "main",
                "sub_item": "a",
                "sub_item_label": "Bank loans",
                "country_code": c,
                "wave_number": wave,
                "survey_period_label": f"W{wave}",
                "need_nb": 5.0,
                "availability_nb": 3.0,
                "financing_gap_wtd": 2.0,
            })
    for sub_item in ("a", "b", "f"):
        rows.append({
            "chart_type": "sk_all",
            "sub_item": sub_item,
            "sub_item_label": f"Label {sub_item}",
            "country_code": "SK",
            "wave_number": 38,
            "survey_period_label": "W38",
            "need_nb": 4.0,
            "availability_nb": 2.0,
            "financing_gap_wtd": 2.0,
        })
    return pd.DataFrame(rows)


def test_build_financing_gap_chart_with_title_and_question():
    df = _make_financing_gap_df()
    sec = {"title": "Financing Gap"}
    png = build_financing_gap_chart(sec, df,
                                     chart_title="Slovak firms face a widening gap",
                                     chart_question="Can you get the financing you need?")
    assert isinstance(png, bytes)
    assert png[:8] == _PNG_MAGIC


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
