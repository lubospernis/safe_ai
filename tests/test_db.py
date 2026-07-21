from unittest.mock import MagicMock, patch

import pandas as pd

from db import fetch_all


def _mock_con(*dfs: pd.DataFrame):
    """A fake duckdb connection whose .execute(sql).df() returns each given
    DataFrame in call order — fetch_all calls execute() once per sql_file,
    then once more per sme_sql_file (if present), in that fixed order."""
    con = MagicMock()
    results = []
    for df in dfs:
        result = MagicMock()
        result.df.return_value = df
        results.append(result)
    con.execute.side_effect = results
    return con


def test_fetch_all_returns_one_df_per_section_id():
    sections = [
        {"id": "sec_a", "sql_file": "business_situation.sql"},
        {"id": "sec_b", "sql_file": "expectations_risk.sql"},
    ]
    df_a = pd.DataFrame([{"wave_number": 38}])
    df_b = pd.DataFrame([{"wave_number": 38}])
    con = _mock_con(df_a, df_b)
    with patch("db._get_connection", return_value=con):
        results = fetch_all(sections)
    assert set(results) == {"sec_a", "sec_b"}
    assert results["sec_a"] is df_a
    assert results["sec_b"] is df_b


def test_fetch_all_adds_sme_suffixed_key_when_sme_sql_file_present():
    sections = [
        {"id": "business_situation", "sql_file": "business_situation.sql",
         "sme_sql_file": "business_situation_sme.sql"},
    ]
    main_df = pd.DataFrame([{"wave_number": 38, "firm_size": "all"}])
    sme_df = pd.DataFrame([{"wave_number": 38, "firm_size": "sme"}])
    con = _mock_con(main_df, sme_df)  # main sql_file executed first, then sme_sql_file
    with patch("db._get_connection", return_value=con):
        results = fetch_all(sections)
    assert set(results) == {"business_situation", "business_situation__sme"}
    assert results["business_situation"] is main_df
    assert results["business_situation__sme"] is sme_df


def test_fetch_all_omits_sme_key_when_sme_sql_file_absent():
    sections = [{"id": "outlook", "sql_file": "outlook.sql"}]
    df = pd.DataFrame([{"wave_number": 38}])
    con = _mock_con(df)
    with patch("db._get_connection", return_value=con):
        results = fetch_all(sections)
    assert set(results) == {"outlook"}
    assert "outlook__sme" not in results
