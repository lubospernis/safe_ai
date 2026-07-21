from unittest.mock import MagicMock, patch

import pandas as pd

from db import fetch_all, run_agentic_query_turns


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


# ── run_agentic_query_turns ──────────────────────────────────────────────────
# Extracted from llm.py::get_section_content_agentic so reports/qa_tool.py can
# reuse the exact same query_mart tool loop instead of a second copy of it.


def _usage():
    u = MagicMock()
    u.input_tokens = 10
    u.output_tokens = 10
    u.cache_creation_input_tokens = 0
    u.cache_read_input_tokens = 0
    return u


def _text_response(text="done", stop_reason="end_turn"):
    resp = MagicMock()
    resp.stop_reason = stop_reason
    resp.content = [MagicMock(type="text", text=text)]
    resp.usage = _usage()
    return resp


def _tool_use_response(tool_use_id: str, sql: str, tool_name: str = "query_mart"):
    block = MagicMock()
    block.type = "tool_use"
    block.name = tool_name
    block.id = tool_use_id
    block.input = {"sql": sql}
    resp = MagicMock()
    resp.stop_reason = "tool_use"
    resp.content = [block]
    resp.usage = _usage()
    return resp


def test_run_agentic_query_turns_stops_immediately_with_no_tool_use(sample_cost_tracker):
    client = MagicMock()
    client.messages.create.return_value = _text_response()
    messages = [{"role": "user", "content": "What is the profit net balance?"}]

    result_messages, tool_calls = run_agentic_query_turns(
        client, [{"type": "text", "text": "system"}], messages,
        tool_con=MagicMock(), schema="main_safe", cost_tracker=sample_cost_tracker,
    )

    assert tool_calls == 0
    assert client.messages.create.call_count == 1
    assert result_messages[-1]["role"] == "assistant"


def test_run_agentic_query_turns_executes_query_and_continues(sample_cost_tracker):
    client = MagicMock()
    client.messages.create.side_effect = [
        _tool_use_response("toolu_1", "SELECT 1 FROM main_safe.mart_safe__slovakia_kpis"),
        _text_response("final answer"),
    ]
    messages = [{"role": "user", "content": "question"}]

    with patch("db._run_query_tool", return_value="| col |\n|---|\n| 1 |") as query_mock:
        result_messages, tool_calls = run_agentic_query_turns(
            client, [{"type": "text", "text": "system"}], messages,
            tool_con=MagicMock(), schema="main_safe", cost_tracker=sample_cost_tracker,
        )

    assert tool_calls == 1
    query_mock.assert_called_once()
    assert client.messages.create.call_count == 2
    # tool_result for turn 1, then the assistant's final turn-2 response
    tool_result_msgs = [m for m in result_messages if m.get("role") == "user"
                         and isinstance(m.get("content"), list)]
    assert len(tool_result_msgs) == 1
    assert tool_result_msgs[0]["content"][0]["tool_use_id"] == "toolu_1"


def test_run_agentic_query_turns_respects_max_turns(sample_cost_tracker):
    client = MagicMock()
    # Model keeps calling the tool forever — max_turns must cap it.
    client.messages.create.return_value = _tool_use_response("toolu_x", "SELECT 1")
    messages = [{"role": "user", "content": "question"}]

    with patch("db._run_query_tool", return_value="ok"):
        _, tool_calls = run_agentic_query_turns(
            client, [{"type": "text", "text": "system"}], messages,
            tool_con=MagicMock(), schema="main_safe", cost_tracker=sample_cost_tracker,
            max_turns=2,
        )

    assert tool_calls == 2
    assert client.messages.create.call_count == 2
