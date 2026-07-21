from unittest.mock import MagicMock, patch

from llm import get_section_content_agentic


def _usage():
    u = MagicMock()
    u.input_tokens = 10
    u.output_tokens = 10
    u.cache_creation_input_tokens = 0
    u.cache_read_input_tokens = 0
    return u


def _reasoning_response():
    """First tool-use-loop turn: model does no queries, stops immediately."""
    resp = MagicMock()
    resp.stop_reason = "end_turn"
    resp.content = [MagicMock(type="text", text="reasoning")]
    resp.usage = _usage()
    return resp


def _emit_response(tool_use_id: str, finding: str, bullets: list[str], chart_subtitle: str = ""):
    block = MagicMock()
    block.type = "tool_use"
    block.name = "emit_section_json"
    block.id = tool_use_id
    block.input = {"finding": finding, "bullets": bullets, "chart_subtitle": chart_subtitle}
    resp = MagicMock()
    resp.content = [block]
    resp.usage = _usage()
    return resp


def test_retry_recovers_after_ungrounded_first_attempt(section_stub, net_balance_df, sample_cost_tracker):
    """First emit call cites an invented number; the model is given the specific
    grounding failure and, on retry, cites a real one — the section should come
    back clean with no dropped bullets and no grounding warnings."""
    client = MagicMock()
    client.messages.create.side_effect = [
        _reasoning_response(),
        _emit_response("toolu_1", "Terms worsened", ["Net balance worsened to 99.7pp this wave"]),
        _emit_response("toolu_2", "Terms worsened", ["Net balance worsened to 7.0pp this wave"]),
    ]

    with patch("db._run_query_tool", return_value="{}"):
        result = get_section_content_agentic(
            section_stub, net_balance_df, tool_con=MagicMock(), schema="main_safe",
            mart_catalogue="", cost_tracker=sample_cost_tracker, client=client,
        )

    assert client.messages.create.call_count == 3  # reasoning + 2 emit attempts
    assert result["bullets"] == ["Net balance worsened to 7.0pp this wave"]
    assert result["grounding_warnings"] == []
    assert result["grounding_dropped"] == []


def test_bullet_dropped_after_retries_exhausted(section_stub, net_balance_df, sample_cost_tracker):
    """The model keeps citing an invented number across every retry — after
    MAX_SECTION_REVISION_RETRIES the offending bullet is dropped, not published,
    and the run isn't blocked."""
    client = MagicMock()
    client.messages.create.side_effect = [
        _reasoning_response(),
        _emit_response("toolu_1", "Terms worsened",
                       ["Net balance worsened to 99.7pp", "Net balance was 7.0pp last wave"]),
        _emit_response("toolu_2", "Terms worsened",
                       ["Net balance worsened to 99.7pp", "Net balance was 7.0pp last wave"]),
        _emit_response("toolu_3", "Terms worsened",
                       ["Net balance worsened to 99.7pp", "Net balance was 7.0pp last wave"]),
    ]

    with patch("db._run_query_tool", return_value="{}"):
        result = get_section_content_agentic(
            section_stub, net_balance_df, tool_con=MagicMock(), schema="main_safe",
            mart_catalogue="", cost_tracker=sample_cost_tracker, client=client,
        )

    assert client.messages.create.call_count == 4  # reasoning + 1 initial + 2 retries
    assert result["bullets"] == ["Net balance was 7.0pp last wave"]
    assert result["grounding_dropped"] == ["Net balance worsened to 99.7pp"]
    assert result["grounding_warnings"] == []  # nothing ungrounded remains published
