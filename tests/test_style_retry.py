from unittest.mock import MagicMock, patch

from llm import get_section_content_agentic

# Deliberately over-length (59 words) but using only single-digit pp figures
# that _check_numeric_grounding always skips (iv <= 9) — a pure style
# violation with no grounding issue, so retry/drop behavior can be tested in
# isolation from the grounding checks covered by test_grounding_retry.py.
_LONG_BULLET = (
    "Net balance worsened from 5.0pp in wave 37 to 7.0pp in wave 38 for Slovak firms, "
    "while the EA figure moved from 3.0pp to 5.0pp over the same period, and German firms "
    "similarly saw their net balance rise from 1.5pp to 3.5pp, with all three economies "
    "showing a consistent upward trend across the two waves of collected survey data."
)
_SHORT_BULLET = "Net balance worsened to 7.0pp this wave, widening the credit-line gap."


def _usage():
    u = MagicMock()
    u.input_tokens = 10
    u.output_tokens = 10
    u.cache_creation_input_tokens = 0
    u.cache_read_input_tokens = 0
    return u


def _reasoning_response():
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


def test_retry_recovers_after_overlength_first_attempt(section_stub, net_balance_df, sample_cost_tracker):
    """First emit call has a >35-word bullet; the model is given the specific
    style failure and, on retry, emits a compliant one — comes back clean with
    no style warnings."""
    client = MagicMock()
    client.messages.create.side_effect = [
        _reasoning_response(),
        _emit_response("toolu_1", "Gap widened", [_LONG_BULLET]),
        _emit_response("toolu_2", "Gap widened", [_SHORT_BULLET]),
    ]

    with patch("db._run_query_tool", return_value="{}"):
        result = get_section_content_agentic(
            section_stub, net_balance_df, tool_con=MagicMock(), schema="main_safe",
            mart_catalogue="", cost_tracker=sample_cost_tracker, client=client,
        )

    assert client.messages.create.call_count == 3
    assert result["bullets"] == [_SHORT_BULLET]
    assert result["style_warnings"] == []

    # The retry feedback must name the style problem specifically.
    retry_call = client.messages.create.call_args_list[-1]
    feedback = retry_call.kwargs["messages"][-1]["content"][0]["content"]
    assert "Style check failed" in feedback
    assert "words" in feedback


def test_combined_grounding_and_style_failure_resolved_in_one_round(
    section_stub, net_balance_df, sample_cost_tracker
):
    """A single response with both an ungrounded number AND an over-length
    bullet gets ONE combined feedback message, and both issues are fixed by
    the same retry — the shared retry budget isn't doubled for having two
    problem categories."""
    client = MagicMock()
    client.messages.create.side_effect = [
        _reasoning_response(),
        _emit_response("toolu_1", "Gap widened",
                       ["Net balance worsened to 99.7pp", _LONG_BULLET]),
        _emit_response("toolu_2", "Gap widened",
                       [_SHORT_BULLET, "Net balance was 7.0pp last wave"]),
    ]

    with patch("db._run_query_tool", return_value="{}"):
        result = get_section_content_agentic(
            section_stub, net_balance_df, tool_con=MagicMock(), schema="main_safe",
            mart_catalogue="", cost_tracker=sample_cost_tracker, client=client,
        )

    assert client.messages.create.call_count == 3  # reasoning + 1 initial + 1 retry
    assert result["grounding_warnings"] == []
    assert result["grounding_dropped"] == []
    assert result["style_warnings"] == []

    retry_call = client.messages.create.call_args_list[-1]
    feedback = retry_call.kwargs["messages"][-1]["content"][0]["content"]
    assert "Grounding check failed" in feedback
    assert "Style check failed" in feedback


def test_style_survivor_after_retries_is_kept_not_dropped(
    section_stub, net_balance_df, sample_cost_tracker
):
    """The model keeps emitting an over-length bullet across every retry —
    after MAX_SECTION_REVISION_RETRIES it must be KEPT (not dropped, not
    truncated) and reported in style_warnings for visibility."""
    client = MagicMock()
    client.messages.create.side_effect = [
        _reasoning_response(),
        _emit_response("toolu_1", "Gap widened", [_LONG_BULLET]),
        _emit_response("toolu_2", "Gap widened", [_LONG_BULLET]),
        _emit_response("toolu_3", "Gap widened", [_LONG_BULLET]),
    ]

    with patch("db._run_query_tool", return_value="{}"):
        result = get_section_content_agentic(
            section_stub, net_balance_df, tool_con=MagicMock(), schema="main_safe",
            mart_catalogue="", cost_tracker=sample_cost_tracker, client=client,
        )

    assert client.messages.create.call_count == 4  # reasoning + 1 initial + 2 retries
    assert result["bullets"] == [_LONG_BULLET]  # kept, not dropped
    assert result["style_warnings"]
    assert any("words" in w for w in result["style_warnings"])
